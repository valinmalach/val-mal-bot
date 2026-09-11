"""The autoshoutout list, and what turning up on it costs.

Someone on the list gets one **autoshoutout** per **stream session** — the
first time they chat, raid or redeem — and is then **settled**, so the list is
asked about them once per stream rather than once per chat line. The list
itself is a record in Postgres and outlives any session; what is spent belongs
to the session, which clears it when a stream begins or ends.

Nothing here shouts anyone out directly. It posts `!so <login>` into chat, the
way the raid handler already does, and that line returns through the chat
webhook to the one shoutout handler. That keeps a single implementation of
what a shoutout is, at the price of the wording being `!so`'s.

Imports ``stream_session`` one way. The session owns the spent state and
clears its own; if it imported back to ask what an autoshoutout is, the two
would be cyclic.
"""

import logging
from enum import Enum, auto

from db import repository
from errors import notify, report
from models.twitch_event_subs.channel_chat_message import ChannelChatMessageEventSub
from models.twitch_event_subs.channel_points_custom_reward_redemption_add import (
    ChannelPointsCustomRewardRedemptionAddEventSub,
)
from services.config import config
from services.present import quoted
from services.twitch import stream_session
from services.twitch.chat import say

logger = logging.getLogger(__name__)

__all__ = ["add", "chatted", "raided", "redeemed", "remove", "spend"]


class _Action(Enum):
    """What one appearance concludes should happen."""

    IGNORE = auto()  # nobody is live, or this session has settled them already
    LOOK_UP = auto()  # the list has not been asked about them yet
    SETTLE = auto()  # asked, not on the list; do not ask again this stream
    SHOUT = auto()  # asked, on the list; shout them out and settle them


def _decide(live: bool, settled: bool, listed: bool | None) -> _Action:
    """The whole rule for one appearance, with every input in hand.

    Pure, because this is where the subtle cases live and they are worth
    reading in one place. ``listed`` is None until the list has been asked,
    which is what puts the lookup inside the rule instead of leaving it as an
    optimisation wrapped around it: the cost guarantee — one query per distinct
    chatter per stream — is the rule, not a detail of the caller.
    """
    if not live or settled:
        return _Action.IGNORE
    if listed is None:
        return _Action.LOOK_UP
    return _Action.SHOUT if listed else _Action.SETTLE


async def _consider(broadcaster_id: str, twitch_user_id: int, login: str) -> None:
    """Give this person their autoshoutout, if this is the stream for it."""
    # The main broadcaster's channel, not merely some channel that reached
    # here. `is_live()` answers for the main broadcaster while the line below
    # is posted to whoever the event named, and the two are otherwise
    # unconnected: a chat or redemption subscription for a second channel -
    # most are provisioned by hand, and the app already takes stream.online
    # for other broadcasters - would put `!so` into that
    # channel every time the main broadcaster happened to be live, and settle
    # the person out of the autoshoutout they were actually owed. The same
    # boundary `channel_raid` and `channel_moderate` already apply.
    if not stream_session.is_main_broadcaster(broadcaster_id):
        return

    if (
        _decide(
            stream_session.is_live(), stream_session.is_settled(twitch_user_id), None
        )
        is _Action.IGNORE
    ):
        return

    # Claimed before the lookup, not after it. Chat webhooks are dispatched
    # concurrently, so two lines from one person can both pass the check above
    # while the first is still awaiting the list - and both then shout, and
    # both cost a query. Settling with no await since that check is what stops
    # the second; `controller/twitch._claim` takes a delivery id the same way
    # and says the same thing about adding an await between a check and a take.
    stream_session.settle(twitch_user_id)
    asked_during = stream_session.current_stream_id()

    listed = await repository.is_autoshoutout(twitch_user_id)

    # The same stream, not merely some stream. Reading `is_live()` here would
    # answer yes for a stream that started after the one this chatter spoke in
    # ended, and post their line into it - someone the new stream's viewers
    # never saw, who is no longer settled, because ending cleared that. A
    # narrow window, and the same one `wake` and `live_alert._owns_row` both
    # guard rather than argue about. `settled` is passed False deliberately:
    # this task claimed it a moment ago and would otherwise ignore itself.
    same_stream = stream_session.current_stream_id() == asked_during
    if _decide(same_stream, False, listed) is not _Action.SHOUT:
        return

    # Deferred: commands imports this module for `!aso`, so importing it at the
    # top would be a cycle - the same break `shoutout_queue.drain` makes for
    # `stream_session`.
    from services.twitch.commands import is_twitch_login

    # The third place a login becomes a command, and it goes through the same
    # boundary as the other two: this line is posted to chat and returns
    # through the chat webhook to be dispatched, so a value that cannot name a
    # channel must not be built into one. Settled above regardless, because
    # nothing here can help them this stream either way.
    if not is_twitch_login(login):
        await notify(
            f"Refused an autoshoutout for {quoted(login)}:"
            f" that cannot name a Twitch channel.",
            key="autoshoutout-bad-login",
        )
        return

    await say(broadcaster_id, f"!so {login}", "an autoshoutout")


async def chatted(event_sub: ChannelChatMessageEventSub) -> None:
    """Consider the chatter behind one message. Does not raise."""
    try:
        # The bot is not a viewer turning up. It says `!so <login>` for every
        # raid and every autoshoutout, and each of those lines comes back
        # through this same webhook, so without this the bot is looked up in
        # the list the first time it speaks each stream. Harmless but wasted,
        # and it reads as an oversight rather than a decision.
        if event_sub.event.chatter_user_id == config.setting("twitch_bot_user_id"):
            return

        await _consider(
            event_sub.event.broadcaster_user_id,
            int(event_sub.event.chatter_user_id),
            event_sub.event.chatter_user_login,
        )
    except Exception as e:  # noqa: BLE001
        # Its own guard rather than the chat handler's: an autoshoutout that
        # failed must not stop the command on the same line being dispatched.
        await report(e, "Error considering an autoshoutout for a chatter")


def raided(twitch_user_id: str) -> None:
    """Spend a raider's autoshoutout, because the raid already earned them one.

    Settled rather than shouted: every incoming raid is already answered with
    `!so <raider>` by the raid handler, so the work here is only to stop their
    first chat line producing a second one. Done for every raider, not just
    list members — a raider who is not on the list still needs no autoshoutout
    this stream, and settling them saves the lookup their first line would
    otherwise cost.
    """
    try:
        stream_session.settle(int(twitch_user_id))
    except ValueError:
        logger.warning("Raider id %r is not a number; not settled", twitch_user_id)


async def redeemed(event_sub: ChannelPointsCustomRewardRedemptionAddEventSub) -> None:
    """Consider the redeemer behind one channel-point redemption.

    Any custom reward counts, and one still queued or later refunded counts
    too: the point is that they turned up, not what they bought. No guard of
    its own, unlike `chatted` — nothing follows this on the notification, so
    the handler's own report is the only one it needs.
    """
    await _consider(
        event_sub.event.broadcaster_user_id,
        int(event_sub.event.user_id),
        event_sub.event.user_login,
    )


def spend(broadcaster_id: str, twitch_user_id: int) -> None:
    """Record that `!aso` has already given this person their autoshoutout.

    Only while a stream is running. `!aso` between streams still adds the row
    and still shouts them out, but there is no session for it to spend from,
    so their first appearance next stream earns them a proper one.

    And only from the main broadcaster's channel, for the reason `_consider`
    gives. Commands are answered wherever the bot has a chat subscription, and
    a `!so` posted into that channel is a service to it; settling against this
    session is not, because the session is the main broadcaster's. Taking the
    broadcaster rather than reading it at the call site so that a second caller
    cannot omit the check - the state this writes to belongs to one channel.
    """
    if stream_session.is_main_broadcaster(broadcaster_id) and stream_session.is_live():
        stream_session.settle(twitch_user_id)


async def add(twitch_user_id: int, login: str) -> bool:
    """Put someone on the list; False when they were already on it."""
    return await repository.add_autoshoutout(twitch_user_id, login)


async def remove(twitch_user_id: int) -> bool:
    """Take someone off the list; False when they were not on it."""
    return await repository.remove_autoshoutout(twitch_user_id)
