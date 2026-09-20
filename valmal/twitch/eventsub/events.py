"""What each EventSub notification makes the bot do.

Lives here rather than beside the routes because none of it touches HTTP: it
orchestrates the alert, the session, chat and Helix, and only happens to be
triggered by a webhook.
"""

import asyncio
import logging
import time

from valmal.bot.present import quoted
from valmal.core.config import config
from valmal.core.errors import notify, report
from valmal.twitch.client.api import get_stream, get_user, live_stream
from valmal.twitch.client.chat import say, say_template
from valmal.twitch.client.helix import HelixError
from valmal.twitch.eventsub.commands import dispatch, is_twitch_login
from valmal.twitch.models.api.stream import Stream
from valmal.twitch.models.eventsub.channel_ad_break_begin import (
    ChannelAdBreakBeginEventSub,
)
from valmal.twitch.models.eventsub.channel_chat_message import (
    ChannelChatMessageEventSub,
)
from valmal.twitch.models.eventsub.channel_follow import ChannelFollowEventSub
from valmal.twitch.models.eventsub.channel_moderate import ChannelModerateEventSub
from valmal.twitch.models.eventsub.channel_points_custom_reward_redemption_add import (
    ChannelPointsCustomRewardRedemptionAddEventSub,
)
from valmal.twitch.models.eventsub.channel_raid import ChannelRaidEventSub
from valmal.twitch.models.eventsub.stream_offline import StreamOfflineEventSub
from valmal.twitch.models.eventsub.stream_online import StreamOnlineEventSub
from valmal.twitch.stream import autoshoutout, live_alert, stream_session

logger = logging.getLogger(__name__)

# Twitch announces the stream before Helix lists it. Bounded by the clock rather
# than by a count of attempts: a lookup that is timing out takes most of a minute
# on its own, so thirty of those is a quarter of an hour, not thirty seconds.
_STREAM_WAIT_SECONDS = 60


async def _wait_for_stream_info(
    broadcaster_id: int,
) -> tuple[Stream | None, HelixError | None]:
    """Poll until Twitch admits the stream is up, tolerating a failed lookup.

    Bounded, because Twitch delivers stream.online once and never again: waiting
    forever and giving up both lose the alert, but only one of them says so. The
    last lookup error comes back too, so giving up can tell "Twitch says they
    are offline" from "Twitch could not be reached".
    """
    deadline = time.monotonic() + _STREAM_WAIT_SECONDS
    last_error: HelixError | None = None

    while True:
        try:
            stream_info = live_stream(await get_stream(broadcaster_id))
        except HelixError as e:
            last_error = e
            logger.warning(f"Stream lookup failed for {broadcaster_id}: {last_error}")
            stream_info = None

        if stream_info is not None:
            return stream_info, None
        # Checked after the attempt, so a slow first lookup still gets its turn.
        if time.monotonic() >= deadline:
            return None, last_error
        await asyncio.sleep(1)


async def stream_online(event_sub: StreamOnlineEventSub) -> None:
    raw_broadcaster_id = event_sub.event.broadcaster_user_id
    try:
        # Converted inside the guard: a broadcaster id that is not a number cannot come
        # from Twitch through a verified signature, but converting it above the try
        # would raise past this handler's own report onto the task floor, which names
        # the route and not the event.
        broadcaster_id = int(raw_broadcaster_id)
        stream_info, lookup_error = await _wait_for_stream_info(broadcaster_id)
        if stream_info is None:
            reason = (
                f"the last lookup failed: {lookup_error}"
                if lookup_error is not None
                else "Twitch went on reporting them offline"
            )
            await notify(
                f"Gave up after {_STREAM_WAIT_SECONDS}s waiting for Twitch to confirm"
                f" broadcaster {broadcaster_id} is live, so no alert was posted and no"
                f" stream session was started: {reason}",
                key=f"stream-online-gave-up:{broadcaster_id}",
            )
            return

        try:
            user_info = await get_user(broadcaster_id)
        except HelixError as e:
            # Decoration. stream.online does not come again, so the alert must
            # not depend on it.
            await notify(
                f"Announcing broadcaster {broadcaster_id} without their profile: {e}",
                key=f"stream-online-profile:{broadcaster_id}",
            )
            user_info = None

        is_main = stream_info.user_login == config.setting("broadcaster_username")
        channel = (
            config.channel("stream_alerts") if is_main else config.channel("promo")
        )

        # began does not raise: it guards each of its own chat lines, because
        # the alert below must survive a Twitch that will not take a greeting.
        await stream_session.began(broadcaster_id, stream_info)

        await live_alert.announce(broadcaster_id, stream_info, user_info, channel)

    except Exception as e:  # noqa: BLE001
        await report(e, f"Error in stream_online for {quoted(raw_broadcaster_id)}")


async def stream_offline(event_sub: StreamOfflineEventSub) -> None:
    raw_broadcaster_id = event_sub.event.broadcaster_user_id
    try:
        # Inside the guard, for the reason stream_online gives.
        broadcaster_id = int(raw_broadcaster_id)
        # The payload names no stream, so this handler cannot tell which one ended. It
        # wakes both, and each re-checks Helix for its own scope: the updater to find
        # out which alert this was (docs/adr/0001), the session whether anyone is still
        # live at all (docs/adr/0004).
        #
        # Neither can fail the other: both report their own failures and return. They
        # are sequential because the alert wake only sets an Event.
        await live_alert.wake(broadcaster_id)
        await stream_session.wake(broadcaster_id)

    except Exception as e:  # noqa: BLE001
        await report(e, f"Error in stream_offline for {quoted(raw_broadcaster_id)}")


async def channel_chat_message(event_sub: ChannelChatMessageEventSub) -> None:
    try:
        # The shared-chat guard comes first: an autoshoutout is owed to someone who
        # turned up in *this* channel, so a line relayed from another one must be
        # dropped before anything reads it.
        if (
            event_sub.event.source_broadcaster_user_id is not None
            and event_sub.event.source_broadcaster_user_id
            != event_sub.event.broadcaster_user_id
        ):
            return

        # Above the "!" check: turning up is what earns an autoshoutout, and most
        # people turn up by saying something ordinary.
        await autoshoutout.chatted(event_sub)

        if not event_sub.event.message.text.startswith("!"):
            return
        text_without_prefix = event_sub.event.message.text[1:]
        command_parts = text_without_prefix.split(" ", 1)
        command = command_parts[0].lower()
        args = command_parts[1] if len(command_parts) > 1 else ""

        await dispatch(event_sub, command, args)
    except Exception as e:  # noqa: BLE001
        await report(e, "Error processing Twitch chat webhook task")


async def channel_follow(event_sub: ChannelFollowEventSub) -> None:
    try:
        await say_template(
            event_sub.event.broadcaster_user_id,
            "twitch_follow_thanks",
            user=event_sub.event.user_name,
        )
    except Exception as e:  # noqa: BLE001
        await report(e, "Error processing Twitch follow webhook task")


async def channel_ad_break_begin(event_sub: ChannelAdBreakBeginEventSub) -> None:
    broadcaster_id = event_sub.event.broadcaster_user_id
    try:
        ad_duration = event_sub.event.duration_seconds

        # Each step stands alone, so a dropped "ads starting" does not take the "ads
        # over" message and the next break's warning with it.
        # Real minutes, not `// 60`: Twitch's breaks are multiples of thirty seconds, so
        # truncating said "0 minute" for the shortest and "1 minute" for a minute and a
        # half. `g` drops the trailing zero of a whole number.
        await say_template(
            broadcaster_id, "twitch_ad_break_start", minutes=f"{ad_duration / 60:g}"
        )
        await asyncio.sleep(ad_duration)
        await say_template(broadcaster_id, "twitch_ad_break_end")

        stream_session.schedule_ad_break_warning(broadcaster_id)
    except Exception as e:  # noqa: BLE001
        await report(e, "Error processing Twitch ad break webhook task")


async def channel_points_custom_reward_redemption_add(
    event_sub: ChannelPointsCustomRewardRedemptionAddEventSub,
) -> None:
    try:
        await autoshoutout.redeemed(event_sub)
    except Exception as e:  # noqa: BLE001
        await report(e, "Error processing Twitch redemption webhook task")


async def channel_raid(event_sub: ChannelRaidEventSub) -> None:
    try:
        # Both logins are checked before anything is built from them. Outbound, the
        # login becomes a URL posted in chat; inbound, a `!so <login>` line that returns
        # through the chat webhook and is dispatched, so a value that cannot name a
        # channel must reach neither. The payload is signed: this is the boundary `!so`
        # itself uses, not a doubt about Twitch.
        if stream_session.is_main_broadcaster(event_sub.event.from_broadcaster_user_id):
            raided = event_sub.event.to_broadcaster_user_login
            if not is_twitch_login(raided):
                await notify(
                    f"Said nothing about an outgoing raid to {quoted(raided)}:"
                    f" that cannot name a Twitch channel, so it cannot be a URL.",
                    key="raid-out-bad-login",
                )
                return
            await say_template(
                event_sub.event.from_broadcaster_user_id,
                "twitch_raid_out",
                name=event_sub.event.to_broadcaster_user_name,
                url=f"https://www.twitch.tv/{raided}",
            )
        elif stream_session.is_main_broadcaster(event_sub.event.to_broadcaster_user_id):
            raider = event_sub.event.from_broadcaster_user_login
            if not is_twitch_login(raider):
                await notify(
                    f"Refused to shout out an incoming raid from {quoted(raider)}:"
                    f" that cannot name a Twitch channel.",
                    key="raid-bad-login",
                )
                return

            # The raid spends their autoshoutout, but only once the line is out: it is the
            # `!so` that gives them one, so a login that could not be used or a line Twitch
            # refused leaves them owed it. Marked here, not in the shoutout handler, which a
            # mod's manual `!so` also reaches and which must spend nothing.
            if await say(
                event_sub.event.to_broadcaster_user_id,
                f"!so {raider}",
                "the incoming raid shoutout",
            ):
                autoshoutout.raided(event_sub.event.from_broadcaster_user_id)
    except Exception as e:  # noqa: BLE001
        await report(e, "Error processing Twitch raid webhook task")


async def channel_moderate(event_sub: ChannelModerateEventSub) -> None:
    try:
        if event_sub.event.action != "raid" or not stream_session.is_main_broadcaster(
            event_sub.event.broadcaster_user_id
        ):
            return
        await say_template(event_sub.event.broadcaster_user_id, "twitch_raid_farewell")
    except Exception as e:  # noqa: BLE001
        await report(e, "Error processing Twitch moderate webhook task")
