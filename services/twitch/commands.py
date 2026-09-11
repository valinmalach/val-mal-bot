"""Twitch chat commands, dispatched from the twitch_command tables.

Response text lives in the database. Only commands needing real logic have a
function here; `twitch_command.handler` names one of them, `static` sends the
stored responses in order, and `composite` runs other commands.
"""

import logging
import re
import unicodedata
from collections.abc import Awaitable, Callable

from errors import notify
from models.twitch_api_responses.user import User
from models.twitch_event_subs.channel_chat_message import ChannelChatMessageEventSub
from services.config import config, safe_format
from services.twitch import autoshoutout, stream_session
from services.twitch.api import get_channel, get_user_by_username
from services.twitch.chat import say, say_template
from services.twitch.helix import HelixError

from .shoutout_queue import shoutout_queue

logger = logging.getLogger(__name__)

__all__ = ["dispatch", "is_twitch_login"]

COMPOSITE_HANDLER = "composite"

# A Twitch login: up to 25 characters of ASCII letter, digit or underscore.
# Anything else cannot name a channel, so it is refused before the lookup
# rather than after: it costs no Helix call, and nothing a chatter typed
# reaches Twitch as a query parameter on the strength of being a word.
#
# No lower bound, though Twitch has required four since long before this bot.
# That rule binds signups, not accounts, so a legacy handle shorter than four
# is Twitch's to have issued and not this code's to refuse - and refusing one
# would silently drop the shoutout for a raid from that channel, since the
# raid handler posts `!so <login>` and it arrives back through here. The
# charset and the maximum are what make the value safe to hand to Helix; the
# minimum only ever adds false rejections.
_TWITCH_LOGIN = re.compile(r"\A[a-zA-Z0-9_]{1,25}\Z")

# A target is echoed into chat by `!hug` and by any stored response naming
# {target}, so it is bounded. Not the login alphabet: a target is a person as
# the chatter wrote them, and a display name can be Japanese or Korean, so
# keeping only [A-Za-z0-9_] would erase one. Length because a chat line is
# 500 characters and all of them could arrive as one word.
_MAX_TARGET = 50


def is_twitch_login(value: str) -> bool:
    """Whether this could name a Twitch channel.

    The one boundary for every shoutout path. `!so` asks before its lookup, and
    the raid handler asks before composing a `!so` line at all - that line is
    posted to chat and arrives back through the webhook, so a value that cannot
    name a channel should never be built into a command in the first place.
    """
    return _TWITCH_LOGIN.match(value) is not None


def _target(args: str) -> str:
    """The first word of the arguments, stripped of what would make it a command.

    `!` as well as `@`, and stripped rather than removed once, because a target
    reaches chat through `_render` and the bot's own lines come back in through
    the chat webhook. A template beginning with `{target}` would otherwise let
    any chatter post `!so ...` in the bot's voice, and the bot holds a
    moderator badge, so a mod-only command would run for someone who is not a
    mod. No such template exists today; this is what keeps adding one from
    being a privilege escalation.

    Bounded and stripped of invisible characters for the same reason: whatever
    comes back is going into a chat line the bot says.
    """
    first = (args.split(" ", 1)[0] if args else "").lstrip("@!")
    # Control and format characters removed: they are invisible, so they can
    # reorder or hide what the rest of the line says once it reaches chat, and
    # no name needs one.
    kept = "".join(c for c in first if unicodedata.category(c) not in {"Cc", "Cf"})
    return kept[:_MAX_TARGET]


def _render(message: str, event_sub: ChannelChatMessageEventSub, args: str) -> str:
    return safe_format(
        message,
        {
            "chatter": event_sub.event.chatter_user_name,
            "broadcaster": event_sub.event.broadcaster_user_name,
            "target": _target(args),
        },
    )


async def hug(event_sub: ChannelChatMessageEventSub, args: str) -> None:
    target = _target(args)
    key = "twitch_hug_target" if target else "twitch_hug_everyone"
    await say_template(
        event_sub.event.broadcaster_user_id,
        key,
        chatter=event_sub.event.chatter_user_name,
        target=target,
    )


async def shoutout(event_sub: ChannelChatMessageEventSub, args: str) -> bool:
    """Shout a channel out; False when nothing reached chat.

    The answer exists for `!aso`, which must not record a shoutout it did not
    manage to give. False covers all three ways that happens: a name that
    cannot be a login, a channel Twitch does not know, and a line Twitch
    refused. The first two tell chat the same thing, and none of them is a
    shoutout anyone saw.
    """
    broadcaster_id = event_sub.event.broadcaster_user_id
    target = _target(args) or event_sub.event.broadcaster_user_login

    if not is_twitch_login(target):
        # The same answer a real login nobody owns gets: chat has no use for
        # the difference between "no such channel" and "that is not a name".
        await say_template(broadcaster_id, "twitch_shoutout_not_found")
        return False

    try:
        user = await get_user_by_username(target)
        target_channel = await get_channel(int(user.id)) if user else None
    except HelixError as e:
        # A lookup that failed is not the same as a channel that does not
        # exist, but chat has no use for the difference and is owed an answer
        # either way. The admin channel gets the one that is true.
        await notify(
            f"Could not look up {target} for !so: {e}", key="twitch-shoutout-lookup"
        )
        user = None
        target_channel = None

    if not target_channel:
        await say_template(broadcaster_id, "twitch_shoutout_not_found")
        return False

    if user and stream_session.is_live():
        shoutout_queue.add_to_queue(user.login, str(user.id))

    # The answer is whether the line landed, not whether a channel was found.
    # `say_template` reports its own failure and returns False, and a shoutout
    # nobody saw is not one - so `!aso` leaves them unsettled and their next
    # message earns another attempt. The Helix shoutout queued above is not
    # duplicated by that: the queue refuses a target it already holds, and the
    # same-target cooldown stops a second send inside the hour.
    return await say_template(
        broadcaster_id,
        "twitch_shoutout",
        name=target_channel.broadcaster_name,
        login=target_channel.broadcaster_login,
        game=target_channel.game_name,
    )


async def _listed_target(args: str) -> User | None:
    """The Twitch user `!aso`/`!unaso` names, or None with nothing said.

    Both commands need the user id, because the list is keyed by it. Neither
    falls back to the broadcaster the way `!so` does: a bare `!aso` naming
    nobody should not quietly add the channel to its own list.
    """
    target = _target(args)
    if not is_twitch_login(target):
        return None
    return await get_user_by_username(target)


async def auto_shoutout(event_sub: ChannelChatMessageEventSub, args: str) -> None:
    """Add someone to the autoshoutout list, and shout them out now.

    A `!so` that also writes a row, so it calls the shoutout handler rather
    than posting a second `!so` line for the webhook to bring back.
    """
    user = await _listed_target(args)
    if user is None:
        await say_template(
            event_sub.event.broadcaster_user_id, "twitch_shoutout_not_found"
        )
        return

    if not await autoshoutout.add(int(user.id), user.login):
        # Already listed. Nothing changed, so nothing is said - and no shoutout
        # either, because `!so` is what a mod types when they want one now.
        return

    # Spent only if a shoutout was actually given. `shoutout` answers "not
    # found" identically for a channel that is gone and for a lookup that
    # failed, and spending on either would settle someone who was never
    # shouted out - they would get nothing when they later turned up, and
    # re-running `!aso` would do nothing because the row is already there. The
    # row stays either way: wanting them on the list is what `!aso` records,
    # and a shoutout Twitch could not complete does not undo that.
    if await shoutout(event_sub, args):
        autoshoutout.spend(event_sub.event.broadcaster_user_id, int(user.id))


async def un_auto_shoutout(event_sub: ChannelChatMessageEventSub, args: str) -> None:
    """Take someone off the autoshoutout list.

    Confirms only when a row went, because removal has no other visible
    effect: without a line a mod cannot tell it worked. Naming nobody on the
    list is silence, matching `!aso` on somebody already on it. It does not
    un-spend anyone - they already had this stream's autoshoutout.
    """
    user = await _listed_target(args)
    if user is None or not await autoshoutout.remove(int(user.id)):
        return

    await say_template(
        event_sub.event.broadcaster_user_id,
        "twitch_autoshoutout_removed",
        name=user.display_name,
    )


# Awaitable[object], not Awaitable[None]: `shoutout` answers whether it
# shouted, for `!aso`. Nothing dispatched through here reads the answer.
HANDLERS: dict[str, Callable[[ChannelChatMessageEventSub, str], Awaitable[object]]] = {
    "auto_shoutout": auto_shoutout,
    "hug": hug,
    "shoutout": shoutout,
    "un_auto_shoutout": un_auto_shoutout,
}


def _is_mod(event_sub: ChannelChatMessageEventSub) -> bool:
    """Whether the chatter holds a badge that may run a mod-only command."""
    return any(
        badge.set_id in {"moderator", "broadcaster"}
        for badge in event_sub.event.badges or []
    )


async def dispatch(event_sub: ChannelChatMessageEventSub, name: str, args: str) -> None:
    """Run a chat command by name; unknown or disabled names do nothing.

    A wrapper only so that `seen`, which exists for the composite cycle guard,
    stays out of the name every caller uses.
    """
    await _run(event_sub, name, args)


async def _run(
    event_sub: ChannelChatMessageEventSub,
    name: str,
    args: str,
    seen: frozenset[str] = frozenset(),
) -> None:
    """Run one command, and each command it is composed of.

    Permission is checked per command rather than once at the root. Checking
    only what was typed made a composite a way around its children: one that
    is not itself mod-only would run a mod-only member for anybody, and
    nothing stops a migration adding that pairing. Today's only composite is
    mod-only, so this closes the hole rather than a live bypass.

    Only the command the chatter actually named is refused out loud. A member
    they never asked for is skipped in silence, so one refusal cannot become
    one per member of the composite.
    """
    command = config.command(name)
    if command is None:
        return

    if command.mod_only and not _is_mod(event_sub):
        if not seen:
            await say_template(event_sub.event.broadcaster_user_id, "twitch_mod_only")
        return

    if command.handler == COMPOSITE_HANDLER:
        if name in seen:
            logger.warning("Composite command %r is a member of its own cycle", name)
            return
        for child in config.command_components(name):
            await _run(event_sub, child, args, seen | {name})
        return

    handler = HANDLERS.get(command.handler)
    if handler is not None:
        # A backstop for the lookups a handler makes, not for its sends: those
        # go through chat.say, which reports its own. HANDLERS is the one place
        # here that runs code this module does not see.
        try:
            await handler(event_sub, args)
        except HelixError as e:
            await notify(f"Could not run !{name}: {e}", key=f"twitch-command:{name}")
        return

    for message in config.command_responses(name):
        # Each line stands alone: one that Twitch refuses must not silence the
        # rest of a multi-line command. say never raises, so it cannot.
        await say(
            event_sub.event.broadcaster_user_id,
            _render(message, event_sub, args),
            f"a response for !{name}",
        )
