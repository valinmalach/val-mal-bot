"""Twitch chat commands, dispatched from the twitch_command tables.

Response text lives in the database. Only commands needing real logic have a
function here; `twitch_command.handler` names one of them, `static` sends the
stored responses in order, and `composite` runs other commands.
"""

import logging
from collections.abc import Awaitable, Callable

from errors import notify
from models import ChannelChatMessageEventSub
from services.config import config, safe_format
from services.twitch.api import get_channel, get_user_by_username
from services.twitch.chat import say, say_template
from services.twitch.helix import HelixError

from .shoutout_queue import shoutout_queue

logger = logging.getLogger(__name__)

__all__ = ["dispatch"]

COMPOSITE_HANDLER = "composite"


def _target(args: str) -> str:
    return (args.split(" ", 1)[0] if args else "").removeprefix("@")


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


async def shoutout(event_sub: ChannelChatMessageEventSub, args: str) -> None:
    broadcaster_id = event_sub.event.broadcaster_user_id
    target = _target(args) or event_sub.event.broadcaster_user_login

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
        return

    if user and shoutout_queue.activated:
        shoutout_queue.add_to_queue(user.login, str(user.id))

    await say_template(
        broadcaster_id,
        "twitch_shoutout",
        name=target_channel.broadcaster_name,
        login=target_channel.broadcaster_login,
        game=target_channel.game_name,
    )


HANDLERS: dict[str, Callable[[ChannelChatMessageEventSub, str], Awaitable[None]]] = {
    "hug": hug,
    "shoutout": shoutout,
}


async def _check_mod(event_sub: ChannelChatMessageEventSub) -> bool:
    """Whether the chatter may run a mod-only command, telling them if not."""
    if any(
        badge.set_id in {"moderator", "broadcaster"}
        for badge in event_sub.event.badges or []
    ):
        return True

    await say_template(event_sub.event.broadcaster_user_id, "twitch_mod_only")
    return False


async def dispatch(event_sub: ChannelChatMessageEventSub, name: str, args: str) -> None:
    """Run a chat command by name; unknown or disabled names do nothing."""
    command = config.command(name)
    if command is None:
        return
    if command.mod_only and not await _check_mod(event_sub):
        return
    await _run(event_sub, name, args)


async def _run(
    event_sub: ChannelChatMessageEventSub,
    name: str,
    args: str,
    seen: frozenset[str] = frozenset(),
) -> None:
    """Dispatch without the permission check, which a composite does once."""
    command = config.command(name)
    if command is None:
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
