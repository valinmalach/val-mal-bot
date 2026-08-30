"""Saying something in Twitch chat, where silence is survivable but not silent.

Every chat line the bot sends goes through here. ``api.send_chat_message``
raises, because transport does not decide what a failure means; this decides it
once, so the next call site added cannot quietly become the one that drops its
own failure on the floor. Nothing else may call ``send_chat_message``.
"""

from typing import Any

from errors import notify
from services.config import config
from services.twitch.api import send_chat_message
from services.twitch.helix import HelixError


async def say(broadcaster_id: str | int, text: str, what: str) -> bool:
    """Say one line in chat. Returns whether it landed, and never raises.

    ``what`` names the line in the admin channel, and separates one failing
    line from another so neither stands in for the other.
    """
    try:
        await send_chat_message(str(broadcaster_id), text)
    except HelixError as e:
        await notify(
            f"Could not send {what} to broadcaster {broadcaster_id}: {e}",
            key=f"twitch-chat:{what}:{broadcaster_id}",
        )
        return False
    return True


async def say_template(
    broadcaster_id: str | int, template_key: str, **values: Any
) -> bool:
    """Say a line from the templates table, named by its key in any report."""
    return await say(
        broadcaster_id, config.template(template_key, **values), template_key
    )
