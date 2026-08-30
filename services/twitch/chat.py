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
    if not text.strip():
        # A missing message_template renders as "". config.template has already
        # named the row; sending it would only add a 400 from Twitch on top.
        return False

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
    """Say a line from the templates table, named by its key in any report.

    Rendering is guarded too: a ``{channel:key}`` naming a slug that is not in
    the database raises KeyError out of config.render, and callers here are
    promised a chat line that cannot fail them.
    """
    try:
        text = config.template(template_key, **values)
    except Exception as e:  # noqa: BLE001
        await notify(
            f"Could not render {template_key} for broadcaster {broadcaster_id}: {e}",
            key=f"twitch-chat-render:{template_key}",
        )
        return False
    return await say(broadcaster_id, text, template_key)
