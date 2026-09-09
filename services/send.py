"""Saying something in a Discord channel, and editing what was said."""

import logging

import discord
from discord import CategoryChannel, Embed, ForumChannel
from discord.abc import PrivateChannel
from discord.ui import View

from init import bot

logger = logging.getLogger(__name__)


async def _sendable(channel_id: int, quiet: bool):
    """The channel, or None once it has been said that there isn't one.

    Most callers here are audit logging, which discards what it gets back: a
    channel the bot cannot resolve would otherwise stop the log dead with
    nothing anywhere to say it had.
    """
    channel = bot.get_channel(channel_id)
    if channel is not None and not isinstance(
        channel, (ForumChannel, CategoryChannel, PrivateChannel)
    ):
        return channel

    if not quiet:
        # Deferred: errors imports this module.
        from errors import notify

        await notify(
            f"Channel {channel_id} could not be resolved, so nothing sent to it"
            f" is arriving.",
            key=f"channel-unresolved:{channel_id}",
        )
    return None


async def send_message(
    content: str,
    channel_id: int,
    file: discord.File | None = None,
    quiet: bool = False,
    allowed_mentions: discord.AllowedMentions | None = None,
) -> int | None:
    # quiet is for errors.py alone: announcing an unreachable admin channel
    # through the admin channel does not terminate.
    channel = await _sendable(channel_id, quiet)
    if channel is None:
        return None
    mentions = allowed_mentions or discord.AllowedMentions()
    if file:
        return (await channel.send(content, file=file, allowed_mentions=mentions)).id
    return (await channel.send(content, allowed_mentions=mentions)).id


async def send_embed(
    embed: Embed,
    channel_id: int,
    view: View | None = None,
    content: str | None = None,
) -> int | None:
    channel = await _sendable(channel_id, quiet=False)
    if channel is None:
        return None
    if view:
        return (await channel.send(content=content, embed=embed, view=view)).id
    return (await channel.send(content=content, embed=embed)).id


async def edit_embed(
    message_id: int,
    embed: Embed,
    channel_id: int,
    view: View | None = None,
    content: str | None = None,
) -> bool:
    """False when the channel cannot be resolved, so callers can tell a no-op from an edit."""
    channel = bot.get_channel(channel_id)
    if channel is None or isinstance(
        channel, (ForumChannel, CategoryChannel, PrivateChannel)
    ):
        logger.warning(
            f"Channel {channel_id} unavailable; skipped editing message {message_id}"
        )
        return False
    message = await channel.fetch_message(message_id)
    if view:
        await message.edit(content=content, embed=embed, view=view)
    else:
        await message.edit(content=content, embed=embed, view=None)
    return True
