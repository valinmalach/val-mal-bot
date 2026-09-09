"""How a Discord user or channel is written into a message."""

from discord import (
    DMChannel,
    GroupChannel,
    Member,
    Object,
    PartialInviteChannel,
    PartialMessageable,
    Thread,
    User,
)
from discord.abc import GuildChannel, PrivateChannel

# Every channel type the callers hold: a message's, an invite's, and whatever
# bot.get_channel hands back. The concrete guild channels all come in under
# GuildChannel, and DMChannel and GroupChannel under PrivateChannel.
MentionableChannel = (
    GuildChannel
    | PrivateChannel
    | Thread
    | PartialInviteChannel
    | PartialMessageable
    | Object
    | None
)


def get_pfp(member: User | Member) -> str:
    return member.avatar.url if member.avatar else member.default_avatar.url


def get_discriminator(member: User | Member) -> str:
    return "" if member.discriminator == "0" else f"#{member.discriminator}"


def get_channel_mention(channel: MentionableChannel) -> str:
    if channel is None or isinstance(channel, Object):
        return "Unknown Channel"
    if isinstance(channel, GroupChannel):
        return f"{channel.name}"
    if isinstance(channel, DMChannel):
        return "a DM"
    if isinstance(channel, PrivateChannel):
        return "a private channel"
    return f"{channel.mention}"
