"""How a Discord user or channel is written into a message."""

from discord import (
    CategoryChannel,
    DMChannel,
    ForumChannel,
    GroupChannel,
    Member,
    Object,
    PartialInviteChannel,
    PartialMessageable,
    StageChannel,
    TextChannel,
    Thread,
    User,
    VoiceChannel,
)
from discord.abc import GuildChannel, PrivateChannel


def get_pfp(member: User | Member) -> str:
    return member.avatar.url if member.avatar else member.default_avatar.url


def get_discriminator(member: User | Member) -> str:
    return "" if member.discriminator == "0" else f"#{member.discriminator}"


def get_channel_mention(
    channel: (
        VoiceChannel
        | StageChannel
        | ForumChannel
        | TextChannel
        | CategoryChannel
        | PartialInviteChannel
        | DMChannel
        | PartialMessageable
        | GroupChannel
        | Thread
        | PrivateChannel
        | GuildChannel
        | Object
        | None
    ),
) -> str:
    if channel is None or isinstance(channel, Object):
        return "Unknown Channel"
    if isinstance(channel, GroupChannel):
        return f"{channel.name}"
    if isinstance(channel, DMChannel):
        return "a DM"
    if isinstance(channel, PrivateChannel):
        return "a private channel"
    return f"{channel.mention}"
