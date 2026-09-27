"""How a Discord user, channel or untrusted value is written into a message."""

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


# Long enough to recognise what was rejected, short enough that a value built
# to fill a message cannot.
_MAX_ECHOED = 50


def quoted(value: str) -> str:
    """An untrusted value, safe to put in a Discord message.

    Nearly every caller is echoing back something that was *rejected* - a login
    that failed its grammar, say - and that is precisely the value most likely
    to carry markup, because passing the grammar is what would have ruled it
    out.

    A code span rather than escape_markdown, and the two do not compose. A span
    renders nothing inside it, which covers markdown escape_markdown handles
    *and* a mention, which it does not touch - `<@id>` comes back unchanged and
    would ping. What a span cannot survive is a backtick, and escaping one does
    not help: a backslash is literal inside a span, so an escaped backtick
    still closes it and lets the rest out. So backticks are removed, and the
    span does the rest of the work.
    """
    return f"`{value[:_MAX_ECHOED].replace('`', '')}`"
