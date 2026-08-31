"""The audit log: one entry point per thing worth recording.

Callers say what happened and pass the domain objects it happened to. Everything
else — which colour, which author line, whether it takes one embed or several,
and which channel it lands in — is settled here, so a change to how the log
looks is a change to one file.
"""

from collections.abc import Sequence
from typing import Literal

import pendulum
from discord import (
    Attachment,
    Embed,
    Invite,
    Member,
    Message,
    Object,
    Role,
    Thread,
    User,
)
from discord.abc import GuildChannel, PrivateChannel
from discord.ext.commands import CommandError, Context
from discord.utils import escape_markdown
from pendulum import DateTime

from constants import (
    DEFAULT_MISSING_CONTENT,
    EMPTY_CONTENT,
    UNKNOWN_USER,
    UNNAMED_EVENT,
)
from services.config import config
from services.helper.helper import (
    get_age,
    get_channel_mention,
    get_discriminator,
    get_pfp,
    send_embed,
)

# Exactly what bot.get_channel hands back, which is what the message events carry.
AuditChannel = GuildChannel | Thread | PrivateChannel | None

# Discord rejects the whole embed if any one of these is exceeded, so they are
# enforced here rather than trusted to the callers.
_FIELD_LIMIT = 1024
_DESCRIPTION_LIMIT = 4096
_AUTHOR_LIMIT = 256


def _cap(text: str, limit: int) -> str:
    if len(text) <= limit:
        return text
    cut = text[: limit - 3]
    # An escape sequence cut in half leaves a lone backslash, which would eat the
    # first character of the ellipsis.
    if (len(cut) - len(cut.rstrip("\\"))) % 2:
        cut = cut[:-1]
    return f"{cut}..."


def _field(value: str) -> str:
    """A field value is rejected for being empty as well as for being too long."""
    return _cap(value, _FIELD_LIMIT) or EMPTY_CONTENT


def _said(text: str) -> str:
    """Text a person wrote, going somewhere Discord renders markdown.

    Escaped, not merely capped: a description or a field value renders a masked
    link, so an unescaped message could put a label of its choosing on a URL of
    its choosing in front of whoever reads the audit log.
    """
    return _field(escape_markdown(text))


def _named(user: User | Member) -> str:
    """Escaped: a description renders markdown where an author line does not, and
    bot and unmigrated names are not held to the modern username charset."""
    return f"{escape_markdown(user.name)}{get_discriminator(user)}"


def _quoted(text: str | None) -> str:
    """A person's words, or the bot's marker for words it no longer has.

    Only the person's are escaped: the marker is the bot's own text and its
    formatting is deliberate.
    """
    return DEFAULT_MISSING_CONTENT if text is None else _said(text)


def _embed(description: str, color: str) -> Embed:
    return Embed(
        description=_cap(description, _DESCRIPTION_LIMIT),
        color=config.color(color),
        timestamp=pendulum.now(),
    )


def _by(embed: Embed, user: User | Member | None) -> None:
    """Author line naming a person. None is a deleter the audit log did not name.

    Discord renders an author name as plain text, so nothing here is escaped.
    """
    if user is None:
        embed.set_author(name=UNKNOWN_USER, icon_url=None)
        return
    embed.set_author(
        name=_cap(f"{user.name}{get_discriminator(user)}", _AUTHOR_LIMIT),
        icon_url=get_pfp(user),
    )


def _captioned(embed: Embed, text: str, icon: str | None) -> None:
    """Author line naming the event instead of a person.

    The fallback is for a template row that has gone missing: config has already
    said so, and an empty author name would lose the entry on top of that.
    """
    embed.set_author(name=_cap(text, _AUTHOR_LIMIT) or UNNAMED_EVENT, icon_url=icon)


async def _send(embed: Embed) -> None:
    await send_embed(embed, config.channel("audit_logs"))


async def member_joined(member: Member) -> None:
    url = get_pfp(member)
    embed = _embed(
        f"{member.mention} {_named(member)}",
        "embed_color_join",
    )
    _captioned(embed, config.template("audit_member_joined"), url)
    embed.set_thumbnail(url=url).add_field(
        name="**Account Age**",
        value=_field(get_age(pendulum.instance(member.created_at))),
        inline=False,
    ).set_footer(text=f"ID: {member.id}")
    await _send(embed)


async def member_left(user: User | Member) -> None:
    url = get_pfp(user)
    roles = user.roles[1:] if isinstance(user, Member) else []
    # The blank lines stand in for the roles field when there is none, so the
    # embed keeps its height either way.
    padding = "" if roles else "\n\n\n"
    embed = _embed(
        f"{user.mention} {_named(user)}{padding}",
        "embed_color_danger",
    )
    _captioned(embed, config.template("audit_member_left"), url)
    embed.set_thumbnail(url=url).set_footer(text=f"ID: {user.id}")
    if roles:
        embed.add_field(
            name="**Roles**",
            value=_field(" ".join([role.mention for role in roles])),
            inline=False,
        )
    await _send(embed)


async def _ban_state(user: User | Member, action: Literal["ban", "unban"]) -> None:
    url = get_pfp(user)
    embed = _embed(
        f"{user.mention} {_named(user)}",
        "embed_color_danger" if action == "ban" else "embed_color_info",
    )
    _captioned(embed, f"User {action.capitalize()}ed", url)
    embed.set_thumbnail(url=url).set_footer(text=f"ID: {user.id}")
    await _send(embed)


async def banned(user: User | Member) -> None:
    await _ban_state(user, "ban")


async def unbanned(user: User | Member) -> None:
    await _ban_state(user, "unban")


def _guild_of(invite: Invite) -> tuple[str, str | None]:
    guild = invite.guild
    name = guild.name if guild and not isinstance(guild, Object) else "Unknown Guild"
    icon = (
        guild.icon.url
        if guild and not isinstance(guild, Object) and guild.icon
        else None
    )
    return name, icon


async def invite_created(invite: Invite) -> None:
    guild_name, guild_icon = _guild_of(invite)
    channel_mention = get_channel_mention(invite.channel)
    inviter_mention = f" by {invite.inviter.mention}" if invite.inviter else ""
    expiry = (
        f"<t:{int(invite.expires_at.timestamp())}:R>" if invite.expires_at else "Never"
    )
    embed = _embed(
        f"**Invite [{invite.code}]({invite.url}) to {channel_mention}"
        f" created{inviter_mention}**\nExpires: {expiry}",
        "embed_color_info",
    )
    _captioned(embed, guild_name, guild_icon)
    await _send(embed)


async def invite_deleted(invite: Invite) -> None:
    guild_name, guild_icon = _guild_of(invite)
    embed = _embed(
        f"**Invite [{invite.code}]({invite.url}) deleted**", "embed_color_danger"
    )
    _captioned(embed, guild_name, guild_icon)
    await _send(embed)


async def _role_change(member: Member, roles: list[Role], added: bool) -> None:
    roles_str = " ".join([role.mention for role in roles])
    embed = _embed(
        f"**{member.mention} was {'given' if added else 'removed from'}"
        f" the role{'' if len(roles) == 1 else 's'} {roles_str}**",
        "embed_color_info",
    )
    _by(embed, member)
    embed.set_footer(text=f"ID: {member.id}")
    await _send(embed)


async def role_added(member: Member, roles: list[Role]) -> None:
    await _role_change(member, roles, added=True)


async def role_removed(member: Member, roles: list[Role]) -> None:
    await _role_change(member, roles, added=False)


async def nickname_changed(member: Member, before: str, after: str) -> None:
    embed = _embed(
        config.template("audit_nickname_changed", mention=member.mention),
        "embed_color_info",
    )
    _by(embed, member)
    embed.set_footer(text=f"ID: {member.id}").add_field(
        name="**Before**", value=_said(before), inline=False
    ).add_field(name="**After**", value=_said(after), inline=False)
    await _send(embed)


async def pfp_changed(member: Member) -> None:
    embed = _embed(
        config.template("audit_pfp_changed", mention=member.mention),
        "embed_color_info",
    )
    _by(embed, member)
    embed.set_thumbnail(url=get_pfp(member)).set_footer(text=f"ID: {member.id}")
    await _send(embed)


async def timed_out(member: Member, until: DateTime) -> None:
    embed = _embed(
        config.template(
            "audit_timed_out",
            mention=member.mention,
            expiry=f"<t:{int(until.timestamp())}:R>",
        ),
        "embed_color_info",
    )
    _by(embed, member)
    embed.set_footer(text=f"ID: {member.id}")
    await _send(embed)


async def timeout_lifted(member: Member) -> None:
    embed = _embed(
        config.template("audit_timeout_removed", mention=member.mention),
        "embed_color_info",
    )
    _by(embed, member)
    embed.set_footer(text=f"ID: {member.id}")
    await _send(embed)


async def message_edited(after: Message, before_content: str | None) -> None:
    embed = _embed(
        f"**Message edited in {get_channel_mention(after.channel)}**"
        f" [Jump to Message]({after.jump_url})",
        "embed_color_info",
    )
    _by(embed, after.author)
    embed.set_footer(text=f"User ID: {after.author.id}").add_field(
        name="**Before**", value=_quoted(before_content), inline=False
    ).add_field(name="**After**", value=_said(after.content), inline=False)
    await _send(embed)


async def pin_changed(message: Message) -> None:
    embed = _embed(
        f"**Message {'pinned' if message.pinned else 'unpinned'} in"
        f" {get_channel_mention(message.channel)}**"
        f" [Jump to Message]({message.jump_url})",
        "embed_color_info",
    )
    _by(embed, message.author)
    embed.set_footer(text=f"User ID: {message.author.id}")
    await _send(embed)


async def message_deleted(
    *,
    content: str,
    attachments: Sequence[Attachment],
    message_id: int,
    author: User | Member,
    deleted_by: User | Member | None,
    channel: AuditChannel,
) -> None:
    """One embed for the message, then one more for each attachment it carried."""
    channel_mention = get_channel_mention(channel)
    deleter = "" if deleted_by is None else f" by {deleted_by.mention}"
    footer = f"Author: {author.id} | Message ID: {message_id}"

    embed = _embed(
        f"**Message sent by {author.mention} deleted{deleter} in {channel_mention}**",
        "embed_color_danger",
    )
    _by(embed, author)
    embed.set_footer(text=footer)
    if content:
        embed.add_field(name="**Message**", value=_said(content), inline=False)
    await _send(embed)

    for attachment in attachments:
        embed = _embed(
            config.template(
                "audit_attachment_deleted",
                mention=author.mention,
                channel=channel_mention,
            ),
            "embed_color_danger",
        )
        _by(embed, author)
        embed.set_footer(text=footer).set_image(url=attachment.url)
        await _send(embed)


async def message_deleted_uncached(
    *,
    content: str | None,
    message_id: int,
    deleted_by: User | Member | None,
    channel: AuditChannel,
) -> None:
    """A deletion the bot has no copy of the message for, only the stored text."""
    mention = UNKNOWN_USER if deleted_by is None else deleted_by.mention
    deleter_id = "Unknown ID" if deleted_by is None else deleted_by.id
    embed = _embed(
        f"**Message deleted by {mention} in {get_channel_mention(channel)}**",
        "embed_color_danger",
    )
    _by(embed, deleted_by)
    embed.add_field(
        name="**Message**", value=_quoted(content), inline=False
    ).set_footer(text=f"Deleter: {deleter_id} | Message ID: {message_id}")
    await _send(embed)


async def bulk_deleted(
    *, count: int, deleted_by: User | Member | None, channel: AuditChannel
) -> None:
    embed = _embed(
        f"**Bulk Delete in {get_channel_mention(channel)}, {count} messages deleted**",
        "embed_color_info",
    )
    _by(embed, deleted_by)
    await _send(embed)


async def command_failed(ctx: Context, error: CommandError) -> None:
    """Named for the handler's real scope: not-found is only the reachable case.

    Both values carry what the user typed, so both are escaped - a log entry
    must not be able to forge formatting in the channel that records it.
    """
    embed = _embed(
        f"**Command error in {get_channel_mention(ctx.channel)}**"
        f" [Jump to Message]({ctx.message.jump_url})",
        "embed_color_info",
    )
    _by(embed, ctx.author)
    embed.set_footer(text=f"User ID: {ctx.author.id}").add_field(
        name="**Command**",
        value=_said(ctx.message.content),
        inline=False,
    ).add_field(name="**Error**", value=_said(str(error)), inline=False)
    await _send(embed)
