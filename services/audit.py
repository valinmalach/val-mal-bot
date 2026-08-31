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
    CategoryChannel,
    Embed,
    ForumChannel,
    Invite,
    Member,
    Message,
    Object,
    Role,
    StageChannel,
    TextChannel,
    Thread,
    User,
    VoiceChannel,
)
from discord.abc import PrivateChannel
from pendulum import DateTime

from constants import UNKNOWN_USER
from services.config import config
from services.helper.helper import (
    get_age,
    get_channel_mention,
    get_discriminator,
    get_pfp,
    send_embed,
)

# What bot.get_channel hands back, which is what the message events carry.
AuditChannel = (
    VoiceChannel
    | StageChannel
    | ForumChannel
    | TextChannel
    | CategoryChannel
    | Thread
    | PrivateChannel
    | None
)

_FIELD_LIMIT = 1024


def _truncate(content: str) -> str:
    if len(content) > _FIELD_LIMIT:
        return f"{content[: _FIELD_LIMIT - 3]}..."
    return content


def _embed(description: str, color: str) -> Embed:
    return Embed(
        description=description, color=config.color(color), timestamp=pendulum.now()
    )


def _by(embed: Embed, user: User | Member | None) -> Embed:
    """Author line naming a person. None is a deleter the audit log did not name."""
    if user is None:
        return embed.set_author(name=UNKNOWN_USER, icon_url=None)
    return embed.set_author(
        name=f"{user.name}{get_discriminator(user)}", icon_url=get_pfp(user)
    )


def _captioned(embed: Embed, text: str, icon: str | None) -> Embed:
    """Author line naming the event instead of a person."""
    return embed.set_author(name=text, icon_url=icon)


async def _send(embed: Embed) -> None:
    await send_embed(embed, config.channel("audit_logs"))


async def member_joined(member: Member) -> None:
    url = get_pfp(member)
    embed = _embed(
        f"{member.mention} {member.name}{get_discriminator(member)}",
        "embed_color_join",
    )
    _captioned(embed, config.template("audit_member_joined"), url)
    embed.set_thumbnail(url=url).add_field(
        name="**Account Age**",
        value=get_age(pendulum.instance(member.created_at)),
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
        f"{user.mention} {user.name}{get_discriminator(user)}{padding}",
        "embed_color_danger",
    )
    _captioned(embed, config.template("audit_member_left"), url)
    embed.set_thumbnail(url=url).set_footer(text=f"ID: {user.id}")
    if roles:
        embed.add_field(
            name="**Roles**",
            value=" ".join([f"{role.mention}" for role in roles]),
            inline=False,
        )
    await _send(embed)


async def _ban_state(user: User | Member, action: Literal["ban", "unban"]) -> None:
    url = get_pfp(user)
    embed = _embed(
        f"{user.mention} {user.name}{get_discriminator(user)}",
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
    _captioned(embed, f"{guild_name}", guild_icon)
    await _send(embed)


async def invite_deleted(invite: Invite) -> None:
    guild_name, guild_icon = _guild_of(invite)
    embed = _embed(
        f"**Invite [{invite.code}]({invite.url}) deleted**", "embed_color_danger"
    )
    _captioned(embed, f"{guild_name}", guild_icon)
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
        name="**Before**", value=f"{before}", inline=False
    ).add_field(name="**After**", value=f"{after}", inline=False)
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


async def message_edited(after: Message, before_content: str) -> None:
    embed = _embed(
        f"**Message edited in {get_channel_mention(after.channel)}**"
        f" [Jump to Message]({after.jump_url})",
        "embed_color_info",
    )
    _by(embed, after.author)
    embed.set_footer(text=f"User ID: {after.author.id}").add_field(
        name="**Before**", value=f"{_truncate(before_content)}", inline=False
    ).add_field(name="**After**", value=f"{_truncate(after.content)}", inline=False)
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
        embed.add_field(name="**Message**", value=f"{_truncate(content)}", inline=False)
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
    content: str,
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
        name="**Message**", value=f"{_truncate(content)}", inline=False
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
