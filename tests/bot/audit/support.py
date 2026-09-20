from datetime import UTC, datetime
from types import SimpleNamespace
from typing import Any
from unittest.mock import MagicMock

import discord

AUDIT_CHANNEL = 4242
DEFAULT_AVATAR = "https://cdn.example/default.png"
AVATAR = "https://cdn.example/avatar.png"

# Each sentence says which row it is, so a test can tell "one role" from "several"
# and "deleted by somebody" from "deleted" without reproducing the seeded English.
TEMPLATES = {
    "audit_member_joined": "Member Joined",
    "audit_member_left": "Member Left",
    "audit_user_banned": "User Banned",
    "audit_user_unbanned": "User Unbanned",
    "audit_invite_created": "created {code} {url} {channel} {expiry}",
    "audit_invite_created_by": "created_by {code} {url} {channel} {inviter} {expiry}",
    "audit_invite_deleted": "deleted {code} {url}",
    "audit_role_added": "role_added {mention} {roles}",
    "audit_roles_added": "roles_added {mention} {roles}",
    "audit_role_removed": "role_removed {mention} {roles}",
    "audit_roles_removed": "roles_removed {mention} {roles}",
    "audit_nickname_changed": "nickname {mention}",
    "audit_pfp_changed": "pfp {mention}",
    "audit_timed_out": "timed_out {mention} {expiry}",
    "audit_timeout_removed": "timeout_removed {mention}",
    "audit_message_edited": "edited {channel} {url}",
    "audit_message_pinned": "pinned {channel} {url}",
    "audit_message_unpinned": "unpinned {channel} {url}",
    "audit_message_deleted": "deleted {mention} {channel}",
    "audit_message_deleted_by": "deleted_by {mention} {deleter} {channel}",
    "audit_message_deleted_uncached": "uncached {mention} {channel}",
    "audit_attachment_deleted": "attachment {mention} {channel}",
    "audit_bulk_deleted": "bulk {count} {channel}",
    "audit_command_failed": "failed {channel} {url}",
}
COLORS = {
    "embed_color_join": 0x111111,
    "embed_color_danger": 0x222222,
    "embed_color_info": 0x333333,
}


def person(
    kind: type = discord.Member,
    id: int = 7,
    name: str = "val_mal",
    discriminator: str = "0",
    avatar: str | None = AVATAR,
) -> Any:
    who = MagicMock(spec=kind)
    who.id = id
    who.name = name
    who.discriminator = discriminator
    who.mention = f"<@{id}>"
    who.avatar = SimpleNamespace(url=avatar) if avatar else None
    who.default_avatar = SimpleNamespace(url=DEFAULT_AVATAR)
    if kind is discord.Member:
        who.roles = [role(1, "everyone")]
        who.created_at = datetime(2026, 1, 1, tzinfo=UTC)
    return who


def role(id: int, name: str = "role") -> Any:
    made = MagicMock(spec=discord.Role)
    made.id = id
    made.mention = f"<@&{id}>"
    made.name = name
    return made


def channel(id: int = 55) -> Any:
    made = MagicMock(spec=discord.TextChannel)
    made.id = id
    made.mention = f"<#{id}>"
    return made


def message(
    author: Any | None = None,
    content: str = "hello",
    pinned: bool = False,
    where: Any | None = None,
) -> Any:
    made = MagicMock(spec=discord.Message)
    made.author = author or person()
    made.content = content
    made.pinned = pinned
    made.channel = where or channel()
    made.jump_url = "https://discord.com/channels/1/55/9"
    return made


def guild(name: str = "Home", icon: str | None = "https://cdn.example/g.png") -> Any:
    made = MagicMock(spec=discord.Guild)
    made.name = name
    made.icon = SimpleNamespace(url=icon) if icon else None
    return made


def invite(
    inviter: Any | None = None,
    expires_at: datetime | None = None,
    where: Any = "unset",
) -> Any:
    made = MagicMock(spec=discord.Invite)
    made.code = "abc"
    made.url = "https://discord.gg/abc"
    made.guild = guild() if where == "unset" else where
    made.inviter = inviter
    made.channel = channel()
    made.expires_at = expires_at
    return made


def attachment(url: str = "https://cdn.example/file.png") -> Any:
    return SimpleNamespace(url=url)


class Audit:
    """Everything the audit module sent, and to which channel."""

    def __init__(self) -> None:
        self.sent: list[tuple[discord.Embed, int]] = []

    @property
    def embed(self) -> discord.Embed:
        assert len(self.sent) == 1, f"expected one embed, got {len(self.sent)}"
        return self.sent[0][0]

    @property
    def description(self) -> str:
        return self.embed.description or ""

    @property
    def embeds(self) -> list[discord.Embed]:
        return [embed for embed, _ in self.sent]

    def field(self, name: str) -> str | None:
        return next((f.value for f in self.embed.fields if f.name == name), None)
