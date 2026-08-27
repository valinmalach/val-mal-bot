"""Discord-side configuration: channels, self-assignable roles and embeds.

Stored text may carry ``{channel:promo}`` / ``{role:follower}`` placeholders.
"""

from sqlalchemy import BigInteger, Text, UniqueConstraint
from sqlmodel import Field

from db.base import TimestampMixin, enum_column
from db.models.enums import AutoResponseMatch

__all__ = [
    "DiscordAutoResponse",
    "DiscordChannel",
    "DiscordEmbed",
    "DiscordEmbedField",
    "DiscordRole",
]


class DiscordChannel(TimestampMixin, table=True):
    """A named channel the bot posts to, e.g. ``audit_logs`` or ``welcome``."""

    __tablename__ = "discord_channel"

    key: str = Field(primary_key=True, max_length=64)
    channel_id: int = Field(
        sa_type=BigInteger,
        unique=True,
        sa_column_kwargs={"nullable": False},
    )
    description: str | None = Field(default=None, max_length=255)


class DiscordEmbed(TimestampMixin, table=True):
    """A reusable embed such as the rules post or a role-picker panel.

    ``key`` also identifies the persistent view, so buttons survive restarts.
    """

    __tablename__ = "discord_embed"

    key: str = Field(primary_key=True, max_length=64)
    title: str | None = Field(default=None, max_length=256)
    description: str | None = Field(default=None, sa_type=Text)
    color: int | None = Field(default=None)
    channel_key: str | None = Field(
        default=None,
        max_length=64,
        foreign_key="discord_channel.key",
        ondelete="SET NULL",
    )
    position: int = Field(default=0, description="Order when posted as a group")


class DiscordRole(TimestampMixin, table=True):
    """A guild role the bot hands out, plus the button that toggles it."""

    __tablename__ = "discord_role"

    key: str = Field(primary_key=True, max_length=64)
    role_id: int = Field(
        sa_type=BigInteger,
        unique=True,
        sa_column_kwargs={"nullable": False},
    )
    name: str = Field(
        max_length=100,
        description="Exact Discord role name, emoji prefix included",
    )
    emoji: str | None = Field(
        default=None,
        max_length=32,
        unique=True,
        description="Emoji on the toggle button; replaces EMOJI_ROLE_MAP",
    )
    custom_id: str | None = Field(
        default=None,
        max_length=100,
        unique=True,
        description="Persistent discord.ui.Button custom_id",
    )
    embed_key: str | None = Field(
        default=None,
        max_length=64,
        foreign_key="discord_embed.key",
        ondelete="SET NULL",
        description="Role-picker embed this role is listed on",
    )
    position: int = Field(default=0, description="Order within the embed")
    assignable: bool = Field(default=True)
    description: str | None = Field(default=None, max_length=255)


class DiscordEmbedField(TimestampMixin, table=True):
    """One field of a :class:`DiscordEmbed`, ordered by ``position``."""

    __tablename__ = "discord_embed_field"
    __table_args__ = (UniqueConstraint("embed_key", "position"),)

    id: int | None = Field(default=None, primary_key=True)
    embed_key: str = Field(
        max_length=64,
        foreign_key="discord_embed.key",
        ondelete="CASCADE",
        index=True,
    )
    position: int = Field(default=0)
    name: str = Field(default="", max_length=256)
    value: str = Field(sa_type=Text)
    inline: bool = Field(default=False)


class DiscordAutoResponse(TimestampMixin, table=True):
    """Canned reply to a plain chat message, e.g. ``ping`` -> ``pong``."""

    __tablename__ = "discord_auto_response"

    id: int | None = Field(default=None, primary_key=True)
    trigger: str = Field(max_length=255, unique=True)
    response: str = Field(sa_type=Text)
    match_type: AutoResponseMatch = Field(
        default=AutoResponseMatch.EXACT,
        sa_type=enum_column(AutoResponseMatch, "auto_response_match"),
        sa_column_kwargs={"nullable": False},
    )
    case_sensitive: bool = Field(default=False)
    enabled: bool = Field(default=True)
