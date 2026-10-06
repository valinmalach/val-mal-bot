"""The rows the bot reads, one frozen dataclass per table.

Each carries its model's name and columns (ADR 0005): the models in
``valmal/db/models/`` describe the schema for Alembic and the tests, and these
are what the repository hands the bot, without loading SQLModel. A test holds
the two to the same column names and nullability. Defaults mirror the models',
so a row can be written as the model was; the database always supplies them.
"""

from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any, TypeVar

from asyncpg import Record

from valmal.db.enums import AutoResponseMatch, SettingValueType, TokenType

__all__ = [
    "AppSetting",
    "DiscordAutoResponse",
    "DiscordChannel",
    "DiscordEmbed",
    "DiscordEmbedField",
    "DiscordMessage",
    "DiscordRole",
    "DiscordUser",
    "LiveAlert",
    "MessageTemplate",
    "OAuthToken",
    "TwitchAutoShoutout",
    "TwitchCommand",
    "TwitchCommandComponent",
    "TwitchCommandResponse",
    "build",
]


# Not a PEP 695 parameter list, hence the UP047: Sourcery silently skips its
# custom rules in a file that has one (see AGENTS.md).
_R = TypeVar("_R")


def build(row: type[_R], record: Record, **converted: Any) -> _R:  # noqa: UP047
    """A row from a record, with ``converted`` replacing the columns that need it."""
    return row(**{**dict(record.items()), **converted})


def _now() -> datetime:
    return datetime.now(UTC)


@dataclass(frozen=True, slots=True, kw_only=True)
class _Timestamped:
    created_at: datetime = field(default_factory=_now)
    updated_at: datetime = field(default_factory=_now)


@dataclass(frozen=True, slots=True, kw_only=True)
class OAuthToken(_Timestamped):
    key: TokenType
    access_token: str
    refresh_token: str | None = None
    expires_at: datetime | None = None
    scopes: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True, kw_only=True)
class MessageTemplate(_Timestamped):
    key: str
    content: str
    description: str | None = None


@dataclass(frozen=True, slots=True, kw_only=True)
class AppSetting(_Timestamped):
    key: str
    value: str | None = None
    value_type: SettingValueType = SettingValueType.STRING
    description: str | None = None


@dataclass(frozen=True, slots=True, kw_only=True)
class DiscordChannel(_Timestamped):
    key: str
    channel_id: int
    description: str | None = None


@dataclass(frozen=True, slots=True, kw_only=True)
class DiscordEmbed(_Timestamped):
    key: str
    title: str | None = None
    description: str | None = None
    color: int | None = None
    channel_key: str | None = None
    position: int = 0


@dataclass(frozen=True, slots=True, kw_only=True)
class DiscordRole(_Timestamped):
    key: str
    role_id: int
    name: str
    emoji: str | None = None
    custom_id: str | None = None
    embed_key: str | None = None
    position: int = 0
    assignable: bool = True
    description: str | None = None


@dataclass(frozen=True, slots=True, kw_only=True)
class DiscordEmbedField(_Timestamped):
    id: int
    embed_key: str
    position: int = 0
    name: str = ""
    value: str
    inline: bool = False


@dataclass(frozen=True, slots=True, kw_only=True)
class DiscordAutoResponse(_Timestamped):
    id: int
    trigger: str
    response: str
    match_type: AutoResponseMatch = AutoResponseMatch.EXACT
    case_sensitive: bool = False
    enabled: bool = True


@dataclass(frozen=True, slots=True, kw_only=True)
class DiscordUser(_Timestamped):
    id: int
    username: str
    birthday: datetime | None = None
    is_birthday_leap: bool | None = None
    birthday_timezone: str | None = None


@dataclass(frozen=True, slots=True, kw_only=True)
class DiscordMessage:
    id: int
    contents: str | None = None
    guild_id: int
    author_id: int
    channel_id: int
    attachment_urls: tuple[str, ...] = ()
    created_at: datetime = field(default_factory=_now)


@dataclass(frozen=True, slots=True, kw_only=True)
class LiveAlert(_Timestamped):
    broadcaster_id: int
    channel_id: int
    message_id: int
    stream_id: int
    stream_started_at: datetime


@dataclass(frozen=True, slots=True, kw_only=True)
class TwitchAutoShoutout(_Timestamped):
    twitch_user_id: int
    login: str


@dataclass(frozen=True, slots=True, kw_only=True)
class TwitchCommand(_Timestamped):
    name: str
    handler: str = "static"
    description: str | None = None
    mod_only: bool = False
    enabled: bool = True
    cooldown_seconds: int = 0
    position: int = 0


@dataclass(frozen=True, slots=True, kw_only=True)
class TwitchCommandResponse(_Timestamped):
    id: int
    command_name: str
    position: int = 0
    message: str


@dataclass(frozen=True, slots=True, kw_only=True)
class TwitchCommandComponent(_Timestamped):
    id: int
    parent_name: str
    child_name: str
    position: int = 0
