"""Postgres schema and session plumbing for the bot.

Tables in ``db/models``, revisions in ``migrations/``; see ``db/README.md``.
"""

from db.config import (
    get_database_url,
    normalize_database_url,
)
from db.models import (
    AppSetting,
    AutoResponseMatch,
    DiscordAutoResponse,
    DiscordChannel,
    DiscordEmbed,
    DiscordEmbedField,
    DiscordMessage,
    DiscordRole,
    DiscordUser,
    LiveAlert,
    MessageTemplate,
    OAuthToken,
    OAuthTokenKey,
    SettingValueType,
    TwitchCommand,
    TwitchCommandComponent,
    TwitchCommandResponse,
)
from db.session import (
    dispose_engine,
    get_engine,
    get_session_factory,
    session_scope,
)

__all__ = [
    "AppSetting",
    "AutoResponseMatch",
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
    "OAuthTokenKey",
    "SettingValueType",
    "TwitchCommand",
    "TwitchCommandComponent",
    "TwitchCommandResponse",
    "dispose_engine",
    "get_database_url",
    "get_engine",
    "get_session_factory",
    "normalize_database_url",
    "session_scope",
]
