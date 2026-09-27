"""Every table in the database.

Importing this registers the tables on the :data:`metadata` it exports.
"""

from sqlmodel import SQLModel

from valmal.db.base import CreatedAtMixin, TimestampMixin
from valmal.db.models.auth import OAuthToken
from valmal.db.models.content import MessageTemplate
from valmal.db.models.discord_config import (
    DiscordAutoResponse,
    DiscordChannel,
    DiscordEmbed,
    DiscordEmbedField,
    DiscordRole,
)
from valmal.db.models.enums import AutoResponseMatch, SettingValueType
from valmal.db.models.records import (
    DiscordMessage,
    DiscordUser,
    LiveAlert,
    TwitchAutoShoutout,
)
from valmal.db.models.settings import AppSetting
from valmal.db.models.twitch_config import (
    TwitchCommand,
    TwitchCommandComponent,
    TwitchCommandResponse,
)

# Populated by the imports above: importing a table class registers it here.
metadata = SQLModel.metadata

__all__ = [
    "AppSetting",
    "AutoResponseMatch",
    "CreatedAtMixin",
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
    "SettingValueType",
    "TimestampMixin",
    "TwitchAutoShoutout",
    "TwitchCommand",
    "TwitchCommandComponent",
    "TwitchCommandResponse",
    "metadata",
]
