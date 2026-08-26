"""Every table in the database.

Importing this registers the tables on the :data:`metadata` it exports.
"""

from sqlmodel import SQLModel

from db.base import CreatedAtMixin, TimestampMixin
from db.models.auth import OAuthToken
from db.models.content import MessageTemplate
from db.models.discord_config import (
    DiscordAutoResponse,
    DiscordChannel,
    DiscordEmbed,
    DiscordEmbedField,
    DiscordRole,
)
from db.models.enums import AutoResponseMatch, OAuthTokenKey, SettingValueType
from db.models.records import DiscordMessage, DiscordUser, LiveAlert
from db.models.settings import AppSetting
from db.models.twitch_config import (
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
    "OAuthTokenKey",
    "SettingValueType",
    "TimestampMixin",
    "TwitchCommand",
    "TwitchCommandComponent",
    "TwitchCommandResponse",
    "metadata",
]
