"""Enumerations backing the database tables."""

from enum import Enum

__all__ = ["AutoResponseMatch", "SettingValueType", "TokenType"]


class SettingValueType(str, Enum):
    """How the text in ``app_setting.value`` should be interpreted."""

    STRING = "string"
    INTEGER = "integer"
    BOOLEAN = "boolean"
    JSON = "json"


class AutoResponseMatch(str, Enum):
    """How an incoming message is compared against an auto-response trigger."""

    EXACT = "exact"
    PREFIX = "prefix"
    CONTAINS = "contains"


class TokenType(str, Enum):
    """Which Twitch identity an ``oauth_token`` row holds a grant for."""

    App = "app"
    User = "user"
    Broadcaster = "broadcaster"
