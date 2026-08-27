"""Enumerations backing the configuration tables."""

from enum import Enum

__all__ = ["AutoResponseMatch", "OAuthTokenKey", "SettingValueType"]


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


class OAuthTokenKey(str, Enum):
    """Which Twitch identity a stored token belongs to.

    Mirrors ``constants.TokenType``; the two should converge on this one.
    """

    APP = "app"
    USER = "user"
    BROADCASTER = "broadcaster"
