"""Twitch OAuth tokens, replacing the ``data/twitch/*.txt`` files.

Plaintext for now; encrypting under an ``.env`` key is a later step.
"""

from datetime import datetime

from sqlalchemy import Text, text
from sqlalchemy.dialects.postgresql import JSONB
from sqlmodel import Field

from db.base import UTC_TIMESTAMP, TimestampMixin, enum_column
from db.models.enums import OAuthTokenKey

__all__ = ["OAuthToken"]


class OAuthToken(TimestampMixin, table=True):
    """One row per Twitch identity the bot authenticates as.

    A null ``expires_at`` means unknown, so that token refreshes on a 401 instead.
    """

    __tablename__ = "oauth_token"

    key: OAuthTokenKey = Field(
        primary_key=True,
        sa_type=enum_column(OAuthTokenKey, "oauth_token_key"),
    )
    access_token: str = Field(sa_type=Text)
    refresh_token: str | None = Field(default=None, sa_type=Text)
    expires_at: datetime | None = Field(
        default=None,
        sa_type=UTC_TIMESTAMP,
    )
    scopes: list[str] = Field(
        default_factory=list,
        sa_type=JSONB,
        sa_column_kwargs={
            "nullable": False,
            "server_default": text("'[]'::jsonb"),
        },
    )
