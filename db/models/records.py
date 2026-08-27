"""Runtime records the bot reads and writes at runtime.

Column names are snake_case even where the old parquet schema was not.
"""

from datetime import datetime

from sqlalchemy import BigInteger, Text, text
from sqlalchemy.dialects.postgresql import JSONB
from sqlmodel import Field

from db.base import UTC_TIMESTAMP, CreatedAtMixin, TimestampMixin

__all__ = ["DiscordMessage", "DiscordUser", "LiveAlert"]


class DiscordUser(TimestampMixin, table=True):
    """A guild member and, optionally, their next birthday.

    ``birthday`` holds the next occurrence in UTC, which the birthday task matches on.
    """

    __tablename__ = "discord_user"

    id: int = Field(
        sa_type=BigInteger,
        primary_key=True,
        sa_column_kwargs={"autoincrement": False},
    )
    username: str = Field(max_length=255)
    birthday: datetime | None = Field(
        default=None,
        sa_type=UTC_TIMESTAMP,
        index=True,
    )
    is_birthday_leap: bool | None = Field(default=None)


class DiscordMessage(CreatedAtMixin, table=True):
    """Cached message content, used to report edits and deletions."""

    __tablename__ = "discord_message"

    id: int = Field(
        sa_type=BigInteger,
        primary_key=True,
        sa_column_kwargs={"autoincrement": False},
    )
    contents: str | None = Field(default=None, sa_type=Text)
    guild_id: int = Field(sa_type=BigInteger)
    author_id: int = Field(sa_type=BigInteger, index=True)
    channel_id: int = Field(sa_type=BigInteger, index=True)
    attachment_urls: list[str] = Field(
        default_factory=list,
        sa_type=JSONB,
        sa_column_kwargs={
            "nullable": False,
            "server_default": text("'[]'::jsonb"),
        },
    )


class LiveAlert(TimestampMixin, table=True):
    """A live-alert message still being updated for a running stream.

    Keyed by broadcaster, which is what the parquet ``id`` column held.
    """

    __tablename__ = "live_alert"

    broadcaster_id: int = Field(
        sa_type=BigInteger,
        primary_key=True,
        sa_column_kwargs={"autoincrement": False},
    )
    channel_id: int = Field(sa_type=BigInteger)
    message_id: int = Field(sa_type=BigInteger)
    stream_id: int = Field(sa_type=BigInteger)
    stream_started_at: datetime = Field(sa_type=UTC_TIMESTAMP)
