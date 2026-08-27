"""Load the parquet files under data/ into their tables.

Separate from seed_data because this is real history rather than a snapshot of
constants. The files are committed so the first deploy carries them; seed.py
skips this silently once data/ is dropped.

Rows are INSERT ... ON CONFLICT DO NOTHING on natural primary keys, so a stale
copy of a parquet file can never overwrite what the running bot has since
written, and the load can be repeated against a fresher copy.
"""

import json
import logging
from collections.abc import Iterator
from datetime import datetime
from pathlib import Path
from typing import Any, cast

import polars as pl
from sqlalchemy import CursorResult
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession
from sqlmodel import SQLModel

from db import DiscordMessage, DiscordUser, LiveAlert

logger = logging.getLogger("seed")

DATA_DIR = Path("data")
CHUNK = 500


def _parse_timestamp(value: str | None) -> datetime | None:
    """Parse an RFC3339 string, tolerating both the millisecond and bare forms.

    The bot writes ``...:00.000Z`` but older rows are ``...:00Z``.
    """
    if not value:
        return None
    try:
        return datetime.fromisoformat(value)
    except ValueError:
        logger.warning("skipping unparseable timestamp %r", value)
        return None


def _chunks(rows: list[dict[str, Any]]) -> Iterator[list[dict[str, Any]]]:
    for start in range(0, len(rows), CHUNK):
        yield rows[start : start + CHUNK]


def _user_rows(df: pl.DataFrame) -> list[dict[str, Any]]:
    return [
        {
            "id": row["id"],
            "username": row["username"],
            "birthday": _parse_timestamp(row["birthday"]),
            "is_birthday_leap": row["isBirthdayLeap"],
        }
        for row in df.iter_rows(named=True)
    ]


def _message_rows(df: pl.DataFrame) -> list[dict[str, Any]]:
    return [
        {
            "id": row["id"],
            "contents": row["contents"],
            "guild_id": row["guild_id"],
            "author_id": row["author_id"],
            "channel_id": row["channel_id"],
            "attachment_urls": json.loads(row["attachment_urls"] or "[]"),
        }
        for row in df.iter_rows(named=True)
    ]


def _alert_rows(df: pl.DataFrame) -> list[dict[str, Any]]:
    rows = []
    for row in df.iter_rows(named=True):
        started = _parse_timestamp(row["stream_started_at"])
        if started is None:
            logger.warning("skipping live alert %s: no usable start time", row["id"])
            continue
        rows.append(
            {
                "broadcaster_id": row["id"],
                "channel_id": row["channel_id"],
                "message_id": row["message_id"],
                "stream_id": row["stream_id"],
                "stream_started_at": started,
            }
        )
    return rows


SOURCES = [
    ("discord_user", DiscordUser, "users.parquet", _user_rows),
    ("discord_message", DiscordMessage, "messages.parquet", _message_rows),
    ("live_alert", LiveAlert, "live_alerts.parquet", _alert_rows),
]


def available() -> bool:
    return any((DATA_DIR / name).is_file() for _, _, name, _ in SOURCES)


async def backfill(session: AsyncSession) -> dict[str, int]:
    inserted: dict[str, int] = {}
    for name, model, filename, to_rows in SOURCES:
        path = DATA_DIR / filename
        if not path.is_file():
            logger.info("%-26s no %s, skipped", name, filename)
            continue

        rows = to_rows(pl.read_parquet(path))
        count = 0
        for chunk in _chunks(rows):
            statement = insert(cast(type[SQLModel], model)).values(chunk)
            result = cast(
                CursorResult[Any],
                await session.execute(statement.on_conflict_do_nothing()),
            )
            count += result.rowcount
        inserted[name] = count
        logger.info(
            "%-26s %4d inserted, %4d already present", name, count, len(rows) - count
        )
    return inserted
