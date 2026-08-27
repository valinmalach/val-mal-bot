"""Database access for the records the bot reads and writes at runtime.

Writes are immediate rather than queued behind a flush interval, as the parquet
cache did, so a restart cannot lose them.
"""

from collections.abc import Mapping
from datetime import datetime
from typing import Any

from sqlalchemy import delete, select, update
from sqlalchemy.dialects.postgresql import insert
from sqlmodel import col

from db.models import DiscordMessage, DiscordUser, LiveAlert
from db.session import session_scope

__all__ = [
    "delete_live_alert",
    "delete_message",
    "delete_user",
    "get_live_alert",
    "get_message",
    "get_user",
    "list_live_alerts",
    "reschedule_birthdays",
    "upsert_live_alert",
    "upsert_message",
    "upsert_user",
    "upsert_username",
    "users_due_birthday",
    "users_with_past_birthday",
]


async def _upsert(model: Any, values: dict[str, Any], key: list[str]) -> None:
    statement = insert(model).values(**values)
    updates = {k: getattr(statement.excluded, k) for k in values if k not in key}
    async with session_scope() as session:
        await session.execute(
            statement.on_conflict_do_update(index_elements=key, set_=updates)
        )


async def get_user(user_id: int) -> DiscordUser | None:
    async with session_scope() as session:
        return await session.get(DiscordUser, user_id)


async def upsert_user(
    user_id: int,
    username: str,
    birthday: datetime | None = None,
    is_birthday_leap: bool | None = None,
) -> None:
    await _upsert(
        DiscordUser,
        {
            "id": user_id,
            "username": username,
            "birthday": birthday,
            "is_birthday_leap": is_birthday_leap,
        },
        ["id"],
    )


async def upsert_username(user_id: int, username: str) -> None:
    """Record a user without touching a birthday they may already have."""
    await _upsert(DiscordUser, {"id": user_id, "username": username}, ["id"])


async def delete_user(user_id: int) -> None:
    async with session_scope() as session:
        await session.execute(delete(DiscordUser).where(col(DiscordUser.id) == user_id))


async def users_due_birthday(moment: datetime) -> list[DiscordUser]:
    """Users whose next birthday has arrived, including any a missed tick left behind.

    A range, not an equality test: matching the exact minute meant one late tick
    skipped that birthday for good.
    """
    async with session_scope() as session:
        result = await session.execute(
            select(DiscordUser).where(col(DiscordUser.birthday) <= moment)
        )
        return list(result.scalars().all())


async def users_with_past_birthday(moment: datetime) -> list[DiscordUser]:
    """Users whose stored next birthday has already gone by."""
    async with session_scope() as session:
        result = await session.execute(
            select(DiscordUser).where(col(DiscordUser.birthday) < moment)
        )
        return list(result.scalars().all())


async def reschedule_birthdays(birthdays: Mapping[int, datetime]) -> None:
    """Move each listed user's next birthday, in one transaction."""
    if not birthdays:
        return
    async with session_scope() as session:
        for user_id, birthday in birthdays.items():
            await session.execute(
                update(DiscordUser)
                .where(col(DiscordUser.id) == user_id)
                .values(birthday=birthday)
            )


async def get_message(message_id: int) -> DiscordMessage | None:
    async with session_scope() as session:
        return await session.get(DiscordMessage, message_id)


async def upsert_message(
    message_id: int,
    contents: str | None,
    guild_id: int,
    author_id: int,
    channel_id: int,
    attachment_urls: list[str],
) -> None:
    await _upsert(
        DiscordMessage,
        {
            "id": message_id,
            "contents": contents,
            "guild_id": guild_id,
            "author_id": author_id,
            "channel_id": channel_id,
            "attachment_urls": attachment_urls,
        },
        ["id"],
    )


async def delete_message(message_id: int) -> None:
    async with session_scope() as session:
        await session.execute(
            delete(DiscordMessage).where(col(DiscordMessage.id) == message_id)
        )


async def get_live_alert(broadcaster_id: int) -> LiveAlert | None:
    async with session_scope() as session:
        return await session.get(LiveAlert, broadcaster_id)


async def list_live_alerts() -> list[LiveAlert]:
    async with session_scope() as session:
        result = await session.execute(select(LiveAlert))
        return list(result.scalars().all())


async def upsert_live_alert(
    broadcaster_id: int,
    channel_id: int,
    message_id: int,
    stream_id: int,
    stream_started_at: datetime,
) -> None:
    await _upsert(
        LiveAlert,
        {
            "broadcaster_id": broadcaster_id,
            "channel_id": channel_id,
            "message_id": message_id,
            "stream_id": stream_id,
            "stream_started_at": stream_started_at,
        },
        ["broadcaster_id"],
    )


async def delete_live_alert(
    broadcaster_id: int, *, message_id: int | None = None
) -> None:
    """``message_id`` scopes the delete, so a newer alert's row survives a late cleanup."""
    async with session_scope() as session:
        statement = delete(LiveAlert).where(
            col(LiveAlert.broadcaster_id) == broadcaster_id
        )
        if message_id is not None:
            statement = statement.where(col(LiveAlert.message_id) == message_id)
        await session.execute(statement)
