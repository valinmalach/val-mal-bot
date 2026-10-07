"""Every statement the bot runs against Postgres, and the rows they return.

Statements are module-level constants (ADR 0005), here and in
``configuration.py`` for the tables read at startup, so CI can ``prepare()`` each one
against the migrated schema and the tests can name the one a function ran. Each
read lists its columns rather than ``*``, and the rows are built from them; a test
holds every read's columns to its row's fields. Writes are immediate, not queued
behind a flush interval, so a restart cannot lose them.

SQLAlchemy's ``onupdate`` is gone with SQLAlchemy, so every UPDATE and every
``DO UPDATE`` sets ``updated_at`` itself, which a test checks.
"""

from collections.abc import Collection
from datetime import datetime

from valmal.db import rows
from valmal.db.enums import TokenType
from valmal.db.pool import transaction

__all__ = [
    "add_autoshoutout",
    "clear_birthday",
    "delete_live_alert",
    "delete_message",
    "delete_messages",
    "delete_user",
    "get_live_alert",
    "get_message",
    "get_user",
    "is_autoshoutout",
    "list_live_alerts",
    "list_oauth_tokens",
    "remove_autoshoutout",
    "upsert_live_alert",
    "upsert_message",
    "upsert_oauth_token",
    "upsert_user",
    "upsert_username",
    "users_due_birthday",
]

# discord_user

GET_USER = """
SELECT id, username, birthday, is_birthday_leap, birthday_timezone,
       created_at, updated_at
FROM discord_user WHERE id = $1
"""

USERS_DUE_BIRTHDAY = """
SELECT id, username, birthday, is_birthday_leap, birthday_timezone,
       created_at, updated_at
FROM discord_user WHERE birthday <= $1
"""

UPSERT_USER = """
INSERT INTO discord_user (id, username, birthday, is_birthday_leap, birthday_timezone)
VALUES ($1, $2, $3, $4, $5)
ON CONFLICT (id) DO UPDATE SET
    username = EXCLUDED.username,
    birthday = EXCLUDED.birthday,
    is_birthday_leap = EXCLUDED.is_birthday_leap,
    birthday_timezone = EXCLUDED.birthday_timezone,
    updated_at = now()
"""

UPSERT_USERNAME = """
INSERT INTO discord_user (id, username) VALUES ($1, $2)
ON CONFLICT (id) DO UPDATE SET username = EXCLUDED.username, updated_at = now()
"""

CLEAR_BIRTHDAY = """
UPDATE discord_user
SET birthday = NULL, is_birthday_leap = NULL, birthday_timezone = NULL,
    updated_at = now()
WHERE id = $1
"""

DELETE_USER = "DELETE FROM discord_user WHERE id = $1"


async def get_user(user_id: int) -> rows.DiscordUser | None:
    async with transaction() as connection:
        record = await connection.fetchrow(GET_USER, user_id)
    return None if record is None else rows.build(rows.DiscordUser, record)


async def upsert_user(
    user_id: int,
    username: str,
    birthday: datetime,
    is_birthday_leap: bool,
    birthday_timezone: str | None,
) -> None:
    """Write a user and all three birthday columns.

    All three, together: they describe one birthday, and a caller that wrote two
    of them would leave the third describing a different one. Only the timezone may
    be None: a row older than that column has none, and a name the tz database
    dropped is cleared. Erasing a birthday is ``clear_birthday``.
    """
    async with transaction() as connection:
        await connection.execute(
            UPSERT_USER,
            user_id,
            username,
            birthday,
            is_birthday_leap,
            birthday_timezone,
        )


async def clear_birthday(user_id: int) -> None:
    """Null all three birthday columns, for the same reason they are written together."""
    async with transaction() as connection:
        await connection.execute(CLEAR_BIRTHDAY, user_id)


async def upsert_username(user_id: int, username: str) -> None:
    """Record a user without touching a birthday they may already have."""
    async with transaction() as connection:
        await connection.execute(UPSERT_USERNAME, user_id, username)


async def delete_user(user_id: int) -> None:
    async with transaction() as connection:
        await connection.execute(DELETE_USER, user_id)


async def users_due_birthday(moment: datetime) -> list[rows.DiscordUser]:
    """Users whose next birthday has arrived, including any a missed tick left behind.

    A range, not an equality test: matching the exact minute meant one late tick
    skipped that birthday for good.
    """
    async with transaction() as connection:
        records = await connection.fetch(USERS_DUE_BIRTHDAY, moment)
    return [rows.build(rows.DiscordUser, record) for record in records]


# discord_message

GET_MESSAGE = """
SELECT id, contents, guild_id, author_id, channel_id, attachment_urls, created_at
FROM discord_message WHERE id = $1
"""

UPSERT_MESSAGE = """
INSERT INTO discord_message
    (id, contents, guild_id, author_id, channel_id, attachment_urls)
VALUES ($1, $2, $3, $4, $5, $6)
ON CONFLICT (id) DO UPDATE SET
    contents = EXCLUDED.contents,
    guild_id = EXCLUDED.guild_id,
    author_id = EXCLUDED.author_id,
    channel_id = EXCLUDED.channel_id,
    attachment_urls = EXCLUDED.attachment_urls
"""

DELETE_MESSAGE = "DELETE FROM discord_message WHERE id = $1"

DELETE_MESSAGES = "DELETE FROM discord_message WHERE id = ANY($1::bigint[])"


async def get_message(message_id: int) -> rows.DiscordMessage | None:
    async with transaction() as connection:
        record = await connection.fetchrow(GET_MESSAGE, message_id)
    if record is None:
        return None
    return rows.build(
        rows.DiscordMessage, record, attachment_urls=tuple(record["attachment_urls"])
    )


async def upsert_message(
    message_id: int,
    contents: str | None,
    guild_id: int,
    author_id: int,
    channel_id: int,
    attachment_urls: list[str],
) -> None:
    async with transaction() as connection:
        await connection.execute(
            UPSERT_MESSAGE,
            message_id,
            contents,
            guild_id,
            author_id,
            channel_id,
            attachment_urls,
        )


async def delete_message(message_id: int) -> None:
    async with transaction() as connection:
        await connection.execute(DELETE_MESSAGE, message_id)


async def delete_messages(message_ids: Collection[int]) -> None:
    """One statement for a bulk deletion, which Discord sends up to 100 at a time."""
    if not message_ids:
        return
    async with transaction() as connection:
        await connection.execute(DELETE_MESSAGES, list(message_ids))


# live_alert

GET_LIVE_ALERT = """
SELECT broadcaster_id, channel_id, message_id, stream_id, stream_started_at,
       created_at, updated_at
FROM live_alert WHERE broadcaster_id = $1
"""

LIST_LIVE_ALERTS = """
SELECT broadcaster_id, channel_id, message_id, stream_id, stream_started_at,
       created_at, updated_at
FROM live_alert
"""

UPSERT_LIVE_ALERT = """
INSERT INTO live_alert
    (broadcaster_id, channel_id, message_id, stream_id, stream_started_at)
VALUES ($1, $2, $3, $4, $5)
ON CONFLICT (broadcaster_id) DO UPDATE SET
    channel_id = EXCLUDED.channel_id,
    message_id = EXCLUDED.message_id,
    stream_id = EXCLUDED.stream_id,
    stream_started_at = EXCLUDED.stream_started_at,
    updated_at = now()
"""

DELETE_LIVE_ALERT = "DELETE FROM live_alert WHERE broadcaster_id = $1"

DELETE_LIVE_ALERT_FOR_MESSAGE = (
    "DELETE FROM live_alert WHERE broadcaster_id = $1 AND message_id = $2"
)


async def get_live_alert(broadcaster_id: int) -> rows.LiveAlert | None:
    async with transaction() as connection:
        record = await connection.fetchrow(GET_LIVE_ALERT, broadcaster_id)
    return None if record is None else rows.build(rows.LiveAlert, record)


async def list_live_alerts() -> list[rows.LiveAlert]:
    async with transaction() as connection:
        records = await connection.fetch(LIST_LIVE_ALERTS)
    return [rows.build(rows.LiveAlert, record) for record in records]


async def upsert_live_alert(
    broadcaster_id: int,
    channel_id: int,
    message_id: int,
    stream_id: int,
    stream_started_at: datetime,
) -> None:
    async with transaction() as connection:
        await connection.execute(
            UPSERT_LIVE_ALERT,
            broadcaster_id,
            channel_id,
            message_id,
            stream_id,
            stream_started_at,
        )


async def delete_live_alert(
    broadcaster_id: int, *, message_id: int | None = None
) -> None:
    """``message_id`` scopes the delete, so a newer alert's row survives a late cleanup."""
    async with transaction() as connection:
        if message_id is None:
            await connection.execute(DELETE_LIVE_ALERT, broadcaster_id)
        else:
            await connection.execute(
                DELETE_LIVE_ALERT_FOR_MESSAGE, broadcaster_id, message_id
            )


# twitch_autoshoutout

IS_AUTOSHOUTOUT = "SELECT 1 FROM twitch_autoshoutout WHERE twitch_user_id = $1"

ADD_AUTOSHOUTOUT = """
INSERT INTO twitch_autoshoutout (twitch_user_id, login) VALUES ($1, $2)
ON CONFLICT (twitch_user_id) DO NOTHING
RETURNING twitch_user_id
"""

REMOVE_AUTOSHOUTOUT = """
DELETE FROM twitch_autoshoutout WHERE twitch_user_id = $1 RETURNING twitch_user_id
"""


async def is_autoshoutout(twitch_user_id: int) -> bool:
    """Whether this Twitch user is on the autoshoutout list."""
    async with transaction() as connection:
        return await connection.fetchval(IS_AUTOSHOUTOUT, twitch_user_id) is not None


async def add_autoshoutout(twitch_user_id: int, login: str) -> bool:
    """Put a user on the list; False when they were already on it.

    The answer comes from the insert rather than a read before it, so two mods
    running `!aso` on the same channel at once cannot both be told they added
    it — only the insert that took the row reports True.
    """
    async with transaction() as connection:
        # RETURNING yields nothing when the conflict skipped the insert, which
        # is the answer itself. Postgres says whether the row was taken; a read
        # before the write would only say whether it was taken a moment ago.
        taken = await connection.fetchval(ADD_AUTOSHOUTOUT, twitch_user_id, login)
    return taken is not None


async def remove_autoshoutout(twitch_user_id: int) -> bool:
    """Take a user off the list; False when they were not on it."""
    async with transaction() as connection:
        removed = await connection.fetchval(REMOVE_AUTOSHOUTOUT, twitch_user_id)
    return removed is not None


# oauth_token

LIST_OAUTH_TOKENS = """
SELECT key, access_token, refresh_token, expires_at, scopes, created_at, updated_at
FROM oauth_token
"""

# A statement, not a credential: S105 matches on the name.
UPSERT_OAUTH_TOKEN = """
INSERT INTO oauth_token (key, access_token, refresh_token, expires_at, scopes)
VALUES ($1, $2, $3, $4, $5)
ON CONFLICT (key) DO UPDATE SET
    access_token = EXCLUDED.access_token,
    refresh_token = EXCLUDED.refresh_token,
    expires_at = EXCLUDED.expires_at,
    scopes = EXCLUDED.scopes,
    updated_at = now()
"""  # noqa: S105  # nosec B105


async def list_oauth_tokens() -> list[rows.OAuthToken]:
    async with transaction() as connection:
        records = await connection.fetch(LIST_OAUTH_TOKENS)
    return [
        rows.build(
            rows.OAuthToken,
            record,
            key=TokenType(record["key"]),
            scopes=tuple(record["scopes"]),
        )
        for record in records
    ]


async def upsert_oauth_token(
    key: TokenType,
    access_token: str,
    refresh_token: str | None,
    expires_at: datetime | None,
    scopes: list[str],
) -> None:
    async with transaction() as connection:
        await connection.execute(
            UPSERT_OAUTH_TOKEN,
            key.value,
            access_token,
            refresh_token,
            expires_at,
            scopes,
        )
