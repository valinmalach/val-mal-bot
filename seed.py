"""Insert the bot's configuration, skipping anything already present.

Run on every container start, after `alembic upgrade head`. Each row is an
INSERT ... ON CONFLICT DO NOTHING rather than an upsert, so a channel or role ID
edited in the database is never overwritten by the next deploy. That also means
keys added to seed_data later still land, which a bulk "only seed when empty"
check would skip forever.
"""

import asyncio
import json
import logging
from typing import Any, cast

from sqlalchemy import CursorResult
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession
from sqlmodel import SQLModel

import backfill
import seed_data as data
from config import settings
from db import (
    AppSetting,
    DiscordAutoResponse,
    DiscordChannel,
    DiscordEmbed,
    DiscordEmbedField,
    DiscordRole,
    MessageTemplate,
    TwitchCommand,
    TwitchCommandComponent,
    TwitchCommandResponse,
    dispose_engine,
    session_scope,
)

logging.basicConfig(level=logging.INFO, format="%(message)s")
logger = logging.getLogger("seed")


async def _insert(
    session: AsyncSession, model: type[SQLModel], rows: list[dict[str, Any]]
) -> int:
    if not rows:
        return 0
    statement = insert(model).values(rows).on_conflict_do_nothing()  # pyright: ignore[reportArgumentType]
    result = cast(CursorResult[Any], await session.execute(statement))
    return result.rowcount


def _settings_rows() -> list[dict[str, Any]]:
    """The static settings plus the two Twitch IDs still coming from the environment."""
    return [
        *data.SETTINGS,
        {
            "key": "twitch_app_scopes",
            "value": json.dumps(data.TWITCH_APP_SCOPES),
            "value_type": "json",
            "description": "Scopes requested for the app access token",
        },
        {
            "key": "twitch_bot_user_id",
            "value": settings.twitch_bot_user_id,
            "value_type": "string",
            "description": "Twitch account the bot posts as",
        },
        {
            "key": "twitch_broadcaster_id",
            "value": settings.twitch_broadcaster_id,
            "value_type": "string",
            "description": "Twitch account the bot watches",
        },
    ]


async def seed() -> dict[str, int]:
    channels = [
        {"key": key, "channel_id": channel_id}
        for key, channel_id in data.CHANNELS.items()
    ]

    # Ordered by foreign key: embeds reference channels, roles and fields
    # reference embeds, responses and components reference commands.
    plan: list[tuple[str, type[SQLModel], list[dict[str, Any]]]] = [
        ("discord_channel", DiscordChannel, channels),
        ("discord_embed", DiscordEmbed, data.EMBEDS),
        ("discord_role", DiscordRole, data.ROLES),
        ("discord_embed_field", DiscordEmbedField, data.EMBED_FIELDS),
        ("twitch_command", TwitchCommand, data.COMMANDS),
        ("twitch_command_response", TwitchCommandResponse, data.COMMAND_RESPONSES),
        ("twitch_command_component", TwitchCommandComponent, data.COMMAND_COMPONENTS),
        ("app_setting", AppSetting, _settings_rows()),
        ("message_template", MessageTemplate, data.MESSAGE_TEMPLATES),
        ("discord_auto_response", DiscordAutoResponse, data.AUTO_RESPONSES),
    ]

    inserted: dict[str, int] = {}
    async with session_scope() as session:
        for name, model, rows in plan:
            count = await _insert(session, model, rows)
            inserted[name] = count
            logger.info(
                "%-26s %3d inserted, %3d already present",
                name,
                count,
                len(rows) - count,
            )
        # Only present where someone has a copy of the files; the deployed image
        # never does, since data/ is gitignored.
        if backfill.available():
            inserted |= await backfill.backfill(session)
        else:
            logger.info("no parquet files under data/, nothing to backfill")
    return inserted


async def main() -> None:
    try:
        inserted = await seed()
    finally:
        await dispose_engine()
    total = sum(inserted.values())
    logger.info("Seed complete: %d row(s) inserted", total)


if __name__ == "__main__":
    asyncio.run(main())
