"""Values `config.settings` needs before any module can be imported.

`config` validates the environment at import, so this runs before a test module
does. Assigned rather than defaulted: a developer's real `.env` must not reach a
test, and the callback assertions read APP_URL back.
"""

import os

from tests import credentials

os.environ |= {
    "DISCORD_TOKEN": credentials.DISCORD_TOKEN,
    "TWITCH_CLIENT_ID": credentials.CLIENT_ID,
    "TWITCH_CLIENT_SECRET": credentials.CLIENT_SECRET,
    "TWITCH_WEBHOOK_SECRET": credentials.WEBHOOK_SECRET,
    "DATABASE_URL": "postgresql://test:test@localhost:5432/test",
    "APP_URL": "https://bot.example",
    "USE_TEST_BOT": "",
}

import pytest


@pytest.fixture(scope="session")
def anyio_backend() -> str:
    """anyio's default runs every async test on every backend it knows, and trio
    is not installed; the bot runs on asyncio alone."""
    return "asyncio"
