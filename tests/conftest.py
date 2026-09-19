"""Values `config.settings` needs before any module can be imported.

`config` validates the environment at import, so this runs before a test module
does. Assigned rather than defaulted: a developer's real `.env` must not reach a
test, and the callback assertions read APP_URL back.
"""

import os

os.environ |= {
    "DISCORD_TOKEN": "test",
    "TWITCH_CLIENT_ID": "test",
    "TWITCH_CLIENT_SECRET": "test",
    "TWITCH_WEBHOOK_SECRET": "test",
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
