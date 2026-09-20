"""Values `config.settings` needs before any module can be imported.

`config` validates the environment at import, so this runs before a test module
does. Assigned rather than defaulted: a developer's real `.env` must not reach a
test, and the callback assertions read APP_URL back.
"""

import inspect
import os
from collections.abc import AsyncGenerator

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


@pytest.fixture(scope="session")
async def _one_event_loop(anyio_backend: str) -> AsyncGenerator[None]:
    """Held open, so anyio keeps one runner, and so one event loop, for the session.

    A loop per test costs a socket pair each on Windows, and roughly one full run in
    twelve one of those creations blocks for good. Tests that leave a task behind
    are cancelled by the next test's teardown rather than with their own loop.
    """
    yield


def pytest_collection_modifyitems(items: list[pytest.Item]) -> None:
    """Only async tests take the fixture: a sync test cannot be handed an async one."""
    for item in items:
        if isinstance(item, pytest.Function) and inspect.iscoroutinefunction(item.obj):
            item.fixturenames.append("_one_event_loop")
