"""Fixtures for the ConfigCache tests, split across files so each is short
enough to be reviewed whole."""

from collections.abc import AsyncGenerator, Callable
from contextlib import asynccontextmanager
from typing import Any

import pytest

import valmal.core.config as service_config
from tests.config_cache.support import FakeSession
from valmal.core.config import ConfigCache


@pytest.fixture
def notices(monkeypatch: pytest.MonkeyPatch) -> list[tuple[str, str | None]]:
    seen: list[tuple[str, str | None]] = []

    def notify_soon(text: str, *, key: str | None = None) -> None:
        seen.append((text, key))

    monkeypatch.setattr(service_config, "notify_soon", notify_soon)
    return seen


@pytest.fixture
def load(
    monkeypatch: pytest.MonkeyPatch,
) -> Callable[..., Any]:
    """Build a ConfigCache by running the real load() against rows given here."""

    async def build(*rows: Any, cache: ConfigCache | None = None) -> ConfigCache:
        by_model: dict[type, list[Any]] = {}
        for row in rows:
            by_model.setdefault(type(row), []).append(row)
        session = FakeSession(by_model)

        @asynccontextmanager
        async def scope() -> AsyncGenerator[FakeSession]:
            yield session

        monkeypatch.setattr(service_config, "session_scope", scope)
        cache = cache or ConfigCache()
        await cache.load()
        cache.session = session  # pyright: ignore[reportAttributeAccessIssue]
        return cache

    return build
