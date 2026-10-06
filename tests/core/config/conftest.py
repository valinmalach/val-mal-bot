"""Fixtures for the ConfigCache tests."""

from collections.abc import Callable
from typing import Any

import pytest

import valmal.core.config as service_config
import valmal.core.safe_format as safe_format_module
from tests.core.config.support import Notices, configuration
from valmal.core.config import ConfigCache
from valmal.db.configuration import Configuration


@pytest.fixture
def notices(monkeypatch: pytest.MonkeyPatch) -> Notices:
    seen: Notices = []

    def notify_soon(text: str, *, key: str | None = None) -> None:
        seen.append((text, key))

    monkeypatch.setattr(service_config, "notify_soon", notify_soon)
    monkeypatch.setattr(safe_format_module, "notify_soon", notify_soon)
    return seen


@pytest.fixture
def load(
    monkeypatch: pytest.MonkeyPatch,
) -> Callable[..., Any]:
    """Build a ConfigCache by running the real load() against rows given here."""

    async def build(*rows: object, cache: ConfigCache | None = None) -> ConfigCache:
        loaded = configuration(*rows)
        calls: list[None] = []

        async def load_configuration() -> Configuration:
            calls.append(None)
            return loaded

        monkeypatch.setattr(service_config, "load_configuration", load_configuration)
        cache = cache or ConfigCache()
        await cache.load()
        cache.loads = calls  # pyright: ignore[reportAttributeAccessIssue]
        return cache

    return build
