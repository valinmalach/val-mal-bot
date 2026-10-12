"""Fixtures for the Twitch layer."""

from collections.abc import Callable
from datetime import UTC, datetime
from types import SimpleNamespace

import pytest

import valmal.twitch.oauth.token_manager as tm_module
from tests.twitch.support import (
    FakeTokens,
    Script,
    TokenDb,
)
from valmal.core import clock, http_client
from valmal.core.config import config
from valmal.core.http_client import Reply
from valmal.twitch.client import helix
from valmal.twitch.oauth.token_manager import TwitchTokenManager


@pytest.fixture
def tokens(monkeypatch: pytest.MonkeyPatch) -> FakeTokens:
    fake = FakeTokens()
    monkeypatch.setattr(helix, "token_manager", fake)
    return fake


@pytest.fixture
def sleeps(monkeypatch: pytest.MonkeyPatch) -> list[float]:
    """Backoff without waiting. helix.asyncio is replaced by name so the event
    loop's own sleep is untouched."""
    slept: list[float] = []

    async def sleep(seconds: float) -> None:
        slept.append(seconds)

    monkeypatch.setattr(helix, "asyncio", SimpleNamespace(sleep=sleep))
    return slept


@pytest.fixture
def helix_http(
    monkeypatch: pytest.MonkeyPatch, tokens: FakeTokens, sleeps: list[float]
) -> Callable[..., Script]:
    """Point helix at a scripted Twitch: `helix_http(reply(...), reply(...))`."""

    def install(*outcomes: Reply | Exception) -> Script:
        script = Script(*outcomes)
        monkeypatch.setattr(http_client, "request", script)
        return script

    return install


NOW = datetime(2026, 6, 15, 12, tzinfo=UTC)


@pytest.fixture
def now(monkeypatch: pytest.MonkeyPatch) -> datetime:
    """The token manager stamps and compares expiries against clock.now."""

    def now() -> datetime:
        return NOW

    monkeypatch.setattr(clock, "now", now)
    return NOW


@pytest.fixture
def manager(monkeypatch: pytest.MonkeyPatch) -> TwitchTokenManager:
    """A fresh manager, not the process-wide one; the original is restored after."""
    monkeypatch.setattr(TwitchTokenManager, "_instance", None)
    return TwitchTokenManager()


@pytest.fixture
def token_db(monkeypatch: pytest.MonkeyPatch) -> TokenDb:
    db = TokenDb()
    monkeypatch.setattr(tm_module.repository, "list_oauth_tokens", db.list_oauth_tokens)
    monkeypatch.setattr(
        tm_module.repository, "upsert_oauth_token", db.upsert_oauth_token
    )
    return db


@pytest.fixture
def oauth_http(monkeypatch: pytest.MonkeyPatch) -> Callable[..., Script]:
    """Point the token manager's requests at a scripted Twitch."""

    def install(*outcomes: Reply | Exception) -> Script:
        script = Script(*outcomes)
        monkeypatch.setattr(http_client, "request", script)
        return script

    return install


@pytest.fixture
def token_notices(monkeypatch: pytest.MonkeyPatch) -> list[tuple[str, str | None]]:
    seen: list[tuple[str, str | None]] = []

    async def notify(text: str, *, key: str | None = None) -> bool:
        seen.append((text, key))
        return True

    monkeypatch.setattr(tm_module, "notify", notify)
    return seen


@pytest.fixture
def scopes(monkeypatch: pytest.MonkeyPatch) -> list[str]:
    configured = ["chat:read", "moderator:manage:shoutouts"]
    monkeypatch.setattr(config, "_settings", {"twitch_app_scopes": tuple(configured)})
    return configured
