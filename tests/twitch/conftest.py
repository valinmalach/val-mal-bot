"""Fixtures for the Twitch layer, split across files so each is short enough to be
reviewed whole."""

from collections.abc import Callable
from types import SimpleNamespace

import httpx
import pendulum
import pytest

import services.twitch.token_manager as tm_module
from services.config import config
from services.twitch import helix
from services.twitch.token_manager import TwitchTokenManager
from tests.twitch.support import (
    FakeTokens,
    Scope,
    Script,
    TokenDb,
)


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
    """Point helix at a scripted transport: `helix_http(reply(...), reply(...))`."""

    def install(*outcomes: httpx.Response | Exception) -> Script:
        script = Script(*outcomes)
        client = script.client()
        monkeypatch.setattr(helix, "client", lambda: client)
        return script

    return install


NOW = pendulum.datetime(2026, 6, 15, 12)


@pytest.fixture
def now(monkeypatch: pytest.MonkeyPatch) -> pendulum.DateTime:
    """The token manager stamps and compares expiries against pendulum.now."""
    monkeypatch.setattr(pendulum, "now", lambda tz=None: NOW)
    return NOW


@pytest.fixture
def manager(monkeypatch: pytest.MonkeyPatch) -> TwitchTokenManager:
    """A fresh manager, not the process-wide one; the original is restored after."""
    monkeypatch.setattr(TwitchTokenManager, "_instance", None)
    return TwitchTokenManager()


@pytest.fixture
def token_db(monkeypatch: pytest.MonkeyPatch) -> TokenDb:
    db = TokenDb()
    monkeypatch.setattr(tm_module, "session_scope", lambda: Scope(db))
    return db


@pytest.fixture
def oauth_http(monkeypatch: pytest.MonkeyPatch) -> Callable[..., Script]:
    """Point the token manager's own client at a scripted transport."""

    def install(*outcomes: httpx.Response | Exception) -> Script:
        script = Script(*outcomes)
        client = script.client()
        monkeypatch.setattr(tm_module, "client", lambda: client)
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
    monkeypatch.setattr(config, "_settings", {"twitch_app_scopes": configured})
    return configured
