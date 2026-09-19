"""Fixtures for the Twitch layer, split across files so each is short enough to be
reviewed whole."""

from collections.abc import Callable
from types import SimpleNamespace

import httpx
import pytest

from services.twitch import helix
from tests.twitch.support import FakeTokens, Script


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
