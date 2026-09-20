import asyncio

import httpx
import pendulum
import pytest

import services.twitch.token_manager as tm_module
from background import fire_and_forget
from constants import TokenType
from services.twitch.token_manager import TwitchTokenManager
from tests.twitch.support import (
    APP_OK,
    NOW,
    USER_OK,
    Http,
    TokenDb,
    reply,
    stale_refresh_tokens,
)

pytestmark = pytest.mark.anyio


@pytest.fixture(autouse=True)
def _env(scopes: list[str], token_db: TokenDb, now: pendulum.DateTime) -> None:
    """Every refresh reads the configured scopes, stores to the database and
    stamps an expiry."""


class TestOneRefreshAtATime:
    """Twitch invalidates a refresh token the moment it is used, so two callers
    racing on the same one would leave the loser holding a dead token."""

    @pytest.fixture(autouse=True)
    def _have_refresh_tokens(self, manager: TwitchTokenManager) -> None:
        stale_refresh_tokens(manager)

    def gated(
        self, monkeypatch: pytest.MonkeyPatch, *replies: httpx.Response
    ) -> tuple[asyncio.Event, list[httpx.Request]]:
        """A transport that holds every request until the gate opens."""
        gate = asyncio.Event()
        calls: list[httpx.Request] = []

        async def handler(request: httpx.Request) -> httpx.Response:
            calls.append(request)
            await gate.wait()
            return replies[min(len(calls) - 1, len(replies) - 1)]

        client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
        monkeypatch.setattr(tm_module, "client", lambda: client)
        return gate, calls

    async def test_a_caller_that_waited_reuses_the_result_it_did_not_have_to_fetch(
        self, manager: TwitchTokenManager, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        gate, calls = self.gated(monkeypatch, reply(200, USER_OK))

        first = fire_and_forget(manager.refresh(TokenType.User), name="first")
        await asyncio.sleep(0)
        second = fire_and_forget(manager.refresh(TokenType.User), name="second")
        await asyncio.sleep(0)
        gate.set()

        assert await asyncio.gather(first, second) == [True, True]
        assert len(calls) == 1
        assert manager._refresh[TokenType.User] == "new-refresh"

    async def test_a_waiter_tries_for_itself_when_the_first_refresh_failed(
        self, manager: TwitchTokenManager, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        gate, calls = self.gated(
            monkeypatch, reply(400, text="no"), reply(200, USER_OK)
        )

        first = fire_and_forget(manager.refresh(TokenType.User), name="first")
        await asyncio.sleep(0)
        second = fire_and_forget(manager.refresh(TokenType.User), name="second")
        await asyncio.sleep(0)
        gate.set()

        assert await asyncio.gather(first, second) == [False, True]
        assert len(calls) == 2

    async def test_different_identities_do_not_wait_for_each_other(
        self, manager: TwitchTokenManager, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        gate, calls = self.gated(monkeypatch, reply(200, USER_OK))

        user = fire_and_forget(manager.refresh(TokenType.User), name="user")
        broadcaster = fire_and_forget(manager.refresh(TokenType.Broadcaster), name="bc")
        for _ in range(5):
            await asyncio.sleep(0)

        assert len(calls) == 2, "both refreshes reached Twitch before either finished"
        gate.set()
        await asyncio.gather(user, broadcaster)

    async def test_a_token_that_looks_current_but_was_revoked_still_refreshes(
        self, manager: TwitchTokenManager, oauth_http: Http
    ) -> None:
        """Testing the expiry instead made a 401 unrecoverable: a revoked token still
        looks current, so the refresh never ran. Only a token that changed while
        this caller waited proves someone else refreshed."""
        manager._expires_at[TokenType.User] = NOW.add(hours=5)
        script = oauth_http(reply(200, USER_OK))

        assert await manager.refresh(TokenType.User) is True

        assert len(script.requests) == 1

    async def test_an_app_token_being_fetched_for_the_first_time_is_shared_too(
        self, manager: TwitchTokenManager, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        gate, calls = self.gated(monkeypatch, reply(200, APP_OK))

        first = fire_and_forget(manager.refresh(TokenType.App), name="first")
        await asyncio.sleep(0)
        second = fire_and_forget(manager.refresh(TokenType.App), name="second")
        await asyncio.sleep(0)
        gate.set()

        assert await asyncio.gather(first, second) == [True, True]
        assert len(calls) == 1
