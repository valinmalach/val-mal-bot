from typing import Any

import httpx
import pytest

from valmal.core import http_client

pytestmark = pytest.mark.anyio


@pytest.fixture(autouse=True)
def _no_shared_client(monkeypatch: pytest.MonkeyPatch) -> None:
    """The client is a module global; each test starts, and leaves, without one."""
    monkeypatch.setattr(http_client, "_client", None)


async def test_the_client_is_created_lazily_and_shared() -> None:
    assert http_client._client is None

    first = http_client.client()

    assert isinstance(first, httpx.AsyncClient)
    assert http_client.client() is first
    await http_client.aclose()


async def test_the_client_is_configured_for_pooling_and_timeouts() -> None:
    client = http_client.client()

    assert client.follow_redirects is True
    assert client.timeout == httpx.Timeout(
        connect=10.0, read=30.0, write=10.0, pool=10.0
    )
    await http_client.aclose()


async def test_connections_are_pooled_and_kept_alive_across_calls() -> None:
    client = http_client.client()
    pool: Any = client._transport._pool  # pyright: ignore[reportAttributeAccessIssue]

    assert pool._max_connections == 50
    assert pool._max_keepalive_connections == 20
    assert pool._keepalive_expiry == 30.0
    await http_client.aclose()


async def test_http2_is_offered_because_twitch_and_discord_speak_it() -> None:
    client = http_client.client()
    pool: Any = client._transport._pool  # pyright: ignore[reportAttributeAccessIssue]

    assert pool._http2 is True
    await http_client.aclose()


async def test_closing_drops_the_client_so_the_next_call_gets_a_fresh_one() -> None:
    first = http_client.client()

    await http_client.aclose()

    assert http_client._client is None
    assert first.is_closed
    second = http_client.client()
    assert second is not first
    assert not second.is_closed
    await http_client.aclose()


async def test_closing_with_no_client_is_a_no_op() -> None:
    await http_client.aclose()
    await http_client.aclose()

    assert http_client._client is None
