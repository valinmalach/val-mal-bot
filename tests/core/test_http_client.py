import json
from collections.abc import AsyncGenerator
from typing import cast
from urllib.parse import parse_qs

import aiohttp
import pytest
from aiohttp import web
from aiohttp.test_utils import TestServer

from valmal.core import http_client

pytestmark = pytest.mark.anyio


@pytest.fixture(autouse=True)
async def _no_shared_session(monkeypatch: pytest.MonkeyPatch) -> AsyncGenerator[None]:
    """The session is a module global; each test starts, and leaves, without one."""
    monkeypatch.setattr(http_client, "_session", None)
    yield
    await http_client.aclose()


async def _echo(request: web.Request) -> web.Response:
    return web.json_response(
        {
            "method": request.method,
            "path": request.path,
            "query": [[k, v] for k, v in request.query.items()],
            "content_type": request.content_type,
            "body": await request.text(),
            "authorization": request.headers.get("Authorization"),
        },
        headers={"Ratelimit-Reset": "1700000000"},
    )


async def _busy(request: web.Request) -> web.Response:
    return web.Response(status=429, text="busy")


async def _latin1(request: web.Request) -> web.Response:
    return web.Response(status=500, body=b"caf\xe9 error")


@pytest.fixture
async def server() -> AsyncGenerator[TestServer]:
    """A real server on localhost. Each one costs a socket, so they stay in this file."""
    app = web.Application()
    app.router.add_route("*", "/echo", _echo)
    app.router.add_get("/busy", _busy)
    app.router.add_get("/latin1", _latin1)
    server = TestServer(app)
    await server.start_server()
    yield server
    await server.close()


def url(server: TestServer, path: str) -> str:
    return str(server.make_url(path))


class TestSession:
    async def test_it_is_created_lazily_and_shared(self) -> None:
        assert http_client._session is None

        first = http_client._client()

        assert http_client._client() is first

    async def test_it_is_configured_for_pooling_and_timeouts(self) -> None:
        session = http_client._client()
        connector = cast("aiohttp.TCPConnector", session.connector)

        assert session.timeout == aiohttp.ClientTimeout(
            connect=10.0, sock_connect=10.0, sock_read=30.0
        )
        assert connector.limit == 50
        assert connector._keepalive_timeout == 30.0  # pyright: ignore[reportPrivateUsage]

    async def test_closing_drops_it_so_the_next_call_gets_a_fresh_one(self) -> None:
        first = http_client._client()

        await http_client.aclose()

        assert http_client._session is None
        assert first.closed
        second = http_client._client()
        assert second is not first
        assert not second.closed

    async def test_closing_with_no_session_is_a_no_op(self) -> None:
        await http_client.aclose()
        await http_client.aclose()

        assert http_client._session is None


class TestRequest:
    async def test_a_reply_is_read_in_full(self, server: TestServer) -> None:
        reply = await http_client.request("GET", url(server, "/echo"))

        assert reply.status == 200
        assert reply.json()["method"] == "GET"
        assert reply.json()["path"] == "/echo"

    async def test_a_reply_outlives_the_session_that_fetched_it(
        self, server: TestServer
    ) -> None:
        reply = await http_client.request("GET", url(server, "/echo"))

        await http_client.aclose()

        assert reply.json()["method"] == "GET"

    async def test_a_header_is_found_in_any_case(self, server: TestServer) -> None:
        reply = await http_client.request("GET", url(server, "/echo"))

        assert reply.headers["ratelimit-reset"] == "1700000000"
        assert reply.headers["RATELIMIT-RESET"] == "1700000000"

    async def test_a_status_that_is_not_2xx_is_returned_not_raised(
        self, server: TestServer
    ) -> None:
        reply = await http_client.request("GET", url(server, "/busy"))

        assert (reply.status, reply.text) == (429, "busy")

    async def test_a_body_that_is_not_utf8_keeps_its_status(
        self, server: TestServer
    ) -> None:
        """An error page from a proxy need not be UTF-8, and raising on it would
        lose the status a 5xx retry or a 429 re-queue is decided by."""
        reply = await http_client.request("GET", url(server, "/latin1"))

        assert (reply.status, reply.text) == (500, f"caf{chr(0xFFFD)} error")

    async def test_params_go_in_the_query_and_a_list_repeats_its_key(
        self, server: TestServer
    ) -> None:
        reply = await http_client.request(
            "GET", url(server, "/echo"), params={"id": ["1", "2"], "n": 3}
        )

        assert reply.json()["query"] == [["id", "1"], ["id", "2"], ["n", "3"]]

    async def test_json_goes_as_a_json_body(self, server: TestServer) -> None:
        reply = await http_client.request(
            "POST", url(server, "/echo"), json={"message": "hi"}
        )

        sent = reply.json()
        assert sent["content_type"] == "application/json"
        assert json.loads(sent["body"]) == {"message": "hi"}

    async def test_data_goes_as_a_form_body(self, server: TestServer) -> None:
        reply = await http_client.request(
            "POST", url(server, "/echo"), data={"grant_type": "refresh_token"}
        )

        sent = reply.json()
        assert sent["content_type"] == "application/x-www-form-urlencoded"
        assert parse_qs(sent["body"]) == {"grant_type": ["refresh_token"]}
        assert sent["query"] == []

    async def test_headers_are_sent(self, server: TestServer) -> None:
        reply = await http_client.request(
            "GET", url(server, "/echo"), headers={"Authorization": "Bearer t"}
        )

        assert reply.json()["authorization"] == "Bearer t"

    async def test_nothing_listening_is_a_connection_error_helix_retries(
        self, server: TestServer
    ) -> None:
        gone = url(server, "/echo")
        await server.close()

        with pytest.raises(aiohttp.ClientConnectionError):
            await http_client.request("GET", gone)
