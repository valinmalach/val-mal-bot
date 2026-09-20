from collections.abc import Callable

import httpx
import pytest

from constants import TokenType
from tests.twitch.support import Script, reply
from valmal.twitch.client import helix
from valmal.twitch.client.helix import HelixError, fetch, request
from valmal.twitch.models.api.user import UserResponse

pytestmark = pytest.mark.anyio

HttpFactory = Callable[..., Script]


class TestRetry:
    async def test_a_get_retries_a_5xx_with_doubling_backoff(
        self, helix_http: HttpFactory, sleeps: list[float]
    ) -> None:
        script = helix_http(reply(500), reply(502), reply(200, {"ok": 1}))

        response = await request("GET", "/x")

        assert response.json() == {"ok": 1}
        assert len(script.requests) == 3
        assert sleeps == [1.0, 2.0]

    async def test_a_get_gives_up_after_three_attempts_without_a_trailing_sleep(
        self, helix_http: HttpFactory, sleeps: list[float]
    ) -> None:
        script = helix_http(reply(503, text="down"))

        with pytest.raises(HelixError) as caught:
            await request("GET", "/x")

        assert len(script.requests) == 3
        assert sleeps == [1.0, 2.0]
        assert caught.value.status == 503

    async def test_a_post_is_sent_once_because_a_repeat_can_post_twice(
        self, helix_http: HttpFactory, sleeps: list[float]
    ) -> None:
        script = helix_http(reply(500))

        with pytest.raises(HelixError):
            await request("POST", "/chat/messages", json={})

        assert len(script.requests) == 1
        assert not sleeps

    async def test_a_post_marked_repeatable_is_retried(
        self, helix_http: HttpFactory
    ) -> None:
        script = helix_http(reply(503), reply(200, {}))

        await request("POST", "/eventsub/subscriptions", json={}, repeatable=True)

        assert len(script.requests) == 2

    async def test_a_get_marked_not_repeatable_is_sent_once(
        self, helix_http: HttpFactory
    ) -> None:
        script = helix_http(reply(500))

        with pytest.raises(HelixError):
            await request("GET", "/x", repeatable=False)

        assert len(script.requests) == 1

    async def test_a_delete_is_repeatable_by_default(
        self, helix_http: HttpFactory
    ) -> None:
        script = helix_http(reply(502), reply(204))

        await request("DELETE", "/eventsub/subscriptions", params={"id": "1"})

        assert len(script.requests) == 2

    @pytest.mark.parametrize(
        "error",
        [
            httpx.ConnectTimeout("t"),
            httpx.ReadTimeout("t"),
            httpx.PoolTimeout("t"),
            httpx.ConnectError("t"),
            httpx.ReadError("t"),
            httpx.RemoteProtocolError("t"),
        ],
        ids=lambda e: type(e).__name__,
    )
    async def test_a_get_retries_a_transient_transport_error(
        self, error: Exception, helix_http: HttpFactory, sleeps: list[float]
    ) -> None:
        script = helix_http(error, reply(200, {}))

        await request("GET", "/x")

        assert len(script.requests) == 2
        assert sleeps == [1.0]

    async def test_a_transient_error_that_never_clears_is_a_helix_error_with_its_cause(
        self, helix_http: HttpFactory
    ) -> None:
        script = helix_http(httpx.ReadTimeout("slow"))

        with pytest.raises(HelixError, match="GET /x failed") as caught:
            await request("GET", "/x")

        assert len(script.requests) == 3
        assert isinstance(caught.value.__cause__, httpx.ReadTimeout)

    async def test_a_post_is_not_retried_on_a_transient_error(
        self, helix_http: HttpFactory
    ) -> None:
        script = helix_http(httpx.ReadTimeout("slow"))

        with pytest.raises(HelixError):
            await request("POST", "/chat/messages", json={})

        assert len(script.requests) == 1

    @pytest.mark.parametrize(
        "error",
        [
            httpx.UnsupportedProtocol("scheme"),
            httpx.LocalProtocolError("ours"),
            RuntimeError("x"),
        ],
        ids=lambda e: type(e).__name__,
    )
    async def test_an_error_that_would_fail_identically_is_not_retried(
        self, error: Exception, helix_http: HttpFactory
    ) -> None:
        script = helix_http(error)

        with pytest.raises(HelixError) as caught:
            await request("GET", "/x")

        assert len(script.requests) == 1
        assert caught.value.__cause__ is error

    async def test_a_5xx_after_a_refresh_is_still_retried(
        self, helix_http: HttpFactory
    ) -> None:
        script = helix_http(reply(401), reply(500), reply(200, {}))

        await request("GET", "/x")

        assert len(script.requests) == 3


class TestFetch:
    async def test_parses_the_reply_into_the_model(
        self, helix_http: HttpFactory
    ) -> None:
        helix_http(reply(200, {"data": []}))

        assert (await fetch(UserResponse, "GET", "/users")).data == []

    async def test_a_body_that_is_not_json_is_a_helix_error_with_the_status(
        self, helix_http: HttpFactory
    ) -> None:
        helix_http(reply(200, text="<html>not json</html>"))

        with pytest.raises(HelixError, match="would not parse") as caught:
            await fetch(UserResponse, "GET", "/users")

        assert caught.value.status == 200
        assert caught.value.response is not None

    async def test_a_body_of_the_wrong_shape_is_a_helix_error(
        self, helix_http: HttpFactory
    ) -> None:
        helix_http(reply(200, {"unexpected": True}))

        with pytest.raises(HelixError, match="would not parse"):
            await fetch(UserResponse, "GET", "/users")

    async def test_an_empty_204_body_cannot_be_parsed_so_request_is_for_those(
        self, helix_http: HttpFactory
    ) -> None:
        helix_http(reply(204))

        with pytest.raises(HelixError, match="would not parse"):
            await fetch(UserResponse, "DELETE", "/x")

    async def test_a_failed_request_propagates_unchanged(
        self, helix_http: HttpFactory
    ) -> None:
        helix_http(reply(404, text="gone"))

        with pytest.raises(HelixError, match="returned 404"):
            await fetch(UserResponse, "GET", "/users")

    async def test_the_arguments_reach_the_request(
        self, helix_http: HttpFactory
    ) -> None:
        script = helix_http(reply(200, {"data": []}))

        await fetch(
            UserResponse,
            "POST",
            "/x",
            params={"a": "1"},
            json={"b": 2},
            token_type=TokenType.User,
            repeatable=False,
        )

        (sent,) = script.requests
        assert sent.url.query == b"a=1"
        assert sent.content == b'{"b":2}'
        assert sent.headers["Authorization"] == "Bearer user-token"


class TestHelixError:
    def test_status_and_response_default_to_none(self) -> None:
        error = HelixError("plain")

        assert str(error) == "plain"
        assert error.status is None
        assert error.response is None

    def test_a_status_and_response_can_be_carried(self) -> None:
        response = httpx.Response(429)

        error = HelixError("busy", status=429, response=response)

        assert error.status == 429
        assert error.response is response

    def test_it_is_an_exception_callers_can_catch_by_name(self) -> None:
        assert issubclass(HelixError, Exception)
        assert helix.HelixError is HelixError
