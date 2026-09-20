from collections.abc import Callable

import httpx
import pytest

from tests.credentials import CLIENT_ID
from tests.twitch.support import FakeTokens, Script, reply
from valmal.db.models.enums import TokenType
from valmal.twitch.client.helix import HelixError, request

pytestmark = pytest.mark.anyio

HttpFactory = Callable[..., Script]


class TestRequest:
    async def test_a_successful_get_returns_the_response(
        self, helix_http: HttpFactory
    ) -> None:
        script = helix_http(reply(200, {"data": []}))

        response = await request("GET", "/users", params={"login": "bob"})

        assert response.json() == {"data": []}
        (sent,) = script.requests
        assert sent.method == "GET"
        assert str(sent.url) == "https://api.twitch.tv/helix/users?login=bob"

    async def test_every_call_carries_the_client_id_and_a_bearer_token(
        self, helix_http: HttpFactory
    ) -> None:
        script = helix_http(reply(200, {}))

        await request("GET", "/users")

        headers = script.requests[0].headers
        assert headers["Client-ID"] == CLIENT_ID
        assert headers["Authorization"] == "Bearer app-token"

    @pytest.mark.parametrize(
        ("token_type", "expected"),
        [
            (TokenType.App, "Bearer app-token"),
            (TokenType.User, "Bearer user-token"),
            (TokenType.Broadcaster, "Bearer broadcaster-token"),
        ],
    )
    async def test_the_token_type_chooses_the_identity(
        self, token_type: TokenType, expected: str, helix_http: HttpFactory
    ) -> None:
        script = helix_http(reply(200, {}))

        await request("GET", "/x", token_type=token_type)

        assert script.requests[0].headers["Authorization"] == expected

    async def test_a_post_sends_its_json_body(self, helix_http: HttpFactory) -> None:
        script = helix_http(reply(200, {}))

        await request("POST", "/chat/messages", json={"message": "hi"})

        assert script.requests[0].content == b'{"message":"hi"}'
        assert script.requests[0].headers["Content-Type"] == "application/json"

    @pytest.mark.parametrize("status", [200, 201, 202, 204, 299])
    async def test_any_2xx_is_success(
        self, status: int, helix_http: HttpFactory
    ) -> None:
        helix_http(reply(status))

        assert (await request("GET", "/x")).status_code == status

    @pytest.mark.parametrize("status", [400, 403, 404, 409, 422, 429])
    async def test_a_4xx_fails_at_once_and_is_never_retried(
        self, status: int, helix_http: HttpFactory
    ) -> None:
        script = helix_http(reply(status, text="the reason"))

        with pytest.raises(HelixError) as caught:
            await request("GET", "/x")

        assert len(script.requests) == 1
        assert caught.value.status == status
        assert caught.value.response is not None
        assert f"GET /x returned {status}: the reason" in str(caught.value)

    async def test_a_429s_headers_stay_reachable_for_a_caller_that_cares(
        self, helix_http: HttpFactory
    ) -> None:
        helix_http(httpx.Response(429, headers={"Ratelimit-Reset": "1700000000"}))

        with pytest.raises(HelixError) as caught:
            await request("GET", "/x")

        assert caught.value.response.headers["Ratelimit-Reset"] == "1700000000"  # pyright: ignore[reportOptionalMemberAccess]

    async def test_the_error_names_the_method_and_path(
        self, helix_http: HttpFactory
    ) -> None:
        helix_http(reply(404))

        with pytest.raises(HelixError, match=r"DELETE /eventsub/subscriptions"):
            await request("DELETE", "/eventsub/subscriptions")


class TestTokens:
    async def test_a_missing_token_is_fetched_before_the_call(
        self, helix_http: HttpFactory, tokens: FakeTokens
    ) -> None:
        tokens.tokens[TokenType.App] = ""
        script = helix_http(reply(200, {}))

        await request("GET", "/x")

        assert tokens.refreshed == [TokenType.App]
        assert script.requests[0].headers["Authorization"] == "Bearer fresh-1"

    async def test_a_token_due_to_lapse_is_refreshed_before_the_call(
        self, helix_http: HttpFactory, tokens: FakeTokens
    ) -> None:
        tokens.stale.add(TokenType.App)
        script = helix_http(reply(200, {}))

        await request("GET", "/x")

        assert script.requests[0].headers["Authorization"] == "Bearer fresh-1"

    async def test_a_current_token_is_not_refreshed(
        self, helix_http: HttpFactory, tokens: FakeTokens
    ) -> None:
        helix_http(reply(200, {}))

        await request("GET", "/x")

        assert tokens.refreshed == []

    async def test_no_token_and_no_refresh_fails_before_any_request(
        self, helix_http: HttpFactory, tokens: FakeTokens
    ) -> None:
        tokens.tokens[TokenType.User] = ""
        tokens.refresh_result = False
        script = helix_http(reply(200, {}))

        with pytest.raises(HelixError, match="No user token available"):
            await request("GET", "/x", token_type=TokenType.User)

        assert script.requests == []

    async def test_a_refresh_that_raises_becomes_a_helix_error_with_its_cause(
        self, helix_http: HttpFactory, tokens: FakeTokens
    ) -> None:
        """Callers catch only HelixError, so a raw connection reset while
        refreshing would surface as a traceback from whatever was calling."""
        tokens.stale.add(TokenType.App)
        tokens.refresh_error = ConnectionResetError("reset")
        helix_http(reply(200, {}))

        with pytest.raises(
            HelixError, match="refreshing the app token failed"
        ) as caught:
            await request("GET", "/x")

        assert isinstance(caught.value.__cause__, ConnectionResetError)

    async def test_a_401_refreshes_once_and_resends_with_the_new_token(
        self, helix_http: HttpFactory, tokens: FakeTokens
    ) -> None:
        script = helix_http(reply(401), reply(200, {"ok": True}))

        response = await request("GET", "/x")

        assert response.json() == {"ok": True}
        assert tokens.refreshed == [TokenType.App]
        assert [r.headers["Authorization"] for r in script.requests] == [
            "Bearer app-token",
            "Bearer fresh-1",
        ]

    async def test_a_401_on_a_post_is_resent_because_a_rejected_request_took_no_effect(
        self, helix_http: HttpFactory
    ) -> None:
        script = helix_http(reply(401), reply(200, {}))

        await request("POST", "/chat/messages", json={"message": "hi"})

        assert len(script.requests) == 2
        assert script.requests[0].content == script.requests[1].content

    async def test_a_401_whose_refresh_fails_is_a_401_helix_error(
        self, helix_http: HttpFactory, tokens: FakeTokens
    ) -> None:
        tokens.refresh_result = False
        script = helix_http(reply(401, text="bad token"))

        with pytest.raises(
            HelixError, match="unauthorized and the refresh failed"
        ) as caught:
            await request("GET", "/x")

        assert caught.value.status == 401
        assert caught.value.response is not None
        assert len(script.requests) == 1

    async def test_a_401_whose_refresh_raises_becomes_a_helix_error(
        self, helix_http: HttpFactory, tokens: FakeTokens
    ) -> None:
        tokens.refresh_error = RuntimeError("database down")
        helix_http(reply(401))

        with pytest.raises(HelixError, match="refreshing the app token failed"):
            await request("GET", "/x")

    async def test_a_second_401_is_final_not_a_loop(
        self, helix_http: HttpFactory, tokens: FakeTokens
    ) -> None:
        script = helix_http(reply(401))

        with pytest.raises(HelixError) as caught:
            await request("GET", "/x")

        assert caught.value.status == 401
        assert len(script.requests) == 2
        assert tokens.refreshed == [TokenType.App]

    async def test_a_transport_error_after_refreshing_is_a_helix_error(
        self, helix_http: HttpFactory
    ) -> None:
        helix_http(reply(401), httpx.ConnectError("down"))

        with pytest.raises(HelixError, match="failed after refreshing the token"):
            await request("GET", "/x")
