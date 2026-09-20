from collections.abc import Callable

import httpx
import pytest

from constants import TokenType
from tests.credentials import CLIENT_ID, CLIENT_SECRET
from tests.oauth_controller.support import (
    BOT_ID,
    BROADCASTER_CALLBACK,
    BROADCASTER_ID,
    TOKEN_URL,
    USER_CALLBACK,
    VALIDATE_URL,
    Said,
    Stored,
    form,
    new_state,
    token_body,
    validation_body,
)
from tests.twitch.support import Script, reply
from valmal.twitch.oauth import grants as oauth

pytestmark = pytest.mark.anyio

BACKSLASH = chr(92)

# (path, identity, the account's id, what the page calls it)
FLOWS = [
    pytest.param(USER_CALLBACK, TokenType.User, BOT_ID, "bot account", id="user"),
    pytest.param(
        BROADCASTER_CALLBACK,
        TokenType.Broadcaster,
        BROADCASTER_ID,
        "broadcaster account",
        id="broadcaster",
    ),
]

Twitch = Callable[..., Script]


def happy(user_id: str) -> tuple[httpx.Response, httpx.Response]:
    return reply(200, token_body()), reply(200, validation_body(user_id))


@pytest.mark.parametrize(("path", "identity", "user_id", "label"), FLOWS)
class TestTheCodeExchange:
    async def test_stores_the_token_and_says_who_authorized(
        self,
        path: str,
        identity: TokenType,
        user_id: str,
        label: str,
        http: httpx.AsyncClient,
        twitch: Twitch,
        stored: Stored,
    ) -> None:
        twitch(*happy(user_id))
        state = new_state(identity)

        response = await http.get(path, params={"state": state, "code": "the-code"})

        assert response.status_code == 200
        assert response.text == (
            f"Authorization successful for {label} somebody ({user_id}). "
            "You can close this tab."
        )
        kept = stored.user if identity is TokenType.User else stored.broadcaster
        other = stored.broadcaster if identity is TokenType.User else stored.user
        assert [k.access_token for k in kept] == ["access-1"] and other == []

    async def test_sends_the_code_with_the_apps_credentials_and_this_flows_redirect(
        self,
        path: str,
        identity: TokenType,
        user_id: str,
        label: str,
        http: httpx.AsyncClient,
        twitch: Twitch,
    ) -> None:
        script = twitch(*happy(user_id))

        await http.get(path, params={"state": new_state(identity), "code": "c0de"})

        exchange, validate = script.requests
        assert (exchange.method, str(exchange.url)) == ("POST", TOKEN_URL)
        assert form(exchange) == {
            "client_id": CLIENT_ID,
            "client_secret": CLIENT_SECRET,
            "code": "c0de",
            "grant_type": "authorization_code",
            "redirect_uri": f"https://bot.example{path}",
        }
        assert (validate.method, str(validate.url)) == ("GET", VALIDATE_URL)
        assert validate.headers["Authorization"] == "OAuth access-1"

    async def test_the_state_is_single_use(
        self,
        path: str,
        identity: TokenType,
        user_id: str,
        label: str,
        http: httpx.AsyncClient,
        twitch: Twitch,
        stored: Stored,
    ) -> None:
        twitch(*happy(user_id))
        state = new_state(identity)
        await http.get(path, params={"state": state, "code": "c"})

        replay = await http.get(path, params={"state": state, "code": "c"})

        assert replay.status_code == 400
        assert replay.json()["detail"] == "Invalid or expired OAuth state"
        assert len(stored.user) + len(stored.broadcaster) == 1

    async def test_a_state_nobody_issued_is_refused_before_anything_is_sent(
        self,
        path: str,
        identity: TokenType,
        user_id: str,
        label: str,
        http: httpx.AsyncClient,
        twitch: Twitch,
        said: Said,
    ) -> None:
        script = twitch(*happy(user_id))

        response = await http.get(path, params={"state": "forged", "code": "c"})

        assert response.status_code == 400
        assert script.requests == []
        assert said.texts == [
            f"400: Bad request on {path}. Invalid or expired OAuth state."
        ]

    async def test_a_state_for_the_other_flow_is_refused_and_left_usable(
        self,
        path: str,
        identity: TokenType,
        user_id: str,
        label: str,
        http: httpx.AsyncClient,
        twitch: Twitch,
    ) -> None:
        """Otherwise anyone holding one callback could burn the other's pending grant."""
        twitch(*happy(user_id))
        other = TokenType.Broadcaster if identity is TokenType.User else TokenType.User
        state = new_state(other)

        response = await http.get(path, params={"state": state, "code": "c"})

        assert response.status_code == 400
        assert state in oauth._pending_authorizations

    async def test_a_missing_state_is_a_validation_error(
        self,
        path: str,
        identity: TokenType,
        user_id: str,
        label: str,
        http: httpx.AsyncClient,
    ) -> None:
        assert (await http.get(path, params={"code": "c"})).status_code == 422

    @pytest.mark.parametrize("code", [None, ""])
    async def test_no_code_is_refused_without_calling_twitch(
        self,
        code: str | None,
        path: str,
        identity: TokenType,
        user_id: str,
        label: str,
        http: httpx.AsyncClient,
        twitch: Twitch,
    ) -> None:
        script = twitch(*happy(user_id))
        params = {"state": new_state(identity)} | (
            {} if code is None else {"code": code}
        )

        response = await http.get(path, params=params)

        assert response.status_code == 400
        assert response.json()["detail"] == "Missing Twitch authorization code"
        assert script.requests == []


@pytest.mark.parametrize(("path", "identity", "user_id", "label"), FLOWS)
class TestADeniedAuthorization:
    async def test_is_a_400_that_does_not_repeat_the_reason_to_the_caller(
        self,
        path: str,
        identity: TokenType,
        user_id: str,
        label: str,
        http: httpx.AsyncClient,
        twitch: Twitch,
        said: Said,
    ) -> None:
        """Both values arrive in the query string, so neither is Twitch's word."""
        script = twitch(*happy(user_id))

        response = await http.get(
            path,
            params={
                "state": new_state(identity),
                "error": "access_denied",
                "error_description": "The user denied you access",
            },
        )

        assert response.status_code == 400
        assert response.json() == {"detail": "Twitch authorization denied"}
        assert script.requests == []
        ((text, key),) = said.notified
        assert text == (
            f"Twitch authorization denied on {path}: The user denied you access"
        )
        assert key == f"oauth-denied:{path}"

    async def test_falls_back_to_the_error_code_when_there_is_no_description(
        self,
        path: str,
        identity: TokenType,
        user_id: str,
        label: str,
        http: httpx.AsyncClient,
        said: Said,
    ) -> None:
        await http.get(
            path, params={"state": new_state(identity), "error": "access_denied"}
        )

        assert said.texts == [
            f"Twitch authorization denied on {path}: access{BACKSLASH}_denied"
        ]

    async def test_the_reason_is_capped_then_escaped_for_the_admin_channel(
        self,
        path: str,
        identity: TokenType,
        user_id: str,
        label: str,
        http: httpx.AsyncClient,
        said: Said,
    ) -> None:
        await http.get(
            path,
            params={
                "state": new_state(identity),
                "error": "e",
                "error_description": "*" * 500,
            },
        )

        (text,) = said.texts
        reason = text.removeprefix(f"Twitch authorization denied on {path}: ")
        assert reason == f"{BACKSLASH}*" * 200

    async def test_an_empty_error_is_not_a_denial(
        self,
        path: str,
        identity: TokenType,
        user_id: str,
        label: str,
        http: httpx.AsyncClient,
        twitch: Twitch,
    ) -> None:
        twitch(*happy(user_id))

        response = await http.get(
            path, params={"state": new_state(identity), "code": "c", "error": ""}
        )

        assert response.status_code == 200

    async def test_a_denial_uses_the_state_up(
        self,
        path: str,
        identity: TokenType,
        user_id: str,
        label: str,
        http: httpx.AsyncClient,
    ) -> None:
        state = new_state(identity)

        await http.get(path, params={"state": state, "error": "access_denied"})

        assert state not in oauth._pending_authorizations
