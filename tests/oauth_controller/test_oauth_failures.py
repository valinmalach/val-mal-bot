from collections.abc import Callable
from typing import Any

import httpx
import pytest

from constants import TokenType
from tests.oauth_controller.support import (
    BOT_ID,
    BROADCASTER_CALLBACK,
    BROADCASTER_ID,
    SCOPES,
    USER_CALLBACK,
    Said,
    Stored,
    new_state,
    token_body,
    validation_body,
)
from tests.twitch.support import Script, reply

pytestmark = pytest.mark.anyio

Twitch = Callable[..., Script]


async def authorize(
    http: httpx.AsyncClient, path: str = USER_CALLBACK
) -> httpx.Response:
    identity = TokenType.Broadcaster if path == BROADCASTER_CALLBACK else TokenType.User
    return await http.get(path, params={"state": new_state(identity), "code": "c"})


class TestTheExchangeFails:
    @pytest.mark.parametrize("status", [400, 401, 429, 500, 503])
    async def test_a_refused_exchange_is_a_500_and_says_so_in_the_admin_channel(
        self,
        status: int,
        http: httpx.AsyncClient,
        twitch: Twitch,
        stored: Stored,
        said: Said,
    ) -> None:
        script = twitch(reply(status, text="invalid code"))

        response = await authorize(http)

        assert response.status_code == 500
        assert said.texts == [f"Failed to exchange token: {status} invalid code"]
        assert len(script.requests) == 1
        assert stored.user == []

    async def test_the_callers_page_carries_no_detail_from_twitch(
        self, http: httpx.AsyncClient, twitch: Twitch
    ) -> None:
        twitch(reply(400, text="secret-detail"))

        response = await authorize(http)

        assert "secret-detail" not in response.text

    @pytest.mark.parametrize("token_type", ["Bearer", "BEARER", "bearer"])
    async def test_the_token_type_is_compared_without_regard_to_case(
        self,
        token_type: str,
        http: httpx.AsyncClient,
        twitch: Twitch,
        stored: Stored,
    ) -> None:
        """RFC 6749 makes the token type case insensitive."""
        twitch(
            reply(200, token_body(token_type=token_type)), reply(200, validation_body())
        )

        assert (await authorize(http)).status_code == 200
        assert len(stored.user) == 1

    @pytest.mark.parametrize("token_type", ["mac", "", "bearer2", " bearer"])
    async def test_any_other_token_type_is_refused(
        self,
        token_type: str,
        http: httpx.AsyncClient,
        twitch: Twitch,
        stored: Stored,
        said: Said,
    ) -> None:
        script = twitch(reply(200, token_body(token_type=token_type)))

        response = await authorize(http)

        assert response.status_code == 500
        assert said.texts == [
            f"Failed to exchange token: unexpected token type {token_type}"
        ]
        assert len(script.requests) == 1, "an unusable token is never validated"
        assert stored.user == []

    @pytest.mark.parametrize(
        "body",
        [{}, {"access_token": "a"}, token_body(expires_in="soon"), [1, 2]],
    )
    async def test_a_reply_that_is_not_a_token_is_a_reported_500(
        self,
        body: Any,
        http: httpx.AsyncClient,
        twitch: Twitch,
        stored: Stored,
        said: Said,
    ) -> None:
        twitch(
            reply(200, body if body != {} else None, text="{}" if body == {} else None)
        )

        response = await authorize(http)

        assert response.status_code == 500
        assert said.reported == ["500: Internal server error on /twitch/oauth/callback"]
        assert stored.user == []

    async def test_a_reply_that_is_not_json_is_a_reported_500(
        self, http: httpx.AsyncClient, twitch: Twitch, said: Said
    ) -> None:
        twitch(reply(200, text="<html>"))

        assert (await authorize(http)).status_code == 500
        assert len(said.reported) == 1

    async def test_a_network_failure_reaching_twitch_is_a_reported_500(
        self, http: httpx.AsyncClient, twitch: Twitch, said: Said
    ) -> None:
        twitch(httpx.ConnectError("no route"))

        response = await authorize(http)

        assert response.status_code == 500
        assert said.reported == ["500: Internal server error on /twitch/oauth/callback"]

    async def test_the_broadcaster_route_reports_under_its_own_path(
        self, http: httpx.AsyncClient, twitch: Twitch, said: Said
    ) -> None:
        twitch(httpx.ConnectError("no route"))

        await authorize(http, BROADCASTER_CALLBACK)

        assert said.reported == [
            "500: Internal server error on /twitch/oauth/callback/broadcaster"
        ]

    async def test_a_500_is_not_reported_twice(
        self, http: httpx.AsyncClient, twitch: Twitch, said: Said
    ) -> None:
        """An HTTPException the code raised on purpose has already said what it had to."""
        twitch(reply(500, text="down"))

        await authorize(http)

        assert said.reported == []


class TestTheTokenIsValidatedBeforeItIsKept:
    async def test_a_validation_that_fails_is_a_500_and_keeps_nothing(
        self,
        http: httpx.AsyncClient,
        twitch: Twitch,
        stored: Stored,
        said: Said,
    ) -> None:
        twitch(reply(200, token_body()), reply(401, text="invalid access token"))

        response = await authorize(http)

        assert response.status_code == 500
        assert response.json() == {"detail": "Twitch token validation failed"}
        assert said.texts == [
            "Failed to validate the Twitch token from /twitch/oauth/callback: "
            "401 invalid access token"
        ]
        assert stored.user == []

    async def test_the_validation_failure_names_the_endpoint_that_asked(
        self, http: httpx.AsyncClient, twitch: Twitch, said: Said
    ) -> None:
        twitch(reply(200, token_body()), reply(500, text="x"))

        await authorize(http, BROADCASTER_CALLBACK)

        assert "/twitch/oauth/callback/broadcaster" in said.texts[0]

    async def test_a_token_from_another_application_is_rejected(
        self,
        http: httpx.AsyncClient,
        twitch: Twitch,
        stored: Stored,
        said: Said,
    ) -> None:
        twitch(reply(200, token_body()), reply(200, validation_body(client_id="other")))

        response = await authorize(http)

        assert response.status_code == 400
        assert response.json()["detail"] == (
            "Rejected user authorization: "
            "the token belongs to a different Twitch application"
        )
        assert said.texts == [response.json()["detail"]]
        assert stored.user == []

    @pytest.mark.parametrize(
        ("path", "identity_label", "expected"),
        [
            (USER_CALLBACK, "user", BOT_ID),
            (BROADCASTER_CALLBACK, "broadcaster", BROADCASTER_ID),
        ],
    )
    async def test_the_wrong_account_is_rejected_by_name_and_id(
        self,
        path: str,
        identity_label: str,
        expected: str,
        http: httpx.AsyncClient,
        twitch: Twitch,
        stored: Stored,
    ) -> None:
        """Authorizing as somebody else must never store their token as the bot's."""
        twitch(
            reply(200, token_body()),
            reply(200, validation_body(user_id="5", login="impostor")),
        )

        response = await authorize(http, path)

        assert response.status_code == 400
        assert response.json()["detail"] == (
            f"Rejected {identity_label} authorization: expected Twitch user ID "
            f"{expected}, but impostor authorized as ID 5"
        )
        assert stored.user == [] and stored.broadcaster == []

    async def test_missing_scopes_are_named_sorted(
        self, http: httpx.AsyncClient, twitch: Twitch, stored: Stored
    ) -> None:
        twitch(reply(200, token_body()), reply(200, validation_body(scopes=[])))

        response = await authorize(http)

        assert response.status_code == 400
        assert response.json()["detail"].endswith(
            f"missing scopes: {', '.join(sorted(SCOPES))}"
        )
        assert stored.user == []

    async def test_extra_scopes_are_not_a_problem(
        self, http: httpx.AsyncClient, twitch: Twitch, stored: Stored
    ) -> None:
        twitch(
            reply(200, token_body()),
            reply(200, validation_body(scopes=[*SCOPES, "user:read:email"])),
        )

        assert (await authorize(http)).status_code == 200
        assert len(stored.user) == 1

    async def test_every_problem_is_reported_at_once(
        self, http: httpx.AsyncClient, twitch: Twitch
    ) -> None:
        twitch(
            reply(200, token_body()),
            reply(
                200,
                validation_body(
                    client_id="other", user_id="5", login="impostor", scopes=[]
                ),
            ),
        )

        response = await authorize(http)

        detail = response.json()["detail"]
        assert detail.count("; ") == 2
        assert "different Twitch application" in detail
        assert "impostor authorized as ID 5" in detail
        assert "missing scopes" in detail

    async def test_a_validation_reply_that_is_not_a_validation_is_a_reported_500(
        self, http: httpx.AsyncClient, twitch: Twitch, said: Said, stored: Stored
    ) -> None:
        twitch(reply(200, token_body()), reply(200, {"client_id": "test"}))

        response = await authorize(http)

        assert response.status_code == 500
        assert len(said.reported) == 1
        assert stored.user == []


class TestKeepingTheTokenFails:
    async def test_a_token_manager_that_refuses_is_a_reported_500(
        self,
        http: httpx.AsyncClient,
        twitch: Twitch,
        stored: Stored,
        said: Said,
    ) -> None:
        twitch(reply(200, token_body()), reply(200, validation_body()))
        stored.error = ConnectionError("database down")

        response = await authorize(http)

        assert response.status_code == 500
        assert "database down" not in response.text
        assert said.reported == ["500: Internal server error on /twitch/oauth/callback"]

    async def test_the_broadcaster_flow_stores_under_the_broadcaster_only(
        self, http: httpx.AsyncClient, twitch: Twitch, stored: Stored
    ) -> None:
        twitch(reply(200, token_body()), reply(200, validation_body(BROADCASTER_ID)))

        await authorize(http, BROADCASTER_CALLBACK)

        assert len(stored.broadcaster) == 1 and stored.user == []
