from urllib.parse import parse_qs, urlsplit

import httpx
import pytest

from tests.credentials import CLIENT_ID
from tests.twitch.oauth.router.support import SCOPES, new_state
from valmal.db.models.enums import TokenType
from valmal.twitch.oauth import grants as oauth

pytestmark = pytest.mark.anyio


class TestStart:
    async def test_redirects_to_twitchs_consent_screen_for_the_identity(
        self, http: httpx.AsyncClient
    ) -> None:
        state = new_state(TokenType.User)

        response = await http.get(f"/twitch/oauth/start/user?state={state}")

        assert response.status_code == 302
        target = urlsplit(response.headers["location"])
        assert f"{target.scheme}://{target.netloc}{target.path}" == (
            "https://id.twitch.tv/oauth2/authorize"
        )
        query = {k: v[0] for k, v in parse_qs(target.query).items()}
        assert query == {
            "response_type": "code",
            "client_id": CLIENT_ID,
            "redirect_uri": "https://bot.example/twitch/oauth/callback",
            "scope": " ".join(SCOPES),
            "state": state,
            "force_verify": "true",
        }

    async def test_the_broadcaster_flow_returns_to_the_broadcaster_callback(
        self, http: httpx.AsyncClient
    ) -> None:
        state = new_state(TokenType.Broadcaster)

        response = await http.get(f"/twitch/oauth/start/broadcaster?state={state}")

        query = parse_qs(urlsplit(response.headers["location"]).query)
        assert query["redirect_uri"] == [
            "https://bot.example/twitch/oauth/callback/broadcaster"
        ]

    async def test_visiting_it_does_not_use_the_state_up(
        self, http: httpx.AsyncClient
    ) -> None:
        """Only the callback consumes it, so a reload of the start link still works."""
        state = new_state(TokenType.User)

        first = await http.get(f"/twitch/oauth/start/user?state={state}")
        second = await http.get(f"/twitch/oauth/start/user?state={state}")

        assert first.status_code == second.status_code == 302
        assert state in oauth._pending_authorizations

    async def test_a_state_nobody_issued_is_refused(
        self, http: httpx.AsyncClient
    ) -> None:
        response = await http.get("/twitch/oauth/start/user?state=forged")

        assert response.status_code == 400
        assert response.json()["detail"] == "Invalid or expired OAuth state"

    async def test_a_state_issued_for_the_other_identity_is_refused(
        self, http: httpx.AsyncClient
    ) -> None:
        """A link made for the bot account must not start the broadcaster's grant."""
        state = new_state(TokenType.User)

        response = await http.get(f"/twitch/oauth/start/broadcaster?state={state}")

        assert response.status_code == 400

    @pytest.mark.parametrize("identity", ["nobody", "USER", "Broadcaster"])
    async def test_an_identity_that_does_not_exist_is_refused(
        self, identity: str, http: httpx.AsyncClient
    ) -> None:
        """The values are matched exactly, so a case change is not another identity."""
        state = new_state(TokenType.User)

        response = await http.get(f"/twitch/oauth/start/{identity}?state={state}")

        assert response.status_code == 400
        assert "is not a valid TokenType" in response.json()["detail"]

    async def test_the_app_identity_has_no_authorization_flow(
        self, http: httpx.AsyncClient
    ) -> None:
        response = await http.get("/twitch/oauth/start/app?state=x")

        assert response.status_code == 400
        assert "not available for app" in response.json()["detail"]

    async def test_a_missing_state_is_a_validation_error(
        self, http: httpx.AsyncClient
    ) -> None:
        response = await http.get("/twitch/oauth/start/user")

        assert response.status_code == 422

    async def test_a_state_past_its_ten_minutes_is_refused_and_forgotten(
        self, http: httpx.AsyncClient
    ) -> None:
        state = new_state(TokenType.User)
        oauth._pending_authorizations[state] = oauth.PendingAuthorization(
            TokenType.User, expires_at=0.0
        )

        response = await http.get(f"/twitch/oauth/start/user?state={state}")

        assert response.status_code == 400
        assert state not in oauth._pending_authorizations
