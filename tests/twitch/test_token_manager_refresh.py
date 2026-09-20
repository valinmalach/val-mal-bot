from urllib.parse import parse_qs

import httpx
import pendulum
import pytest
from pydantic import ValidationError

from constants import TokenType
from services.config import config
from services.twitch.token_manager import TwitchTokenManager
from tests.credentials import CLIENT_ID, CLIENT_SECRET
from tests.twitch.support import (
    APP_OK,
    NOW,
    TOKEN_URL,
    USER_OK,
    Http,
    Notices,
    TokenDb,
    reply,
)

pytestmark = pytest.mark.anyio


def written(db: TokenDb) -> list[object]:
    """The upserts issued; a select is not a write."""
    return [s for s in db.statements if not hasattr(s, "column_descriptions")]


@pytest.fixture(autouse=True)
def _env(scopes: list[str], token_db: TokenDb, now: pendulum.DateTime) -> None:
    """Every refresh reads the configured scopes, stores to the database and
    stamps an expiry."""


class TestRefreshApp:
    async def test_asks_for_client_credentials_in_the_query_string(
        self, manager: TwitchTokenManager, oauth_http: Http, scopes: list[str]
    ) -> None:
        script = oauth_http(reply(200, APP_OK))

        await manager.refresh_app_access_token()

        (sent,) = script.requests
        assert (sent.method, str(sent.url).split("?")[0]) == ("POST", TOKEN_URL)
        assert dict(sent.url.params) == {
            "client_id": CLIENT_ID,
            "client_secret": CLIENT_SECRET,
            "grant_type": "client_credentials",
            "scope": " ".join(scopes),
        }
        assert sent.content == b""

    async def test_success_stores_the_token_and_its_expiry(
        self, manager: TwitchTokenManager, oauth_http: Http, token_db: TokenDb
    ) -> None:
        oauth_http(reply(200, APP_OK))

        assert await manager.refresh_app_access_token() is True

        assert manager.token(TokenType.App) == "new-app"
        assert manager._expires_at[TokenType.App] == NOW.add(seconds=3600)
        assert len(written(token_db)) == 1

    @pytest.mark.parametrize("status", [400, 401, 403, 500, 503])
    async def test_a_non_2xx_is_false_with_a_notice_and_stores_nothing(
        self,
        status: int,
        manager: TwitchTokenManager,
        oauth_http: Http,
        token_db: TokenDb,
        token_notices: Notices,
    ) -> None:
        oauth_http(reply(status, text="invalid client"))

        assert await manager.refresh_app_access_token() is False

        assert manager.token(TokenType.App) == ""
        assert written(token_db) == []
        ((text, key),) = token_notices
        assert f"{status} invalid client" in text
        assert key == "token-refresh-failed:app"

    @pytest.mark.parametrize("token_type", ["bearer", "Bearer", "BEARER"])
    async def test_the_token_type_is_case_insensitive(
        self, token_type: str, manager: TwitchTokenManager, oauth_http: Http
    ) -> None:
        """RFC 6749."""
        oauth_http(reply(200, {**APP_OK, "token_type": token_type}))

        assert await manager.refresh_app_access_token() is True

    async def test_a_token_type_that_is_not_bearer_is_refused_and_not_stored(
        self,
        manager: TwitchTokenManager,
        oauth_http: Http,
        token_db: TokenDb,
        token_notices: Notices,
    ) -> None:
        oauth_http(reply(200, {**APP_OK, "token_type": "mac"}))

        assert await manager.refresh_app_access_token() is False

        assert manager.token(TokenType.App) == ""
        assert written(token_db) == []
        assert token_notices == [
            ("Unexpected token type: mac", "token-type-unexpected:app")
        ]

    @pytest.mark.parametrize("value", [None, [], "chat:read", ["ok", ""]])
    async def test_bad_scopes_are_refused_before_any_request(
        self,
        value: object,
        manager: TwitchTokenManager,
        oauth_http: Http,
        token_notices: Notices,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        script = oauth_http(reply(200, APP_OK))
        monkeypatch.setitem(config._settings, "twitch_app_scopes", value)

        assert await manager.refresh_app_access_token() is False

        assert script.requests == []
        assert token_notices[0][1] == "app-scopes-invalid"

    async def test_a_body_that_does_not_parse_raises_for_the_caller_to_report(
        self, manager: TwitchTokenManager, oauth_http: Http
    ) -> None:
        oauth_http(reply(200, {"unexpected": True}))

        with pytest.raises(ValidationError):
            await manager.refresh_app_access_token()

    async def test_a_transport_error_propagates_for_the_caller_to_report(
        self, manager: TwitchTokenManager, oauth_http: Http
    ) -> None:
        oauth_http(httpx.ConnectError("down"))

        with pytest.raises(httpx.ConnectError):
            await manager.refresh_app_access_token()


class TestRefreshUser:
    @pytest.fixture(autouse=True)
    def _have_refresh_tokens(self, manager: TwitchTokenManager) -> None:
        manager._access[TokenType.User] = "old-access"
        manager._refresh[TokenType.User] = "old-refresh"
        manager._access[TokenType.Broadcaster] = "old-bc-access"
        manager._refresh[TokenType.Broadcaster] = "old-bc-refresh"

    async def test_sends_the_refresh_token_as_a_form_body_not_a_query(
        self, manager: TwitchTokenManager, oauth_http: Http
    ) -> None:
        script = oauth_http(reply(200, USER_OK))

        await manager.refresh_user_access_token()

        (sent,) = script.requests
        assert sent.url.query == b""
        assert sent.headers["Content-Type"] == "application/x-www-form-urlencoded"
        assert {k: v[0] for k, v in parse_qs(sent.content.decode()).items()} == {
            "client_id": CLIENT_ID,
            "client_secret": CLIENT_SECRET,
            "grant_type": "refresh_token",
            "refresh_token": "old-refresh",
        }

    async def test_success_replaces_both_tokens_since_twitch_invalidates_the_old_one(
        self, manager: TwitchTokenManager, oauth_http: Http, token_db: TokenDb
    ) -> None:
        oauth_http(reply(200, USER_OK))

        assert await manager.refresh_user_access_token() is True

        assert manager.token(TokenType.User) == "new-access"
        assert manager._refresh[TokenType.User] == "new-refresh"
        assert manager._expires_at[TokenType.User] == NOW.add(seconds=14000)
        assert len(written(token_db)) == 1

    async def test_the_broadcaster_flow_uses_and_updates_the_broadcasters_tokens(
        self, manager: TwitchTokenManager, oauth_http: Http
    ) -> None:
        script = oauth_http(reply(200, USER_OK))

        await manager.refresh_user_access_token(broadcaster=True)

        assert b"refresh_token=old-bc-refresh" in script.requests[0].content
        assert manager.token(TokenType.Broadcaster) == "new-access"
        assert manager.token(TokenType.User) == "old-access"

    @pytest.mark.parametrize(
        ("broadcaster", "label"), [(False, "user"), (True, "broadcaster")]
    )
    async def test_no_refresh_token_is_false_with_a_notice_and_no_request(
        self,
        broadcaster: bool,
        label: str,
        manager: TwitchTokenManager,
        oauth_http: Http,
        token_notices: Notices,
    ) -> None:
        manager._refresh.clear()
        script = oauth_http(reply(200, USER_OK))

        assert await manager.refresh_user_access_token(broadcaster) is False

        assert script.requests == []
        assert token_notices == [
            (f"No {label} refresh token available", f"token-refresh-missing:{label}")
        ]

    @pytest.mark.parametrize(
        ("broadcaster", "label"), [(False, "user"), (True, "broadcaster")]
    )
    async def test_a_rejected_refresh_is_false_and_keeps_what_was_held(
        self,
        broadcaster: bool,
        label: str,
        manager: TwitchTokenManager,
        oauth_http: Http,
        token_db: TokenDb,
        token_notices: Notices,
    ) -> None:
        oauth_http(reply(400, text="Invalid refresh token"))

        assert await manager.refresh_user_access_token(broadcaster) is False

        assert manager.token(TokenType.User) == "old-access"
        assert manager._refresh[TokenType.User] == "old-refresh"
        assert written(token_db) == []
        ((text, key),) = token_notices
        assert (
            f"Failed to refresh {label} access token: 400 Invalid refresh token" in text
        )
        assert key == f"token-refresh-failed:{label}"

    @pytest.mark.parametrize("token_type", ["bearer", "Bearer", "BEARER"])
    async def test_the_token_type_is_case_insensitive_for_a_user_too(
        self, token_type: str, manager: TwitchTokenManager, oauth_http: Http
    ) -> None:
        """RFC 6749: the app path had this pinned and the user path did not."""
        oauth_http(reply(200, {**USER_OK, "token_type": token_type}))

        assert await manager.refresh_user_access_token() is True

    async def test_a_token_type_that_is_not_bearer_is_refused(
        self,
        manager: TwitchTokenManager,
        oauth_http: Http,
        token_notices: Notices,
    ) -> None:
        oauth_http(reply(200, {**USER_OK, "token_type": "mac"}))

        assert await manager.refresh_user_access_token() is False

        assert manager.token(TokenType.User) == "old-access"
        assert token_notices[0][1] == "token-type-unexpected:user"

    async def test_a_reply_with_no_expiry_clears_the_known_one(
        self, manager: TwitchTokenManager, oauth_http: Http
    ) -> None:
        manager._expires_at[TokenType.User] = NOW.add(hours=1)
        oauth_http(reply(200, {**USER_OK, "expires_in": None}))

        await manager.refresh_user_access_token()

        assert TokenType.User not in manager._expires_at

    async def test_a_scope_returned_as_one_string_is_stored_as_a_list(
        self, manager: TwitchTokenManager, oauth_http: Http, token_db: TokenDb
    ) -> None:
        from sqlalchemy.dialects import postgresql

        oauth_http(reply(200, {**USER_OK, "scope": "chat:read"}))

        await manager.refresh_user_access_token()

        (statement,) = written(token_db)
        params = statement.compile(dialect=postgresql.dialect()).params  # pyright: ignore[reportAttributeAccessIssue]
        assert params["scopes"] == ["chat:read"]


class TestRefreshRouting:
    @pytest.mark.parametrize(
        ("token_type", "expected"),
        [
            (TokenType.App, ("app", None)),
            (TokenType.User, ("user", False)),
            (TokenType.Broadcaster, ("user", True)),
        ],
    )
    async def test_refresh_picks_the_flow_for_the_identity(
        self,
        token_type: TokenType,
        expected: tuple[str, bool | None],
        manager: TwitchTokenManager,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        called: list[tuple[str, bool | None]] = []

        async def app() -> bool:
            called.append(("app", None))
            return True

        async def user(broadcaster: bool = False) -> bool:
            called.append(("user", broadcaster))
            return True

        monkeypatch.setattr(manager, "refresh_app_access_token", app)
        monkeypatch.setattr(manager, "refresh_user_access_token", user)

        assert await manager.refresh(token_type) is True

        assert called == [expected]
