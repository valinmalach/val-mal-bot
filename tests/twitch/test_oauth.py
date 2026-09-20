from collections.abc import Callable
from types import SimpleNamespace
from urllib.parse import parse_qs, urlsplit

import pytest

from config import settings
from constants import TokenType
from services.config import config
from services.twitch import oauth
from tests.credentials import CLIENT_ID

SCOPES = ["chat:read", "moderator:manage:shoutouts"]


class Clock:
    def __init__(self) -> None:
        self.now = 1000.0


@pytest.fixture
def clock(monkeypatch: pytest.MonkeyPatch) -> Clock:
    """oauth reads time.monotonic; replacing its own `time` name leaves the event
    loop's clock alone."""
    fake = Clock()
    monkeypatch.setattr(oauth, "time", SimpleNamespace(monotonic=lambda: fake.now))
    return fake


@pytest.fixture(autouse=True)
def _state(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(oauth, "_pending_authorizations", {})
    monkeypatch.setattr(
        config,
        "_settings",
        {
            "twitch_app_scopes": list(SCOPES),
            "twitch_bot_user_id": "999",
            "twitch_broadcaster_id": "111",
        },
    )


class TestConfiguredScopes:
    def test_a_valid_list(self) -> None:
        assert oauth.configured_scopes() == SCOPES

    @pytest.mark.parametrize("value", [None, [], "chat:read", {"a": 1}, 5])
    def test_anything_but_a_non_empty_list_is_refused(
        self, value: object, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """The setting is a JSON column somebody edits, so it is validated, not cast."""
        monkeypatch.setitem(config._settings, "twitch_app_scopes", value)

        with pytest.raises(RuntimeError, match="non-empty JSON list"):
            oauth.configured_scopes()

    def test_an_absent_setting_is_refused(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.delitem(config._settings, "twitch_app_scopes")

        with pytest.raises(RuntimeError, match="non-empty JSON list"):
            oauth.configured_scopes()

    @pytest.mark.parametrize("scopes", [["ok", ""], ["ok", 3], ["ok", None], [""]])
    def test_a_list_with_a_blank_or_non_string_scope_is_refused(
        self, scopes: list, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setitem(config._settings, "twitch_app_scopes", scopes)

        with pytest.raises(RuntimeError, match="invalid scope"):
            oauth.configured_scopes()


class TestIdentities:
    def test_the_callback_differs_by_identity(self) -> None:
        assert oauth.callback_uri(TokenType.User) == (
            "https://bot.example/twitch/oauth/callback"
        )
        assert oauth.callback_uri(TokenType.Broadcaster) == (
            "https://bot.example/twitch/oauth/callback/broadcaster"
        )

    def test_a_trailing_slash_in_the_app_url_cannot_double_the_path(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(settings, "app_url", "https://bot.example/")

        assert oauth.callback_uri(TokenType.User) == (
            "https://bot.example/twitch/oauth/callback"
        )

    def test_the_expected_user_is_the_bot_or_the_broadcaster(self) -> None:
        assert oauth.expected_user_id(TokenType.User) == "999"
        assert oauth.expected_user_id(TokenType.Broadcaster) == "111"

    @pytest.mark.parametrize("value", [None, "", 999, ["999"]])
    def test_an_expected_user_that_is_not_a_non_empty_string_is_refused(
        self, value: object, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """An id stored as a JSON number would silently never match Twitch's string."""
        monkeypatch.setitem(config._settings, "twitch_bot_user_id", value)

        with pytest.raises(
            RuntimeError, match="twitch_bot_user_id must be a non-empty string"
        ):
            oauth.expected_user_id(TokenType.User)

    def test_the_broadcaster_setting_is_named_when_it_is_the_broken_one(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.delitem(config._settings, "twitch_broadcaster_id")

        with pytest.raises(RuntimeError, match="twitch_broadcaster_id"):
            oauth.expected_user_id(TokenType.Broadcaster)

    @pytest.mark.parametrize(
        "call",
        [
            oauth.callback_uri,
            oauth.expected_user_id,
            oauth.create_authorization_start_url,
            lambda t: oauth.authorization_url(t, "state"),
            lambda t: oauth.consume_authorization(t, "state"),
        ],
    )
    def test_the_app_identity_has_no_authorization_flow(
        self, call: Callable[[TokenType], object]
    ) -> None:
        """The app row uses client credentials and has no refresh token."""
        with pytest.raises(ValueError, match="not available for app"):
            call(TokenType.App)


class TestStartUrl:
    def test_points_at_this_deployments_start_route_for_that_identity(self) -> None:
        url = urlsplit(oauth.create_authorization_start_url(TokenType.Broadcaster))

        assert (url.scheme, url.netloc) == ("https", "bot.example")
        assert url.path == "/twitch/oauth/start/broadcaster"
        assert list(parse_qs(url.query)) == ["state"]

    def test_the_state_is_unguessable_and_never_repeats(self) -> None:
        states = {
            parse_qs(
                urlsplit(oauth.create_authorization_start_url(TokenType.User)).query
            )["state"][0]
            for _ in range(50)
        }

        assert len(states) == 50
        assert all(len(state) >= 43 for state in states)

    def test_the_state_is_remembered_against_its_identity(self) -> None:
        url = oauth.create_authorization_start_url(TokenType.User)
        state = parse_qs(urlsplit(url).query)["state"][0]

        assert oauth._pending_authorizations[state].token_type is TokenType.User

    def test_a_link_lives_ten_minutes(self, clock: Clock) -> None:
        state = parse_qs(
            urlsplit(oauth.create_authorization_start_url(TokenType.User)).query
        )["state"][0]

        assert oauth._pending_authorizations[state].expires_at == clock.now + 600

    def test_creating_a_link_sweeps_the_expired_ones(self, clock: Clock) -> None:
        oauth.create_authorization_start_url(TokenType.User)
        clock.now += 601

        oauth.create_authorization_start_url(TokenType.User)

        assert len(oauth._pending_authorizations) == 1


class TestAuthorizationUrl:
    def start(self, token_type: TokenType = TokenType.User) -> str:
        return parse_qs(
            urlsplit(oauth.create_authorization_start_url(token_type)).query
        )["state"][0]

    def test_builds_twitchs_consent_url_for_a_live_state(self) -> None:
        state = self.start()

        url = urlsplit(oauth.authorization_url(TokenType.User, state))
        query = parse_qs(url.query)

        assert (url.scheme, url.netloc, url.path) == (
            "https",
            "id.twitch.tv",
            "/oauth2/authorize",
        )
        assert query == {
            "response_type": ["code"],
            "client_id": [CLIENT_ID],
            "redirect_uri": ["https://bot.example/twitch/oauth/callback"],
            "scope": [" ".join(SCOPES)],
            "state": [state],
            "force_verify": ["true"],
        }

    def test_the_broadcaster_flow_redirects_to_its_own_callback(self) -> None:
        state = self.start(TokenType.Broadcaster)

        query = parse_qs(
            urlsplit(oauth.authorization_url(TokenType.Broadcaster, state)).query
        )

        assert query["redirect_uri"] == [
            "https://bot.example/twitch/oauth/callback/broadcaster"
        ]

    def test_an_unknown_state_is_refused(self) -> None:
        with pytest.raises(ValueError, match="Invalid or expired OAuth state"):
            oauth.authorization_url(TokenType.User, "never-issued")

    def test_a_state_issued_for_the_other_identity_is_refused(self) -> None:
        """A link made for the bot must not authorize the broadcaster's account."""
        state = self.start(TokenType.User)

        with pytest.raises(ValueError, match="Invalid or expired"):
            oauth.authorization_url(TokenType.Broadcaster, state)

    def test_an_expired_state_is_refused(self, clock: Clock) -> None:
        state = self.start()
        clock.now += 601

        with pytest.raises(ValueError, match="Invalid or expired"):
            oauth.authorization_url(TokenType.User, state)

    def test_a_state_is_still_good_at_the_last_moment(self, clock: Clock) -> None:
        state = self.start()
        clock.now += 599.9

        assert oauth.authorization_url(TokenType.User, state)

    def test_showing_the_consent_url_does_not_use_the_state_up(self) -> None:
        state = self.start()

        oauth.authorization_url(TokenType.User, state)

        assert oauth.authorization_url(TokenType.User, state)

    def test_a_bad_scopes_setting_fails_here_not_at_twitch(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        state = self.start()
        monkeypatch.setitem(config._settings, "twitch_app_scopes", [])

        with pytest.raises(RuntimeError, match="twitch_app_scopes"):
            oauth.authorization_url(TokenType.User, state)


class TestConsume:
    def start(self, token_type: TokenType = TokenType.User) -> str:
        return parse_qs(
            urlsplit(oauth.create_authorization_start_url(token_type)).query
        )["state"][0]

    def test_a_state_is_good_exactly_once(self) -> None:
        state = self.start()

        assert oauth.consume_authorization(TokenType.User, state) is True
        assert oauth.consume_authorization(TokenType.User, state) is False

    def test_an_unknown_state_is_false(self) -> None:
        assert oauth.consume_authorization(TokenType.User, "never-issued") is False

    def test_a_state_for_the_other_identity_is_false_and_is_not_consumed(self) -> None:
        """A wrong-identity attempt must not burn the legitimate one."""
        state = self.start(TokenType.User)

        assert oauth.consume_authorization(TokenType.Broadcaster, state) is False
        assert oauth.consume_authorization(TokenType.User, state) is True

    def test_an_expired_state_is_false(self, clock: Clock) -> None:
        state = self.start()
        clock.now += 601

        assert oauth.consume_authorization(TokenType.User, state) is False

    def test_a_state_expiring_exactly_now_is_expired(self, clock: Clock) -> None:
        state = self.start()
        clock.now += 600

        assert oauth.consume_authorization(TokenType.User, state) is False

    def test_consuming_one_leaves_the_others(self) -> None:
        first, second = self.start(), self.start()

        oauth.consume_authorization(TokenType.User, first)

        assert oauth.consume_authorization(TokenType.User, second) is True
