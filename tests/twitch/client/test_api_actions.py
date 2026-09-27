from collections.abc import Callable

import pytest

import valmal.twitch.client.api as api
from tests.credentials import WEBHOOK_SECRET
from tests.twitch.support import (
    BOT_SETTINGS,
    OFFLINE,
    ONLINE,
    Script,
    body,
    page,
    reply,
    steps,
    subscription_json,
    user_json,
)
from valmal.core.config import config

pytestmark = pytest.mark.anyio

HttpFactory = Callable[..., Script]


@pytest.fixture(autouse=True)
def _settings(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(config, "_settings", BOT_SETTINGS)


@pytest.fixture
def notices(monkeypatch: pytest.MonkeyPatch) -> list[tuple[str, str | None]]:
    seen: list[tuple[str, str | None]] = []

    async def notify(text: str, *, key: str | None = None) -> bool:
        seen.append((text, key))
        return True

    monkeypatch.setattr(api, "notify", notify)
    return seen


BAD_LOGIN_NOTICE = (
    "Refused a Twitch lookup: what was given cannot be a login.",
    "twitch-lookup-bad-login",
)


class TestChatAndShoutout:
    async def test_a_chat_message_names_the_bot_as_sender(
        self, helix_http: HttpFactory
    ) -> None:
        script = helix_http(reply(200, {}))

        await api.send_chat_message("111", "hello chat")

        (sent,) = script.requests
        assert (sent.method, sent.url.path) == ("POST", "/helix/chat/messages")
        assert body(sent) == {
            "broadcaster_id": "111",
            "sender_id": "999",
            "message": "hello chat",
            "for_source_only": False,
        }

    async def test_a_chat_message_is_never_retried(
        self, helix_http: HttpFactory
    ) -> None:
        """A repeat would post twice."""
        script = helix_http(reply(503))

        with pytest.raises(api.HelixError):
            await api.send_chat_message("111", "hi")

        assert len(script.requests) == 1

    async def test_a_shoutout_is_from_the_broadcaster_as_the_bot_with_the_user_token(
        self, helix_http: HttpFactory
    ) -> None:
        script = helix_http(reply(204))

        await api.send_shoutout("555")

        (sent,) = script.requests
        assert sent.url.path == "/helix/chat/shoutouts"
        assert body(sent) == {
            "from_broadcaster_id": "111",
            "to_broadcaster_id": "555",
            "moderator_id": "999",
        }
        assert sent.headers["Authorization"] == "Bearer user-token"

    async def test_a_shoutout_is_never_retried(self, helix_http: HttpFactory) -> None:
        script = helix_http(reply(500))

        with pytest.raises(api.HelixError):
            await api.send_shoutout("555")

        assert len(script.requests) == 1


class TestSubscriptionCalls:
    async def test_a_subscription_names_its_transport_and_carries_the_secret(
        self, helix_http: HttpFactory
    ) -> None:
        script = helix_http(reply(202, {}))

        await api.create_subscription(
            "channel.follow", "2", {"broadcaster_user_id": "1"}, "https://cb/x"
        )

        assert body(script.requests[0]) == {
            "type": "channel.follow",
            "version": "2",
            "condition": {"broadcaster_user_id": "1"},
            "transport": {
                "method": "webhook",
                "callback": "https://cb/x",
                "secret": WEBHOOK_SECRET,
            },
        }

    async def test_creating_is_retried_because_twitch_rejects_a_duplicate(
        self, helix_http: HttpFactory
    ) -> None:
        script = helix_http(reply(503), reply(202, {}))

        await api.create_subscription("x", "1", {}, "https://cb")

        assert len(script.requests) == 2

    async def test_deleting_asks_by_id(self, helix_http: HttpFactory) -> None:
        script = helix_http(reply(204))

        await api.delete_subscription("abc")

        (sent,) = script.requests
        assert (sent.method, sent.url.path, sent.url.query) == (
            "DELETE",
            "/helix/eventsub/subscriptions",
            b"id=abc",
        )


class TestSubscribeToUser:
    async def test_an_invalid_login_is_refused_before_any_request(
        self, helix_http: HttpFactory, notices: list[tuple[str, str | None]]
    ) -> None:
        script = helix_http(reply(200, {"data": []}))

        assert await api.subscribe_to_user("not a login!") is False
        assert script.requests == []
        assert notices == [BAD_LOGIN_NOTICE], "and the refused value is never echoed"

    async def test_an_unknown_user_is_false_and_said(
        self, helix_http: HttpFactory, notices: list[tuple[str, str | None]]
    ) -> None:
        helix_http(reply(200, {"data": []}))

        assert await api.subscribe_to_user("nobody") is False
        assert notices == [
            ("Twitch has no user called `nobody`.", "twitch-lookup-not-found:nobody")
        ]

    async def test_a_known_user_gets_online_and_offline_at_their_own_callbacks(
        self, helix_http: HttpFactory
    ) -> None:
        script = helix_http(
            reply(200, {"data": [user_json("42", "alice")]}), reply(202, {})
        )

        assert await api.subscribe_to_user("alice") is True

        assert steps(script) == [
            ("GET", "/helix/users"),
            ("POST", "/helix/eventsub/subscriptions"),
            ("POST", "/helix/eventsub/subscriptions"),
        ]
        online, offline = body(script.requests[1]), body(script.requests[2])
        assert (online["type"], online["transport"]["callback"]) == (
            "stream.online",
            ONLINE,
        )
        assert (offline["type"], offline["transport"]["callback"]) == (
            "stream.offline",
            OFFLINE,
        )
        assert (
            online["condition"] == offline["condition"] == {"broadcaster_user_id": "42"}
        )

    async def test_a_failure_other_than_a_conflict_propagates(
        self, helix_http: HttpFactory
    ) -> None:
        script = helix_http(
            reply(200, {"data": [user_json("42")]}), reply(400, text="bad")
        )

        with pytest.raises(api.HelixError, match="400"):
            await api.subscribe_to_user("alice")

        assert steps(script) == [
            ("GET", "/helix/users"),
            ("POST", "/helix/eventsub/subscriptions"),
        ], "only a 409 is worth asking Twitch what is already there"


class TestUnsubscribe:
    async def test_an_invalid_login_is_false_without_a_request(
        self, helix_http: HttpFactory, notices: list[tuple[str, str | None]]
    ) -> None:
        script = helix_http(reply(200, {"data": []}))

        assert await api.unsubscribe_to_user("bad name") is False
        assert script.requests == []
        assert notices == [BAD_LOGIN_NOTICE]

    async def test_an_unknown_user_is_false_and_said(
        self, helix_http: HttpFactory, notices: list[tuple[str, str | None]]
    ) -> None:
        helix_http(reply(200, {"data": []}))

        assert await api.unsubscribe_to_user("nobody") is False
        assert notices == [
            ("Twitch has no user called `nobody`.", "twitch-lookup-not-found:nobody")
        ]

    async def test_no_subscriptions_is_still_success_and_deletes_nothing(
        self, helix_http: HttpFactory
    ) -> None:
        script = helix_http(
            reply(200, {"data": [user_json("42")]}), reply(200, page([]))
        )

        assert await api.unsubscribe_to_user("alice") is True
        assert all(r.method != "DELETE" for r in script.requests)

    async def test_deletes_only_this_users_online_and_offline_subscriptions(
        self, helix_http: HttpFactory
    ) -> None:
        script = helix_http(
            reply(200, {"data": [user_json("42")]}),
            reply(
                200,
                page(
                    [
                        subscription_json(
                            "on", type="stream.online", broadcaster_user_id="42"
                        ),
                        subscription_json(
                            "off", type="stream.offline", broadcaster_user_id="42"
                        ),
                        subscription_json(
                            "raid", type="channel.raid", broadcaster_user_id="42"
                        ),
                        subscription_json(
                            "other", type="stream.online", broadcaster_user_id="7"
                        ),
                    ]
                ),
            ),
            reply(204),
        )

        assert await api.unsubscribe_to_user("alice") is True

        assert sorted(r.url.query for r in script.requests if r.method == "DELETE") == [
            b"id=off",
            b"id=on",
        ]
