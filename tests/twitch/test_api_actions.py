import json
from collections.abc import Callable
from typing import Any

import httpx
import pytest

import services.twitch.api as api
from services.config import config
from tests.credentials import WEBHOOK_SECRET
from tests.twitch.support import (
    Script,
    page,
    reply,
    subscription_json,
    user_json,
)

pytestmark = pytest.mark.anyio

HttpFactory = Callable[..., Script]

ONLINE = "https://bot.example/webhook/twitch"
OFFLINE = "https://bot.example/webhook/twitch/offline"


@pytest.fixture(autouse=True)
def _settings(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        config,
        "_settings",
        {"twitch_bot_user_id": "999", "twitch_broadcaster_id": "111"},
    )


@pytest.fixture
def notices(monkeypatch: pytest.MonkeyPatch) -> list[str]:
    seen: list[str] = []

    async def notify(text: str, *, key: str | None = None) -> bool:
        seen.append(text)
        return True

    monkeypatch.setattr(api, "notify", notify)
    return seen


def steps(script: Script) -> list[tuple[str, str]]:
    return [(r.method, r.url.path) for r in script.requests]


def body(request: httpx.Request) -> dict[str, Any]:
    return json.loads(request.content)


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
        self, helix_http: HttpFactory
    ) -> None:
        script = helix_http(reply(200, {"data": []}))

        assert await api.subscribe_to_user("not a login!") is False
        assert script.requests == []

    async def test_an_unknown_user_is_false(self, helix_http: HttpFactory) -> None:
        helix_http(reply(200, {"data": []}))

        assert await api.subscribe_to_user("nobody") is False

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


class TestConflictOnSubscribe:
    """Twitch's uniqueness key is the type and the condition, not the transport, so
    a 409 says just as readily that a subscription exists pointing somewhere
    useless as that a working one is in place."""

    def outcomes(self, existing: list[dict]) -> tuple:
        return (
            reply(200, {"data": [user_json("42")]}),  # the lookup
            reply(409, text="exists"),  # online: conflict
            reply(200, page(existing)),  # what is there
        )

    async def test_a_working_one_already_in_place_is_left_alone(
        self, helix_http: HttpFactory, notices: list[str]
    ) -> None:
        script = helix_http(
            *self.outcomes(
                [subscription_json("keep", callback=ONLINE, broadcaster_user_id="42")]
            ),
            reply(202, {}),  # offline: created fine
        )

        assert await api.subscribe_to_user("alice") is True

        assert ("DELETE", "/helix/eventsub/subscriptions") not in steps(script)
        assert not notices

    async def test_one_calling_back_elsewhere_is_replaced(
        self, helix_http: HttpFactory, notices: list[str]
    ) -> None:
        script = helix_http(
            *self.outcomes(
                [
                    subscription_json(
                        "old",
                        callback="https://old.example/webhook/twitch",
                        broadcaster_user_id="42",
                    )
                ]
            ),
            reply(204),  # delete
            reply(202, {}),  # online again
            reply(202, {}),  # offline
        )

        assert await api.subscribe_to_user("alice") is True

        assert steps(script) == [
            ("GET", "/helix/users"),
            ("POST", "/helix/eventsub/subscriptions"),  # online: conflict
            ("GET", "/helix/eventsub/subscriptions"),  # what is there
            ("DELETE", "/helix/eventsub/subscriptions"),
            ("POST", "/helix/eventsub/subscriptions"),  # online again
            ("POST", "/helix/eventsub/subscriptions"),  # offline
        ]
        assert script.requests[3].url.query == b"id=old"
        recreated = body(script.requests[4])
        assert (recreated["type"], recreated["transport"]["callback"]) == (
            "stream.online",
            ONLINE,
        )
        assert recreated["condition"] == {"broadcaster_user_id": "42"}
        assert len(notices) == 1
        assert "Replacing the stream.online subscription" in notices[0]
        assert "https://old.example/webhook/twitch" in notices[0]

    async def test_one_twitch_disabled_is_replaced_even_at_the_right_callback(
        self, helix_http: HttpFactory, notices: list[str]
    ) -> None:
        script = helix_http(
            *self.outcomes(
                [
                    subscription_json(
                        "dead",
                        status="notification_failures_exceeded",
                        callback=ONLINE,
                        broadcaster_user_id="42",
                    )
                ]
            ),
            reply(204),
            reply(202, {}),
            reply(202, {}),
        )

        await api.subscribe_to_user("alice")

        assert ("DELETE", "/helix/eventsub/subscriptions") in steps(script)
        assert "status=notification_failures_exceeded" in notices[0]

    async def test_only_the_subscription_for_this_user_and_type_is_considered(
        self, helix_http: HttpFactory, notices: list[str]
    ) -> None:
        script = helix_http(
            *self.outcomes(
                [
                    subscription_json(
                        "other-user", callback=ONLINE, broadcaster_user_id="7"
                    ),
                    subscription_json(
                        "other-type",
                        type="stream.offline",
                        callback=OFFLINE,
                        broadcaster_user_id="42",
                    ),
                ]
            ),
            reply(204),
            reply(202, {}),
            reply(202, {}),
        )

        await api.subscribe_to_user("alice")

        # Neither is the enabled online subscription for user 42, so nothing here
        # counts as already in place, and neither is what the 409 was about.
        assert not [r.url.query for r in script.requests if r.method == "DELETE"]
        assert not notices

    async def test_every_stale_one_is_deleted_and_each_is_reported(
        self, helix_http: HttpFactory, notices: list[str]
    ) -> None:
        script = helix_http(
            *self.outcomes(
                [
                    subscription_json(
                        "a", callback="https://x/1", broadcaster_user_id="42"
                    ),
                    subscription_json(
                        "b", callback="https://x/2", broadcaster_user_id="42"
                    ),
                ]
            ),
            reply(204),
            reply(204),
            reply(202, {}),
            reply(202, {}),
        )

        await api.subscribe_to_user("alice")

        deleted = [r.url.query for r in script.requests if r.method == "DELETE"]
        assert deleted == [b"id=a", b"id=b"]
        assert len(notices) == 2


class TestUnsubscribe:
    async def test_an_invalid_login_is_false_without_a_request(
        self, helix_http: HttpFactory
    ) -> None:
        script = helix_http(reply(200, {"data": []}))

        assert await api.unsubscribe_to_user("bad name") is False
        assert script.requests == []

    async def test_an_unknown_user_is_false(self, helix_http: HttpFactory) -> None:
        helix_http(reply(200, {"data": []}))

        assert await api.unsubscribe_to_user("nobody") is False

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
