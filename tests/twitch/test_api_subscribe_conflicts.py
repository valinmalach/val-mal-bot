from collections.abc import Callable

import pytest

import valmal.twitch.client.api as api
from tests.twitch.support import (
    BOT_SETTINGS,
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
def notices(monkeypatch: pytest.MonkeyPatch) -> list[str]:
    seen: list[str] = []

    async def notify(text: str, *, key: str | None = None) -> bool:
        seen.append(text)
        return True

    monkeypatch.setattr(api, "notify", notify)
    return seen


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
                        callback="https://bot.example/webhook/twitch/offline",
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
