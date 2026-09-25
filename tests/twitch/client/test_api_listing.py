from collections.abc import Callable

import pytest

import valmal.twitch.client.api as api
from tests.twitch.support import (
    Script,
    page,
    reply,
    subscription_json,
    user_json,
)
from valmal.twitch.client import subscription_health as health

pytestmark = pytest.mark.anyio

HttpFactory = Callable[..., Script]


class TestGetUsers:
    async def test_batches_of_a_hundred_in_order(self, helix_http: HttpFactory) -> None:
        ids = [str(i) for i in range(250)]
        script = helix_http(
            *(
                reply(200, {"data": [user_json(i, f"u{i}") for i in ids[a : a + 100]]})
                for a in range(0, 250, 100)
            )
        )

        users = await api.get_users(ids)

        assert [len(r.url.params.get_list("id")) for r in script.requests] == [
            100,
            100,
            50,
        ]
        assert [u.id for u in users] == ids

    async def test_no_ids_makes_no_request(self, helix_http: HttpFactory) -> None:
        script = helix_http(reply(200, {"data": []}))

        assert await api.get_users([]) == []
        assert script.requests == []

    async def test_exactly_a_hundred_is_one_request(
        self, helix_http: HttpFactory
    ) -> None:
        ids = [str(i) for i in range(100)]
        script = helix_http(reply(200, {"data": [user_json(i) for i in ids]}))

        assert len(await api.get_users(ids)) == 100
        assert len(script.requests) == 1


class TestGetSubscriptions:
    async def test_follows_the_cursor_to_the_last_page(
        self, helix_http: HttpFactory
    ) -> None:
        script = helix_http(
            reply(200, page([subscription_json("a")], cursor="next")),
            reply(200, page([subscription_json("b")])),
        )

        subscriptions = await api.get_subscriptions()

        assert [s.id for s in subscriptions] == ["a", "b"]
        assert script.requests[0].url.query == b""
        assert script.requests[1].url.query == b"after=next"

    async def test_none_at_all_is_an_empty_list(self, helix_http: HttpFactory) -> None:
        helix_http(reply(200, page([])))

        assert await api.get_subscriptions() == []

    async def test_an_empty_page_ends_the_walk_even_with_a_cursor(
        self, helix_http: HttpFactory
    ) -> None:
        script = helix_http(reply(200, page([], cursor="loop")))

        assert await api.get_subscriptions() == []
        assert len(script.requests) == 1

    async def test_every_status_is_returned_unfiltered(
        self, helix_http: HttpFactory
    ) -> None:
        """A subscription Twitch disabled is the one worth seeing."""
        helix_http(
            reply(
                200,
                page(
                    [
                        subscription_json("a", status="enabled"),
                        subscription_json("b", status="notification_failures_exceeded"),
                    ]
                ),
            )
        )

        assert {s.status for s in await api.get_subscriptions()} == {
            "enabled",
            "notification_failures_exceeded",
        }

    async def test_broken_ones_are_named_by_type_and_target_with_their_reason(
        self, helix_http: HttpFactory
    ) -> None:
        helix_http(
            reply(
                200,
                page(
                    [
                        subscription_json("ok"),
                        subscription_json(
                            "bad",
                            type="stream.offline",
                            status="user_removed",
                            broadcaster_user_id="7",
                        ),
                        subscription_json(
                            "elsewhere",
                            type="channel.raid",
                            callback="https://old.example/webhook/twitch",
                            to_broadcaster_user_id="8",
                        ),
                    ]
                ),
            )
        )

        assert await health.broken_subscriptions() == {
            "stream.offline (broadcaster 7)": "user_removed",
            "channel.raid (to broadcaster 8)": (
                "calling back on https://old.example/webhook/twitch"
            ),
        }
