"""A signed delivery is also checked for freshness: replay.py and the router's
timestamp check."""

from datetime import datetime
from types import SimpleNamespace
from typing import Any

import httpx
import pytest

from tests.twitch.eventsub.webhook.support import (
    Hooks,
    delivery,
    iso,
    stream_online_payload,
)
from valmal.twitch import timestamps
from valmal.twitch.eventsub import replay

pytestmark = pytest.mark.anyio


async def post(client: httpx.AsyncClient, body: Any, **kwargs: Any) -> httpx.Response:
    raw, headers = delivery(body, **kwargs)
    return await client.post("/t", content=raw, headers=headers)


class TestFreshness:
    @pytest.mark.parametrize("delta", [-600, -1, 0, 1, 600])
    async def test_within_ten_minutes_either_way_is_fresh(
        self, delta: int, client: httpx.AsyncClient
    ) -> None:
        response = await post(client, stream_online_payload(), timestamp=iso(delta))

        assert response.status_code == 202

    @pytest.mark.parametrize("delta", [-601, 601, -86_400, 86_400])
    async def test_beyond_ten_minutes_in_either_direction_is_refused(
        self, delta: int, client: httpx.AsyncClient, hooks: Hooks
    ) -> None:
        """A clock ahead is as wrong as a clock behind."""
        response = await post(client, stream_online_payload(), timestamp=iso(delta))

        assert response.status_code == 403
        assert hooks.dispatched == []
        ((text, key),) = hooks.notified
        assert "Timestamp outside the 600s window" in text
        assert key == "stale-delivery:/t"

    @pytest.mark.parametrize(
        "timestamp", ["", "yesterday", "2026-06-15 12:00:00", "12:00"]
    )
    async def test_an_unreadable_timestamp_is_refused(
        self, timestamp: str, client: httpx.AsyncClient, hooks: Hooks
    ) -> None:
        response = await post(client, stream_online_payload(), timestamp=timestamp)

        assert response.status_code == 403
        ((text, key),) = hooks.notified
        assert text.startswith("403: Forbidden request on /t. Unreadable timestamp: ")
        assert key == "unreadable-timestamp:/t"

    async def test_a_date_that_cannot_exist_names_the_parsers_reason(
        self, client: httpx.AsyncClient, hooks: Hooks
    ) -> None:
        """The shape passed, so only the parser can say what was wrong with it."""
        response = await post(
            client, stream_online_payload(), timestamp="2026-02-30T12:00:00Z"
        )

        assert response.status_code == 403
        ((text, _),) = hooks.notified
        assert "day 30 must be in range" in text

    async def test_a_parser_that_breaks_is_a_reported_500_not_a_refusal(
        self, client: httpx.AsyncClient, hooks: Hooks, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """What pendulum did under lazy imports: a 403 here would have blamed
        Twitch, and spent its failure budget, for a fault of this end."""

        def broken(text: str) -> datetime:
            raise TypeError("'module' object is not callable")

        monkeypatch.setattr(
            timestamps, "datetime", SimpleNamespace(fromisoformat=broken)
        )

        response = await post(client, stream_online_payload(), timestamp=iso(0))

        assert response.status_code == 500
        assert hooks.reported == ["500: Internal server error on /t"]
        assert hooks.notified == []

    async def test_the_window_is_ten_minutes(self) -> None:
        assert replay.MESSAGE_WINDOW_SECONDS == 600
