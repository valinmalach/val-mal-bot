import asyncio
from collections.abc import Awaitable, Callable
from types import SimpleNamespace
from typing import Any

import pendulum
import pytest

from tests.bot.cogs.events_world import NOW, EventsWorld, cog, sent
from valmal.bot.cogs import events
from valmal.bot.cogs.events import Events

pytestmark = pytest.mark.anyio


def deletion(message_id: int = 9) -> Any:
    return SimpleNamespace(
        cached_message=None, guild_id=None, channel_id=55, message_id=message_id
    )


def bulk_deletion(*ids: int) -> Any:
    return SimpleNamespace(message_ids=set(ids), guild_id=None, channel_id=55)


async def store_overtaken_by(
    ev: EventsWorld,
    monkeypatch: pytest.MonkeyPatch,
    delete: Callable[[Events], Awaitable[None]],
) -> Events:
    """Stores message 9 with the upsert held until the deletion has finished."""
    instance = cog(ev)
    started, release = asyncio.Event(), asyncio.Event()

    async def slow_store(*args: Any) -> None:
        started.set()
        await release.wait()
        ev.store(*args)

    monkeypatch.setattr(events.repository, "upsert_message", slow_store)
    storing = asyncio.ensure_future(instance._store_message(sent()))  # pyright: ignore[reportPrivateUsage]
    await started.wait()
    await delete(instance)
    release.set()
    await storing
    return instance


class TestAStoreTheDeleteOvertook:
    async def test_takes_its_row_back_out(
        self, ev: EventsWorld, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        await store_overtaken_by(
            ev, monkeypatch, lambda c: c.on_raw_message_delete(deletion())
        )

        assert ev.deleted == [9, 9]

    async def test_after_a_bulk_delete_too(
        self, ev: EventsWorld, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        await store_overtaken_by(
            ev, monkeypatch, lambda c: c.on_raw_bulk_message_delete(bulk_deletion(9))
        )

        assert ev.deleted == [9]

    async def test_a_store_of_another_message_is_left_alone(
        self, ev: EventsWorld, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        await store_overtaken_by(
            ev, monkeypatch, lambda c: c.on_raw_message_delete(deletion(8))
        )

        assert ev.deleted == [8]


class TestTheDeletedIds:
    async def test_a_store_with_no_delete_writes_once(self, ev: EventsWorld) -> None:
        await cog(ev)._store_message(sent())  # pyright: ignore[reportPrivateUsage]

        assert len(ev.stored) == 1 and ev.deleted == []

    async def test_are_forgotten_once_no_store_can_still_be_in_flight(
        self, ev: EventsWorld, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        instance = cog(ev)
        await instance.on_raw_message_delete(deletion(9))

        def later(tz: object = None) -> pendulum.DateTime:
            return NOW.add(minutes=6)

        monkeypatch.setattr(pendulum, "now", later)
        await instance.on_raw_message_delete(deletion(8))

        assert list(instance._deleted) == [8]  # pyright: ignore[reportPrivateUsage]
