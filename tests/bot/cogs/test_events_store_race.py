import asyncio
from collections.abc import Awaitable, Callable
from types import SimpleNamespace
from typing import Any

import pytest

from tests.bot.cogs.events_world import EventsWorld, cog, sent
from valmal.bot import audit
from valmal.bot.cogs import events
from valmal.bot.cogs.events import Events

pytestmark = pytest.mark.anyio

Delete = Callable[[Events], Awaitable[None]]


def deletion(message_id: int = 9) -> Any:
    return SimpleNamespace(
        cached_message=None, guild_id=None, channel_id=55, message_id=message_id
    )


def bulk_deletion(*ids: int) -> Any:
    return SimpleNamespace(message_ids=set(ids), guild_id=None, channel_id=55)


def delete_one(message_id: int = 9) -> Delete:
    return lambda instance: instance.on_raw_message_delete(deletion(message_id))


def delete_bulk(*ids: int) -> Delete:
    return lambda instance: instance.on_raw_bulk_message_delete(bulk_deletion(*ids))


async def overtaken(
    monkeypatch: pytest.MonkeyPatch,
    instance: Events,
    target: Any,
    name: str,
    handle: Awaitable[None],
    delete: Delete,
) -> None:
    """Runs the handler with `target.name` held until the deletion has finished."""
    started, release = asyncio.Event(), asyncio.Event()
    original = getattr(target, name)

    async def held(*args: Any) -> None:
        started.set()
        await release.wait()
        await original(*args)

    monkeypatch.setattr(target, name, held)
    handling = asyncio.ensure_future(handle)
    await started.wait()
    await delete(instance)
    release.set()
    await handling


class TestAStoreTheDeleteOvertook:
    async def test_takes_its_row_back_out(
        self, ev: EventsWorld, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        instance = cog(ev)
        await overtaken(
            monkeypatch,
            instance,
            events.repository,
            "upsert_message",
            instance.on_message(sent()),
            delete_one(),
        )

        assert ev.deleted == [9, 9]

    async def test_after_a_bulk_delete_too(
        self, ev: EventsWorld, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        instance = cog(ev)
        await overtaken(
            monkeypatch,
            instance,
            events.repository,
            "upsert_message",
            instance.on_message(sent()),
            delete_bulk(9),
        )

        assert ev.deleted == [9]

    async def test_an_edit_still_logging_when_the_delete_lands(
        self, ev: EventsWorld, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """An edit stores only after its audit entry, which can outlast a delete."""
        instance = cog(ev)
        payload: Any = SimpleNamespace(
            message=sent(content="new"), cached_message=sent(content="old")
        )
        await overtaken(
            monkeypatch,
            instance,
            audit,
            "message_edited",
            instance.on_raw_message_edit(payload),
            delete_one(),
        )

        assert ev.deleted == [9, 9]

    async def test_a_store_of_another_message_is_left_alone(
        self, ev: EventsWorld, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        instance = cog(ev)
        await overtaken(
            monkeypatch,
            instance,
            events.repository,
            "upsert_message",
            instance.on_message(sent()),
            delete_one(8),
        )

        assert ev.deleted == [8]


class TestTheMarks:
    async def test_a_delete_with_no_store_in_flight_leaves_none(
        self, ev: EventsWorld
    ) -> None:
        instance = cog(ev)

        await instance.on_raw_message_delete(deletion())
        await instance.on_message(sent())

        assert len(ev.stored) == 1 and ev.deleted == [9]
        assert instance._deleted == set()  # pyright: ignore[reportPrivateUsage]

    async def test_go_with_the_last_store_in_flight(
        self, ev: EventsWorld, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        instance = cog(ev)
        await overtaken(
            monkeypatch,
            instance,
            events.repository,
            "upsert_message",
            instance.on_message(sent()),
            delete_one(),
        )

        assert instance._storing == {}  # pyright: ignore[reportPrivateUsage]
        assert instance._deleted == set()  # pyright: ignore[reportPrivateUsage]

    async def test_go_even_when_the_handler_raises(self, ev: EventsWorld) -> None:
        instance = cog(ev)
        made = sent()
        ev.reply = "pong"
        made.channel.send.side_effect = RuntimeError("down")

        with pytest.raises(RuntimeError):
            await instance.on_message(made)

        assert instance._storing == {}  # pyright: ignore[reportPrivateUsage]

    async def test_stay_while_another_store_of_it_is_in_flight(
        self, ev: EventsWorld
    ) -> None:
        """A message and its edit can both be storing when the delete lands."""
        instance = cog(ev)
        with instance._may_store(9):  # pyright: ignore[reportPrivateUsage]
            with instance._may_store(9):  # pyright: ignore[reportPrivateUsage]
                instance._mark_deleted([9])  # pyright: ignore[reportPrivateUsage]

            assert instance._deleted == {9}  # pyright: ignore[reportPrivateUsage]

        assert instance._deleted == set()  # pyright: ignore[reportPrivateUsage]
