import asyncio
from collections.abc import AsyncIterator
from types import SimpleNamespace
from typing import Any

import discord
import pytest

from tests.bot.audit.support import attachment, channel, person
from tests.bot.cogs.events_world import (
    NOW,
    EventsWorld,
    cog,
    entry,
    guild_with_log,
    sent,
)
from valmal.bot.cogs import events

pytestmark = pytest.mark.anyio


class TestOnRawMessageDelete:
    def payload(self, cached: Any = None, guild_id: int | None = 5) -> Any:
        return SimpleNamespace(
            cached_message=cached, guild_id=guild_id, channel_id=55, message_id=9
        )

    async def test_a_cached_message_is_logged_whole_then_removed_from_storage(
        self, ev: EventsWorld
    ) -> None:
        cached = sent(content="gone", author=person(id=7))
        cached.attachments = [attachment()]
        where = channel()
        ev.channels[55] = where

        await cog(ev).on_raw_message_delete(self.payload(cached))

        ((_, kwargs),) = ev.calls("message_deleted")
        assert kwargs == {
            "content": "gone",
            "attachments": cached.attachments,
            "message_id": 9,
            "author": cached.author,
            "deleted_by": None,
            "channel": where,
        }
        assert ev.order == ["message_deleted", "delete_message"]
        assert ev.deleted == [9]

    async def test_an_uncached_message_falls_back_to_what_was_stored(
        self, ev: EventsWorld
    ) -> None:
        ev.rows[9] = SimpleNamespace(contents="stored", author_id=7)

        await cog(ev).on_raw_message_delete(self.payload())

        ((_, kwargs),) = ev.calls("message_deleted_uncached")
        assert kwargs["content"] == "stored" and kwargs["message_id"] == 9

    async def test_an_uncached_message_with_no_row_has_no_content_to_show(
        self, ev: EventsWorld
    ) -> None:
        await cog(ev).on_raw_message_delete(self.payload())

        ((_, kwargs),) = ev.calls("message_deleted_uncached")
        assert kwargs["content"] is None

    async def test_the_bots_own_message_is_ignored_entirely(
        self, ev: EventsWorld
    ) -> None:
        await cog(ev).on_raw_message_delete(self.payload(sent(author=ev.bot_user)))

        assert ev.audit == [] and ev.deleted == []

    async def test_the_deleter_is_a_recent_entry_about_the_stored_author(
        self, ev: EventsWorld
    ) -> None:
        guild_with_log(ev)
        mod = person(id=3)
        ev.rows[9] = SimpleNamespace(contents="stored", author_id=7)
        ev.audit_entries = [entry(person(id=4), target_id=8), entry(mod, target_id=7)]

        await cog(ev).on_raw_message_delete(self.payload())

        ((_, kwargs),) = ev.calls("message_deleted_uncached")
        assert kwargs["deleted_by"] is mod
        assert ev.audit_asked == [
            {
                "limit": 100,
                "action": discord.AuditLogAction.message_delete,
                "after": NOW.subtract(minutes=5),
                "oldest_first": True,
            }
        ]

    async def test_a_cached_message_names_the_entry_about_its_author(
        self, ev: EventsWorld
    ) -> None:
        guild_with_log(ev)
        mod = person(id=3)
        ev.audit_entries = [entry(person(id=4), target_id=8), entry(mod, target_id=7)]

        await cog(ev).on_raw_message_delete(self.payload(sent(author=person(id=7))))

        assert ev.calls("message_deleted")[0][1]["deleted_by"] is mod

    async def test_a_cached_message_is_matched_on_its_own_author(
        self, ev: EventsWorld
    ) -> None:
        guild_with_log(ev)
        ev.audit_entries = [entry(person(id=3), target_id=8)]

        await cog(ev).on_raw_message_delete(self.payload(sent(author=person(id=7))))

        assert ev.calls("message_deleted")[0][1]["deleted_by"] is None

    async def test_the_audit_log_and_the_stored_copy_are_read_together(
        self, ev: EventsWorld, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Each fake waits for the other to have started, so this only finishes if
        both are in flight at once."""
        audit_started, read_started = asyncio.Event(), asyncio.Event()

        async def entries(**_: object) -> AsyncIterator[Any]:
            audit_started.set()
            await read_started.wait()
            for found in ev.audit_entries:
                yield found

        async def get_message(message_id: int) -> Any:
            read_started.set()
            await audit_started.wait()
            return ev.rows.get(message_id)

        guild_with_log(ev)
        ev.guilds[5].audit_logs = entries
        monkeypatch.setattr(events.repository, "get_message", get_message)

        await asyncio.wait_for(cog(ev).on_raw_message_delete(self.payload()), 1)

        assert len(ev.calls("message_deleted_uncached")) == 1

    async def test_with_no_stored_author_nobody_is_named(self, ev: EventsWorld) -> None:
        """Any entry in the channel would fit, so naming one would be a guess."""
        guild_with_log(ev)
        ev.audit_entries = [entry(person(id=3), target_id=7)]

        await cog(ev).on_raw_message_delete(self.payload())

        assert ev.calls("message_deleted_uncached")[0][1]["deleted_by"] is None

    async def test_no_audit_entry_leaves_the_deleter_unnamed(
        self, ev: EventsWorld
    ) -> None:
        guild_with_log(ev)

        await cog(ev).on_raw_message_delete(self.payload())

        assert ev.calls("message_deleted_uncached")[0][1]["deleted_by"] is None

    @pytest.mark.parametrize("guild_id", [None, 404])
    async def test_a_deletion_with_no_reachable_guild_still_logs(
        self, guild_id: int | None, ev: EventsWorld
    ) -> None:
        await cog(ev).on_raw_message_delete(self.payload(guild_id=guild_id))

        assert ev.calls("message_deleted_uncached")[0][1]["deleted_by"] is None
        assert ev.audit_asked == []

    async def test_a_failed_row_removal_is_reported_by_message_id(
        self, ev: EventsWorld
    ) -> None:
        ev.fail.add("delete_message")

        await cog(ev).on_raw_message_delete(self.payload())

        assert ev.reported == ["Failed to delete message 9"]
