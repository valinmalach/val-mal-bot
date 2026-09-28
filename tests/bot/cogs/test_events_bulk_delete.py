from types import SimpleNamespace
from typing import Any

import discord
import pytest

from tests.bot.audit.support import channel, person
from tests.bot.cogs.events_world import (
    NOW,
    EventsWorld,
    bulk_entry,
    cog,
    guild_with_log,
)

pytestmark = pytest.mark.anyio


class TestOnRawBulkMessageDelete:
    def payload(self, ids: set[int]) -> Any:
        return SimpleNamespace(message_ids=ids, guild_id=5, channel_id=55)

    async def test_logs_one_entry_with_the_count_and_removes_every_row_at_once(
        self, ev: EventsWorld
    ) -> None:
        where = channel()
        ev.channels[55] = where

        await cog(ev).on_raw_bulk_message_delete(self.payload({1, 2, 3}))

        assert ev.calls("bulk_deleted") == [
            ((), {"count": 3, "deleted_by": None, "channel": where})
        ]
        assert ev.deleted_batches == [{1, 2, 3}]
        assert ev.deleted == []

    async def test_the_moderator_whose_purge_entry_targets_this_channel_is_named(
        self, ev: EventsWorld
    ) -> None:
        guild_with_log(ev)
        mod = person(id=3)
        ev.audit_entries = [bulk_entry(person(id=4), channel_id=66), bulk_entry(mod)]

        await cog(ev).on_raw_bulk_message_delete(self.payload({1}))

        assert ev.calls("bulk_deleted")[0][1]["deleted_by"] is mod
        assert ev.audit_asked == [
            {
                "limit": 10,
                "action": discord.AuditLogAction.message_bulk_delete,
                "after": NOW.subtract(minutes=5),
                "oldest_first": False,
            }
        ]

    async def test_a_batch_that_will_not_delete_is_reported_once(
        self, ev: EventsWorld
    ) -> None:
        ev.fail.add("delete_messages")

        await cog(ev).on_raw_bulk_message_delete(self.payload({1, 2, 3}))

        assert ev.reported == ["Failed to delete 3 bulk-deleted messages"]

    async def test_a_purge_of_nothing_logs_a_count_of_zero(
        self, ev: EventsWorld
    ) -> None:
        await cog(ev).on_raw_bulk_message_delete(self.payload(set()))

        assert ev.calls("bulk_deleted")[0][1]["count"] == 0
