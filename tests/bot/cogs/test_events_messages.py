from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock

import discord
import pytest

from tests.bot.audit.support import attachment, channel, person
from tests.bot.cogs.events_world import EventsWorld, cog, guild_with_log, sent

pytestmark = pytest.mark.anyio


class TestOnMessage:
    async def test_stores_the_message_with_where_and_who(self, ev: EventsWorld) -> None:
        made = sent(content="hi", author=person(id=7))
        made.attachments = [attachment("https://a"), attachment("https://b")]

        await cog(ev).on_message(made)

        assert ev.stored == [(9, "hi", 5, 7, 55, ["https://a", "https://b"])]

    async def test_a_direct_message_is_filed_under_the_configured_guild(
        self, ev: EventsWorld
    ) -> None:
        made = sent()
        made.guild = None

        await cog(ev).on_message(made)

        assert ev.stored[0][2] == 42

    async def test_the_bots_own_message_is_neither_stored_nor_answered(
        self, ev: EventsWorld
    ) -> None:
        made = sent(author=ev.bot_user)
        ev.reply = "pong"

        await cog(ev).on_message(made)

        assert ev.stored == []
        made.channel.send.assert_not_awaited()

    async def test_another_bots_message_is_treated_like_anyones(
        self, ev: EventsWorld
    ) -> None:
        await cog(ev).on_message(sent(author=person(id=2)))

        assert len(ev.stored) == 1

    async def test_a_matching_auto_response_is_sent_to_the_same_channel(
        self, ev: EventsWorld
    ) -> None:
        made = sent(content="ping")
        ev.reply = "pong"

        await cog(ev).on_message(made)

        made.channel.send.assert_awaited_once()
        assert made.channel.send.await_args.args == ("pong",)

    async def test_the_reply_can_mention_nobody_because_anyone_can_trigger_it(
        self, ev: EventsWorld
    ) -> None:
        made = sent()
        ev.reply = "hello <@&123>"

        await cog(ev).on_message(made)

        mentions = made.channel.send.await_args.kwargs["allowed_mentions"]
        assert isinstance(mentions, discord.AllowedMentions)
        assert (mentions.everyone, mentions.users, mentions.roles) == (
            False,
            False,
            False,
        )

    async def test_no_matching_response_sends_nothing(self, ev: EventsWorld) -> None:
        made = sent()

        await cog(ev).on_message(made)

        made.channel.send.assert_not_awaited()

    async def test_an_empty_reply_is_still_a_reply_only_none_means_no_match(
        self, ev: EventsWorld
    ) -> None:
        made = sent()
        ev.reply = ""

        await cog(ev).on_message(made)

        made.channel.send.assert_awaited_once()

    async def test_a_failed_store_is_reported_and_the_reply_still_goes_out(
        self, ev: EventsWorld
    ) -> None:
        made = sent()
        ev.fail.add("store")
        ev.reply = "pong"

        await cog(ev).on_message(made)

        assert ev.reported == ["Failed to store message 9"]
        made.channel.send.assert_awaited_once()

    async def test_stores_before_answering(self, ev: EventsWorld) -> None:
        made = sent()
        ev.reply = "pong"
        made.channel.send = AsyncMock(
            side_effect=lambda *a, **k: ev.order.append("send")
        )

        await cog(ev).on_message(made)

        assert ev.order == ["store", "send"]


class TestOnRawMessageEdit:
    def payload(self, before: Any, after: Any) -> Any:
        return SimpleNamespace(message=after, cached_message=before)

    async def test_a_changed_message_is_logged_with_the_old_text_and_stored_anew(
        self, ev: EventsWorld
    ) -> None:
        before, after = sent(content="old"), sent(content="new")

        await cog(ev).on_raw_message_edit(self.payload(before, after))

        assert ev.calls("message_edited") == [((after, "old"), {})]
        assert ev.stored[0][1] == "new"

    async def test_an_edit_that_changed_nothing_is_not_logged_or_stored(
        self, ev: EventsWorld
    ) -> None:
        """Link previews and embeds arrive as edits with the same text."""
        before, after = sent(content="same"), sent(content="same")

        await cog(ev).on_raw_message_edit(self.payload(before, after))

        assert ev.audit == [] and ev.stored == []

    @pytest.mark.parametrize("side", ["before", "after"])
    async def test_an_edit_to_the_bots_own_message_is_ignored(
        self, side: str, ev: EventsWorld
    ) -> None:
        mine = sent(content="a", author=ev.bot_user)
        theirs = sent(content="b")
        pair = (mine, theirs) if side == "before" else (theirs, mine)

        await cog(ev).on_raw_message_edit(self.payload(*pair))

        assert ev.audit == [] and ev.stored == []

    async def test_a_pin_is_logged_even_though_the_text_is_the_same(
        self, ev: EventsWorld
    ) -> None:
        before = sent(content="x", pinned=False)
        after = sent(content="x", pinned=True)

        await cog(ev).on_raw_message_edit(self.payload(before, after))

        assert ev.names == ["pin_changed"]
        assert ev.stored == []

    async def test_a_pin_and_an_edit_together_log_both(self, ev: EventsWorld) -> None:
        before = sent(content="old", pinned=False)
        after = sent(content="new", pinned=True)

        await cog(ev).on_raw_message_edit(self.payload(before, after))

        assert ev.names == ["pin_changed", "message_edited"]

    async def test_a_message_the_cache_dropped_is_compared_with_the_stored_copy(
        self, ev: EventsWorld
    ) -> None:
        ev.rows[9] = SimpleNamespace(contents="stored text")
        after = sent(content="new text")

        await cog(ev).on_raw_message_edit(self.payload(None, after))

        assert ev.calls("message_edited") == [((after, "stored text"), {})]

    async def test_uncached_and_unchanged_since_it_was_stored_is_not_logged(
        self, ev: EventsWorld
    ) -> None:
        ev.rows[9] = SimpleNamespace(contents="same")

        await cog(ev).on_raw_message_edit(self.payload(None, sent(content="same")))

        assert ev.audit == []

    async def test_an_attachment_only_message_stored_empty_is_not_a_change_when_still_empty(
        self, ev: EventsWorld
    ) -> None:
        """A row of "" is a message with no text, not a cache miss."""
        ev.rows[9] = SimpleNamespace(contents="")

        await cog(ev).on_raw_message_edit(self.payload(None, sent(content="")))

        assert ev.audit == []

    async def test_no_stored_copy_at_all_is_logged_as_an_edit_with_no_before(
        self, ev: EventsWorld
    ) -> None:
        after = sent(content="new")

        await cog(ev).on_raw_message_edit(self.payload(None, after))

        assert ev.calls("message_edited") == [((after, None), {})]

    async def test_a_pin_on_an_uncached_message_is_not_logged_because_there_is_nothing_to_compare(
        self, ev: EventsWorld
    ) -> None:
        await cog(ev).on_raw_message_edit(
            self.payload(None, sent(content="x", pinned=True))
        )

        assert "pin_changed" not in ev.names


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
        ev.rows[9] = SimpleNamespace(contents="stored")

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

    async def test_the_person_who_deleted_it_is_the_audit_logs_latest_entry(
        self, ev: EventsWorld
    ) -> None:
        guild_with_log(ev)
        mod = person(id=3)
        ev.audit_entries = [SimpleNamespace(user=mod)]

        await cog(ev).on_raw_message_delete(self.payload())

        ((_, kwargs),) = ev.calls("message_deleted_uncached")
        assert kwargs["deleted_by"] is mod
        assert ev.audit_asked == [
            {"limit": 1, "action": discord.AuditLogAction.message_delete}
        ]

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


class TestOnRawBulkMessageDelete:
    def payload(self, ids: set[int]) -> Any:
        return SimpleNamespace(message_ids=ids, guild_id=5, channel_id=55)

    async def test_logs_one_entry_with_the_count_and_removes_every_row(
        self, ev: EventsWorld
    ) -> None:
        where = channel()
        ev.channels[55] = where

        await cog(ev).on_raw_bulk_message_delete(self.payload({1, 2, 3}))

        assert ev.calls("bulk_deleted") == [
            ((), {"count": 3, "deleted_by": None, "channel": where})
        ]
        assert sorted(ev.deleted) == [1, 2, 3]

    async def test_asks_the_audit_log_for_a_bulk_delete_not_a_single_one(
        self, ev: EventsWorld
    ) -> None:
        guild_with_log(ev)

        await cog(ev).on_raw_bulk_message_delete(self.payload({1}))

        assert ev.audit_asked == [
            {"limit": 1, "action": discord.AuditLogAction.message_bulk_delete}
        ]

    async def test_one_row_that_will_not_delete_does_not_stop_the_rest(
        self, ev: EventsWorld
    ) -> None:
        ev.fail.add("delete_message")

        await cog(ev).on_raw_bulk_message_delete(self.payload({1, 2, 3}))

        assert sorted(ev.reported) == [
            "Failed to delete message 1",
            "Failed to delete message 2",
            "Failed to delete message 3",
        ]

    async def test_a_purge_of_nothing_logs_a_count_of_zero(
        self, ev: EventsWorld
    ) -> None:
        await cog(ev).on_raw_bulk_message_delete(self.payload(set()))

        assert ev.calls("bulk_deleted")[0][1]["count"] == 0
