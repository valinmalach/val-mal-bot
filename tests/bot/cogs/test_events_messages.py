import asyncio
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock

import discord
import pytest

from tests.bot.audit.support import attachment, person
from tests.bot.cogs.events_world import (
    EventsWorld,
    cog,
    sent,
)
from valmal.bot.cogs import events

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

    async def test_another_bots_message_is_stored_but_never_answered(
        self, ev: EventsWorld
    ) -> None:
        made = sent(author=person(id=2, bot=True))
        ev.reply = "pong"

        await cog(ev).on_message(made)

        assert len(ev.stored) == 1
        made.channel.send.assert_not_awaited()

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

    async def test_the_reply_and_the_store_run_together(
        self, ev: EventsWorld, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Each fake waits for the other to have started, so this only finishes if
        neither waits on the other."""
        made = sent()
        ev.reply = "pong"
        stored, replied = asyncio.Event(), asyncio.Event()

        async def store(*_: object) -> None:
            stored.set()
            await replied.wait()

        async def send(*_: object, **__: object) -> None:
            replied.set()
            await stored.wait()

        monkeypatch.setattr(events.repository, "upsert_message", store)
        made.channel.send = send

        await asyncio.wait_for(cog(ev).on_message(made), 1)

        assert stored.is_set() and replied.is_set()

    async def test_a_store_that_raises_past_its_guard_is_raised_too(
        self, ev: EventsWorld, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Only on_error reports it, so the gather must not swallow it."""
        made = sent()
        ev.reply = "pong"
        instance = cog(ev)

        async def store(message: object) -> None:
            raise RuntimeError("built badly")

        monkeypatch.setattr(instance, "_store_message", store)

        with pytest.raises(RuntimeError, match="built badly"):
            await instance.on_message(made)

        made.channel.send.assert_awaited_once()

    async def test_a_failed_reply_still_stores_the_message_and_raises(
        self, ev: EventsWorld
    ) -> None:
        """Raised on to on_error, which reports it; the record is kept regardless."""
        made = sent()
        ev.reply = "pong"
        made.channel.send = AsyncMock(side_effect=discord.DiscordException("down"))

        with pytest.raises(discord.DiscordException):
            await cog(ev).on_message(made)

        assert len(ev.stored) == 1


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
