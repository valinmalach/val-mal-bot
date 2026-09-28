from types import SimpleNamespace
from typing import Any

import discord
import pytest

from tests.bot.audit.support import person
from tests.bot.cogs.events_world import EventsWorld, bulk_entry, cog, entry, sent
from valmal.bot.cogs import events

pytestmark = pytest.mark.anyio


class TestHelpers:
    async def test_a_stored_empty_string_is_a_row_not_a_miss(
        self, ev: EventsWorld
    ) -> None:
        ev.rows[9] = SimpleNamespace(contents="")

        assert await cog(ev)._get_message_content(9) == ""
        assert await cog(ev)._get_message_content(10) is None

    async def test_a_stored_message_with_no_text_is_none_from_its_column(
        self, ev: EventsWorld
    ) -> None:
        ev.rows[9] = SimpleNamespace(contents=None)

        assert await cog(ev)._get_message_content(9) is None

    async def test_is_bot_message_looks_at_every_message_given_and_skips_none(
        self, ev: EventsWorld
    ) -> None:
        instance = cog(ev)
        mine, theirs = sent(author=ev.bot_user), sent()

        assert instance._is_bot_message(theirs, mine) is True
        assert instance._is_bot_message(None, theirs) is False
        assert instance._is_bot_message(None, None) is False
        assert instance._is_bot_message() is False

    async def test_a_failed_write_is_reported_under_its_own_name_and_never_raised(
        self, ev: EventsWorld
    ) -> None:
        async def boom() -> None:
            raise RuntimeError("x")

        await cog(ev)._safe_db_operation("do the thing", boom())

        assert ev.reported == ["Failed to do the thing"]

    async def test_no_guild_id_means_no_audit_lookup(self, ev: EventsWorld) -> None:
        entries = await cog(ev)._recent_audit_entries(
            None, discord.AuditLogAction.message_delete
        )

        assert entries == []


class TestMessageDeleter:
    """Discord logs no entry for deleting your own message, so the newest entry
    is often about another deletion entirely."""

    def test_the_first_entry_about_this_channel_and_author_names_the_deleter(
        self,
    ) -> None:
        mod = person(id=3)
        entries = [entry(person(id=4), target_id=8), entry(mod, target_id=7)]

        assert events._message_deleter(entries, 55, 7) is mod

    def test_an_entry_about_another_channel_names_nobody(self) -> None:
        entries = [entry(person(id=3), channel_id=66, target_id=7)]

        assert events._message_deleter(entries, 55, 7) is None

    def test_an_entry_about_another_author_names_nobody(self) -> None:
        """The case that used to blame a moderator for a self-deletion."""
        entries = [entry(person(id=3), target_id=8)]

        assert events._message_deleter(entries, 55, 7) is None

    def test_with_no_known_author_the_channel_alone_decides(self) -> None:
        mod = person(id=3)

        assert events._message_deleter([entry(mod, target_id=8)], 55, None) is mod

    def test_an_entry_with_no_channel_names_nobody(self) -> None:
        bare: Any = SimpleNamespace(user=person(id=3), extra=None, target=None)

        assert events._message_deleter([bare], 55, None) is None


class TestBulkDeleter:
    def test_the_entry_targeting_this_channel_names_the_deleter(self) -> None:
        mod = person(id=3)
        entries = [bulk_entry(person(id=4), channel_id=66), bulk_entry(mod)]

        assert events._bulk_deleter(entries, 55) is mod

    def test_no_entry_for_this_channel_names_nobody(self) -> None:
        entries = [bulk_entry(person(id=4), channel_id=66)]

        assert events._bulk_deleter(entries, 55) is None
