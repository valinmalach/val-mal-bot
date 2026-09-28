from types import SimpleNamespace
from typing import Any

import discord
import pytest

from tests.bot.audit.support import person
from tests.bot.cogs.events_world import NOW, EventsWorld, bulk_entry, cog, entry, sent
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


class TestMatching:
    def test_a_message_entry_must_be_about_this_channel_and_author(self) -> None:
        matches = events._about_message(55, 7)

        assert matches(entry(person(id=3), target_id=7))
        assert not matches(entry(person(id=3), channel_id=66, target_id=7))
        assert not matches(entry(person(id=3), target_id=8))

    def test_with_no_known_author_the_channel_alone_decides(self) -> None:
        assert events._about_message(55, None)(entry(person(id=3), target_id=8))

    def test_an_entry_with_no_channel_matches_nothing(self) -> None:
        bare: Any = SimpleNamespace(user=person(id=3), extra=None, target=None)

        assert not events._about_message(55, None)(bare)

    def test_a_purge_entry_must_target_this_channel(self) -> None:
        matches = events._about_purge(55)

        assert matches(bulk_entry(person(id=3)))
        assert not matches(bulk_entry(person(id=3), channel_id=66))


class TestDeleter:
    """Discord logs no entry for deleting your own message, and folds a
    moderator's repeat deletions into one entry by raising its count."""

    def test_the_first_matching_entry_names_the_deleter(self, ev: EventsWorld) -> None:
        mod = person(id=3)
        entries = [entry(person(id=4), target_id=8), entry(mod, target_id=7)]

        assert cog(ev)._deleter(entries, events._about_message(55, 7)) is mod

    def test_an_entry_that_already_named_someone_does_not_again(
        self, ev: EventsWorld
    ) -> None:
        """A moderator deletes one of Alice's messages, then Alice deletes another
        of her own: the moderator's entry is still the newest about her."""
        instance, matches = cog(ev), events._about_message(55, 7)
        entries = [entry(person(id=3), target_id=7)]

        instance._deleter(entries, matches)

        assert instance._deleter(entries, matches) is None

    def test_an_entry_whose_count_rose_names_its_deleter_again(
        self, ev: EventsWorld
    ) -> None:
        instance, matches, mod = cog(ev), events._about_message(55, 7), person(id=3)
        first = entry(mod, target_id=7)
        instance._deleter([first], matches)

        again = instance._deleter(
            [entry(mod, target_id=7, id=first.id, count=2)], matches
        )

        assert again is mod

    def test_an_attribution_older_than_the_window_is_dropped(
        self, ev: EventsWorld
    ) -> None:
        """No lookup fetches it again, so keeping it would only grow the record."""
        instance = cog(ev)
        stale = discord.utils.time_snowflake(NOW.subtract(minutes=6))
        instance._attributed = {stale: 1}

        instance._deleter([], events._about_purge(55))

        assert not instance._attributed

    def test_a_second_deletion_takes_the_next_unclaimed_entry(
        self, ev: EventsWorld
    ) -> None:
        instance, matches = cog(ev), events._about_purge(55)
        first, second = person(id=3), person(id=4)
        entries = [bulk_entry(first), bulk_entry(second)]

        assert instance._deleter(entries, matches) is first
        assert instance._deleter(entries, matches) is second
