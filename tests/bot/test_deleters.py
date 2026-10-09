from types import SimpleNamespace
from typing import Any

import discord
import pytest

from tests.bot.audit.support import person
from tests.bot.cogs.events_world import (
    NOW,
    EventsWorld,
    bulk_entry,
    cog,
    entry,
    guild_with_log,
    install,
)
from valmal.bot import deleters
from valmal.bot.deleters import Claims

pytestmark = pytest.mark.anyio


@pytest.fixture
def ev(monkeypatch: pytest.MonkeyPatch) -> EventsWorld:
    return install(monkeypatch)


class TestRecentEntries:
    async def test_no_guild_id_means_no_audit_lookup(self, ev: EventsWorld) -> None:
        entries = await deleters.recent_entries(
            cog(ev).bot, None, discord.AuditLogAction.message_delete
        )

        assert entries == []

    @pytest.mark.parametrize(("found", "notices"), [(100, 1), (99, 0)])
    async def test_a_full_page_says_that_deletions_may_go_unnamed(
        self,
        found: int,
        notices: int,
        ev: EventsWorld,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Oldest first, so the entries a full page leaves out are the newest."""
        said: list[tuple[str, str | None]] = []

        async def notify(text: str, *, key: str | None = None) -> bool:
            said.append((text, key))
            return True

        monkeypatch.setattr(deleters, "notify", notify)
        guild_with_log(ev)
        ev.audit_entries = [entry(person(id=3)) for _ in range(found)]

        entries = await deleters.recent_entries(
            cog(ev).bot, 5, discord.AuditLogAction.message_delete
        )

        assert len(entries) == found
        assert [key for _, key in said] == ["audit-lookback:message_delete"] * notices


class TestMatching:
    def test_a_message_entry_must_be_about_this_channel_and_author(self) -> None:
        matches = deleters.about_message(55, 7)

        assert matches(entry(person(id=3), target_id=7))
        assert not matches(entry(person(id=3), channel_id=66, target_id=7))
        assert not matches(entry(person(id=3), target_id=8))

    def test_an_entry_with_no_channel_matches_nothing(self) -> None:
        bare: Any = SimpleNamespace(user=person(id=3), extra=None, target=None)

        assert not deleters.about_message(55, 7)(bare)

    def test_a_purge_entry_must_target_this_channel_and_count_its_messages(
        self,
    ) -> None:
        matches = deleters.about_purge(55, 20)

        assert matches(bulk_entry(person(id=3), count=20))
        assert not matches(bulk_entry(person(id=3), channel_id=66, count=20))
        assert not matches(bulk_entry(person(id=3), count=50))


class TestDeleter:
    """Discord logs no entry for deleting your own message, and folds a
    moderator's repeat deletions into one entry by raising its count."""

    def test_the_first_matching_entry_names_the_deleter(self, ev: EventsWorld) -> None:
        mod = person(id=3)
        entries = [entry(person(id=4), target_id=8), entry(mod, target_id=7)]

        assert Claims().deleter(entries, deleters.about_message(55, 7), 1) is mod

    def test_an_entry_whose_deletions_are_claimed_names_nobody_more(
        self, ev: EventsWorld
    ) -> None:
        """A moderator deletes one of Alice's messages, then Alice deletes another
        of her own: the moderator's entry is still the newest about her."""
        instance, matches = Claims(), deleters.about_message(55, 7)
        entries = [entry(person(id=3), target_id=7)]

        instance.deleter(entries, matches, 1)

        assert instance.deleter(entries, matches, 1) is None

    def test_deletions_folded_into_one_entry_each_name_the_deleter(
        self, ev: EventsWorld
    ) -> None:
        """Two quick deletions can both be looked up after Discord counted them."""
        instance, matches, mod = Claims(), deleters.about_message(55, 7), person(id=3)
        entries = [entry(mod, target_id=7, count=2)]

        named = [instance.deleter(entries, matches, 1) for _ in range(3)]

        assert named == [mod, mod, None]

    def test_an_entry_counted_up_later_names_its_deleter_again(
        self, ev: EventsWorld
    ) -> None:
        instance, matches, mod = Claims(), deleters.about_message(55, 7), person(id=3)
        first = entry(mod, target_id=7)
        instance.deleter([first], matches, 1)

        again = entry(mod, target_id=7, id=first.id, count=2)

        assert instance.deleter([again], matches, 1) is mod

    def test_a_purge_claims_its_whole_entry(self, ev: EventsWorld) -> None:
        instance, matches = Claims(), deleters.about_purge(55, 20)
        entries = [bulk_entry(person(id=3), count=20)]

        assert instance.deleter(entries, matches, 20) is not None
        assert instance.deleter(entries, matches, 20) is None

    def test_a_record_is_kept_while_a_lookup_could_still_fetch_its_entry(
        self, ev: EventsWorld
    ) -> None:
        """The lookup and the prune read the clock apart, so the record outlives
        the window rather than ending with it."""
        instance = Claims()
        edge = discord.utils.time_snowflake(NOW.subtract(minutes=6))
        stale = discord.utils.time_snowflake(NOW.subtract(minutes=11))
        instance._attributed = {edge: 1, stale: 1}

        instance.deleter([], deleters.about_purge(55, 1), 1)

        assert instance._attributed == {edge: 1}
