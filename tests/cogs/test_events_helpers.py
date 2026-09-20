from types import SimpleNamespace

import discord
import pytest

from tests.cogs.events_world import EventsWorld, cog, sent

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
        assert await cog(ev)._get_audit_user(None, discord.AuditLogAction.kick) is None
