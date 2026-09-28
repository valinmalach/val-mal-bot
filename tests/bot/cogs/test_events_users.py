from types import SimpleNamespace
from typing import Any
from unittest.mock import MagicMock

import discord
import pytest

from tests.bot.audit.support import person
from tests.bot.cogs.events_world import EventsWorld, cog

pytestmark = pytest.mark.anyio


class TestUserUpdate:
    def in_guild(self, ev: EventsWorld, member: Any) -> None:
        guild = MagicMock(spec=discord.Guild)
        guild.get_member = {member.id: member}.get
        ev.guilds[42] = guild

    def pair(self) -> tuple[Any, Any]:
        before = person(kind=discord.User, id=7)
        after = person(kind=discord.User, id=7, avatar="https://cdn.example/new.png")
        return before, after

    async def test_a_new_avatar_is_logged_for_the_member_it_belongs_to(
        self, ev: EventsWorld
    ) -> None:
        member = person(id=7)
        self.in_guild(ev, member)

        await cog(ev).on_user_update(*self.pair())

        assert ev.calls("pfp_changed") == [((member,), {})]

    async def test_a_member_with_a_guild_avatar_looks_no_different(
        self, ev: EventsWorld
    ) -> None:
        member = person(id=7)
        member.guild_avatar = SimpleNamespace(url="https://cdn.example/guild.png")
        self.in_guild(ev, member)

        await cog(ev).on_user_update(*self.pair())

        assert ev.audit == []

    async def test_someone_not_in_the_guild_is_not_logged(
        self, ev: EventsWorld
    ) -> None:
        self.in_guild(ev, person(id=8))

        await cog(ev).on_user_update(*self.pair())

        assert ev.audit == []

    async def test_no_guild_logs_nothing(self, ev: EventsWorld) -> None:
        await cog(ev).on_user_update(*self.pair())

        assert ev.audit == []

    async def test_a_change_that_kept_the_avatar_is_not_logged(
        self, ev: EventsWorld
    ) -> None:
        self.in_guild(ev, person(id=7))
        before, _ = self.pair()

        await cog(ev).on_user_update(before, person(kind=discord.User, id=7))

        assert ev.audit == []
