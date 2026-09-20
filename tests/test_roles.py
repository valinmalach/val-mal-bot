from collections.abc import Coroutine
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import discord
import pytest

from valmal.bot import roles
from valmal.core.config import config

pytestmark = pytest.mark.anyio

GUILD, USER, ROLE = 1, 2, 3


class Guild:
    """One guild with one member, one role and the configuration row that names it."""

    def __init__(self) -> None:
        self.role = MagicMock(spec=discord.Role, id=ROLE, mention="<@&3>")
        self.member = MagicMock(spec=discord.Member, id=USER)
        self.member.add_roles = AsyncMock()
        self.member.remove_roles = AsyncMock()
        self.member.get_role = lambda role_id: self.role if self.holds else None
        self.holds = False
        self.guild: MagicMock | None = MagicMock(spec=discord.Guild)
        self.guild.get_member = lambda user_id: self.members.get(user_id)
        self.guild.get_role = lambda role_id: self.roles.get(role_id)
        self.members: dict[int, Any] = {USER: self.member}
        self.roles: dict[int, Any] = {ROLE: self.role}
        self.fired: list[Coroutine[Any, Any, None]] = []
        self.notified: list[tuple[str, str | None]] = []
        self.sent: list[tuple[str, bool]] = []

    def interaction(self, guild_id: int | None = GUILD) -> Any:
        async def send_message(text: str, *, ephemeral: bool) -> None:
            self.sent.append((text, ephemeral))

        return SimpleNamespace(
            guild_id=guild_id,
            user=SimpleNamespace(id=USER),
            response=SimpleNamespace(send_message=send_message),
        )


def button(custom_id: str | None) -> Any:
    return SimpleNamespace(custom_id=custom_id)


@pytest.fixture
def world(monkeypatch: pytest.MonkeyPatch) -> Guild:
    world = Guild()

    async def notify(text: str, *, key: str | None = None) -> bool:
        world.notified.append((text, key))
        return True

    def fire_and_forget(coro: Coroutine[Any, Any, None], *, name: str) -> None:
        world.fired.append(coro)

    monkeypatch.setattr(
        roles.bot,
        "get_guild",
        lambda guild_id: world.guild if guild_id == GUILD else None,
    )
    monkeypatch.setattr(roles, "notify", notify)
    monkeypatch.setattr(roles, "fire_and_forget", fire_and_forget)
    stored = SimpleNamespace(key="member", role_id=ROLE)
    monkeypatch.setattr(config, "_roles_by_custom_id", {"role_member": stored})
    monkeypatch.setattr(
        config,
        "_templates",
        {
            "discord_role_error": "cannot",
            "discord_role_added": "added {role}",
            "discord_role_removed": "removed {role}",
        },
    )
    return world


class TestGetMemberRole:
    def test_resolves_the_member_and_the_role_a_custom_id_names(
        self, world: Guild
    ) -> None:
        assert roles.get_member_role(GUILD, USER, "role_member") == (
            world.member,
            world.role,
        )

    def test_an_unknown_guild_is_nothing_and_silent(self, world: Guild) -> None:
        assert roles.get_member_role(999, USER, "role_member") == (None, None)
        assert world.fired == []

    def test_a_guild_the_gateway_has_dropped_is_nothing(self, world: Guild) -> None:
        world.guild = None

        assert roles.get_member_role(GUILD, USER, "role_member") == (None, None)

    def test_a_member_who_left_is_nothing_and_silent(self, world: Guild) -> None:
        """Not an admin's problem, unlike a role that has gone."""
        world.members.clear()

        assert roles.get_member_role(GUILD, USER, "role_member") == (None, None)
        assert world.fired == []

    def test_a_custom_id_no_row_holds_is_nothing_and_silent(self, world: Guild) -> None:
        assert roles.get_member_role(GUILD, USER, "role_other") == (None, None)
        assert world.fired == []

    async def test_a_configured_role_the_guild_no_longer_has_is_reported_without_awaiting_it(
        self, world: Guild
    ) -> None:
        """Discord's 3 second acknowledgement deadline must not wait on the admin channel."""
        world.roles.clear()

        assert roles.get_member_role(GUILD, USER, "role_member") == (None, None)

        (pending,) = world.fired
        assert world.notified == []
        await pending
        ((text, key),) = world.notified
        assert "'member'" in text and f"role id {ROLE}" in text
        assert key == "discord-role-missing:member"


class TestToggleRole:
    async def test_a_member_without_the_role_is_given_it(self, world: Guild) -> None:
        assert await roles.toggle_role(GUILD, USER, "role_member") == (True, world.role)

        world.member.add_roles.assert_awaited_once_with(world.role)
        world.member.remove_roles.assert_not_awaited()

    async def test_a_member_with_the_role_loses_it(self, world: Guild) -> None:
        world.holds = True

        assert await roles.toggle_role(GUILD, USER, "role_member") == (
            False,
            world.role,
        )

        world.member.remove_roles.assert_awaited_once_with(world.role)
        world.member.add_roles.assert_not_awaited()

    async def test_nothing_resolvable_is_none_and_touches_no_roles(
        self, world: Guild
    ) -> None:
        assert await roles.toggle_role(GUILD, USER, "role_other") is None

        world.member.add_roles.assert_not_awaited()
        world.member.remove_roles.assert_not_awaited()

    async def test_a_refused_grant_propagates_so_the_button_is_not_told_it_worked(
        self, world: Guild
    ) -> None:
        world.member.add_roles.side_effect = discord.Forbidden(
            MagicMock(status=403, reason="x"), "Missing Permissions"
        )

        with pytest.raises(discord.Forbidden):
            await roles.toggle_role(GUILD, USER, "role_member")


class TestRolesButtonPressed:
    async def test_tells_the_presser_privately_the_role_was_added(
        self, world: Guild
    ) -> None:
        await roles.roles_button_pressed(world.interaction(), button("role_member"))

        assert world.sent == [("added <@&3>", True)]

    async def test_tells_them_it_was_removed(self, world: Guild) -> None:
        world.holds = True

        await roles.roles_button_pressed(world.interaction(), button("role_member"))

        assert world.sent == [("removed <@&3>", True)]

    async def test_an_unresolvable_role_is_the_generic_error_privately(
        self, world: Guild
    ) -> None:
        await roles.roles_button_pressed(world.interaction(), button("role_other"))

        assert world.sent == [("cannot", True)]

    async def test_a_button_with_no_custom_id_is_the_same_answer_and_toggles_nothing(
        self, world: Guild
    ) -> None:
        await roles.roles_button_pressed(world.interaction(), button(None))

        assert world.sent == [("cannot", True)]
        world.member.add_roles.assert_not_awaited()

    async def test_an_empty_custom_id_is_treated_as_none(self, world: Guild) -> None:
        await roles.roles_button_pressed(world.interaction(), button(""))

        assert world.sent == [("cannot", True)]

    async def test_a_press_outside_any_guild_is_the_generic_error(
        self, world: Guild
    ) -> None:
        await roles.roles_button_pressed(
            world.interaction(guild_id=None), button("role_member")
        )

        assert world.sent == [("cannot", True)]
        world.member.add_roles.assert_not_awaited()
