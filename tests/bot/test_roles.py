import asyncio
from collections.abc import Callable, Coroutine
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
        self.role = MagicMock(
            spec=discord.Role, id=ROLE, mention="<@&3>", managed=False
        )
        self.role.is_default.return_value = False
        self.role.__ge__ = MagicMock(return_value=False)
        self.member = MagicMock(spec=discord.Member, id=USER)
        self.member.add_roles = AsyncMock()
        self.member.remove_roles = AsyncMock()
        self.member.get_role = self.held_role
        self.holds = False
        self.guild: MagicMock | None = MagicMock(spec=discord.Guild)
        self.guild.get_member = self.member_by_id
        self.guild.get_role = self.role_by_id
        self.me = MagicMock(spec=discord.Member)
        self.me.guild_permissions.manage_roles = True
        self.guild.me = self.me
        self.guild.fetch_member = AsyncMock(return_value=self.member)
        self.member.guild = self.guild
        self.members: dict[int, Any] = {USER: self.member}
        self.roles: dict[int, Any] = {ROLE: self.role}
        self.fired: list[Coroutine[Any, Any, None]] = []
        self.notified: list[tuple[str, str | None]] = []
        self.reported: list[tuple[Exception, str]] = []
        self.deferred: list[tuple[bool, bool]] = []
        self.sent: list[tuple[str, bool]] = []

    def held_role(self, role_id: int) -> Any:
        return self.role if self.holds else None

    def member_by_id(self, user_id: int) -> Any:
        return self.members.get(user_id)

    def role_by_id(self, role_id: int) -> Any:
        return self.roles.get(role_id)

    def interaction(self, guild_id: int | None = GUILD) -> Any:
        async def defer(*, ephemeral: bool, thinking: bool) -> None:
            self.deferred.append((ephemeral, thinking))

        async def send(text: str, *, ephemeral: bool) -> None:
            self.sent.append((text, ephemeral))

        return SimpleNamespace(
            guild_id=guild_id,
            user=SimpleNamespace(id=USER),
            response=SimpleNamespace(defer=defer),
            followup=SimpleNamespace(send=send),
        )


def button(custom_id: str | None) -> Any:
    return SimpleNamespace(custom_id=custom_id)


@pytest.fixture
def world(monkeypatch: pytest.MonkeyPatch) -> Guild:
    world = Guild()

    async def notify(text: str, *, key: str | None = None) -> bool:
        world.notified.append((text, key))
        return True

    async def report(exc: Exception, context: str) -> None:
        world.reported.append((exc, context))

    def fire_and_forget(coro: Coroutine[Any, Any, None], *, name: str) -> None:
        world.fired.append(coro)

    def get_guild(guild_id: int) -> MagicMock | None:
        return world.guild if guild_id == GUILD else None

    monkeypatch.setattr(roles.bot, "get_guild", get_guild)
    monkeypatch.setattr(roles, "notify", notify)
    monkeypatch.setattr(roles, "report", report)
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


def lacks_manage_roles(w: Guild) -> None:
    w.me.guild_permissions.manage_roles = False


def is_managed(w: Guild) -> None:
    w.role.managed = True


def is_everyone(w: Guild) -> None:
    w.role.is_default.configure_mock(return_value=True)


def is_above_the_bot(w: Guild) -> None:
    w.role.__ge__.configure_mock(return_value=True)


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
        """The presser's reply must not wait on the admin channel."""
        world.roles.clear()

        assert roles.get_member_role(GUILD, USER, "role_member") == (None, None)

        (pending,) = world.fired
        assert world.notified == []
        await pending
        ((text, key),) = world.notified
        assert "'member'" in text and f"role id {ROLE}" in text
        assert key == "discord-role-missing:member"

    @pytest.mark.parametrize(
        ("unmanageable", "reason"),
        [
            (lacks_manage_roles, "the bot lacks the Manage Roles permission"),
            (is_managed, "it is @everyone or managed by an integration"),
            (is_everyone, "it is @everyone or managed by an integration"),
            (is_above_the_bot, "it is at or above the bot's top role"),
        ],
    )
    async def test_a_role_the_bot_cannot_manage_is_nothing_and_says_why(
        self, world: Guild, unmanageable: Callable[[Guild], None], reason: str
    ) -> None:
        unmanageable(world)

        assert roles.get_member_role(GUILD, USER, "role_member") == (None, None)

        (pending,) = world.fired
        await pending
        ((text, key),) = world.notified
        assert "'member'" in text and f"role id {ROLE}" in text and reason in text
        assert key == f"discord-role-unmanageable:member:{reason}"

    def test_the_hierarchy_is_judged_against_the_bots_top_role(
        self, world: Guild
    ) -> None:
        roles.get_member_role(GUILD, USER, "role_member")

        world.role.__ge__.assert_called_once_with(world.me.top_role)
        assert world.fired == []


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

    async def test_a_quick_second_press_undoes_the_first(self, world: Guild) -> None:
        """The cached member still shows the role as it was before the first press."""
        fresh = MagicMock(spec=discord.Member, id=USER)
        fresh.get_role = world.held_role

        async def add_roles(role: object) -> None:
            await asyncio.sleep(0)
            world.holds = True

        async def remove_roles(role: object) -> None:
            await asyncio.sleep(0)
            world.holds = False

        async def fetch_member(user_id: int) -> Any:
            await asyncio.sleep(0)
            return fresh

        def stale(role_id: int) -> None:
            return None

        fresh.add_roles, fresh.remove_roles = add_roles, remove_roles
        world.member.get_role = stale
        assert world.guild is not None
        world.guild.fetch_member = fetch_member

        first, second = await asyncio.gather(
            roles.toggle_role(GUILD, USER, "role_member"),
            roles.toggle_role(GUILD, USER, "role_member"),
        )

        assert (first, second) == ((True, world.role), (False, world.role))
        assert world.holds is False

    async def test_a_member_who_left_meanwhile_is_reported(self, world: Guild) -> None:
        gone = discord.NotFound(MagicMock(status=404, reason="x"), "Unknown Member")
        assert world.guild is not None
        world.guild.fetch_member.side_effect = gone

        assert await roles.toggle_role(GUILD, USER, "role_member") is None

        (pending,) = world.fired
        await pending
        assert world.reported == [(gone, f"Failed to toggle role id {ROLE}")]

    async def test_nothing_resolvable_is_none_and_touches_no_roles(
        self, world: Guild
    ) -> None:
        assert await roles.toggle_role(GUILD, USER, "role_other") is None

        world.member.add_roles.assert_not_awaited()
        world.member.remove_roles.assert_not_awaited()

    @pytest.mark.parametrize("holds", [False, True])
    async def test_a_refused_change_is_none_and_reported_without_awaiting_it(
        self, world: Guild, holds: bool
    ) -> None:
        world.holds = holds
        refused = discord.Forbidden(
            MagicMock(status=403, reason="x"), "Missing Permissions"
        )
        world.member.add_roles.side_effect = refused
        world.member.remove_roles.side_effect = refused

        assert await roles.toggle_role(GUILD, USER, "role_member") is None

        (pending,) = world.fired
        assert world.reported == []
        await pending
        assert world.reported == [(refused, f"Failed to toggle role id {ROLE}")]

    async def test_any_other_api_failure_is_caught_the_same_way(
        self, world: Guild
    ) -> None:
        gone = discord.NotFound(MagicMock(status=404, reason="x"), "Unknown Member")
        world.member.add_roles.side_effect = gone

        assert await roles.toggle_role(GUILD, USER, "role_member") is None

        (pending,) = world.fired
        await pending
        assert world.reported == [(gone, f"Failed to toggle role id {ROLE}")]


class TestRolesButtonPressed:
    async def test_defers_privately_before_changing_the_role(
        self, world: Guild
    ) -> None:
        """The role change is an API call, and Discord fails an unanswered press after 3s."""
        deferred_first: list[bool] = []

        def add_roles(role: object) -> None:
            deferred_first.append(world.deferred == [(True, True)])

        world.member.add_roles.side_effect = add_roles

        await roles.roles_button_pressed(world.interaction(), button("role_member"))

        assert deferred_first == [True]

    async def test_defers_even_when_nothing_can_be_resolved(self, world: Guild) -> None:
        await roles.roles_button_pressed(world.interaction(), button(None))

        assert world.deferred == [(True, True)]
        assert world.sent == [("cannot", True)]

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

    async def test_a_refused_change_is_the_generic_error_privately(
        self, world: Guild
    ) -> None:
        world.member.add_roles.side_effect = discord.Forbidden(
            MagicMock(status=403, reason="x"), "Missing Permissions"
        )

        await roles.roles_button_pressed(world.interaction(), button("role_member"))

        assert world.sent == [("cannot", True)]
        (pending,) = world.fired
        await pending

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
