"""Rules about the bot's commands taken as a whole, so a new one cannot slip past them."""

import importlib
import re
from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import discord
import pytest
from discord import app_commands
from discord.ext.commands import Cog

from valmal.bot.cogs import COGS

pytestmark = pytest.mark.anyio

ROOT = Path(__file__).resolve().parents[2]
# Gated by a runtime identity check instead, which nothing can reconfigure away;
# adding an administrator check on top would lock the owner out of a guild where
# they are not also an admin.
OWNER_GATED = {"twitch-auth", "migrate-subscriptions"}


def cog_classes() -> list[type[Cog]]:
    found: list[type[Cog]] = []
    for name in COGS:
        module = importlib.import_module(name)
        found += [
            value
            for value in vars(module).values()
            if isinstance(value, type)
            and issubclass(value, Cog)
            and value is not Cog
            and value.__module__ == module.__name__
        ]
    return found


def commands() -> list[tuple[str, app_commands.Command[Any, ..., Any]]]:
    return [
        (cls.__name__, command)
        for cls in cog_classes()
        for command in cls.__cog_app_commands__
        if isinstance(command, app_commands.Command)
    ]


def admin_interaction(administrator: bool) -> Any:
    return MagicMock(permissions=discord.Permissions(administrator=administrator))


class TestTheCogList:
    def test_names_every_cog_module_and_nothing_else(self) -> None:
        """Nothing auto-discovers, so a cog missing here is never loaded."""
        on_disk = {
            f"valmal.bot.cogs.{path.stem}"
            for path in (ROOT / "valmal" / "bot" / "cogs").glob("*.py")
            if path.stem != "__init__"
        }

        assert set(COGS) == on_disk

    def test_no_cog_is_listed_twice(self) -> None:
        assert len(COGS) == len(set(COGS))

    @pytest.mark.parametrize("module", COGS)
    async def test_each_module_registers_exactly_one_cog_on_setup(
        self, module: str
    ) -> None:
        bot = MagicMock()
        bot.add_cog = AsyncMock()

        await importlib.import_module(module).setup(bot)

        bot.add_cog.assert_awaited_once()
        assert isinstance(bot.add_cog.await_args.args[0], Cog)

    @pytest.mark.parametrize("module", COGS)
    async def test_the_cog_is_handed_the_bot_it_will_run_on(self, module: str) -> None:
        bot = MagicMock()
        bot.add_cog = AsyncMock()

        await importlib.import_module(module).setup(bot)

        assert bot.add_cog.await_args.args[0].bot is bot


class TestEveryCommand:
    def test_there_are_commands_to_check(self) -> None:
        assert len(commands()) >= 8

    def test_names_are_unique_across_cogs(self) -> None:
        names = [command.qualified_name for _, command in commands()]

        assert len(names) == len(set(names))

    @pytest.mark.parametrize(
        ("cog", "command"), commands(), ids=lambda v: getattr(v, "name", v)
    )
    def test_a_name_and_description_discord_accepts(
        self, cog: str, command: app_commands.Command[Any, ..., Any]
    ) -> None:
        assert re.fullmatch(r"[-_a-z0-9]{1,32}", command.name), command.name
        assert 1 <= len(command.description) <= 100
        assert command.description != "…"

    @pytest.mark.parametrize(
        ("cog", "command"), commands(), ids=lambda v: getattr(v, "name", v)
    )
    def test_every_parameter_is_described_for_the_person_typing_it(
        self, cog: str, command: app_commands.Command[Any, ..., Any]
    ) -> None:
        for parameter in command.parameters:
            assert parameter.description != "…", (command.name, parameter.name)


class TestAdminCommandsAreEnforcedNotJustHinted:
    """default_permissions is only a hint a server admin can reconfigure away."""

    def gated(self) -> list[tuple[str, app_commands.Command[Any, ..., Any]]]:
        return [
            (cog, command)
            for cog, command in commands()
            if command.default_permissions is not None
        ]

    def test_there_are_admin_commands(self) -> None:
        assert len(self.gated()) >= 6

    def test_every_hinted_command_also_checks_at_runtime_or_is_owner_gated(
        self,
    ) -> None:
        unenforced = [
            command.name
            for _, command in self.gated()
            if not command.checks and command.name not in OWNER_GATED
        ]

        assert not unenforced

    def test_an_owner_gated_command_carries_no_administrator_check_on_top(self) -> None:
        for _, command in commands():
            if command.name in OWNER_GATED:
                assert command.checks == [], command.name

    def test_the_owner_gated_commands_exist(self) -> None:
        assert {command.name for _, command in commands()} >= OWNER_GATED

    def test_the_hint_asks_for_administrator(self) -> None:
        for _, command in self.gated():
            assert command.default_permissions is not None
            assert command.default_permissions.administrator, command.name

    @pytest.mark.parametrize("has", [True, False])
    async def test_the_runtime_check_lets_an_administrator_through_and_refuses_anyone_else(
        self, has: bool
    ) -> None:
        for _, command in self.gated():
            if command.name in OWNER_GATED:
                continue
            for check in command.checks:
                if has:
                    assert await discord.utils.maybe_coroutine(
                        check, admin_interaction(True)
                    )
                else:
                    with pytest.raises(app_commands.MissingPermissions):
                        await discord.utils.maybe_coroutine(
                            check, admin_interaction(False)
                        )
