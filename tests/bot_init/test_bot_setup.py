from collections.abc import Coroutine, Iterator
from types import SimpleNamespace
from typing import Any
from unittest.mock import MagicMock

import discord
import pytest
from discord.ext import commands

import views
from cogs.tasks import Tasks
from init import bot_init
from services.twitch.shoutout_queue import shoutout_queue
from services.twitch.token_manager import token_manager
from valmal.core.config import config

pytestmark = pytest.mark.anyio

GUILD = 4321


class Startup:
    """What setup_hook did, in the order it did it."""

    def __init__(self) -> None:
        self.order: list[str] = []
        self.fired: list[tuple[str | None, Coroutine[Any, Any, None]]] = []
        self.synced: list[discord.Object] = []
        self.copied: list[discord.Object] = []
        self.views: list[object] = []
        self.tasks = MagicMock(spec=Tasks)
        self.settings: dict[str, Any] = {"guild_id": GUILD, "command_prefix": "!"}


@pytest.fixture
def startup(monkeypatch: pytest.MonkeyPatch) -> Iterator[Startup]:
    """Coroutines handed to fire_and_forget are closed afterwards, run or not."""
    startup = Startup()

    async def load_config() -> None:
        startup.order.append("config")

    async def load_tokens() -> None:
        startup.order.append("tokens")

    async def drain() -> None:
        startup.order.append("drain")

    def fire_and_forget(
        coro: Coroutine[Any, Any, None], *, name: str | None = None
    ) -> None:
        startup.order.append("fire")
        startup.fired.append((name, coro))

    def setting(key: str, default: Any = None) -> Any:
        return startup.settings.get(key, default)

    monkeypatch.setattr(config, "load", load_config)
    monkeypatch.setattr(token_manager, "load", load_tokens)
    monkeypatch.setattr(shoutout_queue, "drain", drain)
    monkeypatch.setattr(config, "setting", setting)
    monkeypatch.setattr(bot_init, "fire_and_forget", fire_and_forget)
    monkeypatch.setattr(
        views, "persistent_views", lambda: [SimpleNamespace(key=k) for k in "ab"]
    )
    yield startup
    for _, coro in startup.fired:
        coro.close()


@pytest.fixture
def instance(startup: Startup, monkeypatch: pytest.MonkeyPatch) -> bot_init.MyBot:
    """A bot of the same class that has never met Discord, so nothing is sent."""
    bot = bot_init.MyBot(command_prefix="$", intents=discord.Intents.none())

    async def sync(*, guild: discord.abc.Snowflake | None = None) -> list[object]:
        startup.order.append("sync")
        startup.synced.append(guild)  # pyright: ignore[reportArgumentType]
        return []

    def copy_global_to(*, guild: discord.abc.Snowflake) -> None:
        startup.order.append("copy")
        startup.copied.append(guild)  # pyright: ignore[reportArgumentType]

    def add_view(view: object, **_: object) -> None:
        startup.order.append("view")
        startup.views.append(view)

    monkeypatch.setattr(bot.tree, "sync", sync)
    monkeypatch.setattr(bot.tree, "copy_global_to", copy_global_to)
    monkeypatch.setattr(bot, "add_view", add_view)
    monkeypatch.setattr(bot, "get_cog", lambda name: startup.tasks)
    return bot


class TestInit:
    def test_commands_are_case_insensitive(self) -> None:
        bot = bot_init.MyBot(command_prefix="$", intents=discord.Intents.none())

        assert bot.case_insensitive is True

    def test_starts_with_the_prefix_it_was_given(self) -> None:
        bot = bot_init.MyBot(command_prefix="?", intents=discord.Intents.none())

        assert bot.command_prefix == "?"

    def test_the_process_wide_bot_is_one_of_these(self) -> None:
        assert isinstance(bot_init.bot, bot_init.MyBot)


class TestSetupHook:
    async def test_loads_the_configuration_before_anything_reads_it(
        self, instance: bot_init.MyBot, startup: Startup
    ) -> None:
        await instance.setup_hook()

        assert startup.order[:2] == ["config", "tokens"]

    async def test_starts_one_named_drainer_for_the_life_of_the_process(
        self, instance: bot_init.MyBot, startup: Startup
    ) -> None:
        await instance.setup_hook()

        ((name, coro),) = startup.fired
        assert name == "shoutout-queue"
        await coro
        assert startup.order.count("drain") == 1

    async def test_starts_both_task_loops_once_the_gateway_setup_they_wait_on_exists(
        self, instance: bot_init.MyBot, startup: Startup
    ) -> None:
        """From here rather than cog_load, where wait_until_ready() raises at once."""
        await instance.setup_hook()

        startup.tasks.check_birthdays.start.assert_called_once_with()
        startup.tasks.recheck_subscriptions.start.assert_called_once_with()

    async def test_a_missing_tasks_cog_starts_nothing_and_does_not_fail(
        self,
        instance: bot_init.MyBot,
        startup: Startup,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.setattr(instance, "get_cog", lambda name: None)

        await instance.setup_hook()

        startup.tasks.check_birthdays.start.assert_not_called()

    async def test_a_cog_that_is_not_the_tasks_cog_is_not_started_as_one(
        self,
        instance: bot_init.MyBot,
        startup: Startup,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        impostor = MagicMock(spec=commands.Cog)
        monkeypatch.setattr(instance, "get_cog", lambda name: impostor)

        await instance.setup_hook()

        assert not hasattr(impostor, "check_birthdays")

    async def test_the_prefix_comes_from_the_configuration(
        self, instance: bot_init.MyBot, startup: Startup
    ) -> None:
        await instance.setup_hook()

        assert instance.command_prefix == "!"

    async def test_a_missing_prefix_row_falls_back_to_dollar(
        self, instance: bot_init.MyBot, startup: Startup
    ) -> None:
        del startup.settings["command_prefix"]

        await instance.setup_hook()

        assert instance.command_prefix == "$"

    async def test_copies_the_global_commands_to_the_guild_then_syncs_that_guild(
        self, instance: bot_init.MyBot, startup: Startup
    ) -> None:
        """Guild sync is instant; a global one takes up to an hour to appear."""
        await instance.setup_hook()

        assert [g.id for g in startup.copied] == [GUILD]
        assert [g.id for g in startup.synced] == [GUILD]
        assert startup.order.index("copy") < startup.order.index("sync")

    async def test_registers_every_persistent_view_so_buttons_survive_a_restart(
        self, instance: bot_init.MyBot, startup: Startup
    ) -> None:
        await instance.setup_hook()

        assert [v.key for v in startup.views] == ["a", "b"]  # pyright: ignore[reportAttributeAccessIssue]

    async def test_views_are_registered_after_the_sync_so_a_failed_sync_leaves_none_half_done(
        self, instance: bot_init.MyBot, startup: Startup
    ) -> None:
        await instance.setup_hook()

        assert startup.order.index("sync") < startup.order.index("view")

    async def test_a_failed_sync_propagates_so_the_bot_does_not_start_half_configured(
        self,
        instance: bot_init.MyBot,
        startup: Startup,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        async def refuse(**_: object) -> list[object]:
            raise discord.HTTPException(MagicMock(status=500, reason="x"), "down")

        monkeypatch.setattr(instance.tree, "sync", refuse)

        with pytest.raises(discord.HTTPException):
            await instance.setup_hook()

        assert startup.views == []

    async def test_a_configuration_that_will_not_load_stops_before_anything_starts(
        self,
        instance: bot_init.MyBot,
        startup: Startup,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        async def refuse() -> None:
            raise ConnectionError("database down")

        monkeypatch.setattr(config, "load", refuse)

        with pytest.raises(ConnectionError):
            await instance.setup_hook()

        assert startup.order == [] and startup.fired == []


class TestClose:
    async def test_disposes_the_database_pool_after_the_bot_has_closed(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        order: list[str] = []

        async def close(self: commands.Bot) -> None:
            order.append("bot")

        async def dispose() -> None:
            order.append("engine")

        monkeypatch.setattr(commands.Bot, "close", close)
        monkeypatch.setattr("valmal.db.session.dispose_engine", dispose)
        bot = bot_init.MyBot(command_prefix="$", intents=discord.Intents.none())

        await bot.close()

        assert order == ["bot", "engine"]
