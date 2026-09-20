from collections.abc import Callable
from types import SimpleNamespace
from typing import Any
from unittest.mock import MagicMock

import discord
import pytest

import valmal.core.config as service_config
from tests.config_cache.support import role
from valmal.db.models import AutoResponseMatch, DiscordAutoResponse, DiscordChannel

pytestmark = pytest.mark.anyio


# --- auto responses -------------------------------------------------------------


class TestAutoResponse:
    def row(
        self,
        trigger: str,
        response: str = "r",
        match: AutoResponseMatch = AutoResponseMatch.EXACT,
        case_sensitive: bool = False,
    ) -> DiscordAutoResponse:
        return DiscordAutoResponse(
            trigger=trigger,
            response=response,
            match_type=match,
            case_sensitive=case_sensitive,
        )

    async def test_exact_matches_the_whole_message_only(self, load: Any) -> None:
        cache = await load(self.row("ping", "pong"))

        assert cache.auto_response("ping") == "pong"
        assert cache.auto_response("ping!") is None
        assert cache.auto_response("a ping") is None
        assert cache.auto_response("") is None

    async def test_prefix_matches_the_start(self, load: Any) -> None:
        cache = await load(self.row("!rules", "the rules", AutoResponseMatch.PREFIX))

        assert cache.auto_response("!rules please") == "the rules"
        assert cache.auto_response("!rules") == "the rules"
        assert cache.auto_response("read !rules") is None

    async def test_contains_matches_anywhere(self, load: Any) -> None:
        cache = await load(self.row("discord", "here", AutoResponseMatch.CONTAINS))

        assert cache.auto_response("join my discord now") == "here"
        assert cache.auto_response("discord") == "here"
        assert cache.auto_response("disco") is None

    async def test_case_is_ignored_unless_the_row_is_case_sensitive(
        self, load: Any
    ) -> None:
        cache = await load(
            self.row("Hello", "insensitive"),
            self.row("Secret", "sensitive", case_sensitive=True),
        )

        assert cache.auto_response("HELLO") == "insensitive"
        assert cache.auto_response("hello") == "insensitive"
        assert cache.auto_response("Secret") == "sensitive"
        assert cache.auto_response("secret") is None
        assert cache.auto_response("SECRET") is None

    async def test_the_first_row_that_matches_wins(self, load: Any) -> None:
        cache = await load(
            self.row("hel", "prefix wins", AutoResponseMatch.PREFIX),
            self.row("hello", "exact loses"),
        )

        assert cache.auto_response("hello") == "prefix wins"

    async def test_no_rows_no_answer(self, load: Any) -> None:
        assert (await load()).auto_response("anything") is None

    async def test_the_response_is_rendered(self, load: Any) -> None:
        cache = await load(
            DiscordChannel(key="rules", channel_id=42),
            self.row("where", "see {channel:rules}"),
        )

        assert cache.auto_response("where") == "see <#42>"

    async def test_a_dangling_slug_in_a_response_names_its_trigger(
        self, load: Any, notices: list
    ) -> None:
        cache = await load(self.row("where", "{role:gone}"))

        cache.auto_response("where")

        assert "discord_auto_response:where" in notices[0][0]


# --- has_configured_role --------------------------------------------------------


class TestHasConfiguredRole:
    def check(self) -> Callable[[Any], bool]:
        @service_config.has_configured_role("mods")
        async def command(interaction: discord.Interaction) -> None: ...

        (predicate,) = command.__discord_app_commands_checks__  # pyright: ignore[reportFunctionMemberAccess]
        return predicate

    def member(self, *role_ids: int) -> MagicMock:
        member = MagicMock(spec=discord.Member)
        member.roles = [SimpleNamespace(id=i) for i in role_ids]
        return member

    @pytest.fixture(autouse=True)
    def _roles(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(
            service_config.config, "_roles", {"mods": role("mods", 500)}
        )

    def test_true_for_a_member_holding_the_configured_role(self) -> None:
        interaction = SimpleNamespace(user=self.member(1, 500, 2))

        assert self.check()(interaction) is True

    def test_false_for_a_member_without_it(self) -> None:
        assert self.check()(SimpleNamespace(user=self.member(1, 2))) is False

    def test_false_for_a_member_with_no_roles(self) -> None:
        assert self.check()(SimpleNamespace(user=self.member())) is False

    def test_false_for_a_plain_user_outside_a_guild(self) -> None:
        user = MagicMock(spec=discord.User)

        assert self.check()(SimpleNamespace(user=user)) is False

    def test_the_role_id_is_read_when_the_command_runs_not_when_it_is_declared(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        check = self.check()
        member = self.member(900)
        assert check(SimpleNamespace(user=member)) is False

        monkeypatch.setattr(
            service_config.config, "_roles", {"mods": role("mods", 900)}
        )

        assert check(SimpleNamespace(user=member)) is True

    def test_a_role_with_no_row_raises_rather_than_quietly_admitting_anyone(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(service_config.config, "_roles", {})

        with pytest.raises(discord.app_commands.AppCommandError) as raised:
            self.check()(SimpleNamespace(user=self.member(500)))

        assert "No discord_role row keyed 'mods'" in str(raised.value)
        assert isinstance(raised.value.__cause__, KeyError)

    def test_that_fault_is_an_app_command_error_but_not_a_refusal(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """discord.py routes only an AppCommandError to the tree's handler, and a
        CheckFailure would be answered as 'not allowed' instead of reported."""
        monkeypatch.setattr(service_config.config, "_roles", {})

        with pytest.raises(discord.app_commands.AppCommandError) as raised:
            self.check()(SimpleNamespace(user=self.member(500)))

        assert not isinstance(raised.value, discord.app_commands.CheckFailure)
