from typing import Any
from unittest.mock import MagicMock

import discord
import pytest

from cogs.birthday import Birthday
from init import bot_init
from tests.bot_init.support import Errors, interaction
from valmal.core.config import config

pytestmark = pytest.mark.anyio


class TestSlashCommandFloor:
    async def test_is_registered_on_the_tree(self) -> None:
        assert bot_init.bot.tree.on_error is bot_init.on_app_command_error

    async def test_a_command_that_raised_is_reported_then_the_person_is_told(
        self, errors: Errors
    ) -> None:
        asked = interaction()
        boom = discord.app_commands.CommandInvokeError(MagicMock(), ValueError("x"))

        await bot_init.on_app_command_error(asked, boom)

        assert errors.reported == [(boom, "Unhandled error in /birthday set")]
        assert asked.sent == [("response", "it failed", True)]

    async def test_the_report_comes_before_the_answer(self, errors: Errors) -> None:
        """Whatever happens when answering, the original failure is on record."""
        asked = interaction()

        async def slow(text: str, *, ephemeral: bool) -> None:
            errors.calls.append("answer")

        asked.response.send_message = slow

        await bot_init.on_app_command_error(
            asked, discord.app_commands.AppCommandError()
        )

        assert errors.calls == ["report", "answer"]

    async def test_a_refused_permission_answers_the_person_and_reports_nothing(
        self, errors: Errors
    ) -> None:
        asked = interaction("purge")

        await bot_init.on_app_command_error(
            asked, discord.app_commands.MissingPermissions(["administrator"])
        )

        assert errors.reported == []
        assert asked.sent == [("response", "not allowed", True)]

    async def test_a_command_the_tree_could_not_name_is_reported_as_unknown(
        self, errors: Errors
    ) -> None:
        await bot_init.on_app_command_error(
            interaction(None), discord.app_commands.AppCommandError()
        )

        assert errors.reported[0][1] == "Unhandled error in /unknown"

    async def test_an_interaction_already_answered_gets_a_followup_instead(
        self, errors: Errors
    ) -> None:
        asked = interaction(done=True)

        await bot_init.on_app_command_error(
            asked, discord.app_commands.AppCommandError()
        )

        assert asked.sent == [("followup", "it failed", True)]

    async def test_an_answer_that_cannot_be_sent_is_reported_and_never_raised(
        self, errors: Errors
    ) -> None:
        asked = interaction()

        async def refuse(text: str, *, ephemeral: bool) -> None:
            raise discord.HTTPException(MagicMock(status=404, reason="x"), "gone")

        asked.response.send_message = refuse

        await bot_init.on_app_command_error(
            asked, discord.app_commands.AppCommandError()
        )

        assert [c for _, c in errors.reported] == [
            "Unhandled error in /birthday set",
            "Could not tell anyone that /birthday set failed",
        ]

    async def test_a_template_that_cannot_be_read_is_reported_the_same_way(
        self, errors: Errors
    ) -> None:
        del errors.templates["command_failed"]

        await bot_init.on_app_command_error(
            interaction(), discord.app_commands.AppCommandError()
        )

        assert errors.reported[-1][1] == (
            "Could not tell anyone that /birthday set failed"
        )

    async def test_a_refused_permission_that_cannot_be_answered_says_lacked_permission(
        self, errors: Errors
    ) -> None:
        del errors.templates["command_no_permission"]

        await bot_init.on_app_command_error(
            interaction("purge"), discord.app_commands.MissingPermissions(["x"])
        )

        assert errors.reported[-1][1] == (
            "Could not tell anyone that /purge lacked permission"
        )


class TestARefusedCheck:
    @pytest.mark.parametrize(
        "refusal",
        [
            discord.app_commands.MissingPermissions(["administrator"]),
            discord.app_commands.CheckFailure("the follower role check failed"),
            discord.app_commands.MissingRole("follower"),
            discord.app_commands.NoPrivateMessage(),
        ],
        ids=lambda error: type(error).__name__,
    )
    async def test_answers_the_person_and_reports_nothing(
        self, refusal: discord.app_commands.AppCommandError, errors: Errors
    ) -> None:
        asked = interaction("birthday set")

        await bot_init.on_app_command_error(asked, refusal)

        assert errors.reported == []
        assert asked.sent == [("response", "not allowed", True)]

    @pytest.mark.parametrize(
        "fault",
        [
            discord.app_commands.BotMissingPermissions(["manage_messages"]),
            discord.app_commands.CommandOnCooldown(
                discord.app_commands.Cooldown(1, 60), 30.0
            ),
        ],
        ids=lambda error: type(error).__name__,
    )
    async def test_a_check_failure_that_is_no_refusal_is_still_reported(
        self, fault: discord.app_commands.AppCommandError, errors: Errors
    ) -> None:
        """The bot lacking a permission is a fault. A cooldown is no refusal of the
        person, so it must not be answered as one; no command has a cooldown yet, and
        the general report is what will tell somebody the first time one does."""
        asked = interaction("purge")

        await bot_init.on_app_command_error(asked, fault)

        assert errors.reported == [(fault, "Unhandled error in /purge")]
        assert asked.sent == [("response", "it failed", True)]

    async def test_somebody_without_the_follower_role_is_refused_not_reported(
        self, errors: Errors, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """The real check on the real command, into the real handler.

        has_configured_role fails as a plain CheckFailure, which is what
        discord.py raises when a check returns False, and the handler once
        reported that to the admin channel as an unhandled bug on every attempt.
        """
        member = MagicMock(spec=discord.Member)
        member.roles = [MagicMock(id=1)]
        monkeypatch.setattr(config, "_roles", {"follower": MagicMock(role_id=2)})
        asked = interaction("birthday set")
        asked.user = member
        command: Any = Birthday.set_birthday

        assert await command._check_can_run(asked) is False
        refused = discord.app_commands.CheckFailure(
            "The check functions for command 'set' failed."
        )
        await bot_init.on_app_command_error(asked, refused)

        assert errors.reported == []
        assert asked.sent == [("response", "not allowed", True)]

    async def test_somebody_with_the_follower_role_passes_the_same_check(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        member = MagicMock(spec=discord.Member)
        member.roles = [MagicMock(id=2)]
        monkeypatch.setattr(config, "_roles", {"follower": MagicMock(role_id=2)})
        asked = interaction("birthday set")
        asked.user = member
        command: Any = Birthday.set_birthday

        assert await command._check_can_run(asked) is True

    async def test_a_role_row_that_is_gone_is_reported_and_the_person_told_it_failed(
        self, errors: Errors, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """The check cannot resolve the role, which is a fault and not a refusal."""
        member = MagicMock(spec=discord.Member)
        member.roles = []
        monkeypatch.setattr(config, "_roles", {})
        asked = interaction("birthday set")
        asked.user = member
        command: Any = Birthday.set_birthday

        with pytest.raises(discord.app_commands.AppCommandError) as fault:
            await command._check_can_run(asked)
        await bot_init.on_app_command_error(asked, fault.value)

        assert [context for _, context in errors.reported] == [
            "Unhandled error in /birthday set"
        ]
        assert asked.sent == [("response", "it failed", True)]
