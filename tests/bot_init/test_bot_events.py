import logging
from types import SimpleNamespace
from typing import Any
from unittest.mock import MagicMock

import discord
import pytest

from init import bot_init
from services.config import config
from services.twitch import live_alert, stream_session

pytestmark = pytest.mark.anyio


class Errors:
    def __init__(self) -> None:
        self.reported: list[tuple[BaseException, str]] = []
        self.notified: list[str] = []
        self.notify_result = True
        self.fired: list[str | None] = []
        self.templates: dict[str, str] = {
            "discord_startup": "started",
            "command_failed": "it failed",
            "command_no_permission": "not allowed",
        }
        self.calls: list[str] = []


@pytest.fixture
def errors(monkeypatch: pytest.MonkeyPatch) -> Errors:
    errors = Errors()

    async def report(exc: Exception, context: str, **_: object) -> None:
        errors.calls.append("report")
        errors.reported.append((exc, context))

    async def notify(text: str, *, key: str | None = None) -> bool:
        errors.notified.append(text)
        return errors.notify_result

    def template(key: str, **_: object) -> str:
        return errors.templates[key]

    monkeypatch.setattr(bot_init, "report", report)
    monkeypatch.setattr("errors.report", report)
    monkeypatch.setattr(bot_init, "notify", notify)
    monkeypatch.setattr(config, "template", template)
    monkeypatch.setattr(bot_init, "_started", False)
    monkeypatch.setattr(bot_init, "_announced", False)
    return errors


class Startup:
    def __init__(self) -> None:
        self.fail: dict[str, Exception] = {}
        self.ran: list[str] = []


@pytest.fixture
def arms(monkeypatch: pytest.MonkeyPatch) -> Startup:
    startup = Startup()

    async def restore_all() -> None:
        startup.ran.append("restore")
        if "restore" in startup.fail:
            raise startup.fail["restore"]

    async def resume() -> None:
        startup.ran.append("resume")
        if "resume" in startup.fail:
            raise startup.fail["resume"]

    monkeypatch.setattr(live_alert, "restore_all", restore_all)
    monkeypatch.setattr(stream_session, "resume", resume)
    return startup


class TestRunBackgroundTasks:
    async def test_brings_up_both_helpers_and_says_nothing_when_they_succeed(
        self, errors: Errors, arms: Startup
    ) -> None:
        bot_init._started = True

        await bot_init.run_background_tasks()

        assert sorted(arms.ran) == ["restore", "resume"]
        assert errors.reported == []
        assert bot_init._started

    async def test_a_failed_arm_is_reported_by_what_was_lost_and_the_other_still_runs(
        self, errors: Errors, arms: Startup
    ) -> None:
        arms.fail["restore"] = ConnectionError("db")

        await bot_init.run_background_tasks()

        assert "resume" in arms.ran
        ((exc, context),) = errors.reported
        assert isinstance(exc, ConnectionError)
        assert context == "Startup could not restore the live alert updaters"

    async def test_the_other_arm_is_named_for_what_it_lost_too(
        self, errors: Errors, arms: Startup
    ) -> None:
        arms.fail["resume"] = RuntimeError("helix")

        await bot_init.run_background_tasks()

        ((_, context),) = errors.reported
        assert context == "Startup could not resume the stream session"

    async def test_both_failing_is_two_reports(
        self, errors: Errors, arms: Startup
    ) -> None:
        arms.fail = {"restore": ValueError("a"), "resume": ValueError("b")}

        await bot_init.run_background_tasks()

        assert [c for _, c in errors.reported] == [
            "Startup could not restore the live alert updaters",
            "Startup could not resume the stream session",
        ]

    async def test_a_failure_clears_the_started_flag_so_a_reconnect_retries(
        self, errors: Errors, arms: Startup
    ) -> None:
        bot_init._started = True
        arms.fail["resume"] = RuntimeError("helix")

        await bot_init.run_background_tasks()

        assert not bot_init._started

    async def test_success_leaves_the_flag_alone_so_a_reconnect_does_not_repeat_it(
        self, errors: Errors, arms: Startup
    ) -> None:
        bot_init._started = True

        await bot_init.run_background_tasks()

        assert bot_init._started


class TestOnReady:
    @pytest.fixture(autouse=True)
    def _fire(self, monkeypatch: pytest.MonkeyPatch, errors: Errors) -> None:
        def fire_and_forget(coro: Any, *, name: str | None = None) -> None:
            errors.fired.append(name)
            coro.close()

        monkeypatch.setattr(bot_init, "fire_and_forget", fire_and_forget)

    async def test_the_first_ready_starts_the_helpers_and_announces(
        self, errors: Errors
    ) -> None:
        await bot_init.on_ready()

        assert errors.fired == ["startup-tasks"]
        assert errors.notified == ["started"]
        assert bot_init._started is True and bot_init._announced is True

    async def test_a_reconnect_after_success_does_neither_again(
        self, errors: Errors, caplog: pytest.LogCaptureFixture
    ) -> None:
        await bot_init.on_ready()

        with caplog.at_level(logging.INFO, logger=bot_init.logger.name):
            await bot_init.on_ready()

        assert errors.fired == ["startup-tasks"]
        assert errors.notified == ["started"]
        assert "Reconnected to Discord" in caplog.text

    async def test_an_undelivered_announcement_is_retried_but_the_helpers_are_not(
        self, errors: Errors
    ) -> None:
        errors.notify_result = False
        await bot_init.on_ready()
        assert bot_init._announced is False

        errors.notify_result = True
        await bot_init.on_ready()

        assert errors.notified == ["started", "started"]
        assert errors.fired == ["startup-tasks"]
        assert bot_init._announced is True

    async def test_helpers_that_failed_are_retried_on_the_next_ready(
        self, errors: Errors
    ) -> None:
        await bot_init.on_ready()
        bot_init._started = False

        await bot_init.on_ready()

        assert errors.fired == ["startup-tasks", "startup-tasks"]
        assert errors.notified == ["started"]

    async def test_started_is_set_before_the_helpers_run_so_an_overlap_cannot_start_two(
        self, errors: Errors, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        seen: list[bool] = []

        def fire_and_forget(coro: Any, *, name: str | None = None) -> None:
            seen.append(bot_init._started)
            coro.close()

        monkeypatch.setattr(bot_init, "fire_and_forget", fire_and_forget)

        await bot_init.on_ready()

        assert seen == [True]

    async def test_a_failed_send_does_not_stop_the_helpers_starting(
        self, errors: Errors
    ) -> None:
        errors.notify_result = False

        await bot_init.on_ready()

        assert errors.fired == ["startup-tasks"]


class TestGatewayFloor:
    async def test_is_registered_as_the_bots_error_handler(self) -> None:
        assert bot_init.bot.on_error is bot_init.on_error  # pyright: ignore[reportAttributeAccessIssue]

    async def test_reports_the_exception_in_flight_by_the_event_it_came_from(
        self, errors: Errors
    ) -> None:
        try:
            raise ValueError("boom")
        except ValueError:
            await bot_init.on_error("on_member_join", object())

        ((exc, context),) = errors.reported
        assert str(exc) == "boom"
        assert context == "Fatal error with on_member_join event"

    async def test_with_nothing_in_flight_it_reports_nothing(
        self, errors: Errors
    ) -> None:
        await bot_init.on_error("on_ready")

        assert errors.reported == []

    @pytest.mark.parametrize("exit", [KeyboardInterrupt, SystemExit])
    async def test_an_exit_is_not_a_bug_to_report(
        self, exit: type[BaseException], errors: Errors
    ) -> None:
        try:
            raise exit
        except BaseException:  # noqa: BLE001
            await bot_init.on_error("on_ready")

        assert errors.reported == []


def interaction(command: str | None = "birthday set", done: bool = False) -> Any:
    sent: list[tuple[str, str, bool]] = []

    async def send_message(text: str, *, ephemeral: bool) -> None:
        sent.append(("response", text, ephemeral))

    async def followup(text: str, *, ephemeral: bool) -> None:
        sent.append(("followup", text, ephemeral))

    return SimpleNamespace(
        command=SimpleNamespace(qualified_name=command) if command else None,
        response=SimpleNamespace(is_done=lambda: done, send_message=send_message),
        followup=SimpleNamespace(send=followup),
        sent=sent,
    )


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
