import logging
from typing import Any

import pytest

from tests.bot_init.support import Errors
from valmal.bot import client as bot_init
from valmal.twitch.stream import live_alert, stream_session

pytestmark = pytest.mark.anyio


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
