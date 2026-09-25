import asyncio

import pytest

from tests.twitch.stream.live_alert.support import AlertWorld
from valmal.twitch.client.helix import HelixError
from valmal.twitch.stream import live_alert
from valmal.twitch.stream.live_alert_cycle import Action

pytestmark = pytest.mark.anyio

STARTED = "2026-06-15T11:00:00Z"


async def run(message_id: int = 900) -> None:
    await live_alert._run(111, 5001, message_id, 10, STARTED)


class TestCycleOrRetry:
    async def test_an_action_passes_straight_through(
        self, alert_world: AlertWorld
    ) -> None:
        alert_world.actions = [Action.REFRESH]

        result = await live_alert._cycle_or_retry(
            111, 5001, 900, 10, live_alert.pendulum.now(), "<t:1:f>", None
        )

        assert result is Action.REFRESH

    async def test_a_cycle_that_raised_concluded_nothing_and_must_not_take_the_updater_with_it(
        self, alert_world: AlertWorld
    ) -> None:
        alert_world.actions = [HelixError("down")]

        result = await live_alert._cycle_or_retry(
            111, 5001, 900, 10, live_alert.pendulum.now(), "<t:1:f>", None
        )

        assert result is Action.RETRY
        assert alert_world.reported == [
            "Error in the live alert update cycle for broadcaster_id=111"
        ]


class TestGivenUp:
    async def test_says_so_once_naming_the_broadcaster_the_message_and_the_count(
        self, alert_world: AlertWorld
    ) -> None:
        await live_alert._report_given_up(111, 900, 5)

        ((text, key),) = alert_world.notified
        assert "after 5 cycles that concluded nothing (message_id=900)" in text
        assert "the record is kept" in text
        assert key == "live-alert-gave-up:111"


class TestRun:
    async def test_stops_when_a_cycle_says_there_is_nothing_left_to_maintain(
        self, alert_world: AlertWorld
    ) -> None:
        alert_world.actions = [Action.STOP]

        await run()

        assert alert_world.cycles == 1

    async def test_keeps_cycling_while_the_stream_is_live_waiting_a_minute_between(
        self, alert_world: AlertWorld
    ) -> None:
        alert_world.actions = [Action.REFRESH, Action.REFRESH, Action.STOP]

        await run()

        assert alert_world.cycles == 3
        assert alert_world.waits == [60, 60, 60]

    async def test_gives_up_after_exactly_five_cycles_that_concluded_nothing(
        self, alert_world: AlertWorld
    ) -> None:
        """So a stuck alert cannot poll and report forever; the record is left for a
        restart or the next stream.offline."""
        alert_world.actions = [Action.RETRY] * 10

        await run()

        assert alert_world.cycles == 5
        assert [key for _, key in alert_world.notified] == ["live-alert-gave-up:111"]

    async def test_four_inconclusive_cycles_are_not_enough_to_give_up(
        self, alert_world: AlertWorld
    ) -> None:
        alert_world.actions = [Action.RETRY] * 4 + [Action.STOP]

        await run()

        assert alert_world.cycles == 5
        assert alert_world.notified == []

    async def test_a_conclusive_cycle_resets_the_count(
        self, alert_world: AlertWorld
    ) -> None:
        alert_world.actions = (
            [Action.RETRY] * 4 + [Action.REFRESH] + [Action.RETRY] * 4 + [Action.STOP]
        )

        await run()

        assert alert_world.cycles == 10
        assert alert_world.notified == []

    async def test_a_cycle_that_raises_counts_as_inconclusive_and_is_reported_each_time(
        self, alert_world: AlertWorld
    ) -> None:
        alert_world.actions = [RuntimeError("boom")] * 6

        await run()

        assert alert_world.cycles == 5
        assert len(alert_world.reported) == 5

    async def test_a_wake_cuts_the_wait_short_and_is_cleared_for_the_next(
        self, alert_world: AlertWorld
    ) -> None:
        """So an offline webhook is acted on at once rather than up to a full interval later."""
        wakeup = asyncio.Event()
        live_alert._wakeups[900] = wakeup
        wakeup.set()
        alert_world.wait_outcomes = [True]
        alert_world.actions = [Action.STOP]

        await run()

        assert alert_world.cycles == 1
        assert wakeup.is_set() is False

    async def test_the_cycle_gets_the_mention_only_the_alerts_channel_earns(
        self, alert_world: AlertWorld
    ) -> None:
        alert_world.actions = [Action.STOP]

        await run()

        assert alert_world.cycle_args[0][6] == "<@&777>"

    async def test_another_channel_gets_no_mention(
        self, alert_world: AlertWorld
    ) -> None:
        alert_world.actions = [Action.STOP]

        await live_alert._run(111, 5002, 900, 10, STARTED)

        assert alert_world.cycle_args[0][6] is None

    async def test_the_start_is_handed_over_as_a_discord_timestamp(
        self, alert_world: AlertWorld
    ) -> None:
        alert_world.actions = [Action.STOP]

        await run()

        assert alert_world.cycle_args[0][5] == "<t:1781521200:f>"

    async def test_an_unreadable_start_is_reported_by_the_updater_not_raised(
        self, alert_world: AlertWorld
    ) -> None:
        await live_alert._run(111, 5001, 900, 10, "not a timestamp")

        assert alert_world.reported == [
            "Error updating live alert message for broadcaster_id=111"
        ]
        assert alert_world.cycles == 0


class TestForgetUpdater:
    async def test_forgets_a_finished_updater_and_its_wakeup(
        self, alert_world: AlertWorld
    ) -> None:
        task = asyncio.get_running_loop().create_future()
        live_alert._update_tasks[900] = task  # pyright: ignore[reportArgumentType]
        live_alert._wakeups[900] = asyncio.Event()

        live_alert._forget_updater(900, task)  # pyright: ignore[reportArgumentType]

        assert 900 not in live_alert._update_tasks
        assert 900 not in live_alert._wakeups

    async def test_a_replaced_updater_finishing_late_leaves_its_successor_alone(
        self, alert_world: AlertWorld
    ) -> None:
        loop = asyncio.get_running_loop()
        old, new = loop.create_future(), loop.create_future()
        live_alert._update_tasks[900] = new  # pyright: ignore[reportArgumentType]
        live_alert._wakeups[900] = asyncio.Event()

        live_alert._forget_updater(900, old)  # pyright: ignore[reportArgumentType]

        assert live_alert._update_tasks[900] is new
        assert 900 in live_alert._wakeups


class TestStart:
    @pytest.fixture
    def gate(self, monkeypatch: pytest.MonkeyPatch) -> asyncio.Event:
        gate = asyncio.Event()

        async def hold(*args: object) -> None:
            await gate.wait()

        monkeypatch.setattr(live_alert, "_run", hold)
        return gate

    async def test_starts_one_updater_and_creates_its_wakeup_first(
        self, alert_world: AlertWorld, gate: asyncio.Event
    ) -> None:
        """Created here, not in _run, so a wake issued before the task first runs still lands."""
        live_alert._start(111, 5001, 900, 10, STARTED)

        assert 900 in live_alert._update_tasks
        assert 900 in live_alert._wakeups
        assert live_alert._update_tasks[900].get_name() == "live-alert-900"
        gate.set()
        await live_alert._update_tasks[900]

    async def test_is_a_no_op_while_the_updater_is_still_running(
        self, alert_world: AlertWorld, gate: asyncio.Event
    ) -> None:
        live_alert._start(111, 5001, 900, 10, STARTED)
        first = live_alert._update_tasks[900]
        wakeup = live_alert._wakeups[900]

        live_alert._start(111, 5001, 900, 10, STARTED)

        assert live_alert._update_tasks[900] is first
        assert live_alert._wakeups[900] is wakeup
        gate.set()
        await first

    async def test_a_finished_updater_is_replaced_by_a_new_one(
        self, alert_world: AlertWorld, gate: asyncio.Event
    ) -> None:
        """Which is also how wake() repairs a row whose updater died or gave up."""
        gate.set()
        live_alert._start(111, 5001, 900, 10, STARTED)
        first = live_alert._update_tasks[900]
        await first
        for _ in range(3):
            await asyncio.sleep(0)
        live_alert._update_tasks[900] = first

        live_alert._start(111, 5001, 900, 10, STARTED)

        assert live_alert._update_tasks[900] is not first

    async def test_two_alerts_have_two_independent_updaters(
        self, alert_world: AlertWorld, gate: asyncio.Event
    ) -> None:
        live_alert._start(111, 5001, 900, 10, STARTED)
        live_alert._start(111, 5001, 901, 11, STARTED)

        assert set(live_alert._update_tasks) == {900, 901}
        gate.set()
        await asyncio.gather(*live_alert._update_tasks.values())

    async def test_a_finished_updater_removes_itself_from_the_registry(
        self, alert_world: AlertWorld, gate: asyncio.Event
    ) -> None:
        live_alert._start(111, 5001, 900, 10, STARTED)
        task = live_alert._update_tasks[900]

        gate.set()
        await task
        for _ in range(3):
            await asyncio.sleep(0)

        assert 900 not in live_alert._update_tasks
        assert 900 not in live_alert._wakeups
