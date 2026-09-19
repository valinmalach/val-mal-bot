from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pendulum
import pytest

from cogs import tasks
from cogs.tasks import Tasks, undeliverable_summary
from services.twitch.helix import HelixError

pytestmark = pytest.mark.anyio


class Watch:
    def __init__(self) -> None:
        self.broken: dict[str, str] | HelixError = {}
        self.notified: list[str] = []
        self.reported: list[str] = []
        self.calls = 0


@pytest.fixture
def watch(monkeypatch: pytest.MonkeyPatch) -> Watch:
    watch = Watch()

    async def broken_subscriptions() -> dict[str, str]:
        watch.calls += 1
        if isinstance(watch.broken, HelixError):
            raise watch.broken
        return dict(watch.broken)

    async def notify(text: str, *, key: str | None = None) -> bool:
        watch.notified.append(text)
        return True

    async def report(exc: Exception, context: str, **_: object) -> None:
        watch.reported.append(context)

    monkeypatch.setattr(tasks, "broken_subscriptions", broken_subscriptions)
    monkeypatch.setattr(tasks, "notify", notify)
    monkeypatch.setattr(tasks, "report", report)
    monkeypatch.setattr(Tasks, "_known_broken", {})
    return watch


def cog() -> Tasks:
    return Tasks(MagicMock())


async def check(instance: Tasks) -> None:
    loop: Any = Tasks.recheck_subscriptions
    await loop.coro(instance)


class TestUndeliverableSummary:
    def test_counts_by_reason_before_listing_each(self) -> None:
        text = undeliverable_summary(
            {
                "stream.online (1)": "callback unreachable",
                "stream.offline (1)": "callback unreachable",
                "channel.raid (2)": "authorization revoked",
            }
        )

        lines = text.splitlines()
        assert lines[0] == "3 Twitch subscription(s) will not deliver:"
        assert lines[1] == "- 2 callback unreachable"
        assert lines[2] == "- 1 authorization revoked"

    def test_the_most_common_reason_comes_first(self) -> None:
        text = undeliverable_summary({"a": "rare", "b": "common", "c": "common"})

        assert text.index("- 2 common") < text.index("- 1 rare")

    def test_names_every_subscription_with_its_reason_after_the_counts(self) -> None:
        text = undeliverable_summary({"stream.online (1)": "why"})

        assert text.index("- 1 why") < text.index("  stream.online (1): why")

    def test_says_which_two_can_be_replaced_from_discord(self) -> None:
        text = undeliverable_summary({"a": "b"})

        assert "/subscribe replaces the stream.online and stream.offline pair" in text
        assert "registered outside the bot" in text

    def test_ninety_seven_of_one_reason_is_one_headline_not_ninety_seven(self) -> None:
        text = undeliverable_summary({f"sub{n}": "url moved" for n in range(97)})

        assert text.count("- 97 url moved") == 1
        assert text.splitlines()[0] == "97 Twitch subscription(s) will not deliver:"

    def test_the_headline_and_advice_survive_a_cut_because_they_come_first(
        self,
    ) -> None:
        text = undeliverable_summary({f"sub{n}": "url moved" for n in range(97)})

        assert text.index("- 97 url moved") < text.index("/subscribe")
        assert text.index("/subscribe") < text.index("  sub0: url moved")

    def test_nothing_broken_is_a_zero_count(self) -> None:
        assert undeliverable_summary({}).startswith(
            "0 Twitch subscription(s) will not deliver:"
        )


class TestRecheckSubscriptions:
    async def test_a_subscription_that_stops_delivering_is_reported(
        self, watch: Watch
    ) -> None:
        watch.broken = {"stream.online (1)": "callback unreachable"}

        await check(cog())

        (text,) = watch.notified
        assert text.startswith("1 Twitch subscription(s) will not deliver:")
        assert "stream.online (1): callback unreachable" in text

    async def test_a_healthy_pass_says_nothing(self, watch: Watch) -> None:
        await check(cog())

        assert watch.notified == []

    async def test_the_same_breakage_is_not_reported_again_next_hour(
        self, watch: Watch
    ) -> None:
        watch.broken = {"a": "why"}
        instance = cog()

        await check(instance)
        await check(instance)
        await check(instance)

        assert len(watch.notified) == 1

    async def test_only_what_is_new_is_reported_on_a_later_pass(
        self, watch: Watch
    ) -> None:
        instance = cog()
        watch.broken = {"a": "why"}
        await check(instance)

        watch.broken = {"a": "why", "b": "other"}
        await check(instance)

        assert len(watch.notified) == 2
        assert "b: other" in watch.notified[1] and "a: why" not in watch.notified[1]

    async def test_a_subscription_that_recovers_is_said_to_deliver_again(
        self, watch: Watch
    ) -> None:
        instance = cog()
        watch.broken = {"a": "why"}
        await check(instance)

        watch.broken = {}
        await check(instance)

        assert watch.notified[1] == "1 Twitch subscription(s) deliver again:\n- a"

    async def test_new_breakage_and_a_recovery_in_one_pass_are_two_notices(
        self, watch: Watch
    ) -> None:
        instance = cog()
        watch.broken = {"a": "why"}
        await check(instance)

        watch.broken = {"b": "other"}
        await check(instance)

        assert [t.split(":")[0] for t in watch.notified[1:]] == [
            "1 Twitch subscription(s) will not deliver",
            "1 Twitch subscription(s) deliver again",
        ]

    async def test_a_changed_reason_for_the_same_subscription_is_not_new(
        self, watch: Watch
    ) -> None:
        """It is keyed on the subscription; the reason moving is not a new outage."""
        instance = cog()
        watch.broken = {"a": "why"}
        await check(instance)

        watch.broken = {"a": "a different why"}
        await check(instance)

        assert len(watch.notified) == 1

    async def test_it_remembers_the_latest_reason_for_next_time(
        self, watch: Watch
    ) -> None:
        instance = cog()
        watch.broken = {"a": "why"}
        await check(instance)

        watch.broken = {"a": "a different why"}
        await check(instance)

        assert instance._known_broken == {"a": "a different why"}

    async def test_a_failed_lookup_is_reported_and_forgets_nothing(
        self, watch: Watch
    ) -> None:
        instance = cog()
        watch.broken = {"a": "why"}
        await check(instance)

        watch.broken = HelixError("down")
        await check(instance)
        watch.broken = {"a": "why"}
        await check(instance)

        assert watch.reported == ["Could not re-check the Twitch subscriptions"]
        assert len(watch.notified) == 1, "the outage was not mistaken for a recovery"

    async def test_a_failed_lookup_says_nothing_to_the_admin_channel_itself(
        self, watch: Watch
    ) -> None:
        watch.broken = HelixError("down")

        await check(cog())

        assert watch.notified == []


class TestTheSchedule:
    def test_rechecks_every_hour(self) -> None:
        assert Tasks.recheck_subscriptions.hours == 1

    def test_checks_birthdays_on_every_quarter_hour_of_the_day(self) -> None:
        times = Tasks.check_birthdays.time

        assert times is not None
        assert len(times) == 96
        assert {t.minute for t in times} == {0, 15, 30, 45}
        assert sorted(t.hour for t in times) == sorted(list(range(24)) * 4)

    def test_the_quarter_hours_are_in_order_from_midnight(self) -> None:
        times = Tasks._quarter_hours

        assert times[0] == pendulum.Time(0, 0) and times[-1] == pendulum.Time(23, 45)
        assert times == sorted(times)

    def test_the_times_are_utc_because_they_carry_no_zone(self) -> None:
        """discord.py reads a naive time as UTC, which is what the query assumes."""
        assert all(t.tzinfo is None for t in Tasks._quarter_hours)

    async def test_both_loops_wait_for_the_gateway_before_their_first_pass(
        self,
    ) -> None:
        bot = MagicMock()
        bot.wait_until_ready = AsyncMock()
        instance = Tasks(bot)

        await instance._before_recheck_subscriptions()
        await instance._before_check_birthdays()

        assert bot.wait_until_ready.await_count == 2

    def test_the_loops_are_not_started_by_the_cog_itself(self) -> None:
        """setup_hook starts them, after login() has made the gateway state exist."""
        instance = cog()

        assert not Tasks.check_birthdays.is_running()
        assert not Tasks.recheck_subscriptions.is_running()
        assert instance.bot is not None
