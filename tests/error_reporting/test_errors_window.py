import asyncio
from typing import Any

import pytest

from tests.error_reporting.support import Clock
from valmal.core import errors
from valmal.core.background import fire_and_forget

pytestmark = pytest.mark.anyio


# --- _shortened -----------------------------------------------------------------


class TestShortened:
    def test_keeps_whole_lines_and_ends_with_the_note(self) -> None:
        text = "\n".join(f"line {i:03d} " + "x" * 40 for i in range(100))

        result = errors._shortened(text)

        assert result.endswith(errors._OVERFLOW_NOTE)
        assert len(result) <= errors._MAX_CONTENT
        kept = result.split("\n")[:-1]
        assert kept == text.split("\n")[: len(kept)]
        assert all(line.endswith("x" * 40) for line in kept), "a line was cut mid-way"

    def test_the_result_never_exceeds_the_limit_at_any_line_length(self) -> None:
        for width in (1, 7, 50, 333, 1000, 1899, 1900, 5000):
            text = "\n".join("y" * width for _ in range(40))

            assert len(errors._shortened(text)) <= errors._MAX_CONTENT, width

    def test_a_single_line_longer_than_the_budget_keeps_its_head_and_the_note(
        self,
    ) -> None:
        budget = errors._MAX_CONTENT - len(errors._OVERFLOW_NOTE) - 1

        result = errors._shortened("z" * 5000)

        assert result == "z" * budget + "\n" + errors._OVERFLOW_NOTE
        assert len(result) == errors._MAX_CONTENT

    def test_a_long_line_behind_a_short_first_line_keeps_both_the_prefix_and_its_head(
        self,
    ) -> None:
        """The first report after an outage leads with a one-line 'reached nobody'."""
        prefix = "[1 message(s) reached nobody while this channel was unreachable]"
        text = prefix + "\n" + "reading the thing - Type: RuntimeError " + "x" * 5000

        result = errors._shortened(text)

        assert result.startswith(prefix + "\nreading the thing - Type: RuntimeError")
        assert result.endswith(errors._OVERFLOW_NOTE)
        assert len(result) == errors._MAX_CONTENT

    def test_a_long_line_in_the_middle_keeps_the_lines_before_it_whole(self) -> None:
        text = "first\nsecond\n" + "y" * 5000 + "\nnever reached"

        result = errors._shortened(text)

        assert result.startswith("first\nsecond\nyyy")
        assert "never reached" not in result
        assert len(result) == errors._MAX_CONTENT

    def test_a_line_that_would_fit_alone_is_dropped_whole_not_cut(self) -> None:
        """Only a line that can never fit is cut; a list keeps its lines whole."""
        budget = errors._MAX_CONTENT - len(errors._OVERFLOW_NOTE) - 1
        text = "a" * (budget - 10) + "\n" + "b" * 500

        result = errors._shortened(text)

        assert result == "a" * (budget - 10) + "\n" + errors._OVERFLOW_NOTE

    def test_no_room_left_adds_no_empty_line(self) -> None:
        budget = errors._MAX_CONTENT - len(errors._OVERFLOW_NOTE) - 1
        text = "a" * (budget - 1) + "\n" + "b" * 5000

        result = errors._shortened(text)

        assert "\n\n" not in result

    def test_a_line_that_fits_exactly_is_kept_whole(self) -> None:
        budget = errors._MAX_CONTENT - len(errors._OVERFLOW_NOTE) - 1
        line = "w" * budget

        assert errors._shortened(line) == line + "\n" + errors._OVERFLOW_NOTE

    def test_keeps_as_many_lines_as_fit_and_no_more(self) -> None:
        budget = errors._MAX_CONTENT - len(errors._OVERFLOW_NOTE) - 1
        lines = ["a" * 99 for _ in range(50)]  # 100 with its newline

        kept = errors._shortened("\n".join(lines)).split("\n")[:-1]

        assert len(kept) == budget // 100

    def test_blank_lines_count_toward_the_budget(self) -> None:
        text = "\n" * 5000

        assert len(errors._shortened(text)) <= errors._MAX_CONTENT


# --- _prune ---------------------------------------------------------------------


class TestPrune:
    def test_drops_windows_that_expired_more_than_a_window_ago(self) -> None:
        errors._windows["old"] = errors._Window(until=0.0, suppressed=0)
        errors._windows["recent"] = errors._Window(until=990.0, suppressed=0)
        errors._windows["live"] = errors._Window(until=5000.0, suppressed=0)

        errors._prune(now=0.0 + errors._WINDOW_SECONDS + 1)

        assert set(errors._windows) == {"recent", "live"}

    def test_a_window_still_open_is_never_pruned(self) -> None:
        errors._windows["live"] = errors._Window(until=10_000.0, suppressed=3)

        errors._prune(now=100.0)

        assert errors._windows["live"].suppressed == 3

    def test_at_the_cap_it_makes_room_for_one_more_evicting_the_nearest_to_expiry(
        self,
    ) -> None:
        for i in range(errors._MAX_TRACKED):
            errors._windows[f"k{i}"] = errors._Window(until=1000.0 + i, suppressed=0)

        errors._prune(now=0.0)

        assert len(errors._windows) == errors._MAX_TRACKED - 1
        assert "k0" not in errors._windows
        assert f"k{errors._MAX_TRACKED - 1}" in errors._windows

    def test_below_the_cap_nothing_is_evicted(self) -> None:
        for i in range(10):
            errors._windows[f"k{i}"] = errors._Window(until=1000.0 + i, suppressed=0)

        errors._prune(now=0.0)

        assert len(errors._windows) == 10


# --- _send_once -----------------------------------------------------------------


class TestSendOnce:
    async def test_the_first_report_of_anything_is_delivered(
        self, clock: Clock, delivered: list[tuple[str, Any]]
    ) -> None:
        assert await errors._send_once("k", "hello", None) is True

        assert delivered == [("hello", None)]

    async def test_a_repeat_inside_the_window_is_held_back_but_still_true(
        self, clock: Clock, delivered: list[tuple[str, Any]]
    ) -> None:
        await errors._send_once("k", "hello", None)
        clock.now += errors._WINDOW_SECONDS - 1

        assert await errors._send_once("k", "hello", None) is True

        assert len(delivered) == 1
        assert errors._windows["k"].suppressed == 1

    async def test_the_next_delivery_says_how_many_it_stood_in_for(
        self, clock: Clock, delivered: list[tuple[str, Any]]
    ) -> None:
        await errors._send_once("k", "hello", None)
        for _ in range(3):
            await errors._send_once("k", "hello", None)
        clock.now += errors._WINDOW_SECONDS + 1

        await errors._send_once("k", "hello", None)

        assert delivered[-1][0] == "[3 more went unreported] hello"
        assert errors._windows["k"].suppressed == 0

    async def test_the_window_is_exactly_fifteen_minutes(
        self, clock: Clock, delivered: list[tuple[str, Any]]
    ) -> None:
        assert errors._WINDOW_SECONDS == 15 * 60
        await errors._send_once("k", "t", None)

        clock.now += errors._WINDOW_SECONDS - 0.001
        await errors._send_once("k", "t", None)
        assert len(delivered) == 1

        clock.now += 0.001
        await errors._send_once("k", "t", None)
        assert len(delivered) == 2

    async def test_different_keys_do_not_stand_in_for_each_other(
        self, clock: Clock, delivered: list[tuple[str, Any]]
    ) -> None:
        await errors._send_once("a", "same text", None)
        await errors._send_once("b", "same text", None)

        assert len(delivered) == 2

    async def test_the_attachment_travels_with_the_delivery(
        self, clock: Clock, delivered: list[tuple[str, Any]]
    ) -> None:
        await errors._send_once("k", "t", ("f.txt", "content"))

        assert delivered == [("t", ("f.txt", "content"))]

    async def test_a_failed_delivery_opens_no_window_so_the_retry_goes_through(
        self, clock: Clock, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        results = iter([False, True])
        texts: list[str] = []

        async def deliver(text: str, attachment: object) -> bool:
            texts.append(text)
            return next(results)

        monkeypatch.setattr(errors, "_deliver", deliver)

        assert await errors._send_once("k", "boom", None) is False
        clock.now += 1
        assert await errors._send_once("k", "boom", None) is True

        # The failure is counted, so the retry says one occurrence went unseen.
        assert texts == ["boom", "[1 more went unreported] boom"]

    async def test_a_delivery_that_raises_also_opens_no_window(
        self, clock: Clock, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        calls = 0

        async def deliver(text: str, attachment: object) -> bool:
            nonlocal calls
            calls += 1
            if calls == 1:
                raise RuntimeError("channel exploded")
            return True

        monkeypatch.setattr(errors, "_deliver", deliver)

        with pytest.raises(RuntimeError):
            await errors._send_once("k", "boom", None)
        clock.now += 1

        assert await errors._send_once("k", "boom", None) is True
        assert calls == 2

    async def test_repeated_failures_accumulate_in_the_count(
        self, clock: Clock, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        texts: list[str] = []

        async def deliver(text: str, attachment: object) -> bool:
            texts.append(text)
            return len(texts) >= 4

        monkeypatch.setattr(errors, "_deliver", deliver)

        for _ in range(4):
            clock.now += 1
            await errors._send_once("k", "boom", None)

        # Rebuilt from the original text each time, so the prefixes do not nest.
        assert texts == [
            "boom",
            "[1 more went unreported] boom",
            "[2 more went unreported] boom",
            "[3 more went unreported] boom",
        ]

    async def test_two_tasks_racing_on_one_key_deliver_once(
        self, clock: Clock, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """The window is claimed before the send, and nothing between yields."""
        gate = asyncio.Event()
        sent: list[str] = []

        async def deliver(text: str, attachment: object) -> bool:
            await gate.wait()
            sent.append(text)
            return True

        monkeypatch.setattr(errors, "_deliver", deliver)

        first = fire_and_forget(errors._send_once("k", "same", None), name="racer")
        await asyncio.sleep(0)
        second = fire_and_forget(errors._send_once("k", "same", None), name="racer")
        await asyncio.sleep(0)
        gate.set()
        await asyncio.gather(first, second)

        assert sent == ["same"]

    async def test_windows_stay_bounded_when_keys_vary_per_event(
        self, clock: Clock, delivered: list[tuple[str, Any]]
    ) -> None:
        for i in range(errors._MAX_TRACKED * 2):
            clock.now += 0.001
            await errors._send_once(f"vary-{i}", "t", None)

        assert len(errors._windows) <= errors._MAX_TRACKED
