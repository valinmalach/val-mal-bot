import asyncio
from types import SimpleNamespace

import pytest

from tests.twitch.stream.session.support import NOW, Calls, stream
from valmal.twitch.client.helix import HelixError
from valmal.twitch.models.api.ad_schedule import AdSchedule
from valmal.twitch.models.api.stream import Stream
from valmal.twitch.stream import stream_session

pytestmark = pytest.mark.anyio


class TestWake:
    async def test_another_broadcasters_offline_is_ignored(self, calls: Calls) -> None:
        stream_session._start(stream("10"))

        await stream_session.wake(222)

        assert calls.asked == []
        assert stream_session.is_live() is True

    async def test_with_no_session_there_is_nothing_to_check(
        self, calls: Calls
    ) -> None:
        await stream_session.wake(111)

        assert calls.asked == []

    async def test_helix_still_showing_the_stream_keeps_the_session(
        self, calls: Calls
    ) -> None:
        """The webhook is a prompt, not the answer: Twitch sends one for a dropped
        connection as readily as for a finished stream."""
        stream_session._start(stream("10"))
        calls.stream_answer = stream("10")

        await stream_session.wake(111)

        assert stream_session.is_live() is True

    async def test_helix_confirming_it_gone_ends_the_session(
        self, calls: Calls
    ) -> None:
        stream_session._start(stream("10"))
        stream_session.settle(7)
        calls.stream_answer = None

        await stream_session.wake("111")

        assert stream_session.is_live() is False
        assert stream_session.is_settled(7) is False

    async def test_an_error_typed_stream_counts_as_gone(self, calls: Calls) -> None:
        stream_session._start(stream("10"))
        calls.stream_answer = stream("10", type="")

        await stream_session.wake(111)

        assert stream_session.is_live() is False

    async def test_an_unreachable_helix_leaves_the_session_up_with_a_notice(
        self, calls: Calls
    ) -> None:
        stream_session._start(stream("10"))
        calls.stream_answer = HelixError("unreachable")

        await stream_session.wake(111)

        assert stream_session.is_live() is True
        ((text, key),) = calls.notified
        assert "so the stream session is left up" in text
        assert key == "session-wake:111"

    async def test_a_check_about_a_previous_stream_cannot_end_the_next_one(
        self, calls: Calls, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A stream that came straight back began while the check was in flight, and
        the answer is about the one before it."""
        stream_session._start(stream("10"))

        async def get_stream(broadcaster_id: int) -> Stream | None:
            stream_session._start(stream("11"))
            return None

        monkeypatch.setattr(stream_session, "get_stream", get_stream)

        await stream_session.wake(111)

        assert stream_session.current_stream_id() == "11"

    async def test_a_session_ended_by_someone_else_during_the_check_is_left_alone(
        self, calls: Calls, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        stream_session._start(stream("10"))

        async def get_stream(broadcaster_id: int) -> Stream | None:
            stream_session._end()
            return None

        monkeypatch.setattr(stream_session, "get_stream", get_stream)

        await stream_session.wake(111)

        assert stream_session.is_live() is False


def schedule(next_ad_in_seconds: int) -> AdSchedule:
    return AdSchedule(
        snooze_count=0,
        snooze_refresh_at=0,
        next_ad_at=int(NOW.add(seconds=next_ad_in_seconds).timestamp()),
        duration=60,
        last_ad_at=0,
        preroll_free_time=0,
    )


class TestAdBreakWarning:
    async def drain(self) -> None:
        for _ in range(10):
            await asyncio.sleep(0)

    async def test_the_warning_is_five_minutes_before_the_next_ad(
        self, calls: Calls
    ) -> None:
        stream_session._start(stream("10"))
        calls.ad_answer = schedule(3600)

        await stream_session._warn_before_next_ad("111")

        assert calls.slept == [3300.0]
        assert calls.said == [("111", "twitch_ad_break_warning", {})]

    async def test_no_schedule_means_no_warning(self, calls: Calls) -> None:
        stream_session._start(stream("10"))
        calls.ad_answer = None

        await stream_session._warn_before_next_ad("111")

        assert calls.slept == []
        assert calls.said == []
        # An AttributeError on ad_schedule.next_ad_at, caught by the broad handler
        # below, would report a bug here instead of the intended silent no-op.
        assert calls.reported == []

    @pytest.mark.parametrize("seconds", [299, 60, 0, -600])
    async def test_an_ad_less_than_five_minutes_away_is_not_warned_about(
        self, seconds: int, calls: Calls
    ) -> None:
        stream_session._start(stream("10"))
        calls.ad_answer = schedule(seconds)

        await stream_session._warn_before_next_ad("111")

        assert calls.slept == []
        assert calls.said == []

    async def test_a_session_that_ended_during_the_wait_says_nothing(
        self, calls: Calls, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A warning about ads nobody is watching is worse than silence."""
        stream_session._start(stream("10"))
        calls.ad_answer = schedule(3600)

        async def sleep_then_end(seconds: float) -> None:
            stream_session._end()

        monkeypatch.setattr(
            stream_session,
            "asyncio",
            SimpleNamespace(**{**vars(asyncio), "sleep": sleep_then_end}),
        )

        await stream_session._warn_before_next_ad("111")

        assert calls.said == []

    async def test_a_lookup_that_fails_is_reported_not_raised(
        self, calls: Calls
    ) -> None:
        calls.ad_answer = HelixError("down")

        await stream_session._warn_before_next_ad("111")

        assert calls.reported == ["Error scheduling next ad break notification"]

    async def test_a_cancelled_warning_stays_cancelled(
        self, calls: Calls, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        stream_session._start(stream("10"))
        calls.ad_answer = schedule(3600)

        async def sleep(seconds: float) -> None:
            raise asyncio.CancelledError

        monkeypatch.setattr(
            stream_session,
            "asyncio",
            SimpleNamespace(**{**vars(asyncio), "sleep": sleep}),
        )

        with pytest.raises(asyncio.CancelledError):
            await stream_session._warn_before_next_ad("111")

        assert calls.reported == []

    async def test_only_the_main_broadcasters_ad_break_is_scheduled(
        self, calls: Calls
    ) -> None:
        stream_session.schedule_ad_break_warning("222")

        assert stream_session._ad_break_task is None

    async def test_a_new_ad_break_replaces_the_pending_warning(
        self, calls: Calls, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        gate = asyncio.Event()

        async def pending(broadcaster_id: str) -> None:
            await gate.wait()

        monkeypatch.setattr(stream_session, "_warn_before_next_ad", pending)
        stream_session.schedule_ad_break_warning("111")
        first = stream_session._ad_break_task
        await asyncio.sleep(0)

        stream_session.schedule_ad_break_warning("111")
        second = stream_session._ad_break_task
        await self.drain()

        assert first is not None and second is not None and first is not second
        assert first.cancelled()
        assert not second.done()
        gate.set()
        await second

    async def test_cancelling_stops_a_pending_warning_and_forgets_it(
        self, calls: Calls, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        gate = asyncio.Event()

        async def pending(broadcaster_id: str) -> None:
            await gate.wait()

        monkeypatch.setattr(stream_session, "_warn_before_next_ad", pending)
        stream_session.schedule_ad_break_warning("111")
        task = stream_session._ad_break_task
        await asyncio.sleep(0)

        stream_session.cancel_ad_break_warning()
        await self.drain()

        assert task is not None and task.cancelled()
        assert stream_session._ad_break_task is None

    def test_cancelling_with_nothing_pending_is_harmless(self) -> None:
        stream_session.cancel_ad_break_warning()

        assert stream_session._ad_break_task is None

    async def test_ending_the_session_cancels_the_warning(
        self, calls: Calls, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        gate = asyncio.Event()

        async def pending(broadcaster_id: str) -> None:
            await gate.wait()

        monkeypatch.setattr(stream_session, "_warn_before_next_ad", pending)
        stream_session._start(stream("10"))
        stream_session.schedule_ad_break_warning("111")
        task = stream_session._ad_break_task
        await asyncio.sleep(0)

        stream_session._end()
        await self.drain()

        assert task is not None and task.cancelled()
