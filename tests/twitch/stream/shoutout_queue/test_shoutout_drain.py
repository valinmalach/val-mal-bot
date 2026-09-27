from types import SimpleNamespace

import httpx
import pytest

from tests.twitch.stream.shoutout_queue.support import NOW, Stop, World, pending
from valmal.twitch.client.helix import HelixError
from valmal.twitch.stream import stream_session
from valmal.twitch.stream.shoutout_queue import TwitchShoutoutQueue

pytestmark = pytest.mark.anyio


class TestDrainOnce:
    async def test_an_empty_queue_just_waits(
        self, queue: TwitchShoutoutQueue, world: World
    ) -> None:
        world.sleeps_before_stop = 99

        await queue._drain_once()

        assert world.slept == [5]
        assert world.looked_up == []

    async def test_a_queue_of_only_blocked_targets_waits(
        self, queue: TwitchShoutoutQueue, world: World
    ) -> None:
        world.sleeps_before_stop = 99
        queue._shoutout_queue.append(("a", "1"))
        queue._last_shoutout_by_target_id["1"] = NOW

        await queue._drain_once()

        assert world.slept == [5]
        assert pending(queue) == [("a", "1")]

    async def test_sends_one_shoutout_records_it_and_waits_out_the_global_interval(
        self, queue: TwitchShoutoutQueue, world: World
    ) -> None:
        world.sleeps_before_stop = 99
        queue._shoutout_queue.extend([("a", "1"), ("b", "2")])

        await queue._drain_once()

        assert world.shouted == ["1"]
        assert pending(queue) == [("b", "2")]
        assert queue._last_shoutout_by_target_id["1"] == NOW
        assert world.slept == [125]

    async def test_a_success_clears_that_targets_earlier_backoff(
        self, queue: TwitchShoutoutQueue, world: World
    ) -> None:
        world.sleeps_before_stop = 99
        queue._shoutout_queue.append(("a", "1"))
        queue._next_attempt_allowed_by_target_id["1"] = NOW.subtract(minutes=1)

        await queue._drain_once()

        assert "1" not in queue._next_attempt_allowed_by_target_id

    async def test_a_failed_lookup_backs_the_target_off_and_requeues_it_last(
        self, queue: TwitchShoutoutQueue, world: World
    ) -> None:
        """One unreachable lookup must not end the queue, nor spin on it; the
        back-off has to outlast the wait that follows it."""
        world.sleeps_before_stop = 99
        world.users[1] = HelixError("down")
        queue._shoutout_queue.extend([("a", "1"), ("b", "2")])

        await queue._drain_once()

        assert world.shouted == []
        assert pending(queue) == [("b", "2"), ("a", "1")]
        assert queue._next_attempt_allowed_by_target_id["1"] == NOW.add(seconds=300)
        ((text, key),) = world.notified
        assert "Could not look up a for a shoutout" in text
        assert key == "shoutout-lookup:1"
        assert world.slept == [], (
            "no shoutout went out, so the global interval does not apply"
        )

    async def test_an_unknown_user_is_dropped_with_a_notice(
        self, queue: TwitchShoutoutQueue, world: World
    ) -> None:
        world.users[1] = None
        queue._shoutout_queue.append(("ghost", "1"))

        await queue._drain_once()

        assert pending(queue) == []
        assert world.shouted == []
        assert world.notified == [
            ("User ghost not found for shoutout", "shoutout-not-found:1")
        ]

    async def test_a_429_requeues_with_the_wait_the_response_asked_for(
        self, queue: TwitchShoutoutQueue, world: World
    ) -> None:
        world.sleeps_before_stop = 99
        world.shout_error = HelixError(
            "busy",
            status=429,
            response=httpx.Response(429, headers={"Retry-After": "90"}),
        )
        queue._shoutout_queue.append(("a", "1"))

        await queue._drain_once()

        assert pending(queue) == [("a", "1")]
        assert queue._next_attempt_allowed_by_target_id["1"] == NOW.add(seconds=90)
        assert world.reported == []
        assert "1" not in queue._last_shoutout_by_target_id
        assert world.slept == [125]

    async def test_any_other_helix_failure_is_reported_and_the_target_dropped(
        self, queue: TwitchShoutoutQueue, world: World
    ) -> None:
        world.sleeps_before_stop = 99
        world.shout_error = HelixError("boom", status=500, response=httpx.Response(500))
        queue._shoutout_queue.append(("a", "1"))

        await queue._drain_once()

        assert world.reported == ["Failed to send shoutout to a"]
        assert pending(queue) == []
        assert "1" not in queue._last_shoutout_by_target_id
        assert world.slept == [125]

    async def test_a_429_that_carries_no_response_is_reported_like_any_other_failure(
        self, queue: TwitchShoutoutQueue, world: World
    ) -> None:
        world.sleeps_before_stop = 99
        world.shout_error = HelixError("busy", status=429)
        queue._shoutout_queue.append(("a", "1"))

        await queue._drain_once()

        assert world.reported == ["Failed to send shoutout to a"]
        assert pending(queue) == []


class TestDrain:
    async def test_while_nobody_is_live_it_only_waits(
        self, queue: TwitchShoutoutQueue, world: World, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(stream_session, "_stream", None)
        queue._shoutout_queue.append(("a", "1"))

        with pytest.raises(Stop):
            await queue.drain()

        assert world.shouted == []
        assert world.slept == [5, 5, 5]
        assert pending(queue) == [("a", "1")], (
            "a straggler is never shouted into an offline chat"
        )

    async def test_while_live_it_drains(
        self, queue: TwitchShoutoutQueue, world: World, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(stream_session, "_stream", SimpleNamespace(id="10"))
        queue._shoutout_queue.append(("a", "1"))

        with pytest.raises(Stop):
            await queue.drain()

        assert world.shouted == ["1"]

    async def test_a_pass_that_fails_unexpectedly_is_reported_and_the_loop_goes_on(
        self, queue: TwitchShoutoutQueue, world: World, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Without this, one unanticipated failure ended shoutouts for the whole stream."""
        monkeypatch.setattr(stream_session, "_stream", SimpleNamespace(id="10"))
        world.users[1] = ValueError("not anticipated")
        queue._shoutout_queue.extend([("a", "1"), ("b", "2")])

        with pytest.raises(Stop):
            await queue.drain()

        assert world.reported == ["Shoutout queue: a pass failed unexpectedly"]
        assert world.shouted == ["2"], "the queue behind the failure was still served"


class TestClear:
    def test_drops_what_is_pending_but_keeps_the_cooldowns(
        self, queue: TwitchShoutoutQueue, world: World
    ) -> None:
        """The cooldown mirrors one Twitch keeps for an hour whatever this process
        does; forgetting it would only earn a 429 on the next stream."""
        queue._shoutout_queue.append(("a", "1"))
        queue._next_attempt_allowed_by_target_id["1"] = NOW
        queue._last_shoutout_by_target_id["2"] = NOW

        queue.clear()

        assert pending(queue) == []
        assert queue._next_attempt_allowed_by_target_id == {}
        assert queue._last_shoutout_by_target_id == {"2": NOW}
