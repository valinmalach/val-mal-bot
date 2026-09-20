import httpx
import pendulum
import pytest

from services.twitch import shoutout_queue as sq
from services.twitch.shoutout_queue import TwitchShoutoutQueue
from tests.twitch_shoutout_queue.support import (
    COOLDOWN,
    NOW,
    World,
    pending,
    settle_tasks,
)

pytestmark = pytest.mark.anyio


def test_there_is_one_queue_however_often_it_is_constructed(
    queue: TwitchShoutoutQueue,
) -> None:
    assert TwitchShoutoutQueue() is queue
    assert queue is sq.shoutout_queue


class TestAddToQueue:
    async def test_queues_a_login_and_numeric_id(
        self, queue: TwitchShoutoutQueue, world: World
    ) -> None:
        queue.add_to_queue("alice", "42")

        assert pending(queue) == [("alice", "42")]

    async def test_a_target_already_held_is_not_queued_twice_even_under_another_login(
        self, queue: TwitchShoutoutQueue, world: World
    ) -> None:
        queue.add_to_queue("alice", "42")
        queue.add_to_queue("alice_renamed", "42")

        assert pending(queue) == [("alice", "42")]

    async def test_different_targets_keep_their_order(
        self, queue: TwitchShoutoutQueue, world: World
    ) -> None:
        for login, uid in [("b", "2"), ("a", "1"), ("c", "3")]:
            queue.add_to_queue(login, uid)

        assert [uid for _, uid in pending(queue)] == ["2", "1", "3"]

    async def test_leading_zeros_are_still_a_numeric_id(
        self, queue: TwitchShoutoutQueue, world: World
    ) -> None:
        queue.add_to_queue("alice", "007")

        assert pending(queue) == [("alice", "007")]

    @pytest.mark.parametrize(
        ("login", "user_id"),
        [
            ("", "42"),
            ("alice", ""),
            ("alice", "abc"),
            ("alice", "12a"),
            ("alice", " 12"),
            ("alice", "-1"),
            ("alice", "1.5"),
            # isdigit passes a superscript that int() rejects inside the drainer,
            # after the pair has already left the queue.
            ("alice", "²"),
            # int() would take an Arabic-Indic numeral quite happily; a Twitch id is ASCII.
            ("alice", "٢٣"),
        ],
    )
    async def test_a_target_the_drainer_could_not_act_on_is_refused_with_a_notice(
        self, login: str, user_id: str, queue: TwitchShoutoutQueue, world: World
    ) -> None:
        queue.add_to_queue(login, user_id)
        await settle_tasks()

        assert pending(queue) == []
        ((text, key),) = world.notified
        assert "Refused a shoutout" in text
        assert key == "shoutout-bad-target"

    async def test_the_refusal_quotes_what_was_rejected_inertly(
        self, queue: TwitchShoutoutQueue, world: World
    ) -> None:
        queue.add_to_queue("@everyone`", "<@1>")
        await settle_tasks()

        text = world.notified[0][0]
        assert "`@everyone`" in text and "`<@1>`" in text


class TestCanShoutout:
    def test_a_target_never_shouted_out_is_fine(
        self, queue: TwitchShoutoutQueue, world: World
    ) -> None:
        assert queue._can_shoutout_target("42") is True

    def test_the_same_target_waits_out_twitchs_hour_and_a_minute_to_stay_clear_of_it(
        self, queue: TwitchShoutoutQueue, world: World
    ) -> None:
        queue._last_shoutout_by_target_id["42"] = NOW
        world.now = NOW.add(seconds=COOLDOWN - 1)
        assert queue._can_shoutout_target("42") is False

        world.now = NOW.add(seconds=COOLDOWN)
        assert queue._can_shoutout_target("42") is True

    def test_a_backoff_still_running_blocks_even_a_target_never_shouted_out(
        self, queue: TwitchShoutoutQueue, world: World
    ) -> None:
        queue._next_attempt_allowed_by_target_id["42"] = NOW.add(minutes=5)

        assert queue._can_shoutout_target("42") is False

    def test_a_backoff_ending_exactly_now_is_over(
        self, queue: TwitchShoutoutQueue, world: World
    ) -> None:
        """It is allowed to try again at the moment it names, not a tick after."""
        queue._next_attempt_allowed_by_target_id["42"] = NOW.add(seconds=30)

        world.now = NOW.add(seconds=29)
        assert queue._can_shoutout_target("42") is False
        world.now = NOW.add(seconds=30)
        assert queue._can_shoutout_target("42") is True

    def test_an_expired_backoff_falls_through_to_the_cooldown_check(
        self, queue: TwitchShoutoutQueue, world: World
    ) -> None:
        queue._next_attempt_allowed_by_target_id["42"] = NOW.subtract(seconds=1)
        queue._last_shoutout_by_target_id["42"] = NOW.subtract(seconds=10)

        assert queue._can_shoutout_target("42") is False

    def test_targets_are_independent(
        self, queue: TwitchShoutoutQueue, world: World
    ) -> None:
        queue._last_shoutout_by_target_id["42"] = NOW

        assert queue._can_shoutout_target("42") is False
        assert queue._can_shoutout_target("43") is True


class TestNextPair:
    def test_nothing_queued_is_none(
        self, queue: TwitchShoutoutQueue, world: World
    ) -> None:
        assert queue._get_next_available_pair() is None

    def test_skips_the_blocked_and_takes_the_first_eligible_in_order(
        self, queue: TwitchShoutoutQueue, world: World
    ) -> None:
        queue._shoutout_queue.extend([("a", "1"), ("b", "2"), ("c", "3")])
        queue._last_shoutout_by_target_id["1"] = NOW

        assert queue._get_next_available_pair() == ("b", "2")

    def test_all_blocked_is_none(
        self, queue: TwitchShoutoutQueue, world: World
    ) -> None:
        queue._shoutout_queue.append(("a", "1"))
        queue._next_attempt_allowed_by_target_id["1"] = NOW.add(hours=1)

        assert queue._get_next_available_pair() is None


class TestWaitUntilFrom429:
    def response(self, **headers: str) -> httpx.Response:
        return httpx.Response(429, headers=headers)

    def test_retry_after_in_seconds(
        self, queue: TwitchShoutoutQueue, world: World
    ) -> None:
        assert queue._wait_until_from_429(self.response(**{"Retry-After": "30"})) == (
            NOW.add(seconds=30)
        )

    def test_retry_after_wins_over_the_reset_header(
        self, queue: TwitchShoutoutQueue, world: World
    ) -> None:
        reset = str(int(NOW.add(hours=2).timestamp()))

        assert queue._wait_until_from_429(
            self.response(**{"Retry-After": "30", "Ratelimit-Reset": reset})
        ) == NOW.add(seconds=30)

    def test_an_http_date_retry_after_is_skipped_for_the_reset_header(
        self, queue: TwitchShoutoutQueue, world: World
    ) -> None:
        reset = int(NOW.add(minutes=10).timestamp())

        wait = queue._wait_until_from_429(
            self.response(
                **{
                    "Retry-After": "Wed, 21 Oct 2026 07:28:00 GMT",
                    "Ratelimit-Reset": str(reset),
                }
            )
        )

        assert wait == pendulum.from_timestamp(reset, tz=pendulum.UTC)

    def test_a_reset_in_the_past_is_ignored(
        self, queue: TwitchShoutoutQueue, world: World
    ) -> None:
        past = str(int(NOW.subtract(minutes=10).timestamp()))

        assert queue._wait_until_from_429(
            self.response(**{"Ratelimit-Reset": past})
        ) == (NOW.add(seconds=COOLDOWN))

    @pytest.mark.parametrize(
        "headers",
        [
            {},
            {"Retry-After": "soon"},
            {"Ratelimit-Reset": "later"},
            {"Retry-After": ""},
        ],
    )
    def test_no_usable_header_falls_back_to_the_full_cooldown(
        self, headers: dict[str, str], queue: TwitchShoutoutQueue, world: World
    ) -> None:
        assert queue._wait_until_from_429(self.response(**headers)) == (
            NOW.add(seconds=COOLDOWN)
        )
