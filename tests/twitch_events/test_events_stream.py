import pytest

from tests.twitch_events.support import EventWorld, live, profile
from valmal.twitch.client.helix import HelixError
from valmal.twitch.eventsub import events
from valmal.twitch.models.api.stream import Stream
from valmal.twitch.models.eventsub.stream_offline import StreamOfflineEventSub
from valmal.twitch.models.eventsub.stream_online import StreamOnlineEventSub

pytestmark = pytest.mark.anyio


def online(broadcaster_id: str = "111") -> StreamOnlineEventSub:
    return StreamOnlineEventSub.model_validate(
        {
            "subscription": {"type": "stream.online"},
            "event": {
                "id": "10",
                "broadcaster_user_id": broadcaster_id,
                "broadcaster_user_login": "bob",
                "broadcaster_user_name": "Bob",
                "type": "live",
                "started_at": "2026-06-15T11:59:00Z",
            },
        }
    )


def offline(broadcaster_id: str = "111") -> StreamOfflineEventSub:
    return StreamOfflineEventSub.model_validate(
        {
            "subscription": {"type": "stream.offline"},
            "event": {
                "broadcaster_user_id": broadcaster_id,
                "broadcaster_user_login": "bob",
                "broadcaster_user_name": "Bob",
            },
        }
    )


class TestWaitForStreamInfo:
    async def test_a_stream_already_up_is_returned_without_waiting(
        self, world: EventWorld
    ) -> None:
        world.stream_answers = [live()]

        stream, error = await events._wait_for_stream_info(111)

        assert stream is not None and error is None
        assert world.slept == []

    async def test_polls_each_second_until_twitch_admits_the_stream(
        self, world: EventWorld
    ) -> None:
        """Twitch announces the stream before Helix lists it."""
        world.stream_answers = [None, None, live()]

        stream, _ = await events._wait_for_stream_info(111)

        assert stream is not None
        assert world.slept == [1, 1]
        assert world.asked == [111, 111, 111]

    async def test_a_lookup_that_failed_is_retried_not_fatal(
        self, world: EventWorld
    ) -> None:
        world.stream_answers = [HelixError("timeout"), live()]

        stream, error = await events._wait_for_stream_info(111)

        assert stream is not None and error is None

    async def test_an_error_typed_stream_is_not_live(self, world: EventWorld) -> None:
        world.stream_answers = [live().model_copy(update={"type": ""}), live()]

        stream, _ = await events._wait_for_stream_info(111)

        assert stream is not None
        assert len(world.asked) == 2

    async def test_gives_up_after_a_minute_by_the_clock_not_by_a_count_of_attempts(
        self, world: EventWorld
    ) -> None:
        world.stream_answers = [None]

        stream, error = await events._wait_for_stream_info(111)

        assert (stream, error) == (None, None)
        assert sum(world.slept) == events._STREAM_WAIT_SECONDS == 60

    async def test_giving_up_returns_the_last_lookup_error_so_it_can_be_told_apart(
        self, world: EventWorld
    ) -> None:
        """ "Twitch says they are offline" is not "Twitch could not be reached"."""
        world.stream_answers = [None, HelixError("first"), HelixError("last")]

        stream, error = await events._wait_for_stream_info(111)

        assert stream is None
        assert str(error) == "last"

    async def test_a_slow_first_lookup_still_gets_its_turn(
        self, world: EventWorld, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """The deadline is checked after the attempt, so a lookup that itself outlasts
        the whole wait is still answered."""

        async def slow_first(broadcaster_id: int) -> Stream | None:
            world.clock += 999
            return live()

        monkeypatch.setattr(events, "get_stream", slow_first)

        stream, _ = await events._wait_for_stream_info(111)

        assert stream is not None
        assert world.slept == []


class TestStreamOnline:
    async def test_the_main_broadcaster_is_announced_in_the_alerts_channel(
        self, world: EventWorld
    ) -> None:
        world.stream_answers = [live("valinmalach")]
        world.user_answer = profile()

        await events.stream_online(online())

        assert [name for name, _ in world.calls] == ["began", "announce"]
        _, args = world.calls[1]
        assert args[0] == 111
        assert args[3] == 5001

    async def test_anyone_elses_stream_is_announced_in_the_promo_channel(
        self, world: EventWorld
    ) -> None:
        world.stream_answers = [live("someone_else")]

        await events.stream_online(online("222"))

        assert world.calls[1][1][3] == 5002

    async def test_the_session_is_brought_up_before_the_alert_is_posted(
        self, world: EventWorld
    ) -> None:
        world.stream_answers = [live()]

        await events.stream_online(online())

        assert world.calls[0] == ("began", 111)

    async def test_the_alert_carries_the_stream_and_the_profile(
        self, world: EventWorld
    ) -> None:
        stream, user = live(), profile()
        world.stream_answers = [stream]
        world.user_answer = user

        await events.stream_online(online())

        _, (broadcaster_id, got_stream, got_user, _) = world.calls[1]
        assert (broadcaster_id, got_stream, got_user) == (111, stream, user)

    async def test_a_profile_that_cannot_be_fetched_is_only_decoration(
        self, world: EventWorld
    ) -> None:
        """stream.online does not come again, so the alert must not depend on it."""
        world.stream_answers = [live()]
        world.user_answer = HelixError("down")

        await events.stream_online(online())

        assert world.calls[1][1][2] is None
        ((text, key),) = world.notified
        assert "without their profile" in text
        assert key == "stream-online-profile:111"

    async def test_giving_up_says_so_and_posts_nothing(self, world: EventWorld) -> None:
        world.stream_answers = [None]

        await events.stream_online(online())

        assert world.calls == []
        ((text, key),) = world.notified
        assert "Gave up after 60s" in text
        assert "Twitch went on reporting them offline" in text
        assert key == "stream-online-gave-up:111"

    async def test_giving_up_after_a_failed_lookup_says_that_instead(
        self, world: EventWorld
    ) -> None:
        world.stream_answers = [HelixError("connection reset")]

        await events.stream_online(online())

        assert "the last lookup failed: connection reset" in world.notified[0][0]

    async def test_a_broadcaster_id_that_is_not_a_number_is_reported_by_this_handler(
        self, world: EventWorld
    ) -> None:
        """Converted inside the guard: above it, the failure would surface on the task
        floor, which names the route and not the event."""
        await events.stream_online(online("not-a-number"))

        assert world.reported == ["Error in stream_online for `not-a-number`"]

    async def test_a_failure_while_announcing_is_reported_not_raised(
        self, world: EventWorld
    ) -> None:
        world.stream_answers = [live()]
        world.announce_error = RuntimeError("discord is down")

        await events.stream_online(online())

        assert world.reported == ["Error in stream_online for `111`"]

    async def test_the_reported_id_is_quoted_inertly(self, world: EventWorld) -> None:
        await events.stream_online(online("<@1>`x"))

        assert world.reported == ["Error in stream_online for `<@1>x`"]


class TestStreamOffline:
    async def test_wakes_the_alert_updater_then_the_session(
        self, world: EventWorld
    ) -> None:
        """The payload names no stream, so this cannot tell which one ended; each
        re-checks Helix for its own scope."""
        await events.stream_offline(offline())

        assert world.calls == [("alert.wake", 111), ("session.wake", 111)]

    async def test_a_broadcaster_id_that_is_not_a_number_is_reported(
        self, world: EventWorld
    ) -> None:
        await events.stream_offline(offline("nope"))

        assert world.reported == ["Error in stream_offline for `nope`"]
        assert world.calls == []

    async def test_a_failure_is_reported_not_raised(self, world: EventWorld) -> None:
        world.wake_error = RuntimeError("boom")

        await events.stream_offline(offline())

        assert world.reported == ["Error in stream_offline for `111`"]
