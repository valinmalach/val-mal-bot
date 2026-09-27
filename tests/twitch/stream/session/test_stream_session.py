import pytest

from tests.twitch.stream.session.support import Calls, stream
from valmal.core.config import config
from valmal.twitch.client.helix import HelixError
from valmal.twitch.stream import stream_session

pytestmark = pytest.mark.anyio


class TestIdentity:
    @pytest.mark.parametrize("broadcaster", ["111", 111])
    def test_the_main_broadcaster_by_string_or_number(
        self, broadcaster: str | int
    ) -> None:
        assert stream_session.is_main_broadcaster(broadcaster) is True

    @pytest.mark.parametrize("broadcaster", ["112", 112, "", "11", "0111"])
    def test_anyone_else_is_not(self, broadcaster: str | int) -> None:
        assert stream_session.is_main_broadcaster(broadcaster) is False

    def test_with_no_broadcaster_configured_nobody_is(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(config, "_settings", {})

        assert stream_session.is_main_broadcaster("111") is False
        assert stream_session.is_main_broadcaster("None") is False

    def test_nobody_is_live_until_a_session_starts(self) -> None:
        assert stream_session.is_live() is False
        assert stream_session.current_stream_id() is None


class TestStart:
    def test_takes_up_a_stream(self, calls: Calls) -> None:
        stream_session._start(stream("10"))

        assert stream_session.is_live() is True
        assert stream_session.current_stream_id() == "10"
        assert calls.cleared == 1

    def test_coming_up_from_nothing_keeps_what_was_settled(self, calls: Calls) -> None:
        """stream.online can spend a minute confirming with Helix, and a raid landing
        in that window is already answered; clearing its mark would shout the raider
        out a second time on their first chat line."""
        stream_session.settle(7)

        stream_session._start(stream("10"))

        assert stream_session.is_settled(7) is True

    def test_replacing_a_live_stream_clears_what_was_settled(
        self, calls: Calls
    ) -> None:
        """The recovery for a session nothing was ever in a position to end."""
        stream_session._start(stream("10"))
        stream_session.settle(7)

        stream_session._start(stream("11"))

        assert stream_session.current_stream_id() == "11"
        assert stream_session.is_settled(7) is False

    def test_starting_the_stream_already_held_changes_nothing(
        self, calls: Calls
    ) -> None:
        """A redelivered stream.online must not empty a queue of shoutouts already
        promised in chat, or cancel the pending ad-break warning."""
        stream_session._start(stream("10"))
        stream_session.settle(7)
        cleared = calls.cleared

        stream_session._start(stream("10"))

        assert calls.cleared == cleared
        assert stream_session.is_settled(7) is True

    def test_a_new_stream_empties_the_queue_and_the_pending_warning(
        self, calls: Calls
    ) -> None:
        stream_session._start(stream("10"))

        stream_session._start(stream("11"))

        assert calls.cleared == 2


class TestEnd:
    def test_stands_everything_down(self, calls: Calls) -> None:
        stream_session._start(stream("10"))
        stream_session.settle(7)

        stream_session._end()

        assert stream_session.is_live() is False
        assert stream_session.current_stream_id() is None
        assert stream_session.is_settled(7) is False
        assert calls.cleared == 2

    def test_ending_with_nothing_live_is_harmless(self, calls: Calls) -> None:
        stream_session._end()

        assert stream_session.is_live() is False


class TestSettled:
    def test_a_user_is_settled_once_marked(self) -> None:
        assert stream_session.is_settled(5) is False

        stream_session.settle(5)

        assert stream_session.is_settled(5) is True
        assert stream_session.is_settled(6) is False

    def test_it_is_recorded_even_when_nobody_is_live(self) -> None:
        """A raid can arrive before stream.online has confirmed the stream."""
        stream_session.settle(5)

        assert stream_session.is_live() is False
        assert stream_session.is_settled(5) is True

    def test_marking_twice_is_the_same_as_once(self) -> None:
        stream_session.settle(5)
        stream_session.settle(5)

        assert stream_session._settled == {5}


class TestBegan:
    async def test_brings_the_session_up_greets_chat_and_announces_the_stream(
        self, calls: Calls
    ) -> None:
        await stream_session.began(
            111, stream("10", user_name="Bob", game_name="Chess", title="ranked")
        )

        assert stream_session.current_stream_id() == "10"
        assert calls.said == [
            (111, "twitch_stream_greeting", {}),
            (
                111,
                "twitch_stream_announce",
                {"name": "Bob", "game": "Chess", "title": "ranked"},
            ),
        ]

    async def test_another_broadcasters_stream_does_not_touch_the_session(
        self, calls: Calls
    ) -> None:
        await stream_session.began(222, stream("99"))

        assert stream_session.is_live() is False
        assert calls.said == []


class TestResume:
    async def test_a_stream_already_running_is_taken_up_without_a_greeting(
        self, calls: Calls
    ) -> None:
        calls.stream_answer = stream("10")

        await stream_session.resume()

        assert stream_session.current_stream_id() == "10"
        assert calls.said == []
        assert calls.asked == [111]

    async def test_offline_starts_nothing(self, calls: Calls) -> None:
        calls.stream_answer = None

        await stream_session.resume()

        assert stream_session.is_live() is False

    async def test_an_error_typed_stream_is_not_live(self, calls: Calls) -> None:
        calls.stream_answer = stream("10", type="")

        await stream_session.resume()

        assert stream_session.is_live() is False

    @pytest.mark.parametrize(
        "error", [HelixError("down"), TypeError("no id"), ValueError("not a number")]
    )
    async def test_a_failed_check_is_reported_and_never_raised(
        self, error: Exception, calls: Calls
    ) -> None:
        calls.stream_answer = error

        await stream_session.resume()

        assert calls.reported == [
            "Could not check whether the broadcaster is live at startup"
        ]
        assert stream_session.is_live() is False

    @pytest.mark.parametrize("value", [None, "not-a-number"])
    async def test_a_broadcaster_id_that_is_missing_or_not_a_number_is_reported(
        self, value: object, calls: Calls, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(config, "_settings", {"twitch_broadcaster_id": value})

        await stream_session.resume()

        assert len(calls.reported) == 1
        assert calls.asked == []
