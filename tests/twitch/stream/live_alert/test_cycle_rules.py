from types import SimpleNamespace

import aiohttp
import discord
import pytest

from tests.twitch.stream.live_alert.support import alert, http_error, stream
from valmal.twitch.stream.live_alert_cycle import (
    Action,
    _decide,
    _is_transient_edit_error,
    _owns_row,
)


class TestOwnsRow:
    def test_the_stored_alert_is_this_updaters_when_message_and_stream_both_match(
        self,
    ) -> None:
        assert _owns_row(alert(message_id=900, stream_id=10), 900, 10) is True

    @pytest.mark.parametrize(
        ("message_id", "stream_id"), [(901, 10), (900, 11), (901, 11)]
    )
    def test_a_newer_alert_for_the_same_broadcaster_is_not(
        self, message_id: int, stream_id: int
    ) -> None:
        assert (
            _owns_row(alert(message_id=message_id, stream_id=stream_id), 900, 10)
            is False
        )

    def test_no_row_is_nobodys(self) -> None:
        assert _owns_row(None, 900, 10) is False


class TestDecide:
    """The whole rule for one cycle, with every input already in hand."""

    def test_no_row_left_means_stop(self) -> None:
        assert _decide(None, 900, 10, stream()) is Action.STOP
        assert _decide(None, 900, 10, None) is Action.STOP

    @pytest.mark.parametrize("live", [stream(), None])
    def test_a_superseded_alert_is_closed_whether_or_not_a_stream_is_live(
        self, live: object
    ) -> None:
        """The message still shows a live stream, so it is closed; the row belongs to the
        newer alert and the delete will not match it."""
        newer = alert(message_id=901, stream_id=11)

        assert _decide(newer, 900, 10, live) is Action.CLOSE  # pyright: ignore[reportArgumentType]

    def test_the_stream_it_announced_still_live_means_refresh(self) -> None:
        assert _decide(alert(), 900, 10, stream()) is Action.REFRESH

    def test_no_stream_at_all_means_close(self) -> None:
        assert _decide(alert(), 900, 10, None) is Action.CLOSE

    def test_a_different_stream_now_live_means_close_this_one(self) -> None:
        """A live stream that is not the one announced describes a different broadcast."""
        assert _decide(alert(), 900, 10, stream()) is Action.REFRESH
        other = stream()
        other = other.model_copy(update={"id": "11"})

        assert _decide(alert(), 900, 10, other) is Action.CLOSE

    def test_an_error_typed_stream_is_not_live_so_close(self) -> None:
        assert _decide(alert(), 900, 10, stream(type="")) is Action.CLOSE

    def test_the_stream_id_is_compared_as_a_string_against_the_stored_integer(
        self,
    ) -> None:
        assert _decide(alert(stream_id=10), 900, 10, stream()) is Action.REFRESH
        assert _decide(alert(stream_id=1), 900, 1, stream()) is Action.CLOSE

    def test_every_action_is_reachable_and_distinct(self) -> None:
        answers = {
            _decide(None, 900, 10, None),
            _decide(alert(message_id=1), 900, 10, None),
            _decide(alert(), 900, 10, stream()),
        }

        assert answers == {Action.STOP, Action.CLOSE, Action.REFRESH}
        assert Action.RETRY not in answers, "RETRY is only ever concluded by the I/O"


class TestTransientEditError:
    @pytest.mark.parametrize("status", [500, 502, 503, 504, 599])
    def test_a_discord_5xx_is_transient(self, status: int) -> None:
        assert _is_transient_edit_error(http_error(status)) is True

    @pytest.mark.parametrize("status", [400, 401, 403, 404, 429, 499, 600])
    def test_any_other_discord_status_is_not(self, status: int) -> None:
        assert _is_transient_edit_error(http_error(status)) is False

    def test_a_dropped_socket_is_transient(self) -> None:
        """discord.py sends on aiohttp, so this arrives as one of its ClientError
        subclasses and not as anything discord.py names."""
        assert _is_transient_edit_error(aiohttp.ClientConnectionError()) is True
        assert _is_transient_edit_error(aiohttp.ServerDisconnectedError()) is True

    def test_a_timeout_is_transient(self) -> None:
        assert _is_transient_edit_error(TimeoutError()) is True

    @pytest.mark.parametrize(
        "error", [ValueError("x"), KeyError("k"), RuntimeError("r")]
    )
    def test_a_bug_is_not_transient(self, error: Exception) -> None:
        assert _is_transient_edit_error(error) is False

    def test_a_deleted_message_is_not_transient_it_has_its_own_branch(self) -> None:
        gone = discord.NotFound(SimpleNamespace(status=404, reason="Not Found"), "gone")  # pyright: ignore[reportArgumentType]

        assert _is_transient_edit_error(gone) is False
