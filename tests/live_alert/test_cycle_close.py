import pendulum
import pytest

from tests.live_alert.support import (
    NOW,
    CycleWorld,
    alert,
    channel,
    http_error,
    stream,
    user,
    video,
)
from valmal.twitch.client.helix import HelixError
from valmal.twitch.models.api.channel import Channel
from valmal.twitch.models.api.stream import Stream
from valmal.twitch.stream import live_alert_close as close
from valmal.twitch.stream import live_alert_cycle as lac
from valmal.twitch.stream.live_alert_cycle import Action

pytestmark = pytest.mark.anyio

STARTED = pendulum.datetime(2026, 6, 15, 11)


class TestGatherCloseInfo:
    async def gather(
        self, world: CycleWorld, stream_arg: object = None, user_info: object = None
    ) -> tuple[Stream | None, Channel | None, str]:
        return await close.gather_close_info(111, 10, stream_arg, user_info)  # pyright: ignore[reportArgumentType]

    async def test_the_login_comes_from_the_profile_first(
        self, cycle_world: CycleWorld
    ) -> None:
        cycle_world.channel_answer = channel()

        _, _, url = await self.gather(cycle_world, user_info=user(login="from_profile"))

        assert url == "https://www.twitch.tv/from_profile"

    async def test_then_from_the_channel(self, cycle_world: CycleWorld) -> None:
        cycle_world.channel_answer = channel(broadcaster_login="from_channel")

        _, _, url = await self.gather(cycle_world)

        assert url == "https://www.twitch.tv/from_channel"

    async def test_with_neither_it_is_the_generic_link(
        self, cycle_world: CycleWorld
    ) -> None:
        cycle_world.channel_answer = None

        _, _, url = await self.gather(cycle_world)

        assert url == "https://www.twitch.tv/"
        assert cycle_world.notified == []

    async def test_a_failed_channel_lookup_is_said_but_does_not_abandon_the_close(
        self, cycle_world: CycleWorld
    ) -> None:
        cycle_world.channel_answer = HelixError("down")

        _, channel_info, url = await self.gather(cycle_world, user_info=user())

        assert channel_info is None
        assert url == "https://www.twitch.tv/valinmalach"
        ((text, key),) = cycle_world.notified
        assert "without channel details" in text
        assert key == "live-alert-close-channel:111"

    async def test_an_unusable_login_degrades_to_the_generic_link_so_the_alert_still_closes(
        self, cycle_world: CycleWorld
    ) -> None:
        """Retrying would not help: a malformed login is a permanent property of this
        cycle's Helix data, and _close's whole job is to guarantee the alert closes."""
        _, _, url = await self.gather(cycle_world, user_info=user(login="a)b"))

        assert url == "https://www.twitch.tv/"
        ((text, key),) = cycle_world.notified
        assert "unusable login" in text
        assert key == "live-alert-close-login:111"

    async def test_a_live_stream_that_is_not_this_alerts_own_is_not_borrowed(
        self, cycle_world: CycleWorld
    ) -> None:
        """Its title would retitle this message."""
        other = stream().model_copy(update={"id": "11"})
        own = stream()

        assert (await self.gather(cycle_world, other))[0] is None
        assert (await self.gather(cycle_world, own))[0] is own
        assert (await self.gather(cycle_world, None))[0] is None


class TestClose:
    async def close(self, world: CycleWorld, **overrides: object) -> Action:
        args: dict[str, object] = {
            "broadcaster_id": 111,
            "channel_id": 5001,
            "message_id": 900,
            "stream_id": 10,
            "stream": None,
            "user_info": user(),
            "age": "2 hours",
            "content": None,
        }
        return await lac._close(**(args | overrides))  # pyright: ignore[reportArgumentType]

    async def test_swaps_in_the_offline_embed_retires_the_alert_and_forgets_the_row(
        self, cycle_world: CycleWorld
    ) -> None:
        """Forgetting on success is what keeps a restart from resurrecting an updater
        for a dead stream."""
        cycle_world.channel_answer = channel()
        cycle_world.vod_answer = video(id="555")

        assert await self.close(cycle_world) is Action.STOP

        (edit,) = cycle_world.edits
        assert edit["embed"].footer.text == "Streamed for 2 hours"
        assert [f.name for f in edit["embed"].fields] == ["Game", "VOD"]
        assert edit["view"] is None
        assert cycle_world.deleted == [(111, 900)]

    async def test_a_failed_edit_stops_rather_than_retrying_forever(
        self, cycle_world: CycleWorld
    ) -> None:
        cycle_world.edit_error = http_error(403)

        assert await self.close(cycle_world) is Action.STOP
        assert cycle_world.reported == [
            "Error editing offline embed for message_id=900"
        ]
        assert cycle_world.deleted == []

    async def test_a_transient_failure_retries_and_keeps_the_row(
        self, cycle_world: CycleWorld
    ) -> None:
        cycle_world.edit_error = http_error(502)

        assert await self.close(cycle_world) is Action.RETRY
        assert cycle_world.deleted == []

    async def test_with_no_vod_and_no_channel_details_it_still_closes(
        self, cycle_world: CycleWorld
    ) -> None:
        cycle_world.channel_answer = HelixError("down")
        cycle_world.vod_answer = None

        assert await self.close(cycle_world, user_info=None) is Action.STOP
        assert cycle_world.edits[0]["embed"].description == "**Unknown**"


class TestFetchProfile:
    async def test_returns_the_profile(self, cycle_world: CycleWorld) -> None:
        cycle_world.user_answer = user()

        assert await lac._fetch_profile(111) == user()

    async def test_a_failed_lookup_degrades_to_none_with_a_notice(
        self, cycle_world: CycleWorld
    ) -> None:
        """Decoration: losing the alert over an avatar would be worse."""
        cycle_world.user_answer = HelixError("down")

        assert await lac._fetch_profile(111) is None
        ((text, key),) = cycle_world.notified
        assert "without the broadcaster's profile" in text
        assert key == "live-alert-profile:111"


class TestCycle:
    async def cycle(
        self, world: CycleWorld, message_id: int = 900, stream_id: int = 10
    ) -> Action:
        return await lac.cycle(
            111, 5001, message_id, stream_id, STARTED, "<t:1:f>", None
        )

    async def test_no_row_stops_without_asking_helix_anything(
        self, cycle_world: CycleWorld
    ) -> None:
        cycle_world.alert = None

        assert await self.cycle(cycle_world) is Action.STOP
        assert cycle_world.streams_asked == 0
        assert cycle_world.edits == []

    async def test_a_row_that_vanished_is_reported_because_the_message_may_still_read_live(
        self, cycle_world: CycleWorld
    ) -> None:
        """This updater deletes its own row only on the way out, so finding none at the
        top of a cycle means something else did, and nothing is left to close the alert."""
        cycle_world.alert = None

        await self.cycle(cycle_world)

        ((text, key),) = cycle_world.notified
        assert key == "live-alert-row-gone:111"
        assert "111" in text and "900" in text

    async def test_a_live_stream_refreshes_the_embed(
        self, cycle_world: CycleWorld
    ) -> None:
        cycle_world.alert = alert()
        cycle_world.stream_answer = stream()
        cycle_world.user_answer = user()

        assert await self.cycle(cycle_world) is Action.REFRESH
        assert cycle_world.edits[0]["embed"].footer.text == "Live for 1 hour"
        assert cycle_world.edits[0]["embed"].author.icon_url == "https://cdn/p.png"

    async def test_the_age_is_two_units_of_how_long_it_has_been_up(
        self, cycle_world: CycleWorld
    ) -> None:
        cycle_world.alert = alert()
        cycle_world.stream_answer = stream()

        await lac.cycle(
            111,
            5001,
            900,
            10,
            NOW.subtract(hours=2, minutes=30, seconds=5),
            "<t:1:f>",
            None,
        )

        assert (
            cycle_world.edits[0]["embed"].footer.text == "Live for 2 hours, 30 minutes"
        )

    async def test_a_stream_that_ended_closes_the_alert(
        self, cycle_world: CycleWorld
    ) -> None:
        cycle_world.alert = alert()
        cycle_world.stream_answer = None
        cycle_world.channel_answer = channel()

        assert await self.cycle(cycle_world) is Action.STOP
        assert cycle_world.deleted == [(111, 900)]

    async def test_a_superseded_alert_is_closed_without_asking_helix_or_lending_it_a_title(
        self, cycle_world: CycleWorld
    ) -> None:
        """Once the row has moved on Helix has nothing to add, and a newer stream must
        not lend it a title."""
        cycle_world.alert = alert(message_id=901, stream_id=11)
        cycle_world.stream_answer = stream(title="The newer stream")

        assert await self.cycle(cycle_world) is Action.STOP

        assert cycle_world.streams_asked == 0
        assert "The newer stream" not in (
            cycle_world.edits[0]["embed"].description or ""
        )
        # The row belongs to the newer alert, and this delete is scoped to its own message.
        assert cycle_world.deleted == [(111, 900)]

    async def test_a_helix_failure_reading_the_stream_propagates_as_an_inconclusive_cycle(
        self, cycle_world: CycleWorld
    ) -> None:
        cycle_world.alert = alert()
        cycle_world.stream_answer = HelixError("down")

        with pytest.raises(HelixError):
            await self.cycle(cycle_world)

    async def test_a_failed_profile_lookup_does_not_stop_the_refresh(
        self, cycle_world: CycleWorld
    ) -> None:
        cycle_world.alert = alert()
        cycle_world.stream_answer = stream()
        cycle_world.user_answer = HelixError("down")

        assert await self.cycle(cycle_world) is Action.REFRESH

    async def test_dispatch_with_no_stream_for_a_refresh_is_an_inconclusive_cycle(
        self, cycle_world: CycleWorld
    ) -> None:
        """Unreachable through _decide; a guard rather than an assert, which -O strips."""
        result = await lac._dispatch(
            Action.REFRESH, 111, 5001, 900, 10, None, None, "1h", "<t:1:f>", None
        )

        assert result is Action.RETRY
