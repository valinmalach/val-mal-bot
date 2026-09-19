from types import SimpleNamespace

import aiohttp
import discord
import pendulum
import pytest

from services.twitch import live_alert_cycle as lac
from services.twitch.helix import HelixError
from services.twitch.live_alert_cycle import Action
from tests.live_alert.support import (
    CycleWorld,
    stream,
    user,
    video,
)

pytestmark = pytest.mark.anyio

STARTED = pendulum.datetime(2026, 6, 15, 11)


def http_error(status: int) -> discord.HTTPException:
    return discord.HTTPException(SimpleNamespace(status=status, reason="x"), "text")  # pyright: ignore[reportArgumentType]


def not_found() -> discord.NotFound:
    return discord.NotFound(SimpleNamespace(status=404, reason="Not Found"), "gone")  # pyright: ignore[reportArgumentType]


class TestForgetRow:
    async def test_drops_the_row_this_updater_owns(
        self, cycle_world: CycleWorld
    ) -> None:
        await lac._forget_row(111, 900)

        assert cycle_world.deleted == [(111, 900)]

    async def test_a_failed_delete_is_reported_not_raised(
        self, cycle_world: CycleWorld
    ) -> None:
        cycle_world.delete_error = ConnectionError("db down")

        await lac._forget_row(111, 900)

        assert cycle_world.reported == [
            "Failed to delete live alert record for broadcaster_id=111"
        ]


class TestVod:
    async def test_a_stream_id_of_zero_asks_nothing(
        self, cycle_world: CycleWorld
    ) -> None:
        cycle_world.vod_answer = video()

        assert await lac._vod(111, 0) is None

    async def test_returns_the_vod(self, cycle_world: CycleWorld) -> None:
        cycle_world.vod_answer = video(id="555")

        found = await lac._vod(111, 10)

        assert found is not None and found.id == "555"

    async def test_a_failed_lookup_is_reported_and_reads_as_no_vod(
        self, cycle_world: CycleWorld
    ) -> None:
        cycle_world.vod_answer = HelixError("down")

        assert await lac._vod(111, 10) is None
        assert cycle_world.reported == [
            "Failed to fetch VOD info for broadcaster_id=111"
        ]


class TestEditOrRetry:
    async def edit(self, world: CycleWorld, **overrides: object) -> Action:
        args: dict[str, object] = {
            "message_id": 900,
            "channel_id": 5001,
            "broadcaster_id": 111,
            "embed": discord.Embed(),
            "kind": "live",
            "on_error": Action.RETRY,
            "report_context": lambda e: f"context: {e}",
            "on_success": Action.REFRESH,
        }
        return await lac._edit_or_retry(**(args | overrides))  # pyright: ignore[reportArgumentType]

    async def test_a_successful_edit_is_the_callers_success_action(
        self, cycle_world: CycleWorld
    ) -> None:
        assert await self.edit(cycle_world) is Action.REFRESH
        assert cycle_world.deleted == []

    async def test_the_edit_carries_the_message_channel_view_and_content(
        self, cycle_world: CycleWorld
    ) -> None:
        view = discord.ui.View()

        await self.edit(cycle_world, view=view, content="<@&1>")

        (edit,) = cycle_world.edits
        assert (
            edit["message_id"],
            edit["channel_id"],
            edit["view"],
            edit["content"],
        ) == (
            900,
            5001,
            view,
            "<@&1>",
        )

    async def test_forgetting_on_success_is_opt_in(
        self, cycle_world: CycleWorld
    ) -> None:
        await self.edit(cycle_world, forget_on_success=True, on_success=Action.STOP)

        assert cycle_world.deleted == [(111, 900)]

    async def test_a_deleted_message_forgets_the_row_and_stops(
        self, cycle_world: CycleWorld
    ) -> None:
        cycle_world.edit_error = not_found()

        assert await self.edit(cycle_world) is Action.STOP
        assert cycle_world.deleted == [(111, 900)]

    @pytest.mark.parametrize(
        "error",
        [http_error(503), aiohttp.ClientConnectionError(), TimeoutError()],
        ids=["discord-5xx", "socket", "timeout"],
    )
    async def test_a_transient_failure_retries_and_says_nothing(
        self, error: Exception, cycle_world: CycleWorld
    ) -> None:
        cycle_world.edit_error = error

        assert await self.edit(cycle_world) is Action.RETRY
        assert cycle_world.reported == []
        assert cycle_world.deleted == []

    @pytest.mark.parametrize("on_error", [Action.RETRY, Action.STOP])
    async def test_anything_else_is_reported_and_answers_with_the_callers_choice(
        self, on_error: Action, cycle_world: CycleWorld
    ) -> None:
        """A live refresh retries and a close stops; the two never agreed on that."""
        cycle_world.edit_error = http_error(403)

        assert await self.edit(cycle_world, on_error=on_error) is on_error
        assert cycle_world.reported == [f"context: {cycle_world.edit_error}"]

    async def test_a_channel_that_cannot_be_resolved_is_a_retry(
        self, cycle_world: CycleWorld
    ) -> None:
        """edit_embed answers False for a no-op rather than an edit."""
        cycle_world.edit_result = False

        assert await self.edit(cycle_world, forget_on_success=True) is Action.RETRY
        assert cycle_world.deleted == []


class TestRefresh:
    async def refresh(self, world: CycleWorld, **overrides: object) -> Action:
        args: dict[str, object] = {
            "broadcaster_id": 111,
            "channel_id": 5001,
            "message_id": 900,
            "stream": stream(user_login="valinmalach"),
            "user_info": user(),
            "age": "1 hour",
            "started_at_timestamp": "<t:1:f>",
            "content": "<@&7>",
        }
        return await lac._refresh(**(args | overrides))  # pyright: ignore[reportArgumentType]

    async def test_edits_the_message_with_the_live_embed_and_a_watch_button(
        self, cycle_world: CycleWorld
    ) -> None:
        assert await self.refresh(cycle_world) is Action.REFRESH

        (edit,) = cycle_world.edits
        assert edit["embed"].footer.text == "Live for 1 hour"
        assert edit["content"] == "<@&7>"
        (button,) = edit["view"].children
        assert button.url == "https://www.twitch.tv/valinmalach"

    async def test_a_login_that_cannot_be_linked_is_reported_and_retried(
        self, cycle_world: CycleWorld
    ) -> None:
        assert (
            await self.refresh(cycle_world, stream=stream(user_login="a)b"))
            is Action.RETRY
        )

        assert cycle_world.edits == []
        assert cycle_world.reported == [
            "Failed to build the live alert URL for broadcaster_id=111"
        ]

    async def test_a_non_transient_edit_failure_retries_next_cycle(
        self, cycle_world: CycleWorld
    ) -> None:
        cycle_world.edit_error = http_error(403)

        assert await self.refresh(cycle_world) is Action.RETRY
        assert cycle_world.reported == [
            "Discord HTTP error 403 when editing live embed for message_id=900"
        ]

    async def test_a_failure_that_is_not_a_discord_error_is_named_generically(
        self, cycle_world: CycleWorld
    ) -> None:
        cycle_world.edit_error = ValueError("bug")

        await self.refresh(cycle_world)

        assert cycle_world.reported == ["Error editing live embed for message_id=900"]
