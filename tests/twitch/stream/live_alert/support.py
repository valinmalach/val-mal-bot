"""Shared by the live alert tests."""

from datetime import UTC, datetime
from types import SimpleNamespace
from typing import Any

import discord
import pendulum

from tests.twitch.support import channel_json, stream_json, user_json, video_json
from valmal.db.models import LiveAlert
from valmal.twitch.models.api.channel import Channel
from valmal.twitch.models.api.stream import Stream
from valmal.twitch.models.api.user import User
from valmal.twitch.models.api.video import Video

NOW = pendulum.datetime(2026, 6, 15, 12)


def http_error(status: int) -> discord.HTTPException:
    return discord.HTTPException(SimpleNamespace(status=status, reason="x"), "text")  # pyright: ignore[reportArgumentType]


TEMPLATES = {
    "stream_watch_button": "Watch",
    "stream_live_title": "{name} is live!",
    "stream_field_game": "Game",
    "stream_field_viewers": "Viewers",
    "stream_field_started_at": "Started",
    "stream_footer_online": "Live for {age}",
    "stream_offline_title": "{name} was live",
    "stream_footer_offline": "Streamed for {age}",
    "stream_field_vod": "VOD",
    "stream_field_vod_value": "[Watch]({url})",
}


def stream(**overrides: Any) -> Stream:
    return Stream.model_validate(stream_json(**({"id": "10"} | overrides)))


def user(**overrides: Any) -> User:
    return User.model_validate(
        user_json(**({"id": "111", "login": "valinmalach"} | overrides))
    )


def channel(**overrides: Any) -> Channel:
    return Channel.model_validate(
        channel_json("111", "valinmalach", "Valin", "Chess") | overrides
    )


def video(**overrides: Any) -> Video:
    return Video.model_validate(
        video_json(**({"id": "555", "stream_id": "10"} | overrides))
    )


def alert(
    broadcaster_id: int = 111,
    channel_id: int = 5001,
    message_id: int = 900,
    stream_id: int = 10,
) -> LiveAlert:
    return LiveAlert(
        broadcaster_id=broadcaster_id,
        channel_id=channel_id,
        message_id=message_id,
        stream_id=stream_id,
        stream_started_at=datetime(2026, 6, 15, 11, 0, tzinfo=UTC),
    )


class CycleWorld:
    """Every collaborator of one alert cycle, recorded and scriptable."""

    def __init__(self) -> None:
        self.alert: LiveAlert | None = None
        self.deleted: list[tuple[int, int | None]] = []
        self.delete_error: Exception | None = None
        self.stream_answer: Stream | Exception | None = None
        self.streams_asked = 0
        self.channel_answer: Channel | Exception | None = None
        self.user_answer: User | Exception | None = None
        self.vod_answer: Video | Exception | None = None
        self.edits: list[dict[str, Any]] = []
        self.edit_result: bool = True
        self.edit_error: Exception | None = None
        self.notified: list[tuple[str, str | None]] = []
        self.reported: list[str] = []

    @staticmethod
    def _answer(value: Any) -> Any:
        if isinstance(value, Exception):
            raise value
        return value

    async def get_live_alert(self, broadcaster_id: int) -> LiveAlert | None:
        return self.alert

    async def delete_live_alert(
        self, broadcaster_id: int, *, message_id: int | None = None
    ) -> None:
        if self.delete_error is not None:
            raise self.delete_error
        self.deleted.append((broadcaster_id, message_id))

    async def get_stream(self, broadcaster_id: int) -> Stream | None:
        self.streams_asked += 1
        return self._answer(self.stream_answer)

    async def get_channel(self, broadcaster_id: int) -> Channel | None:
        return self._answer(self.channel_answer)

    async def get_user(self, broadcaster_id: int) -> User | None:
        return self._answer(self.user_answer)

    async def get_stream_vod(self, user_id: int, stream_id: int) -> Video | None:
        return self._answer(self.vod_answer)

    async def edit_embed(
        self,
        message_id: int,
        embed: Any,
        channel_id: int,
        view: Any = None,
        content: str | None = None,
    ) -> bool:
        self.edits.append(
            {
                "message_id": message_id,
                "channel_id": channel_id,
                "embed": embed,
                "view": view,
                "content": content,
            }
        )
        if self.edit_error is not None:
            raise self.edit_error
        return self.edit_result

    async def notify(self, text: str, *, key: str | None = None) -> bool:
        self.notified.append((text, key))
        return True

    async def report(self, exc: Exception, context: str, **_: object) -> None:
        self.reported.append(context)


class AlertWorld:
    """Every collaborator of the alert lifecycle, recorded and scriptable."""

    def __init__(self) -> None:
        self.actions: list[Any] = []
        self.cycles = 0
        self.cycle_args: list[tuple[Any, ...]] = []
        self.waits: list[float] = []
        self.wait_outcomes: list[bool] = []
        self.slept: list[float] = []
        self.stored: list[tuple[Any, ...]] = []
        self.store_error: Exception | None = None
        self.rows: list[LiveAlert] = []
        self.row: LiveAlert | None = None
        self.read_error: Exception | None = None
        self.sent: list[dict[str, Any]] = []
        self.message_id: int | None = 900
        self.notified: list[tuple[str, str | None]] = []
        self.reported: list[str] = []
        self.started: list[tuple[Any, ...]] = []

    async def cycle(self, *args: Any) -> Any:
        self.cycles += 1
        self.cycle_args.append(args)
        outcome = self.actions.pop(0) if self.actions else None
        if isinstance(outcome, Exception):
            raise outcome
        return outcome

    async def wait_for(self, awaitable: Any, timeout: float) -> None:
        awaitable.close()
        self.waits.append(timeout)
        woken = self.wait_outcomes.pop(0) if self.wait_outcomes else False
        if not woken:
            raise TimeoutError

    async def sleep(self, seconds: float) -> None:
        self.slept.append(seconds)

    async def upsert_live_alert(self, *args: Any) -> None:
        if self.store_error is not None:
            raise self.store_error
        self.stored.append(args)

    async def get_live_alert(self, broadcaster_id: int) -> LiveAlert | None:
        if self.read_error is not None:
            raise self.read_error
        return self.row

    async def list_live_alerts(self) -> list[LiveAlert]:
        return list(self.rows)

    async def send_embed(
        self, embed: Any, channel_id: int, view: Any = None, content: str | None = None
    ) -> int | None:
        self.sent.append(
            {"embed": embed, "channel_id": channel_id, "view": view, "content": content}
        )
        return self.message_id

    async def notify(self, text: str, *, key: str | None = None) -> bool:
        self.notified.append((text, key))
        return True

    async def report(self, exc: Exception, context: str, **_: object) -> None:
        self.reported.append(context)

    def start(self, *args: Any) -> None:
        self.started.append(args)
