"""Shared by the stream session tests."""

from datetime import UTC, datetime
from typing import Any

from tests.twitch.support import stream_json
from valmal.twitch.models.api.ad_schedule import AdSchedule
from valmal.twitch.models.api.stream import Stream

NOW = datetime(2026, 6, 15, 12, tzinfo=UTC)


def stream(id: str = "10", **overrides: Any) -> Stream:
    return Stream.model_validate(stream_json(id, **overrides))


class Calls:
    """What the session said and asked, in order."""

    def __init__(self) -> None:
        self.said: list[tuple[str | int, str, dict[str, Any]]] = []
        self.reported: list[str] = []
        self.notified: list[tuple[str, str | None]] = []
        self.cleared = 0
        self.stream_answer: Stream | Exception | None = None
        self.ad_answer: AdSchedule | Exception | None = None
        self.asked: list[int] = []
        self.slept: list[float] = []
