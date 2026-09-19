"""Shared by the stream session tests."""

from typing import Any

import pendulum

from models.twitch_api_responses.ad_schedule import AdSchedule
from models.twitch_api_responses.stream import Stream
from tests.twitch.support import stream_json

NOW = pendulum.datetime(2026, 6, 15, 12)


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
