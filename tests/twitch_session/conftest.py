"""Fixtures for the stream session tests, split across files so each is short enough
to be reviewed whole."""

import asyncio
from types import SimpleNamespace
from typing import Any

import pendulum
import pytest

from models.twitch_api_responses.ad_schedule import AdSchedule
from models.twitch_api_responses.stream import Stream
from services.config import config
from services.twitch import stream_session
from tests.twitch_session.support import NOW, Calls


@pytest.fixture
def calls(monkeypatch: pytest.MonkeyPatch) -> Calls:
    calls = Calls()

    async def say_template(broadcaster_id: str | int, key: str, **values: Any) -> bool:
        calls.said.append((broadcaster_id, key, values))
        return True

    async def report(exc: Exception, context: str, **_: object) -> None:
        calls.reported.append(context)

    async def notify(text: str, *, key: str | None = None) -> bool:
        calls.notified.append((text, key))
        return True

    async def get_stream(broadcaster_id: int) -> Stream | None:
        calls.asked.append(broadcaster_id)
        if isinstance(calls.stream_answer, Exception):
            raise calls.stream_answer
        return calls.stream_answer

    async def get_ad_schedule(broadcaster_id: int) -> AdSchedule | None:
        if isinstance(calls.ad_answer, Exception):
            raise calls.ad_answer
        return calls.ad_answer

    async def sleep(seconds: float) -> None:
        calls.slept.append(seconds)

    monkeypatch.setattr(stream_session, "say_template", say_template)
    monkeypatch.setattr(stream_session, "report", report)
    monkeypatch.setattr(stream_session, "notify", notify)
    monkeypatch.setattr(stream_session, "get_stream", get_stream)
    monkeypatch.setattr(stream_session, "get_ad_schedule", get_ad_schedule)
    monkeypatch.setattr(
        stream_session, "asyncio", SimpleNamespace(**{**vars(asyncio), "sleep": sleep})
    )
    monkeypatch.setattr(
        stream_session.shoutout_queue,
        "clear",
        lambda: setattr(calls, "cleared", calls.cleared + 1),
    )
    monkeypatch.setattr(pendulum, "now", lambda tz=None: NOW)
    return calls


@pytest.fixture(autouse=True)
def _fresh_session(monkeypatch: pytest.MonkeyPatch) -> None:
    """The session is module state; each test starts with nobody live."""
    monkeypatch.setattr(stream_session, "_stream", None)
    monkeypatch.setattr(stream_session, "_settled", set())
    monkeypatch.setattr(stream_session, "_ad_break_task", None)
    monkeypatch.setattr(config, "_settings", {"twitch_broadcaster_id": "111"})
