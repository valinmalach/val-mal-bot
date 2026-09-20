"""Fixtures for the Twitch event handler tests."""

import asyncio
from types import SimpleNamespace

import pytest

from services.twitch import autoshoutout, events, live_alert, stream_session
from tests.twitch_events.support import EventWorld
from valmal.core.config import config


@pytest.fixture
def world(monkeypatch: pytest.MonkeyPatch) -> EventWorld:
    world = EventWorld()
    patches = [
        (events, "notify", world.notify),
        (events, "report", world.report),
        (events, "get_stream", world.get_stream),
        (events, "get_user", world.get_user),
        (events, "say", world.say),
        (events, "say_template", world.say_template),
        (events, "dispatch", world.dispatch),
        (events, "asyncio", SimpleNamespace(**{**vars(asyncio), "sleep": world.sleep})),
        (events, "time", SimpleNamespace(monotonic=lambda: world.clock)),
        (stream_session, "began", world.began),
        (stream_session, "wake", world.wake_session),
        (stream_session, "schedule_ad_break_warning", world.schedule_warning),
        (live_alert, "announce", world.announce),
        (live_alert, "wake", world.wake_alert),
        (autoshoutout, "chatted", world.chatted),
        (autoshoutout, "redeemed", world.redeemed),
        (autoshoutout, "raided", world.raided),
    ]
    for target, name, fake in patches:
        monkeypatch.setattr(target, name, fake)
    monkeypatch.setattr(
        config,
        "_settings",
        {"twitch_broadcaster_id": "111", "broadcaster_username": "valinmalach"},
    )
    monkeypatch.setattr(config, "_channels", {"stream_alerts": 5001, "promo": 5002})
    return world
