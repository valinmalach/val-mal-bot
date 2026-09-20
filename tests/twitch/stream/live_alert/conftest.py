"""Fixtures for the live alert tests, split across files so each is short enough to
be reviewed whole."""

import asyncio
from types import SimpleNamespace

import pendulum
import pytest

from tests.twitch.stream.live_alert.support import (
    NOW,
    TEMPLATES,
    AlertWorld,
    CycleWorld,
)
from valmal.core.config import config
from valmal.db.models import DiscordRole
from valmal.twitch.stream import live_alert
from valmal.twitch.stream import live_alert_close as close_module
from valmal.twitch.stream import live_alert_cycle as cycle_module


@pytest.fixture
def embed_config(monkeypatch: pytest.MonkeyPatch) -> None:
    """The templates and colours the embeds read, and a fixed clock."""
    monkeypatch.setattr(config, "_templates", dict(TEMPLATES))
    monkeypatch.setattr(
        config,
        "_settings",
        {"embed_color_stream": 0x9146FF},
    )
    monkeypatch.setattr(config, "_channels", {"stream_alerts": 5001, "promo": 5002})
    monkeypatch.setattr(config, "_roles", {})
    monkeypatch.setattr(pendulum, "now", lambda tz=None: NOW)


@pytest.fixture
def cycle_world(monkeypatch: pytest.MonkeyPatch, embed_config: None) -> CycleWorld:
    world = CycleWorld()
    patches = [
        (cycle_module.repository, "get_live_alert", world.get_live_alert),
        (cycle_module.repository, "delete_live_alert", world.delete_live_alert),
        (cycle_module, "get_stream", world.get_stream),
        (close_module, "get_channel", world.get_channel),
        (cycle_module, "get_user", world.get_user),
        (close_module, "get_stream_vod", world.get_stream_vod),
        (cycle_module, "edit_embed", world.edit_embed),
        (cycle_module, "notify", world.notify),
        (cycle_module, "report", world.report),
        (close_module, "notify", world.notify),
        (close_module, "report", world.report),
    ]
    for target, name, fake in patches:
        monkeypatch.setattr(target, name, fake)
    return world


@pytest.fixture
def alert_world(monkeypatch: pytest.MonkeyPatch, embed_config: None) -> AlertWorld:
    world = AlertWorld()
    patches = [
        (live_alert, "cycle", world.cycle),
        (live_alert, "send_embed", world.send_embed),
        (live_alert, "notify", world.notify),
        (live_alert, "report", world.report),
        (live_alert.repository, "upsert_live_alert", world.upsert_live_alert),
        (live_alert.repository, "get_live_alert", world.get_live_alert),
        (live_alert.repository, "list_live_alerts", world.list_live_alerts),
        (
            live_alert,
            "asyncio",
            SimpleNamespace(
                **{**vars(asyncio), "wait_for": world.wait_for, "sleep": world.sleep}
            ),
        ),
        (live_alert, "_update_tasks", {}),
        (live_alert, "_wakeups", {}),
    ]
    for target, name, fake in patches:
        monkeypatch.setattr(target, name, fake)
    monkeypatch.setattr(
        config,
        "_roles",
        {"live_alerts": DiscordRole(key="live_alerts", role_id=777, name="x")},
    )
    return world
