"""Fixtures for the shoutout queue tests, split across files so each is short enough
to be reviewed whole."""

import asyncio
from types import SimpleNamespace

import pendulum
import pytest

from services.twitch import shoutout_queue as sq
from services.twitch.shoutout_queue import TwitchShoutoutQueue
from tests.twitch.support import user_json
from tests.twitch_shoutout_queue.support import Stop, World
from valmal.twitch.models.api.user import User


@pytest.fixture
def world(monkeypatch: pytest.MonkeyPatch) -> World:
    world = World()

    async def get_user(user_id: int) -> User | None:
        world.looked_up.append(user_id)
        answer = world.users.get(user_id, User.model_validate(user_json(str(user_id))))
        if isinstance(answer, Exception):
            raise answer
        return answer

    async def send_shoutout(to_broadcaster_id: str) -> None:
        if world.shout_error is not None:
            raise world.shout_error
        world.shouted.append(to_broadcaster_id)

    async def notify(text: str, *, key: str | None = None) -> bool:
        world.notified.append((text, key))
        return True

    async def report(exc: Exception, context: str, **_: object) -> None:
        world.reported.append(context)

    async def sleep(seconds: float) -> None:
        world.slept.append(seconds)
        if len(world.slept) >= world.sleeps_before_stop:
            raise Stop

    monkeypatch.setattr(sq, "get_user", get_user)
    monkeypatch.setattr(sq, "send_shoutout", send_shoutout)
    monkeypatch.setattr(sq, "notify", notify)
    monkeypatch.setattr(sq, "report", report)
    monkeypatch.setattr(
        sq, "asyncio", SimpleNamespace(**{**vars(asyncio), "sleep": sleep})
    )
    monkeypatch.setattr(pendulum, "now", lambda tz=None: world.now)
    return world


@pytest.fixture
def queue(monkeypatch: pytest.MonkeyPatch) -> TwitchShoutoutQueue:
    """The queue is one shared instance with class-level state; each test gets it empty."""
    monkeypatch.setattr(TwitchShoutoutQueue, "_shoutout_queue", [])
    monkeypatch.setattr(TwitchShoutoutQueue, "_last_shoutout_by_target_id", {})
    monkeypatch.setattr(TwitchShoutoutQueue, "_next_attempt_allowed_by_target_id", {})
    return TwitchShoutoutQueue()
