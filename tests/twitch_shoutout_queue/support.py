"""Shared by the shoutout queue tests."""

import asyncio

import pendulum

from services.twitch.shoutout_queue import TwitchShoutoutQueue
from valmal.twitch.models.api.user import User

NOW = pendulum.datetime(2026, 6, 15, 12)
COOLDOWN = 61 * 60


class Stop(BaseException):
    """Ends drain(), whose loop only stops for something that is not an Exception."""


class World:
    def __init__(self) -> None:
        self.now = NOW
        self.users: dict[int, User | Exception | None] = {}
        self.looked_up: list[int] = []
        self.shouted: list[str] = []
        self.shout_error: Exception | None = None
        self.notified: list[tuple[str, str | None]] = []
        self.reported: list[str] = []
        self.slept: list[float] = []
        self.sleeps_before_stop = 3


def pending(queue: TwitchShoutoutQueue) -> list[tuple[str, str]]:
    return list(queue._shoutout_queue)


async def settle_tasks() -> None:
    for _ in range(5):
        await asyncio.sleep(0)
