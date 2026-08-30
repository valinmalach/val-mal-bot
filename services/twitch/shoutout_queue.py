import asyncio
import contextlib
import logging
from typing import ClassVar, Self, cast

import httpx
import pendulum

from errors import notify, report
from services.twitch.api import get_user, send_shoutout
from services.twitch.helix import HelixError

logger = logging.getLogger(__name__)

# Twitch: 1 shoutout to the same channel every 60 minutes; stay strictly above that window.
_MIN_SAME_TARGET_COOLDOWN_SECONDS = 61 * 60
# Helix: 1 shoutout every 2 minutes (global); small buffer after success.
_GLOBAL_SHOUTOUT_INTERVAL_SECONDS = 125


class TwitchShoutoutQueue:
    _instance: TwitchShoutoutQueue | None = None
    _activated: bool = False
    _shoutout_queue: ClassVar[list[tuple[str, str]]] = []
    _last_shoutout_by_target_id: ClassVar[dict[str, pendulum.DateTime]] = {}
    _next_attempt_allowed_by_target_id: ClassVar[dict[str, pendulum.DateTime]] = {}

    def __new__(cls) -> Self:
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cast("Self", cls._instance)

    @property
    def activated(self) -> bool:
        return self._activated

    def add_to_queue(self, login: str, user_id: str) -> None:
        if all(uid != user_id for _, uid in self._shoutout_queue):
            self._shoutout_queue.append((login, user_id))

    def _can_shoutout_target(self, user_id: str) -> bool:
        now = pendulum.now()
        next_ok = self._next_attempt_allowed_by_target_id.get(user_id)
        if next_ok is not None and now < next_ok:
            return False
        last = self._last_shoutout_by_target_id.get(user_id)
        if last is None:
            return True
        return (now - last).total_seconds() >= _MIN_SAME_TARGET_COOLDOWN_SECONDS

    def _get_next_available_pair(self) -> tuple[str, str] | None:
        return next(
            (
                (login, uid)
                for login, uid in self._shoutout_queue
                if self._can_shoutout_target(uid)
            ),
            None,
        )

    def _wait_until_from_429(self, response: httpx.Response) -> pendulum.DateTime:
        now = pendulum.now()
        if ra := response.headers.get("Retry-After"):
            with contextlib.suppress(ValueError):
                return now.add(seconds=int(ra))
        if rr := response.headers.get("Ratelimit-Reset"):
            with contextlib.suppress(ValueError):
                reset = pendulum.from_timestamp(int(rr), tz=pendulum.UTC)
                if reset > now:
                    return reset
        return now.add(seconds=_MIN_SAME_TARGET_COOLDOWN_SECONDS)

    async def activate(self) -> None:
        if self._activated:
            return
        try:
            self._activated = True
            while self._activated:
                if len(self._shoutout_queue) == 0:
                    await asyncio.sleep(5)
                    continue

                pair = self._get_next_available_pair()

                if pair is None:
                    await asyncio.sleep(5)
                    continue

                login, user_id_str = pair
                self._shoutout_queue.remove(pair)

                try:
                    user = await get_user(int(user_id_str))
                except HelixError as e:
                    # One unreachable lookup must not end the queue.
                    logger.warning("Could not look up %s for shoutout: %s", login, e)
                    self.add_to_queue(login, user_id_str)
                    await asyncio.sleep(5)
                    continue

                if not user:
                    logger.warning(
                        "User id %s (%s) not found for shoutout", user_id_str, login
                    )
                    await notify(f"User {login} not found for shoutout")
                    continue

                try:
                    await send_shoutout(user.id)
                except HelixError as e:
                    if e.status == 429 and e.response is not None:
                        wait_until = self._wait_until_from_429(e.response)
                        self._next_attempt_allowed_by_target_id[user_id_str] = (
                            wait_until
                        )
                        self.add_to_queue(login, user_id_str)
                        logger.warning(
                            "Shoutout rate limited for %s (id=%s), re-queued; next attempt after %s",
                            login,
                            user_id_str,
                            wait_until,
                        )
                        await asyncio.sleep(_GLOBAL_SHOUTOUT_INTERVAL_SECONDS)
                        continue

                    await report(e, f"Failed to send shoutout to {login}")
                    await asyncio.sleep(_GLOBAL_SHOUTOUT_INTERVAL_SECONDS)
                    continue

                self._last_shoutout_by_target_id[user_id_str] = pendulum.now()
                self._next_attempt_allowed_by_target_id.pop(user_id_str, None)
                await asyncio.sleep(_GLOBAL_SHOUTOUT_INTERVAL_SECONDS)
        except Exception as e:  # noqa: BLE001
            await report(e, "Error in activate method")

    def deactivate(self) -> None:
        self._activated = False
        self._shoutout_queue.clear()
        self._next_attempt_allowed_by_target_id.clear()


shoutout_queue = TwitchShoutoutQueue()
