import asyncio
import io
import logging
import os
import traceback
from typing import Optional

import discord
import httpx
import pendulum
from dotenv import load_dotenv

from constants import BOT_ADMIN_CHANNEL, ErrorDetails, TokenType
from services.helper.helper import send_message
from services.helper.twitch import call_twitch
from services.twitch.api import get_user

load_dotenv()

logger = logging.getLogger(__name__)

TWITCH_BOT_USER_ID = os.getenv("TWITCH_BOT_USER_ID")
TWITCH_BROADCASTER_ID = os.getenv("TWITCH_BROADCASTER_ID")

# Twitch: 1 shoutout to the same channel every 60 minutes; stay strictly above that window.
_MIN_SAME_TARGET_COOLDOWN_SECONDS = 61 * 60
# Helix: 1 shoutout every 2 minutes (global); small buffer after success.
_GLOBAL_SHOUTOUT_INTERVAL_SECONDS = 125


class TwitchShoutoutQueue:
    _instance: Optional["TwitchShoutoutQueue"] = None
    _activated: bool = False
    _shoutout_queue: list[tuple[str, str]] = []
    _last_shoutout_by_target_id: dict[str, DateTime] = {}
    _next_attempt_allowed_by_target_id: dict[str, DateTime] = {}

    def __new__(cls) -> "TwitchShoutoutQueue":
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    @property
    def activated(self) -> bool:
        return self._activated

    def add_to_queue(self, login: str, user_id: str) -> None:
        if not any(uid == user_id for _, uid in self._shoutout_queue):
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

    def _get_next_available_pair(self) -> Optional[tuple[str, str]]:
        return next(
            (
                (login, uid)
                for login, uid in self._shoutout_queue
                if self._can_shoutout_target(uid)
            ),
            None,
        )

    def _wait_until_from_429(self, response: httpx.Response) -> DateTime:
        now = pendulum.now()
        ra = response.headers.get("Retry-After")
        if ra:
            try:
                return now.add(seconds=int(ra))
            except ValueError:
                pass
        rr = response.headers.get("Ratelimit-Reset")
        if rr:
            try:
                reset = pendulum.from_timestamp(int(rr), tz=pendulum.UTC)
                if reset > now:
                    return reset
            except ValueError:
                pass
        return now.add(seconds=_MIN_SAME_TARGET_COOLDOWN_SECONDS)

    async def activate(self) -> None:
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

                user = await get_user(int(user_id_str))
                if not user:
                    logger.warning("User id %s (%s) not found for shoutout", user_id_str, login)
                    await send_message(
                        f"User {login} not found for shoutout", BOT_ADMIN_CHANNEL
                    )
                    continue

                url = "https://api.twitch.tv/helix/chat/shoutouts"
                data = {
                    "from_broadcaster_id": TWITCH_BROADCASTER_ID,
                    "to_broadcaster_id": user.id,
                    "moderator_id": TWITCH_BOT_USER_ID,
                }
                response = await call_twitch("POST", url, data, TokenType.User)
                if response is not None and 200 <= response.status_code < 300:
                    self._last_shoutout_by_target_id[user_id_str] = pendulum.now()
                    self._next_attempt_allowed_by_target_id.pop(user_id_str, None)
                    await asyncio.sleep(_GLOBAL_SHOUTOUT_INTERVAL_SECONDS)
                    continue

                if response is not None and response.status_code == 429:
                    wait_until = self._wait_until_from_429(response)
                    self._next_attempt_allowed_by_target_id[user_id_str] = wait_until
                    self.add_to_queue(login, user_id_str)
                    logger.warning(
                        "Shoutout rate limited for %s (id=%s), re-queued; next attempt after %s — %s",
                        login,
                        user_id_str,
                        wait_until,
                        response.text[:200],
                    )
                    await asyncio.sleep(_GLOBAL_SHOUTOUT_INTERVAL_SECONDS)
                    continue

                logger.error(
                    "Failed to send shoutout to %s: %s %s",
                    login,
                    response.status_code if response else "No response",
                    response.text if response else "",
                )
                await send_message(
                    f"Failed to send shoutout to {login}: {response.status_code if response else 'No response'} {response.text if response else ''}",
                    BOT_ADMIN_CHANNEL,
                )
        except Exception as e:
            error_details: ErrorDetails = {
                "type": type(e).__name__,
                "message": str(e),
                "args": e.args,
                "traceback": traceback.format_exc(),
            }
            error_msg = f"Error in activate method - Type: {error_details['type']}, Message: {error_details['message']}, Args: {error_details['args']}"
            logger.error(f"{error_msg}\nTraceback:\n{error_details['traceback']}")
            traceback_buffer = io.BytesIO(error_details["traceback"].encode("utf-8"))
            traceback_file = discord.File(traceback_buffer, filename="traceback.txt")
            await send_message(error_msg, BOT_ADMIN_CHANNEL, file=traceback_file)

    def deactivate(self) -> None:
        self._activated = False
        self._shoutout_queue = []
        self._next_attempt_allowed_by_target_id.clear()


shoutout_queue = TwitchShoutoutQueue()
