import asyncio
import logging
import os
from typing import Optional

import pendulum
import sentry_sdk
from dotenv import load_dotenv
from pendulum import DateTime

from constants import BOT_ADMIN_CHANNEL

from ..helper.helper import send_message
from ..helper.twitch import call_twitch
from ..twitch.api import get_user_by_username

load_dotenv()

TWITCH_BOT_USER_ID = os.getenv("TWITCH_BOT_USER_ID")
TWITCH_BROADCASTER_ID = os.getenv("TWITCH_BROADCASTER_ID")

logger = logging.getLogger(__name__)


class TwitchShoutoutQueue:
    _instance: Optional["TwitchShoutoutQueue"] = None
    _activated: bool = False
    _shoutout_queue: list[str] = []
    _last_shoutout_times: dict[str, DateTime] = {}

    def __new__(cls) -> "TwitchShoutoutQueue":
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    @property
    def activated(self) -> bool:
        return self._activated

    @sentry_sdk.trace()
    def add_to_queue(self, username: str) -> None:
        if username not in self._shoutout_queue:
            self._shoutout_queue.append(username)

    @sentry_sdk.trace()
    def _can_shoutout_user(self, username: str) -> bool:
        """Check if a user can be shouted out (60-minute rate limit)"""
        if username not in self._last_shoutout_times:
            return True

        last_shoutout = self._last_shoutout_times[username]
        time_since_last = pendulum.now() - last_shoutout
        return time_since_last.total_minutes() >= 59

    @sentry_sdk.trace()
    def _get_next_available_user(self) -> Optional[str]:
        """Get the next user that can be shouted out, or None if none available"""
        return next(
            (
                username
                for username in self._shoutout_queue
                if self._can_shoutout_user(username)
            ),
            None,
        )

    @sentry_sdk.trace()
    async def activate(self) -> None:
        try:
            self._activated = True
            while self._activated:
                if len(self._shoutout_queue) == 0:
                    await asyncio.sleep(5)
                    continue

                username = self._get_next_available_user()

                if username is None:
                    await asyncio.sleep(5)
                    continue

                self._shoutout_queue.remove(username)

                user = await get_user_by_username(username)
                if not user:
                    logger.warning(f"User {username} not found")
                    await send_message(f"User {username} not found", BOT_ADMIN_CHANNEL)
                    continue

                url = "https://api.twitch.tv/helix/chat/shoutouts"
                data = {
                    "from_broadcaster_id": TWITCH_BROADCASTER_ID,
                    "to_broadcaster_id": user.id,
                    "moderator_id": TWITCH_BOT_USER_ID,
                }
                response = await call_twitch("POST", url, data, user_token=True)
                if (
                    response is None
                    or response.status_code < 200
                    or response.status_code >= 300
                ):
                    logger.error(
                        f"Failed to send shoutout to {username}: {response.status_code if response else 'No response'} {response.text if response else ''}"
                    )
                    await send_message(
                        f"Failed to send shoutout to {username}: {response.status_code if response else 'No response'} {response.text if response else ''}",
                        BOT_ADMIN_CHANNEL,
                    )
                else:
                    self._last_shoutout_times[username] = pendulum.now()

                    # Twitch rate limit: 1 shoutout per 2 minutes + 5 seconds buffer
                    await asyncio.sleep(125)
        except Exception as e:
            logger.error(f"Error in activate method: {e}")
            sentry_sdk.capture_exception(e)
            await send_message(f"Error in shoutout queue: {e}", BOT_ADMIN_CHANNEL)

    async def deactivate(self) -> None:
        self._activated = False
        self._shoutout_queue = []


shoutout_queue = TwitchShoutoutQueue()
