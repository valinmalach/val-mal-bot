import asyncio
import logging
import os
from typing import Optional

import sentry_sdk
from dotenv import load_dotenv

from constants import BOT_ADMIN_CHANNEL
from services import call_twitch, get_user_by_username, send_message
from services.twitch_token_manager import token_manager

load_dotenv()

TWITCH_BOT_USER_ID = os.getenv("TWITCH_BOT_USER_ID")
TWITCH_BROADCASTER_ID = os.getenv("TWITCH_BROADCASTER_ID")

logger = logging.getLogger(__name__)


@sentry_sdk.trace()
async def refresh_access_token() -> bool:
    return await token_manager.refresh_access_token()


class TwitchShoutoutQueue:
    _instance: Optional["TwitchShoutoutQueue"] = None
    _activated: bool = False
    _shoutout_queue: list[str] = []

    def __new__(cls) -> "TwitchShoutoutQueue":
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    @property
    def activated(self) -> bool:
        return self._activated

    @activated.setter
    def activated(self, value: bool) -> None:
        self._activated = value

    @property
    def shoutout_queue(self) -> list[str]:
        return self._shoutout_queue

    @shoutout_queue.setter
    def shoutout_queue(self, value: list[str]) -> None:
        self._shoutout_queue = value

    def add_to_queue(self, username: str) -> None:
        if username not in self._shoutout_queue:
            self._shoutout_queue.append(username)

    @sentry_sdk.trace()
    async def activate(self) -> None:
        try:
            self._activated = True
            while self._activated:
                if len(self._shoutout_queue) == 0:
                    await asyncio.sleep(5)
                    continue

                username = self._shoutout_queue.pop(0)
                user = await get_user_by_username(username)
                if not user:
                    logger.warning(f"User {username} not found")
                    await send_message(f"User {username} not found", BOT_ADMIN_CHANNEL)
                    continue
                if not token_manager.access_token:
                    refresh_success = await refresh_access_token()
                    if not refresh_success:
                        logger.warning(
                            "No access token available and failed to refresh"
                        )
                        await send_message(
                            "No access token available and failed to refresh",
                            BOT_ADMIN_CHANNEL,
                        )
                        continue

                url = "https://api.twitch.tv/helix/chat/shoutouts"
                data = {
                    "from_broadcaster_id": TWITCH_BROADCASTER_ID,
                    "to_broadcaster_id": user.id,
                    "moderator_id": TWITCH_BOT_USER_ID,
                }
                response = await call_twitch("POST", url, data)
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
