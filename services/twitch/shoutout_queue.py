import asyncio
import io
import logging
import os
import traceback
from typing import Optional

import discord
import pendulum
from dotenv import load_dotenv
from pendulum import DateTime

from constants import BOT_ADMIN_CHANNEL, ErrorDetails, TokenType
from services.helper.helper import send_message
from services.helper.twitch import call_twitch
from services.twitch.api import get_user_by_username

load_dotenv()

logger = logging.getLogger(__name__)

TWITCH_BOT_USER_ID = os.getenv("TWITCH_BOT_USER_ID")
TWITCH_BROADCASTER_ID = os.getenv("TWITCH_BROADCASTER_ID")


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

    def add_to_queue(self, username: str) -> None:
        if username not in self._shoutout_queue:
            self._shoutout_queue.append(username)

    def _can_shoutout_user(self, username: str) -> bool:
        """Check if a user can be shouted out (60-minute rate limit)"""
        if username not in self._last_shoutout_times:
            return True

        last_shoutout = self._last_shoutout_times[username]
        time_since_last = pendulum.now() - last_shoutout
        return time_since_last.total_minutes() >= 59

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

    async def activate(self) -> None:
        try:
            self._activated = True
            while self._activated:
                await self._process_shoutout_queue()
        except Exception as e:
            await self._handle_activation_error(e)

    async def _process_shoutout_queue(self) -> None:
        """Process the shoutout queue once"""
        if len(self._shoutout_queue) == 0:
            await asyncio.sleep(5)
            return None

        username = self._get_next_available_user()
        if username is None:
            await asyncio.sleep(5)
            return None

        await self._execute_shoutout(username)

    async def _execute_shoutout(self, username: str) -> None:
        """Execute a shoutout for the given username"""
        self._shoutout_queue.remove(username)

        user = await get_user_by_username(username)
        if not user:
            await self._handle_user_not_found(username)
            return None

        success = await self._send_shoutout_request(username, user.id)
        if success:
            self._last_shoutout_times[username] = pendulum.now()
            # Twitch rate limit: 1 shoutout per 2 minutes + 5 seconds buffer
            await asyncio.sleep(125)

    async def _handle_user_not_found(self, username: str) -> None:
        """Handle case when user is not found"""
        logger.warning(f"User {username} not found")
        await send_message(f"User {username} not found", BOT_ADMIN_CHANNEL)

    async def _send_shoutout_request(self, username: str, user_id: str) -> bool:
        """Send shoutout request to Twitch API. Returns True if successful."""
        url = "https://api.twitch.tv/helix/chat/shoutouts"
        data = {
            "from_broadcaster_id": TWITCH_BROADCASTER_ID,
            "to_broadcaster_id": user_id,
            "moderator_id": TWITCH_BOT_USER_ID,
        }

        response = await call_twitch("POST", url, data, TokenType.User)
        if response is None or not (200 <= response.status_code < 300):
            await self._handle_shoutout_failure(username, response)
            return False
        return True

    async def _handle_shoutout_failure(self, username: str, response) -> None:
        """Handle failed shoutout request"""
        status_code = response.status_code if response else "No response"
        response_text = response.text if response else ""

        error_msg = (
            f"Failed to send shoutout to {username}: {status_code} {response_text}"
        )
        logger.error(error_msg)
        await send_message(error_msg, BOT_ADMIN_CHANNEL)

    async def _handle_activation_error(self, e: Exception) -> None:
        """Handle errors in the activate method"""
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


shoutout_queue = TwitchShoutoutQueue()
