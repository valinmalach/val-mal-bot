import logging
import os
from typing import Optional

import httpx
import sentry_sdk
from dotenv import load_dotenv

from constants import BOT_ADMIN_CHANNEL
from models import AuthResponse

from .. import send_message

load_dotenv()

TWITCH_CLIENT_ID = os.getenv("TWITCH_CLIENT_ID")
TWITCH_CLIENT_SECRET = os.getenv("TWITCH_CLIENT_SECRET")

logger = logging.getLogger(__name__)


class TwitchTokenManager:
    _instance: Optional["TwitchTokenManager"] = None
    _access_token: str = ""

    def __new__(cls) -> "TwitchTokenManager":
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    @property
    def access_token(self) -> str:
        return self._access_token

    @access_token.setter
    def access_token(self, value: str) -> None:
        self._access_token = value

    @sentry_sdk.trace()
    async def refresh_access_token(self) -> bool:
        scopes = [
            "channel:bot",
            "channel:read:ads",
            "channel:read:redemptions",
            "moderator:manage:announcements",
            "moderator:manage:banned_users",
            "moderator:manage:blocked_terms",
            "moderator:manage:chat_messages",
            "moderator:manage:chat_settings",
            "moderator:manage:shoutouts",
            "moderator:manage:unban_requests",
            "moderator:manage:warnings",
            "moderator:read:chatters",
            "moderator:read:followers",
            "moderator:read:moderators",
            "moderator:read:vips",
            "user:bot",
            "user:read:chat",
            "user:write:chat",
        ]
        params = {
            "client_id": TWITCH_CLIENT_ID,
            "client_secret": TWITCH_CLIENT_SECRET,
            "grant_type": "client_credentials",
            "scope": " ".join(scopes),
        }
        response = httpx.post("https://id.twitch.tv/oauth2/token", params=params)

        if response.status_code < 200 or response.status_code >= 300:
            logger.error(f"Token refresh failed with status={response.status_code}")
            await send_message(
                f"Failed to refresh access token: {response.status_code} {response.text}",
                BOT_ADMIN_CHANNEL,
            )
            return False

        auth_response = AuthResponse.model_validate(response.json())

        if auth_response.token_type == "bearer":
            self._access_token = auth_response.access_token
            return True
        else:
            logger.error(f"Unexpected token type received: {auth_response.token_type}")
            await send_message(
                f"Unexpected token type: {auth_response.token_type}", BOT_ADMIN_CHANNEL
            )
            return False


token_manager = TwitchTokenManager()
