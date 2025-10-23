import logging
import os
from typing import Optional

import aiofiles
from dotenv import load_dotenv

from constants import (
    APP_ACCESS_TOKEN_FILE,
    BOT_ADMIN_CHANNEL,
    BROADCASTER_ACCESS_TOKEN_FILE,
    BROADCASTER_REFRESH_TOKEN_FILE,
    USER_ACCESS_TOKEN_FILE,
    USER_REFRESH_TOKEN_FILE,
)
from models import AuthResponse, RefreshResponse
from services.helper.helper import send_message
from services.helper.http_client import http_client_manager

load_dotenv()

logger = logging.getLogger(__name__)

TWITCH_CLIENT_ID = os.getenv("TWITCH_CLIENT_ID")
TWITCH_CLIENT_SECRET = os.getenv("TWITCH_CLIENT_SECRET")


class TwitchTokenManager:
    _instance: Optional["TwitchTokenManager"] = None
    _app_access_token: str = ""
    _user_refresh_token: str = ""
    _user_access_token: str = ""
    _broadcaster_refresh_token: str = ""
    _broadcaster_access_token: str = ""

    def __new__(cls) -> "TwitchTokenManager":
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._load_app_access_token()
            cls._instance._load_user_refresh_token()
            cls._instance._load_user_access_token()
            cls._instance._load_broadcaster_refresh_token()
            cls._instance._load_broadcaster_access_token()
        return cls._instance

    def _load_app_access_token(self) -> None:
        """Load app access token from file if it exists."""
        try:
            with open(APP_ACCESS_TOKEN_FILE, "r") as f:
                self._app_access_token = f.read().strip()
                logger.info("App access token loaded from file")
        except FileNotFoundError:
            logger.info("No existing app access token file found")
        except Exception as e:
            logger.error(f"Error loading app access token from file: {e}")

    def _load_user_refresh_token(self) -> None:
        """Load user refresh token from file if it exists."""
        try:
            with open(USER_REFRESH_TOKEN_FILE, "r") as f:
                self._user_refresh_token = f.read().strip()
                logger.info("User refresh token loaded from file")
        except FileNotFoundError:
            logger.info("No existing user refresh token file found")
        except Exception as e:
            logger.error(f"Error loading user refresh token from file: {e}")

    def _load_user_access_token(self) -> None:
        """Load user access token from file if it exists."""
        try:
            with open(USER_ACCESS_TOKEN_FILE, "r") as f:
                self._user_access_token = f.read().strip()
                logger.info("User access token loaded from file")
        except FileNotFoundError:
            logger.info("No existing user access token file found")
        except Exception as e:
            logger.error(f"Error loading user access token from file: {e}")

    def _load_broadcaster_refresh_token(self) -> None:
        """Load broadcaster refresh token from file if it exists."""
        try:
            with open(BROADCASTER_REFRESH_TOKEN_FILE, "r") as f:
                self._broadcaster_refresh_token = f.read().strip()
                logger.info("Broadcaster refresh token loaded from file")
        except FileNotFoundError:
            logger.info("No existing broadcaster refresh token file found")
        except Exception as e:
            logger.error(f"Error loading broadcaster refresh token from file: {e}")

    def _load_broadcaster_access_token(self) -> None:
        """Load broadcaster access token from file if it exists."""
        try:
            with open(BROADCASTER_ACCESS_TOKEN_FILE, "r") as f:
                self._broadcaster_access_token = f.read().strip()
                logger.info("Broadcaster access token loaded from file")
        except FileNotFoundError:
            logger.info("No existing broadcaster access token file found")
        except Exception as e:
            logger.error(f"Error loading broadcaster access token from file: {e}")

    @property
    def app_access_token(self) -> str:
        return self._app_access_token

    @property
    def user_access_token(self) -> str:
        return self._user_access_token

    @property
    def broadcaster_access_token(self) -> str:
        return self._broadcaster_access_token

    async def refresh_app_access_token(self) -> bool:
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
        response = await http_client_manager.request(
            "POST", "https://id.twitch.tv/oauth2/token", params=params
        )

        if response.status_code < 200 or response.status_code >= 300:
            logger.error(f"Token refresh failed with status={response.status_code}")
            await send_message(
                f"Failed to refresh access token: {response.status_code} {response.text}",
                BOT_ADMIN_CHANNEL,
            )
            return False

        auth_response = AuthResponse.model_validate(response.json())

        if auth_response.token_type == "bearer":
            self._app_access_token = auth_response.access_token
            async with aiofiles.open(APP_ACCESS_TOKEN_FILE, "w") as f:
                await f.write(self._app_access_token)
            return True
        else:
            logger.error(f"Unexpected token type received: {auth_response.token_type}")
            await send_message(
                f"Unexpected token type: {auth_response.token_type}", BOT_ADMIN_CHANNEL
            )
            return False

    async def set_user_access_token(self, auth_response: RefreshResponse) -> None:
        self._user_access_token = auth_response.access_token
        self._user_refresh_token = auth_response.refresh_token
        async with aiofiles.open(USER_ACCESS_TOKEN_FILE, "w") as f:
            await f.write(self._user_access_token)
        async with aiofiles.open(USER_REFRESH_TOKEN_FILE, "w") as f:
            await f.write(self._user_refresh_token)

    async def set_broadcaster_access_token(
        self, auth_response: RefreshResponse
    ) -> None:
        self._broadcaster_access_token = auth_response.access_token
        self._broadcaster_refresh_token = auth_response.refresh_token
        async with aiofiles.open(BROADCASTER_ACCESS_TOKEN_FILE, "w") as f:
            await f.write(self._broadcaster_access_token)
        async with aiofiles.open(BROADCASTER_REFRESH_TOKEN_FILE, "w") as f:
            await f.write(self._broadcaster_refresh_token)

    async def refresh_user_access_token(self, broadcaster: bool = False) -> bool:
        if not broadcaster and not self._user_refresh_token:
            logger.error("No user refresh token available")
            await send_message("No user refresh token available", BOT_ADMIN_CHANNEL)
            return False
        elif broadcaster and not self._broadcaster_refresh_token:
            logger.error("No broadcaster refresh token available")
            await send_message(
                "No broadcaster refresh token available", BOT_ADMIN_CHANNEL
            )
            return False

        params = {
            "client_id": TWITCH_CLIENT_ID,
            "client_secret": TWITCH_CLIENT_SECRET,
            "grant_type": "refresh_token",
            "refresh_token": self._broadcaster_refresh_token
            if broadcaster
            else self._user_refresh_token,
        }
        response = await http_client_manager.request(
            "POST", "https://id.twitch.tv/oauth2/token", params=params
        )

        if response.status_code < 200 or response.status_code >= 300:
            logger.error(
                f"{'Broadcaster' if broadcaster else 'User'} token refresh failed with status={response.status_code}"
            )
            await send_message(
                f"Failed to refresh {'broadcaster' if broadcaster else 'user'} access token: {response.status_code} {response.text}",
                BOT_ADMIN_CHANNEL,
            )
            return False

        auth_response = RefreshResponse.model_validate(response.json())

        if auth_response.token_type == "bearer":
            await self.set_broadcaster_access_token(
                auth_response
            ) if broadcaster else await self.set_user_access_token(auth_response)
            return True
        else:
            logger.error(f"Unexpected token type received: {auth_response.token_type}")
            await send_message(
                f"Unexpected token type: {auth_response.token_type}", BOT_ADMIN_CHANNEL
            )
            return False


token_manager = TwitchTokenManager()
