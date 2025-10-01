import logging
import os
from typing import Optional

import httpx
import sentry_sdk
from dotenv import load_dotenv

from constants import BOT_ADMIN_CHANNEL
from models import AuthResponse, RefreshResponse
from services.helper.helper import send_message

load_dotenv()

TWITCH_CLIENT_ID = os.getenv("TWITCH_CLIENT_ID")
TWITCH_CLIENT_SECRET = os.getenv("TWITCH_CLIENT_SECRET")

logger = logging.getLogger(__name__)


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

    @sentry_sdk.trace()
    def _load_app_access_token(self) -> None:
        """Load app access token from file if it exists."""
        try:
            with open("data/twitch/app_access_token.txt", "r") as f:
                self._app_access_token = f.read().strip()
                logger.info("App access token loaded from file")
        except FileNotFoundError:
            logger.info("No existing app access token file found")
        except Exception as e:
            logger.error(f"Error loading app access token from file: {e}")

    @sentry_sdk.trace()
    def _load_user_refresh_token(self) -> None:
        """Load user refresh token from file if it exists."""
        try:
            with open("data/twitch/user_refresh_token.txt", "r") as f:
                self._user_refresh_token = f.read().strip()
                logger.info("User refresh token loaded from file")
        except FileNotFoundError:
            logger.info("No existing user refresh token file found")
        except Exception as e:
            logger.error(f"Error loading user refresh token from file: {e}")

    @sentry_sdk.trace()
    def _load_user_access_token(self) -> None:
        """Load user access token from file if it exists."""
        try:
            with open("data/twitch/user_access_token.txt", "r") as f:
                self._user_access_token = f.read().strip()
                logger.info("User access token loaded from file")
        except FileNotFoundError:
            logger.info("No existing user access token file found")
        except Exception as e:
            logger.error(f"Error loading user access token from file: {e}")

    @sentry_sdk.trace()
    def _load_broadcaster_refresh_token(self) -> None:
        """Load broadcaster refresh token from file if it exists."""
        try:
            with open("data/twitch/broadcaster_refresh_token.txt", "r") as f:
                self._broadcaster_refresh_token = f.read().strip()
                logger.info("Broadcaster refresh token loaded from file")
        except FileNotFoundError:
            logger.info("No existing broadcaster refresh token file found")
        except Exception as e:
            logger.error(f"Error loading broadcaster refresh token from file: {e}")

    @sentry_sdk.trace()
    def _load_broadcaster_access_token(self) -> None:
        """Load broadcaster access token from file if it exists."""
        try:
            with open("data/twitch/broadcaster_access_token.txt", "r") as f:
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

    @sentry_sdk.trace()
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
            self._app_access_token = auth_response.access_token
            os.makedirs("data/twitch", exist_ok=True)
            with open("data/twitch/app_access_token.txt", "w") as f:
                f.write(self._app_access_token)
            return True
        else:
            logger.error(f"Unexpected token type received: {auth_response.token_type}")
            await send_message(
                f"Unexpected token type: {auth_response.token_type}", BOT_ADMIN_CHANNEL
            )
            return False

    @sentry_sdk.trace()
    async def set_user_access_token(self, auth_response: RefreshResponse) -> None:
        self._user_access_token = auth_response.access_token
        self._user_refresh_token = auth_response.refresh_token
        os.makedirs("data/twitch", exist_ok=True)
        with open("data/twitch/user_access_token.txt", "w") as f:
            f.write(self._user_access_token)
        with open("data/twitch/user_refresh_token.txt", "w") as f:
            f.write(self._user_refresh_token)

    @sentry_sdk.trace()
    async def set_broadcaster_access_token(
        self, auth_response: RefreshResponse
    ) -> None:
        self._broadcaster_access_token = auth_response.access_token
        self._broadcaster_refresh_token = auth_response.refresh_token
        os.makedirs("data/twitch", exist_ok=True)
        with open("data/twitch/broadcaster_access_token.txt", "w") as f:
            f.write(self._broadcaster_access_token)
        with open("data/twitch/broadcaster_refresh_token.txt", "w") as f:
            f.write(self._broadcaster_refresh_token)

    @sentry_sdk.trace()
    async def refresh_user_access_token(self) -> bool:
        if not self._user_refresh_token:
            logger.error("No user refresh token available")
            await send_message("No user refresh token available", BOT_ADMIN_CHANNEL)
            return False

        params = {
            "client_id": TWITCH_CLIENT_ID,
            "client_secret": TWITCH_CLIENT_SECRET,
            "grant_type": "refresh_token",
            "refresh_token": self._user_refresh_token,
        }
        response = httpx.post("https://id.twitch.tv/oauth2/token", params=params)

        if response.status_code < 200 or response.status_code >= 300:
            logger.error(
                f"User token refresh failed with status={response.status_code}"
            )
            await send_message(
                f"Failed to refresh user access token: {response.status_code} {response.text}",
                BOT_ADMIN_CHANNEL,
            )
            return False

        auth_response = RefreshResponse.model_validate(response.json())

        if auth_response.token_type == "bearer":
            await self.set_user_access_token(auth_response)
            return True
        else:
            logger.error(f"Unexpected token type received: {auth_response.token_type}")
            await send_message(
                f"Unexpected token type: {auth_response.token_type}", BOT_ADMIN_CHANNEL
            )
            return False

    @sentry_sdk.trace()
    async def refresh_broadcaster_access_token(self) -> bool:
        if not self._broadcaster_refresh_token:
            logger.error("No broadcaster refresh token available")
            await send_message(
                "No broadcaster refresh token available", BOT_ADMIN_CHANNEL
            )
            return False

        params = {
            "client_id": TWITCH_CLIENT_ID,
            "client_secret": TWITCH_CLIENT_SECRET,
            "grant_type": "refresh_token",
            "refresh_token": self._broadcaster_refresh_token,
        }
        response = httpx.post("https://id.twitch.tv/oauth2/token", params=params)

        if response.status_code < 200 or response.status_code >= 300:
            logger.error(
                f"Broadcaster token refresh failed with status={response.status_code}"
            )
            await send_message(
                f"Failed to refresh broadcaster access token: {response.status_code} {response.text}",
                BOT_ADMIN_CHANNEL,
            )
            return False

        auth_response = RefreshResponse.model_validate(response.json())

        if auth_response.token_type == "bearer":
            await self.set_broadcaster_access_token(auth_response)
            return True
        else:
            logger.error(f"Unexpected token type received: {auth_response.token_type}")
            await send_message(
                f"Unexpected token type: {auth_response.token_type}", BOT_ADMIN_CHANNEL
            )
            return False


token_manager = TwitchTokenManager()
