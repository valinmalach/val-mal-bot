import logging
import os
from typing import Literal, Optional

import httpx
import sentry_sdk
from dotenv import load_dotenv

from constants import (
    BOT_ADMIN_CHANNEL,
    TokenType,
)
from models import ChannelChatMessageEventSub
from services.helper.helper import send_message
from services.twitch.token_manager import token_manager

load_dotenv()

TWITCH_CLIENT_ID = os.getenv("TWITCH_CLIENT_ID")
TWITCH_BOT_USER_ID = os.getenv("TWITCH_BOT_USER_ID")

logger = logging.getLogger(__name__)


@sentry_sdk.trace()
async def call_twitch(
    method: Literal["GET", "POST"],
    url: str,
    json: Optional[dict] = None,
    token_type: TokenType = TokenType.App,
) -> Optional[httpx.Response]:
    try:
        refresh_success = True
        if token_type == TokenType.App and not token_manager.app_access_token:
            refresh_success = await token_manager.refresh_app_access_token()
        elif token_type == TokenType.User and not token_manager.user_access_token:
            refresh_success = await token_manager.refresh_user_access_token()
        elif (
            token_type == TokenType.Broadcaster
            and not token_manager.broadcaster_access_token
        ):
            refresh_success = await token_manager.refresh_user_access_token(True)

        if not refresh_success:
            logger.warning("No access token available and failed to refresh")
            await send_message(
                "No access token available and failed to refresh",
                BOT_ADMIN_CHANNEL,
            )
            return None

        token = (
            token_manager.app_access_token
            if token_type == TokenType.App
            else token_manager.user_access_token
            if token_type == TokenType.User
            else token_manager.broadcaster_access_token
        )
        headers = {
            "Client-ID": TWITCH_CLIENT_ID,
            "Authorization": f"Bearer {token}",
        }

        if method.upper() == "GET":
            response = httpx.get(url, headers=headers, params=json)
        elif method.upper() == "POST":
            response = httpx.post(url, headers=headers, json=json)
        else:
            logger.error(f"Unsupported HTTP method: {method}")
            await send_message(
                f"Unsupported HTTP method: {method}",
                BOT_ADMIN_CHANNEL,
            )
            return None

        if response.status_code == 401:
            logger.warning("Unauthorized request, refreshing token...")
            if token_type == TokenType.App:
                refresh_success = await token_manager.refresh_app_access_token()
            elif token_type == TokenType.User:
                refresh_success = await token_manager.refresh_user_access_token()
            else:
                refresh_success = await token_manager.refresh_user_access_token(True)

            if not refresh_success:
                return None

            token = (
                token_manager.app_access_token
                if token_type == TokenType.App
                else token_manager.user_access_token
                if token_type == TokenType.User
                else token_manager.broadcaster_access_token
            )
            headers["Authorization"] = f"Bearer {token}"
            if method.upper() == "GET":
                response = httpx.get(url, headers=headers, params=json)
            elif method.upper() == "POST":
                response = httpx.post(url, headers=headers, json=json)
        return response

    except Exception as e:
        logger.error(f"Exception during Twitch API call: {e}")
        sentry_sdk.capture_exception(e)
        await send_message(
            f"Exception during Twitch API call: {e}",
            BOT_ADMIN_CHANNEL,
        )
        return None


@sentry_sdk.trace()
async def check_mod(event_sub: ChannelChatMessageEventSub) -> bool:
    has_mod = any(
        badge.set_id in {"moderator", "broadcaster"}
        for badge in event_sub.event.badges or []
    )
    broadcaster_id = event_sub.event.broadcaster_user_id
    if not has_mod:
        message = "Only moderators can use this command."
        await twitch_send_message(broadcaster_id, message)
        return False
    return True


@sentry_sdk.trace()
async def twitch_send_message(broadcaster_id: str, message: str) -> None:
    try:
        url = "https://api.twitch.tv/helix/chat/messages"
        data = {
            "broadcaster_id": broadcaster_id,
            "sender_id": TWITCH_BOT_USER_ID,
            "message": message,
            "for_source_only": False,
        }
        response = await call_twitch("POST", url, data)
        if (
            response is None
            or response.status_code < 200
            or response.status_code >= 300
        ):
            logger.warning(
                f"Failed to send message: {response.status_code if response else 'No response'}"
            )
            await send_message(
                f"Failed to send message: {response.status_code if response else 'No response'} {response.text if response else ''}",
                BOT_ADMIN_CHANNEL,
            )
            return
    except Exception as e:
        logger.error(f"Error sending Twitch message: {e}")
        sentry_sdk.capture_exception(e)
        await send_message(f"Error sending Twitch message: {e}", BOT_ADMIN_CHANNEL)
