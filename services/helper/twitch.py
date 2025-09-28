import logging
import os
from typing import Literal, Optional

import httpx
import sentry_sdk
from dotenv import load_dotenv

from constants import (
    BOT_ADMIN_CHANNEL,
)
from models import ChannelChatMessageEventSub
from services.twitch.token_manager import token_manager

from . import (
    send_message,
)

load_dotenv()

TWITCH_CLIENT_ID = os.getenv("TWITCH_CLIENT_ID")
TWITCH_BOT_USER_ID = os.getenv("TWITCH_BOT_USER_ID")

logger = logging.getLogger(__name__)


@sentry_sdk.trace()
async def call_twitch(
    method: Literal["GET", "POST"], url: str, json: Optional[dict]
) -> Optional[httpx.Response]:
    try:
        headers = {
            "Client-ID": TWITCH_CLIENT_ID,
            "Authorization": f"Bearer {token_manager.access_token}",
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
            if await token_manager.refresh_access_token():
                headers["Authorization"] = f"Bearer {token_manager.access_token}"
                if method.upper() == "GET":
                    response = httpx.get(url, headers=headers, params=json)
                elif method.upper() == "POST":
                    response = httpx.post(url, headers=headers, json=json)
            else:
                return None

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
        if not token_manager.access_token:
            refresh_success = await token_manager.refresh_access_token()
            if not refresh_success:
                logger.warning("No access token available and failed to refresh")
                await send_message(
                    "No access token available and failed to refresh",
                    BOT_ADMIN_CHANNEL,
                )
                return

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
