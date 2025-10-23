import io
import logging
import os
import traceback
from typing import Literal, Optional

import discord
from dotenv import load_dotenv
from httpx import Response

from constants import (
    BOT_ADMIN_CHANNEL,
    ErrorDetails,
    TokenType,
)
from models import ChannelChatMessageEventSub
from services.helper.helper import send_message
from services.helper.http_client import http_client_manager
from services.twitch.token_manager import token_manager

load_dotenv()

logger = logging.getLogger(__name__)

TWITCH_CLIENT_ID = os.getenv("TWITCH_CLIENT_ID")
TWITCH_BOT_USER_ID = os.getenv("TWITCH_BOT_USER_ID")


async def log_error(message: str, traceback_str: str) -> None:
    traceback_buffer = io.BytesIO(traceback_str.encode("utf-8"))
    traceback_file = discord.File(traceback_buffer, filename="traceback.txt")
    await send_message(message, BOT_ADMIN_CHANNEL, file=traceback_file)


async def _ensure_token_available(token_type: TokenType) -> bool:
    """Ensure the appropriate token is available, refreshing if necessary."""
    if token_type == TokenType.App and not token_manager.app_access_token:
        return await token_manager.refresh_app_access_token()
    elif token_type == TokenType.User and not token_manager.user_access_token:
        return await token_manager.refresh_user_access_token()
    elif (
        token_type == TokenType.Broadcaster
        and not token_manager.broadcaster_access_token
    ):
        return await token_manager.refresh_user_access_token(True)
    return True


def _get_token_for_type(token_type: TokenType) -> Optional[str]:
    """Get the appropriate token based on token type."""
    if token_type == TokenType.App:
        return token_manager.app_access_token
    elif token_type == TokenType.User:
        return token_manager.user_access_token
    else:
        return token_manager.broadcaster_access_token


async def _refresh_token_for_type(token_type: TokenType) -> bool:
    """Refresh the appropriate token based on token type."""
    if token_type == TokenType.App:
        return await token_manager.refresh_app_access_token()
    elif token_type == TokenType.User:
        return await token_manager.refresh_user_access_token()
    else:
        return await token_manager.refresh_user_access_token(True)


async def _make_http_request(
    method: str, url: str, headers: dict, json: Optional[dict]
) -> Optional[Response]:
    """Make the HTTP request based on method."""
    if method.upper() == "GET":
        return await http_client_manager.request(
            "GET", url, headers=headers, params=json
        )
    elif method.upper() == "POST":
        return await http_client_manager.request(
            "POST", url, headers=headers, json=json
        )
    else:
        logger.error(f"Unsupported HTTP method: {method}")
        await send_message(f"Unsupported HTTP method: {method}", BOT_ADMIN_CHANNEL)
        return None


async def _handle_unauthorized_response(
    method: str, url: str, headers: dict, json: Optional[dict], token_type: TokenType
) -> Optional[Response]:
    """Handle 401 unauthorized response by refreshing token and retrying."""
    logger.warning("Unauthorized request, refreshing token...")
    refresh_success = await _refresh_token_for_type(token_type)

    if not refresh_success:
        return None

    token = _get_token_for_type(token_type)
    headers["Authorization"] = f"Bearer {token}"
    return await _make_http_request(method, url, headers, json)


async def call_twitch(
    method: Literal["GET", "POST"],
    url: str,
    json: Optional[dict] = None,
    token_type: TokenType = TokenType.App,
) -> Optional[Response]:
    try:
        # Ensure token is available
        refresh_success = await _ensure_token_available(token_type)
        if not refresh_success:
            logger.warning("No access token available and failed to refresh")
            await send_message(
                "No access token available and failed to refresh", BOT_ADMIN_CHANNEL
            )
            return None

        # Get token and prepare headers
        token = _get_token_for_type(token_type)
        headers = {
            "Client-ID": TWITCH_CLIENT_ID,
            "Authorization": f"Bearer {token}",
        }

        # Make initial request
        response = await _make_http_request(method, url, headers, json)
        if response is None:
            return None

        # Handle unauthorized response
        if response.status_code == 401:
            response = await _handle_unauthorized_response(
                method, url, headers, json, token_type
            )

        return response

    except Exception as e:
        error_details: ErrorDetails = {
            "type": type(e).__name__,
            "message": str(e),
            "args": e.args,
            "traceback": traceback.format_exc(),
        }
        error_msg = f"Exception during Twitch API call - Type: {error_details['type']}, Message: {error_details['message']}, Args: {error_details['args']}"
        logger.error(f"{error_msg}\nTraceback:\n{error_details['traceback']}")
        await log_error(error_msg, error_details["traceback"])
        return None


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
        error_details: ErrorDetails = {
            "type": type(e).__name__,
            "message": str(e),
            "args": e.args,
            "traceback": traceback.format_exc(),
        }
        error_msg = f"Error sending Twitch message - Type: {error_details['type']}, Message: {error_details['message']}, Args: {error_details['args']}"
        logger.error(f"{error_msg}\nTraceback:\n{error_details['traceback']}")
        await log_error(error_msg, error_details["traceback"])
