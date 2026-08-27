import io
import logging
import traceback
from typing import Literal

import discord
from httpx import Response

from config import settings
from constants import ErrorDetails, TokenType
from models import ChannelChatMessageEventSub
from services.config import config
from services.helper.helper import send_message
from services.helper.http_client import http_client_manager, is_transient_network_error
from services.twitch.token_manager import token_manager

logger = logging.getLogger(__name__)


async def log_error(message: str, traceback_str: str) -> None:
    traceback_buffer = io.BytesIO(traceback_str.encode("utf-8"))
    traceback_file = discord.File(traceback_buffer, filename="traceback.txt")
    await send_message(message, config.channel("bot_admin"), file=traceback_file)


async def _ensure_token_available(token_type: TokenType) -> bool:
    """Ensure a usable token is available, refreshing if missing or due.

    The 401 handling below still covers tokens with no known expiry.
    """
    if _get_token_for_type(token_type) and not token_manager.needs_refresh(token_type):
        return True
    return await _refresh_token_for_type(token_type)


def _get_token_for_type(token_type: TokenType) -> str | None:
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
    method: str, url: str, headers: dict, json: dict | None
) -> Response | None:
    """Make the HTTP request based on method."""
    if method.upper() == "GET":
        return await http_client_manager.request(
            "GET", url, headers=headers, params=json
        )
    elif method.upper() == "POST":
        return await http_client_manager.request(
            "POST", url, headers=headers, json=json
        )
    elif method.upper() == "DELETE":
        return await http_client_manager.request("DELETE", url, headers=headers)
    else:
        logger.error(f"Unsupported HTTP method: {method}")
        await send_message(
            f"Unsupported HTTP method: {method}", config.channel("bot_admin")
        )
        return None


async def _handle_unauthorized_response(
    method: str, url: str, headers: dict, json: dict | None, token_type: TokenType
) -> Response | None:
    """Handle 401 unauthorized response by refreshing token and retrying."""
    logger.warning("Unauthorized request, refreshing token...")
    refresh_success = await _refresh_token_for_type(token_type)

    if not refresh_success:
        return None

    token = _get_token_for_type(token_type)
    headers["Authorization"] = f"Bearer {token}"
    return await _make_http_request(method, url, headers, json)


async def call_twitch(
    method: Literal["GET", "POST", "DELETE"],
    url: str,
    json: dict | None = None,
    token_type: TokenType = TokenType.App,
) -> Response | None:
    try:
        refresh_success = await _ensure_token_available(token_type)
        if not refresh_success:
            logger.warning("No access token available and failed to refresh")
            await send_message(
                "No access token available and failed to refresh",
                config.channel("bot_admin"),
            )
            return None

        token = _get_token_for_type(token_type)
        headers = {
            "Client-ID": settings.twitch_client_id,
            "Authorization": f"Bearer {token}",
        }

        response = await _make_http_request(method, url, headers, json)
        if response is None:
            return None

        if response.status_code == 401:
            response = await _handle_unauthorized_response(
                method, url, headers, json, token_type
            )

        return response

    except Exception as e:
        is_retryable = is_transient_network_error(e)

        if is_retryable:
            # Re-raise retryable errors so retry_api_call can handle them
            raise

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
            "sender_id": settings.twitch_bot_user_id,
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
                config.channel("bot_admin"),
            )
            return
    except Exception as e:  # noqa: BLE001
        error_details: ErrorDetails = {
            "type": type(e).__name__,
            "message": str(e),
            "args": e.args,
            "traceback": traceback.format_exc(),
        }
        error_msg = f"Error sending Twitch message - Type: {error_details['type']}, Message: {error_details['message']}, Args: {error_details['args']}"
        logger.error(f"{error_msg}\nTraceback:\n{error_details['traceback']}")
        await log_error(error_msg, error_details["traceback"])
