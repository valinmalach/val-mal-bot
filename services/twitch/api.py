import asyncio
import io
import itertools
import logging
import os
import traceback
from typing import List, Literal, Optional

import discord
import pendulum
import polars as pl
from discord.ui import View
from dotenv import load_dotenv

from constants import (
    BOT_ADMIN_CHANNEL,
    BROADCASTER_USERNAME,
    LIVE_ALERTS,
    LIVE_ALERTS_ROLE,
    STREAM_ALERTS_CHANNEL,
    ErrorDetails,
    TokenType,
)
from models import (
    AdSchedule,
    AdScheduleResponse,
    Channel,
    ChannelResponse,
    Stream,
    StreamResponse,
    Subscription,
    SubscriptionResponse,
    User,
    UserResponse,
    Video,
    VideoResponse,
)
from services.helper.helper import (
    delete_row_from_parquet,
    edit_embed,
    get_age,
    parse_rfc3339,
    read_parquet_cached,
    send_message,
)
from services.helper.twitch import call_twitch

load_dotenv()

logger = logging.getLogger(__name__)

APP_URL = os.getenv("APP_URL")
TWITCH_WEBHOOK_SECRET = os.getenv("TWITCH_WEBHOOK_SECRET")


async def log_error(message: str, traceback_str: str) -> None:
    traceback_buffer = io.BytesIO(traceback_str.encode("utf-8"))
    traceback_file = discord.File(traceback_buffer, filename="traceback.txt")
    await send_message(message, BOT_ADMIN_CHANNEL, file=traceback_file)


async def retry_api_call(func, *args, max_retries=3, delay=1, **kwargs):
    """Retry API calls with exponential backoff for connection issues."""
    for attempt in range(max_retries):
        try:
            return await func(*args, **kwargs)
        except Exception as e:
            if attempt == max_retries - 1:
                raise e

            error_str = str(e).lower()
            if all(
                term not in error_str
                for term in [
                    "connectionterminated",
                    "connection",
                    "timeout",
                    "network",
                ]
            ):
                raise e
            wait_time = delay * (2**attempt)
            logger.warning(
                f"Connection error on attempt {attempt + 1}, retrying in {wait_time}s: {e}"
            )
            await asyncio.sleep(wait_time)


async def _handle_subscription_request_error(e: Exception) -> None:
    """Handle errors from subscription API requests."""
    error_details: ErrorDetails = {
        "type": type(e).__name__,
        "message": str(e),
        "args": e.args,
        "traceback": traceback.format_exc(),
    }
    error_msg = f"Error fetching subscriptions after retries - Type: {error_details['type']}, Message: {error_details['message']}, Args: {error_details['args']}"
    logger.error(f"{error_msg}\nTraceback:\n{error_details['traceback']}")
    await log_error(error_msg, error_details["traceback"])


async def _handle_subscription_response_error(response) -> None:
    """Handle HTTP response errors from subscription API."""
    logger.warning(
        f"Error fetching subscriptions: {response.status_code if response else 'No response'}"
    )
    await send_message(
        f"Failed to fetch subscriptions: {response.status_code if response else 'No response'} {response.text if response else ''}",
        BOT_ADMIN_CHANNEL,
    )


def _is_valid_response(response) -> bool:
    """Check if the response is valid (not None and has success status code)."""
    return response is not None and 200 <= response.status_code < 300


async def _fetch_subscription_batch(
    cursor: Optional[str],
) -> Optional[SubscriptionResponse]:
    """Fetch a single batch of subscriptions from the API."""
    url = "https://api.twitch.tv/helix/eventsub/subscriptions"
    params = {"status": "enabled"}
    if cursor:
        params["after"] = cursor

    try:
        response = await retry_api_call(call_twitch, "GET", url, params)
    except Exception as e:
        await _handle_subscription_request_error(e)
        return None

    if response is None or not _is_valid_response(response):
        await _handle_subscription_response_error(response)
        return None

    return SubscriptionResponse.model_validate(response.json())


async def get_subscriptions() -> Optional[List[Subscription]]:
    """Fetch all enabled subscriptions from Twitch API with pagination support."""
    all_subscriptions: List[Subscription] = []
    cursor: Optional[str] = None

    while True:
        subscription_response = await _fetch_subscription_batch(cursor)
        if subscription_response is None:
            return None

        if not subscription_response.data:
            break

        all_subscriptions.extend(subscription_response.data)
        cursor = subscription_response.pagination.cursor
        if not cursor:
            break

    return all_subscriptions


async def get_user(id: int) -> Optional[User]:
    url = f"https://api.twitch.tv/helix/users?id={id}"

    try:
        response = await retry_api_call(call_twitch, "GET", url)
    except Exception as e:
        error_details: ErrorDetails = {
            "type": type(e).__name__,
            "message": str(e),
            "args": e.args,
            "traceback": traceback.format_exc(),
        }
        error_msg = f"Error fetching user info after retries - Type: {error_details['type']}, Message: {error_details['message']}, Args: {error_details['args']}"
        logger.error(f"{error_msg}\nTraceback:\n{error_details['traceback']}")
        await log_error(error_msg, error_details["traceback"])
        return None

    if response is None or response.status_code < 200 or response.status_code >= 300:
        logger.warning(
            f"Failed to fetch user info: {response.status_code if response else 'No response'}"
        )
        await send_message(
            f"Failed to fetch user info: {response.status_code if response else 'No response'} {response.text if response else ''}",
            BOT_ADMIN_CHANNEL,
        )
        return None
    user_info_response = UserResponse.model_validate(response.json())
    return user_info_response.data[0] if user_info_response.data else None


async def get_user_by_username(username: str) -> Optional[User]:
    url = f"https://api.twitch.tv/helix/users?login={username}"

    try:
        response = await retry_api_call(call_twitch, "GET", url)
    except Exception as e:
        error_details: ErrorDetails = {
            "type": type(e).__name__,
            "message": str(e),
            "args": e.args,
            "traceback": traceback.format_exc(),
        }
        error_msg = f"Failed to fetch user info after retries - Type: {error_details['type']}, Message: {error_details['message']}, Args: {error_details['args']}"
        logger.error(f"{error_msg}\nTraceback:\n{error_details['traceback']}")
        await log_error(error_msg, error_details["traceback"])
        return None

    if response is None or response.status_code < 200 or response.status_code >= 300:
        logger.warning(
            f"Failed to fetch user info: {response.status_code if response else 'No response'}"
        )
        await send_message(
            f"Failed to fetch user info: {response.status_code if response else 'No response'} {response.text if response else ''}",
            BOT_ADMIN_CHANNEL,
        )
        return
    user_info_response = UserResponse.model_validate(response.json())
    return user_info_response.data[0] if user_info_response.data else None


async def twitch_event_subscription(
    sub_type: Literal["online", "offline"], user_id: str
) -> bool:
    url = "https://api.twitch.tv/helix/eventsub/subscriptions"
    body = {
        "type": f"stream.{sub_type}",
        "version": "1",
        "condition": {"broadcaster_user_id": user_id},
        "transport": {
            "method": "webhook",
            "callback": f"{APP_URL}/webhook/twitch{'' if sub_type == 'online' else '/offline'}",
            "secret": TWITCH_WEBHOOK_SECRET,
        },
    }

    try:
        response = await retry_api_call(call_twitch, "POST", url, body)
    except Exception as e:
        error_details: ErrorDetails = {
            "type": type(e).__name__,
            "message": str(e),
            "args": e.args,
            "traceback": traceback.format_exc(),
        }
        error_msg = f"Failed to subscribe to {type} event after retries - Type: {error_details['type']}, Message: {error_details['message']}, Args: {error_details['args']}"
        logger.error(f"{error_msg}\nTraceback:\n{error_details['traceback']}")
        await log_error(error_msg, error_details["traceback"])
        return False

    if response is None or response.status_code < 200 or response.status_code >= 300:
        logger.warning(
            f"Failed to subscribe to {type} event: {response.status_code if response else 'No response'}"
        )
        await send_message(
            f"Failed to subscribe to {type} event: {response.status_code if response else 'No response'} {response.text if response else ''}",
            BOT_ADMIN_CHANNEL,
        )
        return False
    return True


async def subscribe_to_user(username: str) -> bool:
    user = await get_user_by_username(username)
    if not user:
        logger.warning(f"User not found: {username}")
        await send_message(f"User not found: {username}", BOT_ADMIN_CHANNEL)
        return False

    return await twitch_event_subscription(
        "online", user.id
    ) and await twitch_event_subscription("offline", user.id)


async def _fetch_user_batch(batch: List[str]) -> Optional[List[User]]:
    """Fetch a single batch of users from the API."""
    if not batch:
        return []

    url = f"https://api.twitch.tv/helix/users?id={'&id='.join(batch)}"

    try:
        response = await retry_api_call(call_twitch, "GET", url)
    except Exception as e:
        error_details: ErrorDetails = {
            "type": type(e).__name__,
            "message": str(e),
            "args": e.args,
            "traceback": traceback.format_exc(),
        }
        error_msg = f"Error fetching user infos after retries - Type: {error_details['type']}, Message: {error_details['message']}, Args: {error_details['args']}"
        logger.error(f"{error_msg}\nTraceback:\n{error_details['traceback']}")
        await log_error(error_msg, error_details["traceback"])
        return None

    if response is None or not _is_valid_response(response):
        logger.warning(
            f"Failed batch fetch of user infos: {response.status_code if response else 'No response'}"
        )
        await send_message(
            f"Failed to fetch users infos: {response.status_code if response else 'No response'} {response.text if response else ''}",
            BOT_ADMIN_CHANNEL,
        )
        return None

    user_info_response = UserResponse.model_validate(response.json())
    return user_info_response.data


async def get_users(ids: List[str]) -> Optional[List[User]]:
    """Fetch users from Twitch API in batches of 100."""
    batches_iterator = itertools.batched(ids, 100)
    batches_list = [list(batch) for batch in batches_iterator]

    users: List[User] = []

    for batch in batches_list:
        batch_users = await _fetch_user_batch(batch)
        if batch_users is None:
            return None
        users.extend(batch_users)

    return users


async def get_channel(id: int) -> Optional[Channel]:
    url = f"https://api.twitch.tv/helix/channels?broadcaster_id={id}"

    try:
        response = await retry_api_call(call_twitch, "GET", url)
    except Exception as e:
        error_details: ErrorDetails = {
            "type": type(e).__name__,
            "message": str(e),
            "args": e.args,
            "traceback": traceback.format_exc(),
        }
        error_msg = f"Failed to fetch channel info after retries - Type: {error_details['type']}, Message: {error_details['message']}, Args: {error_details['args']}"
        logger.error(f"{error_msg}\nTraceback:\n{error_details['traceback']}")
        await log_error(error_msg, error_details["traceback"])
        return None

    if response is None or response.status_code < 200 or response.status_code >= 300:
        logger.warning(
            f"Failed to fetch channel info: {response.status_code if response else 'No response'}"
        )
        await send_message(
            f"Failed to fetch channel info: {response.status_code if response else 'No response'} {response.text if response else ''}",
            BOT_ADMIN_CHANNEL,
        )
        return
    channel_info_response = ChannelResponse.model_validate(response.json())
    return channel_info_response.data[0] if channel_info_response.data else None


async def get_stream_info(broadcaster_id: int) -> Optional[Stream]:
    url = f"https://api.twitch.tv/helix/streams?user_id={broadcaster_id}"

    try:
        response = await retry_api_call(call_twitch, "GET", url)
    except Exception as e:
        error_details: ErrorDetails = {
            "type": type(e).__name__,
            "message": str(e),
            "args": e.args,
            "traceback": traceback.format_exc(),
        }
        logger.error(
            f"Failed to fetch stream info after retries - Type: {error_details['type']}, Message: {error_details['message']}, Args: {error_details['args']}"
        )
        await log_error(
            f"Failed to fetch stream info after retries - Type: {error_details['type']}, Message: {error_details['message']}, Args: {error_details['args']}",
            error_details["traceback"],
        )
        return None

    if response is None or response.status_code < 200 or response.status_code >= 300:
        logger.warning(
            f"Failed to fetch stream info: {response.status_code if response else 'No response'}"
        )
        await send_message(
            f"Failed to fetch stream info: {response.status_code if response else 'No response'} {response.text if response else ''}",
            BOT_ADMIN_CHANNEL,
        )
        return
    stream_info_response = StreamResponse.model_validate(response.json())
    return stream_info_response.data[0] if stream_info_response.data else None


async def get_stream_vod(user_id: int, stream_id: int) -> Optional[Video]:
    url = f"https://api.twitch.tv/helix/videos?user_id={user_id}&type=archive"

    try:
        response = await retry_api_call(call_twitch, "GET", url)
    except Exception as e:
        error_details: ErrorDetails = {
            "type": type(e).__name__,
            "message": str(e),
            "args": e.args,
            "traceback": traceback.format_exc(),
        }
        logger.error(
            f"Failed to fetch VOD info after retries - Type: {error_details['type']}, Message: {error_details['message']}, Args: {error_details['args']}"
        )
        await log_error(
            f"Failed to fetch VOD info after retries - Type: {error_details['type']}, Message: {error_details['message']}, Args: {error_details['args']}",
            error_details["traceback"],
        )
        return None

    if response is None or response.status_code < 200 or response.status_code >= 300:
        logger.warning(
            f"Failed to fetch VOD info: {response.status_code if response else 'No response'}"
        )
        await send_message(
            f"Failed to fetch VOD info: {response.status_code if response else 'No response'} {response.text if response else ''}",
            BOT_ADMIN_CHANNEL,
        )
        return
    video_info_response = VideoResponse.model_validate(response.json())
    return next(
        (
            video
            for video in video_info_response.data
            if video.stream_id == str(stream_id)
        ),
        None,
    )


async def get_ad_schedule(broadcaster_id: int) -> Optional[AdSchedule]:
    url = f"https://api.twitch.tv/helix/channels/ads?broadcaster_id={broadcaster_id}"

    try:
        response = await retry_api_call(
            call_twitch, "GET", url, None, TokenType.Broadcaster
        )
    except Exception as e:
        error_details: ErrorDetails = {
            "type": type(e).__name__,
            "message": str(e),
            "args": e.args,
            "traceback": traceback.format_exc(),
        }
        error_msg = f"Failed to fetch ad schedule after retries - Type: {error_details['type']}, Message: {error_details['message']}, Args: {error_details['args']}"
        logger.error(f"{error_msg}\nTraceback:\n{error_details['traceback']}")
        await log_error(error_msg, error_details["traceback"])
        return None

    if response is None or response.status_code < 200 or response.status_code >= 300:
        logger.warning(
            f"Failed to fetch ad schedule: {response.status_code if response else 'No response'}"
        )
        await send_message(
            f"Failed to fetch ad schedule: {response.status_code if response else 'No response'} {response.text if response else ''}",
            BOT_ADMIN_CHANNEL,
        )
        return
    ad_schedule_response = AdScheduleResponse.model_validate(response.json())
    return ad_schedule_response.data[0] if ad_schedule_response.data else None


def _cleanup_broadcaster_tasks(
    broadcaster_id: int, stream_info: Optional[Stream], user_info: Optional[User]
) -> None:
    """Handle cleanup tasks specific to the broadcaster."""
    if (stream_info and stream_info.user_login == BROADCASTER_USERNAME) or (
        user_info and user_info.login == BROADCASTER_USERNAME
    ):
        from controller.twitch import _ad_break_notification_tasks
        from services.twitch.shoutout_queue import shoutout_queue

        shoutout_queue.deactivate()
        existing_task = _ad_break_notification_tasks.get(str(broadcaster_id))
        if existing_task and not existing_task.done():
            existing_task.cancel()


async def _fetch_vod_info_safely(
    broadcaster_id: int, stream_id: int
) -> Optional[Video]:
    """Safely fetch VOD information with error handling."""
    try:
        return await get_stream_vod(broadcaster_id, stream_id)
    except Exception as e:
        error_details: ErrorDetails = {
            "type": type(e).__name__,
            "message": str(e),
            "args": e.args,
            "traceback": traceback.format_exc(),
        }
        error_msg = f"Failed to fetch VOD info for broadcaster_id={broadcaster_id} - Type: {error_details['type']}, Message: {error_details['message']}, Args: {error_details['args']}"
        logger.error(f"{error_msg}\nTraceback:\n{error_details['traceback']}")
        await log_error(error_msg, error_details["traceback"])
        return None


def _get_stream_title(
    stream_info: Optional[Stream], vod_info: Optional[Video], channel: Optional[Channel]
) -> str:
    """Get the stream title from available sources."""
    if stream_info:
        return stream_info.title
    if vod_info:
        return vod_info.title
    return channel.title if channel else "Unknown"


def _get_user_name(stream_info: Optional[Stream], user_info: Optional[User]) -> str:
    """Get the user display name from available sources."""
    if stream_info:
        return stream_info.user_name
    return user_info.display_name if user_info else "Unknown"


def _get_game_name(stream_info: Optional[Stream], channel: Optional[Channel]) -> str:
    """Get the game name from available sources."""
    if stream_info:
        return stream_info.game_name
    return channel.game_name if channel else "Unknown"


def _create_offline_embed(
    stream_info: Optional[Stream],
    vod_info: Optional[Video],
    channel: Optional[Channel],
    user_info: Optional[User],
    url: str,
    age: str,
    now: pendulum.DateTime,
) -> discord.Embed:
    """Create the offline embed with all necessary information."""
    embed = (
        discord.Embed(
            description=f"**{_get_stream_title(stream_info, vod_info, channel)}**",
            color=0x9046FF,
            timestamp=now,
        )
        .set_author(
            name=f"{_get_user_name(stream_info, user_info)} was live",
            icon_url=user_info.profile_image_url if user_info else None,
            url=url,
        )
        .add_field(
            name="**Game**",
            value=_get_game_name(stream_info, channel),
            inline=True,
        )
        .set_footer(
            text=f"Online for {age} | Offline at",
        )
    )

    if vod_info:
        embed = embed.add_field(
            name="**VOD**",
            value=f"[**Click to view**]({vod_info.url})",
            inline=True,
        )

    return embed


async def _handle_embed_edit_error(
    e: Exception, message_id: int, broadcaster_id: int
) -> None:
    """Handle errors that occur during embed editing."""
    if isinstance(e, discord.NotFound):
        logger.warning(
            f"Message not found when editing offline embed for message_id={message_id}; aborting"
        )
        try:
            delete_row_from_parquet(broadcaster_id, LIVE_ALERTS)
        except Exception as delete_error:
            error_details: ErrorDetails = {
                "type": type(delete_error).__name__,
                "message": str(delete_error),
                "args": delete_error.args,
                "traceback": traceback.format_exc(),
            }
            error_msg = f"Failed to delete live alert record for broadcaster_id={broadcaster_id} - Type: {error_details['type']}, Message: {error_details['message']}, Args: {error_details['args']}"
            logger.error(f"{error_msg}\nTraceback:\n{error_details['traceback']}")
            await log_error(error_msg, error_details["traceback"])
    else:
        error_details: ErrorDetails = {
            "type": type(e).__name__,
            "message": str(e),
            "args": e.args,
            "traceback": traceback.format_exc(),
        }
        error_msg = f"Error editing offline embed for message_id={message_id} - Type: {error_details['type']}, Message: {error_details['message']}, Args: {error_details['args']}"
        logger.error(f"{error_msg}\nTraceback:\n{error_details['traceback']}")
        await log_error(error_msg, error_details["traceback"])


async def trigger_offline_sequence(
    broadcaster_id: int,
    stream_id: int,
    stream_info: Optional[Stream],
    now: pendulum.DateTime,
    user_info: Optional[User],
    url: str,
    age: str,
    message_id: int,
    channel_id: int,
    content: Optional[str],
    channel: Optional[Channel],
) -> None:
    # Handle cleanup tasks for valinmalach
    _cleanup_broadcaster_tasks(broadcaster_id, stream_info, user_info)

    # Fetch VOD information
    vod_info = await _fetch_vod_info_safely(broadcaster_id, stream_id)

    # Create the offline embed
    embed = _create_offline_embed(
        stream_info, vod_info, channel, user_info, url, age, now
    )

    # Edit the embed and handle any errors
    try:
        await edit_embed(message_id, embed, channel_id, content=content)
    except Exception as e:
        await _handle_embed_edit_error(e, message_id, broadcaster_id)


async def _validate_alert_exists(broadcaster_id: int) -> Optional[dict]:
    """Check if alert record exists and return it."""
    df = await read_parquet_cached(LIVE_ALERTS)
    alert_row = df.filter(pl.col("id") == broadcaster_id)
    return None if alert_row.height == 0 else alert_row.row(0, named=True)


def _should_trigger_offline_sequence(
    alert: dict, stream_info: Optional[Stream], stream_id: int
) -> bool:
    """Determine if offline sequence should be triggered."""
    return (
        stream_info is None
        or alert.get("stream_id", "") != stream_id
        or stream_info.id != str(stream_id)
    )


def _create_live_embed(
    stream_info: Stream,
    user_info: Optional[User],
    url: str,
    age: str,
    started_at_timestamp: str,
    now: pendulum.DateTime,
) -> discord.Embed:
    """Create the live stream embed."""
    raw_thumb_url = stream_info.thumbnail_url.replace("{width}x{height}", "400x225")
    cache_busted_thumb_url = f"{raw_thumb_url}?cb={int(pendulum.now().timestamp())}"

    return (
        discord.Embed(
            description=f"[**{stream_info.title}**]({url})",
            color=0x9046FF,
            timestamp=now,
        )
        .set_author(
            name=f"{stream_info.user_name} is now live!",
            icon_url=user_info.profile_image_url if user_info else None,
            url=url,
        )
        .add_field(
            name="**Game**",
            value=f"{stream_info.game_name}",
            inline=True,
        )
        .add_field(
            name="**Viewers**",
            value=f"{stream_info.viewer_count}",
            inline=True,
        )
        .add_field(
            name="**Started At**",
            value=started_at_timestamp,
            inline=True,
        )
        .set_image(url=cache_busted_thumb_url)
        .set_footer(
            text=f"Online for {age} | Last updated",
        )
    )


def _create_live_view(url: str) -> View:
    """Create the view with watch stream button."""
    view = View(timeout=None)
    view.add_item(
        discord.ui.Button(label="Watch Stream", style=discord.ButtonStyle.link, url=url)
    )
    return view


async def _handle_live_embed_edit_error(
    e: Exception, message_id: int, broadcaster_id: int
) -> bool:
    """Handle errors when editing live embed. Returns True if should continue, False if should abort."""
    if isinstance(e, discord.NotFound):
        logger.warning(
            f"Message not found when editing live embed for message_id={message_id}; aborting"
        )
        try:
            delete_row_from_parquet(broadcaster_id, LIVE_ALERTS)
        except Exception as delete_error:
            error_details: ErrorDetails = {
                "type": type(delete_error).__name__,
                "message": str(delete_error),
                "args": delete_error.args,
                "traceback": traceback.format_exc(),
            }
            error_msg = f"Failed to delete live alert record for broadcaster_id={broadcaster_id} - Type: {error_details['type']}, Message: {error_details['message']}, Args: {error_details['args']}"
            logger.error(f"{error_msg}\nTraceback:\n{error_details['traceback']}")
            await log_error(error_msg, error_details["traceback"])
        return False
    elif isinstance(e, discord.HTTPException) and e.status == 503:
        logger.warning(
            f"Discord API temporarily unavailable (503) for message_id={message_id}; will retry next cycle"
        )
        return True
    else:
        error_details: ErrorDetails = {
            "type": type(e).__name__,
            "message": str(e),
            "args": e.args,
            "traceback": traceback.format_exc(),
        }
        if isinstance(e, discord.HTTPException):
            error_msg = f"Discord HTTP error {e.status} when editing live embed for message_id={message_id} - Type: {error_details['type']}, Message: {error_details['message']}, Args: {error_details['args']}"
        else:
            error_msg = f"Error editing live embed for message_id={message_id} - Type: {error_details['type']}, Message: {error_details['message']}, Args: {error_details['args']}"
        logger.error(f"{error_msg}\nTraceback:\n{error_details['traceback']}")
        await log_error(error_msg, error_details["traceback"])
        return True


async def _update_live_embed(
    message_id: int,
    channel_id: int,
    broadcaster_id: int,
    stream_info: Stream,
    user_info: Optional[User],
    url: str,
    age: str,
    started_at_timestamp: str,
    content: Optional[str],
    now: pendulum.DateTime,
) -> bool:
    """Update the live embed. Returns True if should continue, False if should abort."""
    embed = _create_live_embed(
        stream_info, user_info, url, age, started_at_timestamp, now
    )
    view = _create_live_view(url)

    try:
        await edit_embed(message_id, embed, channel_id, view, content=content)
        return True
    except Exception as e:
        return await _handle_live_embed_edit_error(e, message_id, broadcaster_id)


async def _run_update_cycle(
    broadcaster_id: int,
    channel_id: int,
    message_id: int,
    stream_id: int,
    started_at: pendulum.DateTime,
    started_at_timestamp: str,
    content: Optional[str],
) -> None:
    """Run a single update cycle for the live alert."""
    # Fetch current data
    alert = await _validate_alert_exists(broadcaster_id)
    if alert is None:
        return

    stream_info = await get_stream_info(broadcaster_id)
    user_info = await get_user(broadcaster_id)
    channel_info = await get_channel(broadcaster_id)

    # Check if we should trigger offline sequence
    if stream_info is None or _should_trigger_offline_sequence(
        alert, stream_info, stream_id
    ):
        if user_info:
            login = user_info.login
        elif channel_info:
            login = channel_info.broadcaster_login
        else:
            login = ""
        url = f"https://www.twitch.tv/{login}"
        now = pendulum.now()
        age = get_age(started_at, limit_units=2)
        await trigger_offline_sequence(
            broadcaster_id,
            stream_id,
            stream_info,
            now,
            user_info,
            url,
            age,
            message_id,
            channel_id,
            content,
            channel_info,
        )
        return

    # Update live embed
    url = f"https://www.twitch.tv/{stream_info.user_login}"
    now = pendulum.now()
    age = get_age(started_at, limit_units=2)

    should_continue = await _update_live_embed(
        message_id,
        channel_id,
        broadcaster_id,
        stream_info,
        user_info,
        url,
        age,
        started_at_timestamp,
        content,
        now,
    )

    if not should_continue:
        return


async def update_alert(
    broadcaster_id: int,
    channel_id: int,
    message_id: int,
    stream_id: int,
    stream_started_at: str,
) -> None:
    try:
        await asyncio.sleep(60)

        content = (
            f"<@&{LIVE_ALERTS_ROLE}>" if channel_id == STREAM_ALERTS_CHANNEL else None
        )
        started_at = parse_rfc3339(stream_started_at)
        started_at_timestamp = f"<t:{int(started_at.timestamp())}:f>"

        # Initial validation
        alert = await _validate_alert_exists(broadcaster_id)
        if alert is None:
            return

        # Main update loop
        while True:
            await _run_update_cycle(
                broadcaster_id,
                channel_id,
                message_id,
                stream_id,
                started_at,
                started_at_timestamp,
                content,
            )

            await asyncio.sleep(60)

            # Check if alert still exists and stream is still live
            alert = await _validate_alert_exists(broadcaster_id)
            if alert is None:
                break

            stream_info = await get_stream_info(broadcaster_id)
            if stream_info is None:
                break

    except Exception as e:
        error_details: ErrorDetails = {
            "type": type(e).__name__,
            "message": str(e),
            "args": e.args,
            "traceback": traceback.format_exc(),
        }
        error_msg = f"Error updating live alert message for broadcaster_id={broadcaster_id} - Type: {error_details['type']}, Message: {error_details['message']}, Args: {error_details['args']}"
        logger.error(f"{error_msg}\nTraceback:\n{error_details['traceback']}")
        await log_error(error_msg, error_details["traceback"])
