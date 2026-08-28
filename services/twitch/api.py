import asyncio
import io
import itertools
import logging
import traceback
from collections.abc import Awaitable, Callable
from enum import Enum, auto
from typing import Any, Literal

import discord
import pendulum
from discord.ui import View

from config import settings
from constants import ErrorDetails, TokenType
from db import LiveAlert, repository
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
from services.config import config
from services.helper.helper import (
    edit_embed,
    get_age,
    parse_rfc3339,
    send_message,
)
from services.helper.http_client import is_transient_network_error
from services.helper.twitch import call_twitch

logger = logging.getLogger(__name__)

_UPDATE_INTERVAL_SECONDS = 60
# Stop after this many cycles that concluded nothing, so a stuck alert cannot poll
# (and report) forever; the record is left behind for the next restart to retry.
_MAX_INCONCLUSIVE_CYCLES = 5


async def log_error(message: str, traceback_str: str) -> None:
    traceback_buffer = io.BytesIO(traceback_str.encode("utf-8"))
    traceback_file = discord.File(traceback_buffer, filename="traceback.txt")
    await send_message(message, config.channel("bot_admin"), file=traceback_file)


def _create_error_details(e: Exception) -> ErrorDetails:
    """Create a standardized error details dictionary from an exception."""
    return {
        "type": type(e).__name__,
        "message": str(e),
        "args": e.args,
        "traceback": traceback.format_exc(),
    }


async def _log_and_report_error(error_msg: str, error_details: ErrorDetails) -> None:
    """Log an error and report it to the admin channel."""
    logger.error(f"{error_msg}\nTraceback:\n{error_details['traceback']}")
    await log_error(error_msg, error_details["traceback"])


async def _handle_api_exception(e: Exception, context: str) -> None:
    """Handle exceptions from API calls with standardized logging and reporting."""
    error_details = _create_error_details(e)
    error_msg = f"{context} - Type: {error_details['type']}, Message: {error_details['message']}, Args: {error_details['args']}"
    await _log_and_report_error(error_msg, error_details)


async def _handle_invalid_response(response, context: str) -> None:
    """Handle invalid HTTP responses with standardized logging and reporting."""
    status = response.status_code if response else "No response"
    text = response.text if response else ""
    logger.warning(f"{context}: {status}")
    await send_message(f"{context}: {status} {text}", config.channel("bot_admin"))


async def retry_api_call[T](
    func: Callable[..., Awaitable[T]],
    *args: Any,
    max_retries: int = 3,
    delay: float = 1,
    **kwargs: Any,
) -> T:
    """Retry API calls with exponential backoff for connection issues and 5xx server errors."""
    for attempt in range(max_retries):
        try:
            response = await func(*args, **kwargs)

            status_code: Any | None = getattr(response, "status_code", None)
            if status_code is not None and 500 <= status_code < 600:
                if attempt == max_retries - 1:
                    return response

                wait_time = delay * (2**attempt)
                logger.warning(
                    f"Server error {status_code} on attempt {attempt + 1}, retrying in {wait_time}s"
                )
                await asyncio.sleep(wait_time)
                continue

            return response
        except Exception as e:
            if attempt == max_retries - 1:
                raise

            if not is_transient_network_error(e):
                raise
            wait_time = delay * (2**attempt)
            logger.warning(
                f"Connection error on attempt {attempt + 1}, retrying in {wait_time}s: {e}"
            )
            await asyncio.sleep(wait_time)

    raise RuntimeError("retry_api_call exhausted without returning or raising")


async def _handle_subscription_request_error(e: Exception) -> None:
    """Handle errors from subscription API requests."""
    await _handle_api_exception(e, "Error fetching subscriptions after retries")


async def _handle_subscription_response_error(response) -> None:
    """Handle HTTP response errors from subscription API."""
    await _handle_invalid_response(response, "Error fetching subscriptions")


def _is_valid_response(response) -> bool:
    """Check if the response is valid (not None and has success status code)."""
    return response is not None and 200 <= response.status_code < 300


async def _fetch_subscription_batch(
    cursor: str | None,
) -> SubscriptionResponse | None:
    """Fetch a single batch of subscriptions from the API."""
    url = "https://api.twitch.tv/helix/eventsub/subscriptions"
    params = {"status": "enabled"}
    if cursor:
        params["after"] = cursor

    try:
        response = await retry_api_call(call_twitch, "GET", url, params)
    except Exception as e:  # noqa: BLE001
        await _handle_subscription_request_error(e)
        return None

    if response is None or not _is_valid_response(response):
        await _handle_subscription_response_error(response)
        return None

    return SubscriptionResponse.model_validate(response.json())


async def get_subscriptions() -> list[Subscription] | None:
    """Fetch all enabled subscriptions from Twitch API with pagination support."""
    all_subscriptions: list[Subscription] = []
    cursor: str | None = None

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


async def get_user(id: int) -> User | None:
    url = f"https://api.twitch.tv/helix/users?id={id}"

    try:
        response = await retry_api_call(call_twitch, "GET", url)
    except Exception as e:  # noqa: BLE001
        await _handle_api_exception(e, "Failed to fetch user info after retries")
        return None

    if response is None or not _is_valid_response(response):
        await _handle_invalid_response(response, "Failed to fetch user info")
        return None
    user_info_response = UserResponse.model_validate(response.json())
    return user_info_response.data[0] if user_info_response.data else None


async def get_user_by_username(username: str) -> User | None:
    url = f"https://api.twitch.tv/helix/users?login={username}"

    try:
        response = await retry_api_call(call_twitch, "GET", url)
    except Exception as e:  # noqa: BLE001
        await _handle_api_exception(e, "Failed to fetch user info after retries")
        return None

    if response is None or not _is_valid_response(response):
        await _handle_invalid_response(response, "Failed to fetch user info")
        return None
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
            "callback": f"{settings.app_url}/webhook/twitch{'' if sub_type == 'online' else '/offline'}",
            "secret": settings.twitch_webhook_secret,
        },
    }

    try:
        response = await retry_api_call(call_twitch, "POST", url, body)
    except Exception as e:  # noqa: BLE001
        await _handle_api_exception(
            e, f"Failed to subscribe to {sub_type} event after retries"
        )
        return False

    if response is None or not _is_valid_response(response):
        await _handle_invalid_response(
            response, f"Failed to subscribe to {sub_type} event"
        )
        return False
    return True


async def subscribe_to_user(username: str) -> bool:
    user = await get_user_by_username(username)
    if not user:
        logger.warning(f"User not found: {username}")
        await send_message(f"User not found: {username}", config.channel("bot_admin"))
        return False

    return await twitch_event_subscription(
        "online", user.id
    ) and await twitch_event_subscription("offline", user.id)


async def _delete_subscription(subscription_id: str) -> bool:
    """Delete a Twitch EventSub subscription by ID."""
    url = f"https://api.twitch.tv/helix/eventsub/subscriptions?id={subscription_id}"

    try:
        response = await retry_api_call(call_twitch, "DELETE", url)
    except Exception as e:  # noqa: BLE001
        await _handle_api_exception(
            e,
            f"Failed to unsubscribe subscription_id={subscription_id} after retries",
        )
        return False

    if response is None or not _is_valid_response(response):
        await _handle_invalid_response(
            response, f"Failed to unsubscribe subscription_id={subscription_id}"
        )
        return False

    return True


async def unsubscribe_to_user(username: str) -> bool:
    """Unsubscribe from online/offline EventSub subscriptions for a user."""
    user = await get_user_by_username(username)
    if not user:
        logger.warning(f"User not found: {username}")
        await send_message(f"User not found: {username}", config.channel("bot_admin"))
        return False

    subscriptions = await get_subscriptions()
    if subscriptions is None:
        return False

    matching_subscriptions = [
        subscription
        for subscription in subscriptions
        if subscription.type in {"stream.online", "stream.offline"}
        and subscription.condition.broadcaster_user_id == user.id
    ]

    if not matching_subscriptions:
        logger.info(f"No online/offline subscriptions found for user: {username}")
        return True

    results = []
    for subscription in matching_subscriptions:
        results.append(await _delete_subscription(subscription.id))

    return all(results)


async def _fetch_user_batch(batch: list[str]) -> list[User] | None:
    """Fetch a single batch of users from the API."""
    if not batch:
        return []

    url = f"https://api.twitch.tv/helix/users?id={'&id='.join(batch)}"

    try:
        response = await retry_api_call(call_twitch, "GET", url)
    except Exception as e:  # noqa: BLE001
        await _handle_api_exception(e, "Error fetching user infos after retries")
        return None

    if response is None or not _is_valid_response(response):
        await _handle_invalid_response(response, "Failed to fetch users infos")
        return None

    user_info_response = UserResponse.model_validate(response.json())
    return user_info_response.data


async def get_users(ids: list[str]) -> list[User] | None:
    """Fetch users from Twitch API in batches of 100."""
    batches_iterator = itertools.batched(ids, 100)
    batches_list = [list(batch) for batch in batches_iterator]

    users: list[User] = []

    for batch in batches_list:
        batch_users = await _fetch_user_batch(batch)
        if batch_users is None:
            return None
        users.extend(batch_users)

    return users


async def get_channel(id: int) -> Channel | None:
    url = f"https://api.twitch.tv/helix/channels?broadcaster_id={id}"

    try:
        response = await retry_api_call(call_twitch, "GET", url)
    except Exception as e:  # noqa: BLE001
        await _handle_api_exception(e, "Failed to fetch channel info after retries")
        return None

    if response is None or not _is_valid_response(response):
        await _handle_invalid_response(response, "Failed to fetch channel info")
        return None
    channel_info_response = ChannelResponse.model_validate(response.json())
    return channel_info_response.data[0] if channel_info_response.data else None


async def get_stream_state(broadcaster_id: int) -> tuple[Stream | None, bool]:
    """(stream, lookup_ok): a lookup that failed reads as (None, False), never as offline."""
    url = f"https://api.twitch.tv/helix/streams?user_id={broadcaster_id}"

    try:
        response = await retry_api_call(call_twitch, "GET", url)
    except Exception as e:  # noqa: BLE001
        await _handle_api_exception(e, "Failed to fetch stream info after retries")
        return None, False

    if response is None or not _is_valid_response(response):
        await _handle_invalid_response(response, "Failed to fetch stream info")
        return None, False

    try:
        stream_info_response = StreamResponse.model_validate(response.json())
    except Exception as e:  # noqa: BLE001
        # A 200 whose body will not parse is still a lookup that told us nothing.
        await _handle_api_exception(e, "Could not read the stream info response")
        return None, False

    return (stream_info_response.data[0] if stream_info_response.data else None), True


async def get_stream_info(broadcaster_id: int) -> Stream | None:
    stream_info, _ = await get_stream_state(broadcaster_id)
    return stream_info


async def get_stream_vod(user_id: int, stream_id: int) -> Video | None:
    url = f"https://api.twitch.tv/helix/videos?user_id={user_id}&type=archive"

    try:
        response = await retry_api_call(call_twitch, "GET", url)
    except Exception as e:  # noqa: BLE001
        await _handle_api_exception(e, "Failed to fetch VOD info after retries")
        return None

    if response is None or not _is_valid_response(response):
        await _handle_invalid_response(response, "Failed to fetch VOD info")
        return None
    video_info_response = VideoResponse.model_validate(response.json())
    return next(
        (
            video
            for video in video_info_response.data
            if video.stream_id == str(stream_id)
        ),
        None,
    )


async def get_ad_schedule(broadcaster_id: int) -> AdSchedule | None:
    url = f"https://api.twitch.tv/helix/channels/ads?broadcaster_id={broadcaster_id}"

    try:
        response = await retry_api_call(
            call_twitch, "GET", url, None, TokenType.Broadcaster
        )
    except Exception as e:  # noqa: BLE001
        await _handle_api_exception(e, "Failed to fetch ad schedule after retries")
        return None

    if response is None or not _is_valid_response(response):
        await _handle_invalid_response(response, "Failed to fetch ad schedule")
        return None
    ad_schedule_response = AdScheduleResponse.model_validate(response.json())
    return ad_schedule_response.data[0] if ad_schedule_response.data else None


def _cleanup_broadcaster_tasks(
    broadcaster_id: int, stream_info: Stream | None, user_info: User | None
) -> None:
    """Stand the broadcaster's live helpers down, but only once nothing is streaming."""
    if stream_info is not None:
        return
    if user_info is None or user_info.login != config.setting("broadcaster_username"):
        return

    from controller.twitch import _ad_break_notification_tasks
    from services.twitch.shoutout_queue import shoutout_queue

    shoutout_queue.deactivate()
    existing_task = _ad_break_notification_tasks.get(str(broadcaster_id))
    if existing_task and not existing_task.done():
        existing_task.cancel()


async def _fetch_vod_info_safely(broadcaster_id: int, stream_id: int) -> Video | None:
    """Safely fetch VOD information with error handling."""
    try:
        return await get_stream_vod(broadcaster_id, stream_id)
    except Exception as e:  # noqa: BLE001
        await _handle_api_exception(
            e, f"Failed to fetch VOD info for broadcaster_id={broadcaster_id}"
        )
        return None


def _get_stream_title(
    stream_info: Stream | None, vod_info: Video | None, channel: Channel | None
) -> str:
    """Get the stream title from available sources."""
    if stream_info:
        return stream_info.title
    if vod_info:
        return vod_info.title
    return channel.title if channel else "Unknown"


def _get_user_name(stream_info: Stream | None, user_info: User | None) -> str:
    """Get the user display name from available sources."""
    if stream_info:
        return stream_info.user_name
    return user_info.display_name if user_info else "Unknown"


def _get_game_name(stream_info: Stream | None, channel: Channel | None) -> str:
    """Get the game name from available sources."""
    if stream_info:
        return stream_info.game_name
    return channel.game_name if channel else "Unknown"


def _create_offline_embed(
    stream_info: Stream | None,
    vod_info: Video | None,
    channel: Channel | None,
    user_info: User | None,
    url: str,
    age: str,
    now: pendulum.DateTime,
) -> discord.Embed:
    """Create the offline embed with all necessary information."""
    embed = (
        discord.Embed(
            description=f"**{_get_stream_title(stream_info, vod_info, channel)}**",
            color=config.color("embed_color_stream"),
            timestamp=now,
        )
        .set_author(
            name=config.template(
                "stream_offline_title", name=_get_user_name(stream_info, user_info)
            ),
            icon_url=user_info.profile_image_url if user_info else None,
            url=url,
        )
        .add_field(
            name=config.template("stream_field_game"),
            value=_get_game_name(stream_info, channel),
            inline=True,
        )
        .set_footer(
            text=config.template("stream_footer_offline", age=age),
        )
    )

    if vod_info:
        embed = embed.add_field(
            name=config.template("stream_field_vod"),
            value=config.template("stream_field_vod_value", url=vod_info.url),
            inline=True,
        )

    return embed


class _CycleOutcome(Enum):
    """What one update cycle concluded about the alert it owns."""

    CONTINUE = auto()  # embed refreshed, stream still live
    RETRY = auto()  # nothing could be concluded; try the next cycle
    STOP = auto()  # the alert is finished with, for better or worse


def _is_superseded(alert: LiveAlert, message_id: int, stream_id: int) -> bool:
    """True once a newer alert has replaced the one this updater was started for."""
    return alert.message_id != message_id or alert.stream_id != stream_id


def _is_transient_edit_error(e: Exception) -> bool:
    """Discord edits fail transiently on 5xx replies and on dropped sockets."""
    if isinstance(e, discord.HTTPException) and 500 <= e.status < 600:
        return True
    if is_transient_network_error(e):
        return True
    return "clientoserror" in f"{type(e).__name__} {e}".lower()


async def _clear_alert(broadcaster_id: int, message_id: int) -> None:
    """Drop the alert row this updater owns; a newer alert's row is left in place."""
    try:
        await repository.delete_live_alert(broadcaster_id, message_id=message_id)
    except Exception as e:  # noqa: BLE001
        await _handle_api_exception(
            e,
            f"Failed to delete live alert record for broadcaster_id={broadcaster_id}",
        )


async def _handle_offline_edit_error(
    e: Exception, message_id: int, broadcaster_id: int
) -> _CycleOutcome:
    """Decide the cycle's outcome from a failed offline-embed edit."""
    if isinstance(e, discord.NotFound):
        logger.warning(
            f"Message not found when editing offline embed for message_id={message_id}; clearing alert"
        )
        await _clear_alert(broadcaster_id, message_id)
        return _CycleOutcome.STOP

    if _is_transient_edit_error(e):
        logger.warning(
            f"Transient network error when editing offline embed for message_id={message_id}: {e}"
        )
        return _CycleOutcome.RETRY

    await _handle_api_exception(
        e, f"Error editing offline embed for message_id={message_id}"
    )
    return _CycleOutcome.STOP


async def trigger_offline_sequence(
    broadcaster_id: int,
    stream_id: int,
    stream_info: Stream | None,
    now: pendulum.DateTime,
    user_info: User | None,
    url: str,
    age: str,
    message_id: int,
    channel_id: int,
    content: str | None,
    channel: Channel | None,
) -> _CycleOutcome:
    _cleanup_broadcaster_tasks(broadcaster_id, stream_info, user_info)

    vod_info = await _fetch_vod_info_safely(broadcaster_id, stream_id)

    embed = _create_offline_embed(
        stream_info, vod_info, channel, user_info, url, age, now
    )

    try:
        edited = await edit_embed(message_id, embed, channel_id, content=content)
    except Exception as e:  # noqa: BLE001
        return await _handle_offline_edit_error(e, message_id, broadcaster_id)

    if not edited:
        return _CycleOutcome.RETRY

    # The stream is over: without this the record outlives it and every restart
    # resurrects an updater for a dead stream.
    await _clear_alert(broadcaster_id, message_id)
    return _CycleOutcome.STOP


def _create_live_embed(
    stream_info: Stream,
    user_info: User | None,
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
            color=config.color("embed_color_stream"),
            timestamp=now,
        )
        .set_author(
            name=config.template("stream_live_title", name=stream_info.user_name),
            icon_url=user_info.profile_image_url if user_info else None,
            url=url,
        )
        .add_field(
            name=config.template("stream_field_game"),
            value=f"{stream_info.game_name}",
            inline=True,
        )
        .add_field(
            name=config.template("stream_field_viewers"),
            value=f"{stream_info.viewer_count}",
            inline=True,
        )
        .add_field(
            name=config.template("stream_field_started_at"),
            value=started_at_timestamp,
            inline=True,
        )
        .set_image(url=cache_busted_thumb_url)
        .set_footer(
            text=config.template("stream_footer_online", age=age),
        )
    )


def _create_live_view(url: str) -> View:
    """Create the view with watch stream button."""
    view = View(timeout=None)
    view.add_item(
        discord.ui.Button(
            label=config.template("stream_watch_button"),
            style=discord.ButtonStyle.link,
            url=url,
        )
    )
    return view


async def _handle_live_embed_edit_error(
    e: Exception, message_id: int, broadcaster_id: int
) -> _CycleOutcome:
    """Decide the cycle's outcome from a failed live-embed edit."""
    if isinstance(e, discord.NotFound):
        logger.warning(
            f"Message not found when editing live embed for message_id={message_id}; clearing alert"
        )
        await _clear_alert(broadcaster_id, message_id)
        return _CycleOutcome.STOP

    if _is_transient_edit_error(e):
        logger.warning(
            f"Transient error when editing live embed for message_id={message_id}; will retry next cycle: {e}"
        )
        return _CycleOutcome.RETRY

    error_details = _create_error_details(e)
    if isinstance(e, discord.HTTPException):
        error_msg = f"Discord HTTP error {e.status} when editing live embed for message_id={message_id} - Type: {error_details['type']}, Message: {error_details['message']}, Args: {error_details['args']}"
    else:
        error_msg = f"Error editing live embed for message_id={message_id} - Type: {error_details['type']}, Message: {error_details['message']}, Args: {error_details['args']}"
    await _log_and_report_error(error_msg, error_details)
    return _CycleOutcome.RETRY


async def _update_live_embed(
    message_id: int,
    channel_id: int,
    broadcaster_id: int,
    stream_info: Stream,
    user_info: User | None,
    url: str,
    age: str,
    started_at_timestamp: str,
    content: str | None,
    now: pendulum.DateTime,
) -> _CycleOutcome:
    """Refresh the live embed."""
    embed = _create_live_embed(
        stream_info, user_info, url, age, started_at_timestamp, now
    )
    view = _create_live_view(url)

    try:
        edited = await edit_embed(message_id, embed, channel_id, view, content=content)
    except Exception as e:  # noqa: BLE001
        return await _handle_live_embed_edit_error(e, message_id, broadcaster_id)

    return _CycleOutcome.CONTINUE if edited else _CycleOutcome.RETRY


async def _take_alert_offline(
    broadcaster_id: int,
    channel_id: int,
    message_id: int,
    stream_id: int,
    started_at: pendulum.DateTime,
    stream_info: Stream | None,
    user_info: User | None,
    content: str | None,
) -> _CycleOutcome:
    """Swap the live embed for the offline one and retire the alert."""
    channel_info = await get_channel(broadcaster_id)

    if user_info:
        login = user_info.login
    elif channel_info:
        login = channel_info.broadcaster_login
    else:
        login = ""

    return await trigger_offline_sequence(
        broadcaster_id,
        stream_id,
        stream_info,
        pendulum.now(),
        user_info,
        f"https://www.twitch.tv/{login}",
        get_age(started_at, limit_units=2),
        message_id,
        channel_id,
        content,
        channel_info,
    )


async def _run_update_cycle(
    broadcaster_id: int,
    channel_id: int,
    message_id: int,
    stream_id: int,
    started_at: pendulum.DateTime,
    started_at_timestamp: str,
    content: str | None,
) -> _CycleOutcome:
    """Run a single update cycle for the live alert."""
    alert = await repository.get_live_alert(broadcaster_id)
    if alert is None:
        logger.info(
            f"No live alert record left for broadcaster_id={broadcaster_id}; stopping updates for message_id={message_id}"
        )
        return _CycleOutcome.STOP

    stream_info, lookup_ok = await get_stream_state(broadcaster_id)
    if not lookup_ok:
        # A failed lookup is not an offline stream, so conclude nothing from it.
        return _CycleOutcome.RETRY

    user_info = await get_user(broadcaster_id)

    if (
        stream_info is None
        or stream_info.id != str(stream_id)
        or _is_superseded(alert, message_id, stream_id)
    ):
        return await _take_alert_offline(
            broadcaster_id,
            channel_id,
            message_id,
            stream_id,
            started_at,
            stream_info,
            user_info,
            content,
        )

    return await _update_live_embed(
        message_id,
        channel_id,
        broadcaster_id,
        stream_info,
        user_info,
        f"https://www.twitch.tv/{stream_info.user_login}",
        get_age(started_at, limit_units=2),
        started_at_timestamp,
        content,
        pendulum.now(),
    )


async def update_alert(
    broadcaster_id: int,
    channel_id: int,
    message_id: int,
    stream_id: int,
    stream_started_at: str,
) -> None:
    try:
        content = (
            f"<@&{config.role('live_alerts')}>"
            if channel_id == config.channel("stream_alerts")
            else None
        )
        started_at = parse_rfc3339(stream_started_at)
        started_at_timestamp = f"<t:{int(started_at.timestamp())}:f>"

        inconclusive = 0
        while True:
            await asyncio.sleep(_UPDATE_INTERVAL_SECONDS)

            try:
                outcome = await _run_update_cycle(
                    broadcaster_id,
                    channel_id,
                    message_id,
                    stream_id,
                    started_at,
                    started_at_timestamp,
                    content,
                )
            except Exception as e:  # noqa: BLE001
                # A cycle that raised concluded nothing, and must not take the
                # updater with it; the cap below still stops a hopeless one.
                await _handle_api_exception(
                    e,
                    f"Error in the live alert update cycle for broadcaster_id={broadcaster_id}",
                )
                outcome = _CycleOutcome.RETRY

            if outcome is _CycleOutcome.STOP:
                return

            inconclusive = inconclusive + 1 if outcome is _CycleOutcome.RETRY else 0
            if inconclusive >= _MAX_INCONCLUSIVE_CYCLES:
                logger.warning(
                    f"Giving up live alert updates for message_id={message_id} after {inconclusive} inconclusive cycles; the record is kept for the next restart"
                )
                return

    except Exception as e:  # noqa: BLE001
        await _handle_api_exception(
            e, f"Error updating live alert message for broadcaster_id={broadcaster_id}"
        )


_live_alert_update_tasks: dict[int, asyncio.Task] = {}


def _forget_alert_updater(message_id: int, finished: asyncio.Task) -> None:
    if _live_alert_update_tasks.get(message_id) is finished:
        del _live_alert_update_tasks[message_id]


def start_alert_updater(
    broadcaster_id: int,
    channel_id: int,
    message_id: int,
    stream_id: int,
    stream_started_at: str,
) -> None:
    """Start the updater for one alert message, unless it already has one.

    on_ready fires again on every gateway resume, which would otherwise stack a
    second updater per alert on each reconnect.
    """
    existing = _live_alert_update_tasks.get(message_id)
    if existing is not None and not existing.done():
        logger.info(f"Live alert updater already running for message_id={message_id}")
        return

    task = asyncio.create_task(
        update_alert(
            broadcaster_id, channel_id, message_id, stream_id, stream_started_at
        )
    )
    _live_alert_update_tasks[message_id] = task
    task.add_done_callback(
        lambda finished, mid=message_id: _forget_alert_updater(mid, finished)
    )
