import asyncio
import itertools
import logging
from collections.abc import Awaitable, Callable
from typing import Any, Literal

from config import settings
from constants import TokenType
from errors import notify, report
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
from services.helper.http_client import is_transient_network_error
from services.helper.twitch import call_twitch

logger = logging.getLogger(__name__)

_UPDATE_INTERVAL_SECONDS = 60
# Stop after this many cycles that concluded nothing, so a stuck alert cannot poll
# (and report) forever; the record is left behind for the next restart to retry.
_MAX_INCONCLUSIVE_CYCLES = 5


async def _handle_invalid_response(response, context: str) -> None:
    """Handle invalid HTTP responses with standardized logging and reporting."""
    status = response.status_code if response else "No response"
    text = response.text if response else ""
    logger.warning(f"{context}: {status}")
    await notify(f"{context}: {status} {text}")


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
    await report(e, "Error fetching subscriptions after retries")


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
        await report(e, "Failed to fetch user info after retries")
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
        await report(e, "Failed to fetch user info after retries")
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
        await report(e, f"Failed to subscribe to {sub_type} event after retries")
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
        await notify(f"User not found: {username}")
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
        await report(
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
        await notify(f"User not found: {username}")
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
        await report(e, "Error fetching user infos after retries")
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
        await report(e, "Failed to fetch channel info after retries")
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
        await report(e, "Failed to fetch stream info after retries")
        return None, False

    if response is None or not _is_valid_response(response):
        await _handle_invalid_response(response, "Failed to fetch stream info")
        return None, False

    try:
        stream_info_response = StreamResponse.model_validate(response.json())
    except Exception as e:  # noqa: BLE001
        # A 200 whose body will not parse is still a lookup that told us nothing.
        await report(e, "Could not read the stream info response")
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
        await report(e, "Failed to fetch VOD info after retries")
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
        await report(e, "Failed to fetch ad schedule after retries")
        return None

    if response is None or not _is_valid_response(response):
        await _handle_invalid_response(response, "Failed to fetch ad schedule")
        return None
    ad_schedule_response = AdScheduleResponse.model_validate(response.json())
    return ad_schedule_response.data[0] if ad_schedule_response.data else None
