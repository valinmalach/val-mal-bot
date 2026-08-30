"""The Helix endpoints the bot uses.

``None`` and an empty list mean Twitch has nothing to give; a call that did not
complete raises ``HelixError``. The two are no longer the same answer.
"""

import itertools
import logging
from typing import Literal

from config import settings
from constants import TokenType
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
from services.twitch import helix

logger = logging.getLogger(__name__)


async def get_user(id: int) -> User | None:
    """The user with this id, or None when Twitch has no such user."""
    payload = await helix.fetch(UserResponse, "GET", "/users", params={"id": id})
    return payload.data[0] if payload.data else None


async def get_user_by_username(username: str) -> User | None:
    payload = await helix.fetch(
        UserResponse, "GET", "/users", params={"login": username}
    )
    return payload.data[0] if payload.data else None


async def get_users(ids: list[str]) -> list[User]:
    """Every user named, fetched in the batches of 100 Helix allows."""
    users: list[User] = []
    for batch in itertools.batched(ids, 100):
        payload = await helix.fetch(
            UserResponse, "GET", "/users", params={"id": list(batch)}
        )
        users.extend(payload.data)
    return users


async def get_channel(id: int) -> Channel | None:
    payload = await helix.fetch(
        ChannelResponse, "GET", "/channels", params={"broadcaster_id": id}
    )
    return payload.data[0] if payload.data else None


async def get_stream(broadcaster_id: int) -> Stream | None:
    """The broadcaster's live stream, or None when they are offline.

    A lookup that fails raises, so offline and unknown are different answers.
    """
    payload = await helix.fetch(
        StreamResponse, "GET", "/streams", params={"user_id": broadcaster_id}
    )
    return payload.data[0] if payload.data else None


async def get_stream_vod(user_id: int, stream_id: int) -> Video | None:
    payload = await helix.fetch(
        VideoResponse,
        "GET",
        "/videos",
        params={"user_id": user_id, "type": "archive"},
    )
    return next(
        (video for video in payload.data if video.stream_id == str(stream_id)), None
    )


async def get_ad_schedule(broadcaster_id: int) -> AdSchedule | None:
    payload = await helix.fetch(
        AdScheduleResponse,
        "GET",
        "/channels/ads",
        params={"broadcaster_id": broadcaster_id},
        token_type=TokenType.Broadcaster,
    )
    return payload.data[0] if payload.data else None


async def get_subscriptions() -> list[Subscription]:
    """Every enabled EventSub subscription, following Helix's pagination."""
    subscriptions: list[Subscription] = []
    cursor: str | None = None

    while True:
        params: dict[str, str] = {"status": "enabled"}
        if cursor:
            params["after"] = cursor

        payload = await helix.fetch(
            SubscriptionResponse, "GET", "/eventsub/subscriptions", params=params
        )
        if not payload.data:
            break

        subscriptions.extend(payload.data)
        cursor = payload.pagination.cursor
        if not cursor:
            break

    return subscriptions


async def send_chat_message(broadcaster_id: str, message: str) -> None:
    """Post to a broadcaster's chat. Never retried: a repeat would post twice."""
    await helix.request(
        "POST",
        "/chat/messages",
        json={
            "broadcaster_id": broadcaster_id,
            "sender_id": config.setting("twitch_bot_user_id"),
            "message": message,
            "for_source_only": False,
        },
    )


async def send_shoutout(to_broadcaster_id: str) -> None:
    """Shout a channel out. Never retried: a repeat would shout out twice."""
    await helix.request(
        "POST",
        "/chat/shoutouts",
        json={
            "from_broadcaster_id": config.setting("twitch_broadcaster_id"),
            "to_broadcaster_id": to_broadcaster_id,
            "moderator_id": config.setting("twitch_bot_user_id"),
        },
        token_type=TokenType.User,
    )


async def _subscribe(sub_type: Literal["online", "offline"], user_id: str) -> None:
    # Repeatable: Twitch rejects a duplicate subscription rather than creating
    # one, so the worst a repeat costs is a conflict. See docs/adr/0002.
    await helix.request(
        "POST",
        "/eventsub/subscriptions",
        json={
            "type": f"stream.{sub_type}",
            "version": "1",
            "condition": {"broadcaster_user_id": user_id},
            "transport": {
                "method": "webhook",
                "callback": f"{settings.app_url}/webhook/twitch{'' if sub_type == 'online' else '/offline'}",
                "secret": settings.twitch_webhook_secret,
            },
        },
        repeatable=True,
    )


async def subscribe_to_user(username: str) -> bool:
    """False when Twitch has no such user; a failed call raises."""
    user = await get_user_by_username(username)
    if not user:
        logger.warning(f"User not found: {username}")
        return False

    await _subscribe("online", user.id)
    await _subscribe("offline", user.id)
    return True


async def unsubscribe_to_user(username: str) -> bool:
    """False when Twitch has no such user; a failed call raises."""
    user = await get_user_by_username(username)
    if not user:
        logger.warning(f"User not found: {username}")
        return False

    matching = [
        subscription
        for subscription in await get_subscriptions()
        if subscription.type in {"stream.online", "stream.offline"}
        and subscription.condition.broadcaster_user_id == user.id
    ]
    if not matching:
        logger.info(f"No online/offline subscriptions found for user: {username}")
        return True

    for subscription in matching:
        await helix.request(
            "DELETE", "/eventsub/subscriptions", params={"id": subscription.id}
        )
    return True
