"""The Helix endpoints the bot uses.

``None`` and an empty list mean Twitch has nothing to give; a call that did not
complete raises ``HelixError``. The two are no longer the same answer.
"""

import itertools
import logging
from typing import Literal

from config import settings
from constants import TokenType
from errors import notify
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
from services.twitch.helix import HelixError

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
    """Every EventSub subscription in any state, following Helix's pagination.

    Unfiltered on purpose. A subscription Twitch has disabled is the one worth
    seeing, and hiding it makes a broadcaster whose alerts will never fire look
    exactly like one who was never subscribed.
    """
    subscriptions: list[Subscription] = []
    cursor: str | None = None

    while True:
        params: dict[str, str] = {}
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


def callback_prefix() -> str:
    """What every EventSub callback for this deployment starts with.

    All seven webhook routes live under it, so a callback that does not begin
    here belongs to another deployment or to this one before it moved. rstrip
    to match services/twitch/oauth.py: a trailing slash in APP_URL would
    otherwise build a //webhook/twitch that Twitch dutifully calls and FastAPI
    does not route.
    """
    return f"{settings.app_url.rstrip('/')}/webhook/twitch"


def _callback_url(sub_type: Literal["online", "offline"]) -> str:
    return f"{callback_prefix()}{'' if sub_type == 'online' else '/offline'}"


async def _matching_subscriptions(
    sub_type: Literal["online", "offline"], user_id: str
) -> list[Subscription]:
    return [
        subscription
        for subscription in await get_subscriptions()
        if subscription.type == f"stream.{sub_type}"
        and subscription.condition.broadcaster_user_id == user_id
    ]


async def _create_subscription(
    sub_type: Literal["online", "offline"], user_id: str
) -> None:
    # Repeatable: Twitch rejects a duplicate rather than creating a second, so
    # the worst a repeat costs is the 409 its caller handles. See docs/adr/0002.
    await helix.request(
        "POST",
        "/eventsub/subscriptions",
        json={
            "type": f"stream.{sub_type}",
            "version": "1",
            "condition": {"broadcaster_user_id": user_id},
            "transport": {
                "method": "webhook",
                "callback": _callback_url(sub_type),
                "secret": settings.twitch_webhook_secret,
            },
        },
        repeatable=True,
    )


async def _subscribe(sub_type: Literal["online", "offline"], user_id: str) -> None:
    """Subscribe, replacing an existing subscription that would not deliver.

    Twitch answers 409 for a duplicate, and its uniqueness key is the type and
    the condition alone -- not the transport. So a 409 says just as readily that
    a subscription exists pointing at a callback this deployment no longer
    answers on, or one Twitch disabled after too many failed deliveries, as that
    a working one is already in place. Only the last of those is the goal
    already met; reporting the other two as subscribed would promise an alert
    that can never fire.
    """
    try:
        await _create_subscription(sub_type, user_id)
        return
    except HelixError as e:
        if e.status != 409:
            raise

    callback = _callback_url(sub_type)
    existing = await _matching_subscriptions(sub_type, user_id)
    if any(
        subscription.status == "enabled" and subscription.transport.callback == callback
        for subscription in existing
    ):
        logger.info(f"Already subscribed to stream.{sub_type} for user_id={user_id}")
        return

    # Whatever is there will not deliver, and it is what the 409 was about, so
    # it has to go before Twitch will accept the replacement.
    for subscription in existing:
        await notify(
            f"Replacing the stream.{sub_type} subscription for user_id={user_id}:"
            f" it was status={subscription.status} calling back on"
            f" {subscription.transport.callback}, so it was delivering nothing.",
            key=f"subscription-replaced:{sub_type}:{user_id}",
        )
        await helix.request(
            "DELETE", "/eventsub/subscriptions", params={"id": subscription.id}
        )
    await _create_subscription(sub_type, user_id)


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


def subscription_target(subscription: Subscription) -> str:
    """Whichever id identifies this subscription, since the field varies by type."""
    condition = subscription.condition
    for label, value in (
        ("broadcaster", condition.broadcaster_user_id),
        ("to broadcaster", condition.to_broadcaster_user_id),
        ("from broadcaster", condition.from_broadcaster_user_id),
        ("user", condition.user_id),
    ):
        if value:
            return f"{label} {value}"
    return f"id {subscription.id}"


def undeliverable(subscription: Subscription) -> str | None:
    """Why this subscription will not reach the bot, or None when it will.

    Two things are checkable without knowing which event types the bot expects,
    and only two: Twitch having disabled it, and a callback pointing elsewhere.
    Matching whole URLs instead would call every event type the bot does not
    create itself — chat, follows, raids — undeliverable on every start. The
    case this cannot see, a callback on the right host with a wrong path, fails
    its deliveries until Twitch disables it, which the first check then catches.
    """
    if subscription.status != "enabled":
        return subscription.status
    callback = subscription.transport.callback or ""
    if not callback.startswith(callback_prefix()):
        return f"calling back on {callback or 'nothing'}"
    return None
