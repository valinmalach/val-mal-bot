"""The Helix endpoints the bot uses.

``None`` and an empty list mean Twitch has nothing to give; a call that did not
complete raises ``HelixError``. The two are no longer the same answer.
"""

import itertools
import logging
from typing import Any, Literal

from valmal.bot.present import quoted
from valmal.core.config import config
from valmal.core.errors import notify
from valmal.core.settings import settings
from valmal.db.models.enums import TokenType
from valmal.twitch.client import helix
from valmal.twitch.client.helix import HelixError
from valmal.twitch.models.api.ad_schedule import AdSchedule, AdScheduleResponse
from valmal.twitch.models.api.channel import Channel, ChannelResponse
from valmal.twitch.models.api.stream import Stream, StreamResponse, StreamType
from valmal.twitch.models.api.subscription import Subscription, SubscriptionResponse
from valmal.twitch.models.api.user import User, UserResponse
from valmal.twitch.models.api.video import Video, VideoResponse

logger = logging.getLogger(__name__)

# A subscription at our callback in either of these is the goal already met.
# Verification is a seconds-long transient on the way to "enabled": one caught
# mid-way is about to arrive by itself, and replacing it destroys that.
# migrate_plan.decide refuses to touch one for the same reason.
_WORKING_STATUSES = frozenset({"enabled", "webhook_callback_verification_pending"})


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
    for batch in itertools.batched(ids, 100, strict=False):
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


def live_stream(stream: Stream | None) -> Stream | None:
    """The stream Helix says is running, or None -- the one answer every caller
    that asks "is this live" has to agree on. ``StreamType`` has exactly two
    members, ``live`` and ``error``; an error-typed stream is refused here
    once rather than by each caller re-deriving the same check.
    """
    return stream if stream is not None and stream.type == StreamType.live else None


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

    Every webhook route begins with it, so a callback that does not begin here
    belongs to another deployment or to this one before it moved. Deliberately
    not a count: this said "seven" from the commit that added the check until the
    eighth route landed in valmal/twitch/eventsub/router.py without touching this file, which
    is exactly what a number written down in the wrong module does.
    """
    return callback_url("/webhook/twitch")


def callback_url(path: str) -> str:
    """The absolute callback this deployment answers one webhook path on.

    rstrip to match valmal/twitch/oauth/grants.py: a trailing slash in APP_URL would
    otherwise build a //webhook/twitch that Twitch dutifully calls and FastAPI
    does not route.
    """
    return f"{settings.app_url.rstrip('/')}{path}"


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


async def create_subscription(
    sub_type: str, version: str, condition: dict[str, Any], callback: str
) -> None:
    """Create one EventSub subscription of any type, at this callback.

    ``version`` is the caller's because it varies by type -- channel.follow and
    channel.moderate are 2, the rest are 1 -- and a subscription being recreated
    carries its own, which is the only one that reproduces it.

    Repeatable: Twitch rejects a duplicate rather than creating a second, so the
    worst a repeat costs is the 409 its caller handles. See docs/adr/0002.
    """
    await helix.request(
        "POST",
        "/eventsub/subscriptions",
        json={
            "type": sub_type,
            "version": version,
            "condition": condition,
            "transport": {
                "method": "webhook",
                "callback": callback,
                "secret": settings.twitch_webhook_secret,
            },
        },
        repeatable=True,
    )


async def delete_subscription(subscription_id: str) -> None:
    await helix.request(
        "DELETE", "/eventsub/subscriptions", params={"id": subscription_id}
    )


async def _create_subscription(
    sub_type: Literal["online", "offline"], user_id: str
) -> None:
    await create_subscription(
        f"stream.{sub_type}",
        "1",
        {"broadcaster_user_id": user_id},
        _callback_url(sub_type),
    )


async def _subscribe(sub_type: Literal["online", "offline"], user_id: str) -> None:
    """Subscribe, replacing an existing subscription that would not deliver.

    Twitch answers 409 for a duplicate, and its uniqueness key is the type and
    the condition alone -- not the transport. So a 409 says just as readily that
    a subscription exists pointing at a callback this deployment no longer
    answers on, or one Twitch disabled after too many failed deliveries, as that
    a working one -- enabled, or still mid-verification -- is already in place.
    Only the last of those is the goal already met; reporting the other two as
    subscribed would promise an alert that can never fire.
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
        subscription.status in _WORKING_STATUSES
        and subscription.transport.callback == callback
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
        await delete_subscription(subscription.id)
    await _create_subscription(sub_type, user_id)


async def _named_user(username: str) -> User | None:
    """The user this names, or None -- refusing anything that cannot name one.

    The grammar is asked for here rather than trusted from the caller: these two
    are public, and a value that cannot name a channel should reach neither a
    Helix query parameter nor a log line on the strength of having been passed
    in. Deferred because valmal.twitch.eventsub.commands imports this module; it owns
    the one grammar, and a second copy here is what issue #38 is about.

    Both refusals are said in the admin channel, not only logged. The value that
    failed the grammar is never echoed, since it can hold anything; a login that
    passed it is safe to name, and is put in a code span all the same.
    """
    from valmal.twitch.eventsub.commands import is_twitch_login

    if not is_twitch_login(username):
        await notify(
            "Refused a Twitch lookup: what was given cannot be a login.",
            key="twitch-lookup-bad-login",
        )
        return None
    user = await get_user_by_username(username)
    if not user:
        await notify(
            f"Twitch has no user called {quoted(username)}.",
            key=f"twitch-lookup-not-found:{username}",
        )
    return user


async def subscribe_to_user(username: str) -> bool:
    """False when Twitch has no such user; a failed call raises."""
    user = await _named_user(username)
    if not user:
        return False

    await _subscribe("online", user.id)
    await _subscribe("offline", user.id)
    return True


async def unsubscribe_to_user(username: str) -> bool:
    """False when Twitch has no such user; a failed call raises."""
    user = await _named_user(username)
    if not user:
        return False

    matching = [
        subscription
        for subscription in await get_subscriptions()
        if subscription.type in {"stream.online", "stream.offline"}
        and subscription.condition.broadcaster_user_id == user.id
    ]
    if not matching:
        logger.info("No online/offline subscriptions for user: %r", username)
        return True

    for subscription in matching:
        await delete_subscription(subscription.id)
    return True
