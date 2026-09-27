"""One valid body per EventSub type, and the route and handler each must reach."""

from typing import Any

from tests.twitch.eventsub.webhook.support import stream_online_payload

_BROADCASTER = {
    "broadcaster_user_id": "1",
    "broadcaster_user_login": "bob",
    "broadcaster_user_name": "Bob",
}

EVENT_BODIES: dict[str, dict[str, Any]] = {
    "stream.online": stream_online_payload()["event"],
    "stream.offline": dict(_BROADCASTER),
    "channel.chat.message": {
        **_BROADCASTER,
        "chatter_user_id": "3",
        "chatter_user_login": "chatter",
        "chatter_user_name": "Chatter",
        "message": {"text": "hello"},
        "badges": [],
    },
    "channel.follow": {
        "user_id": "4",
        "user_login": "fan",
        "user_name": "Fan",
        **_BROADCASTER,
        "followed_at": "2026-06-15T11:59:00Z",
    },
    "channel.ad_break.begin": {
        "duration_seconds": 60,
        "started_at": "2026-06-15T11:59:00Z",
        "is_automatic": True,
        **_BROADCASTER,
        "requester_user_id": "2",
        "requester_user_login": "req",
        "requester_user_name": "Req",
    },
    "channel.raid": {
        "from_broadcaster_user_id": "6",
        "from_broadcaster_user_login": "raider",
        "from_broadcaster_user_name": "Raider",
        "to_broadcaster_user_id": "1",
        "to_broadcaster_user_login": "bob",
        "to_broadcaster_user_name": "Bob",
        "viewers": 42,
    },
    "channel.moderate": {"broadcaster_user_id": "1", "action": "ban"},
    "channel.channel_points_custom_reward_redemption.add": {
        "broadcaster_user_id": "1",
        "user_id": "5",
        "user_login": "redeemer",
    },
}

ROUTES: dict[str, tuple[str, str]] = {
    "stream.online": ("/webhook/twitch", "stream_online"),
    "stream.offline": ("/webhook/twitch/offline", "stream_offline"),
    "channel.chat.message": ("/webhook/twitch/chat", "channel_chat_message"),
    "channel.follow": ("/webhook/twitch/follow", "channel_follow"),
    "channel.ad_break.begin": ("/webhook/twitch/adbreak", "channel_ad_break_begin"),
    "channel.raid": ("/webhook/twitch/raid", "channel_raid"),
    "channel.moderate": ("/webhook/twitch/moderate", "channel_moderate"),
    "channel.channel_points_custom_reward_redemption.add": (
        "/webhook/twitch/redemption",
        "channel_points_custom_reward_redemption_add",
    ),
}


def notification(sub_type: str) -> dict[str, Any]:
    return {"subscription": {"type": sub_type}, "event": dict(EVENT_BODIES[sub_type])}
