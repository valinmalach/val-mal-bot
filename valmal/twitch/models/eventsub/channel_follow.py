from typing import Literal

import msgspec


class ChannelFollowSubscription(msgspec.Struct):
    type: Literal["channel.follow"]


class ChannelFollowEvent(msgspec.Struct):
    user_id: str
    user_login: str
    user_name: str
    broadcaster_user_id: str
    broadcaster_user_login: str
    broadcaster_user_name: str
    followed_at: str


class ChannelFollowEventSub(msgspec.Struct):
    subscription: ChannelFollowSubscription
    event: ChannelFollowEvent
