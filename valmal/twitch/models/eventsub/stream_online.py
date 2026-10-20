from typing import Literal

import msgspec


class StreamOnlineSubscription(msgspec.Struct):
    type: Literal["stream.online"]


class StreamOnlineEvent(msgspec.Struct):
    id: str
    broadcaster_user_id: str
    broadcaster_user_login: str
    broadcaster_user_name: str
    type: str
    started_at: str


class StreamOnlineEventSub(msgspec.Struct):
    subscription: StreamOnlineSubscription
    event: StreamOnlineEvent
