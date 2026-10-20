from typing import Literal

import msgspec


class StreamOfflineSubscription(msgspec.Struct):
    type: Literal["stream.offline"]


class StreamOfflineEvent(msgspec.Struct):
    broadcaster_user_id: str
    broadcaster_user_login: str
    broadcaster_user_name: str


class StreamOfflineEventSub(msgspec.Struct):
    subscription: StreamOfflineSubscription
    event: StreamOfflineEvent
