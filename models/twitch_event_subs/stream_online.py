from typing import Literal

from pydantic import BaseModel

from .common import Subscription


class StreamOnlineCondition(BaseModel):
    broadcaster_user_id: str


class StreamOnlineSubscription(Subscription):
    type: Literal["stream.online"]
    condition: StreamOnlineCondition


class StreamOnlineEvent(BaseModel):
    id: str
    broadcaster_user_id: str
    broadcaster_user_login: str
    broadcaster_user_name: str
    type: str
    started_at: str


class StreamOnlineEventSub(BaseModel):
    subscription: StreamOnlineSubscription
    event: StreamOnlineEvent
