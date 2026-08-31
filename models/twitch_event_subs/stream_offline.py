from typing import Literal

from pydantic import BaseModel

from .common import Subscription


class StreamOfflineCondition(BaseModel):
    broadcaster_user_id: str


class StreamOfflineSubscription(Subscription):
    type: Literal["stream.offline"]
    condition: StreamOfflineCondition


class StreamOfflineEvent(BaseModel):
    broadcaster_user_id: str
    broadcaster_user_login: str
    broadcaster_user_name: str


class StreamOfflineEventSub(BaseModel):
    subscription: StreamOfflineSubscription
    event: StreamOfflineEvent
