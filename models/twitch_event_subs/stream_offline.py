from typing import Literal

from pydantic import BaseModel


class StreamOfflineSubscription(BaseModel):
    type: Literal["stream.offline"]


class StreamOfflineEvent(BaseModel):
    broadcaster_user_id: str
    broadcaster_user_login: str
    broadcaster_user_name: str


class StreamOfflineEventSub(BaseModel):
    subscription: StreamOfflineSubscription
    event: StreamOfflineEvent
