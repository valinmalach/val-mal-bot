from typing import Literal

from pydantic import BaseModel


class StreamOnlineSubscription(BaseModel):
    type: Literal["stream.online"]


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
