from pydantic import BaseModel

from models.twitch_event_subs.common import Subscription


class StreamOnlineCondition(BaseModel):
    broadcaster_user_id: str


class StreamOnlineSubscription(Subscription):
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
