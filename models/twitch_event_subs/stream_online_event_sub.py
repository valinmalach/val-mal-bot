from pydantic import BaseModel


class StreamOnlineCondition(BaseModel):
    broadcaster_user_id: str


class StreamOnlineSubscription(BaseModel):
    id: str
    type: str
    version: str
    status: str
    cost: int
    condition: StreamOnlineCondition
    created_at: str


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
