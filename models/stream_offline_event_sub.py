from pydantic import BaseModel


class StreamOfflineCondition(BaseModel):
    broadcaster_user_id: str


class StreamOfflineSubscription(BaseModel):
    id: str
    type: str
    version: str
    status: str
    cost: int
    condition: StreamOfflineCondition
    created_at: str


class StreamOfflineEvent(BaseModel):
    broadcaster_user_id: str
    broadcaster_user_login: str
    broadcaster_user_name: str


class StreamOfflineEventSub(BaseModel):
    subscription: StreamOfflineSubscription
    event: StreamOfflineEvent
