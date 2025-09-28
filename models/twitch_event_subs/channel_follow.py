from pydantic import BaseModel

from .common import Subscription


class ChannelFollowCondition(BaseModel):
    broadcaster_user_id: str
    moderator_user_id: str


class ChannelFollowSubscription(Subscription):
    condition: ChannelFollowCondition


class ChannelFollowEvent(BaseModel):
    user_id: str
    user_login: str
    user_name: str
    broadcaster_user_id: str
    broadcaster_user_login: str
    broadcaster_user_name: str
    followed_at: str


class ChannelFollowEventSub(BaseModel):
    subscription: ChannelFollowSubscription
    event: ChannelFollowEvent
