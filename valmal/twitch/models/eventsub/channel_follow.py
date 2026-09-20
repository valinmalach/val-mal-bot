from typing import Literal

from pydantic import BaseModel


class ChannelFollowSubscription(BaseModel):
    type: Literal["channel.follow"]


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
