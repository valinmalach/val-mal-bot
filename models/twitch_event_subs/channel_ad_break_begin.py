from pydantic import BaseModel

from .common import Subscription


class ChannelAdBreakBeginCondition(BaseModel):
    broadcaster_user_id: str


class ChannelAdBreakBeginSubscription(Subscription):
    condition: ChannelAdBreakBeginCondition


class ChannelAdBreakBeginEvent(BaseModel):
    duration_seconds: int
    started_at: str
    is_automatic: bool
    broadcaster_user_id: str
    broadcaster_user_login: str
    broadcaster_user_name: str
    requester_user_id: str
    requester_user_login: str
    requester_user_name: str


class ChannelAdBreakBeginEventSub(BaseModel):
    subscription: ChannelAdBreakBeginSubscription
    event: ChannelAdBreakBeginEvent
