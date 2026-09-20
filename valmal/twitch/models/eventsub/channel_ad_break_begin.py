from typing import Literal

from pydantic import BaseModel


class ChannelAdBreakBeginSubscription(BaseModel):
    type: Literal["channel.ad_break.begin"]


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
