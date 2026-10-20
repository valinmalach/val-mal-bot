from typing import Literal

import msgspec


class ChannelAdBreakBeginSubscription(msgspec.Struct):
    type: Literal["channel.ad_break.begin"]


class ChannelAdBreakBeginEvent(msgspec.Struct):
    duration_seconds: int
    started_at: str
    is_automatic: bool
    broadcaster_user_id: str
    broadcaster_user_login: str
    broadcaster_user_name: str
    requester_user_id: str
    requester_user_login: str
    requester_user_name: str


class ChannelAdBreakBeginEventSub(msgspec.Struct):
    subscription: ChannelAdBreakBeginSubscription
    event: ChannelAdBreakBeginEvent
