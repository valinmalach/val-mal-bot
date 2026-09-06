from typing import Literal

from pydantic import BaseModel

from .common import Subscription


class ChannelRaidCondition(BaseModel):
    to_broadcaster_user_id: str | None = None
    from_broadcaster_user_id: str | None = None


class ChannelRaidSubscription(Subscription):
    type: Literal["channel.raid"]
    condition: ChannelRaidCondition


class ChannelRaidEvent(BaseModel):
    from_broadcaster_user_id: str
    from_broadcaster_user_login: str
    from_broadcaster_user_name: str
    to_broadcaster_user_id: str
    to_broadcaster_user_login: str
    to_broadcaster_user_name: str
    viewers: int


class ChannelRaidEventSub(BaseModel):
    subscription: ChannelRaidSubscription
    event: ChannelRaidEvent
