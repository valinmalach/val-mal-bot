from typing import Optional

from pydantic import BaseModel

from .common import Subscription


class ChannelRaidCondition(BaseModel):
    to_broadcaster_user_id: Optional[str] = None
    from_broadcaster_user_id: Optional[str] = None


class ChannelRaidSubscription(Subscription):
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
