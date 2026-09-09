from typing import Literal

from pydantic import BaseModel


class ChannelRaidSubscription(BaseModel):
    type: Literal["channel.raid"]


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
