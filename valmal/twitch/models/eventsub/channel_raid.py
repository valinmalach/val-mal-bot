from typing import Literal

import msgspec


class ChannelRaidSubscription(msgspec.Struct):
    type: Literal["channel.raid"]


class ChannelRaidEvent(msgspec.Struct):
    from_broadcaster_user_id: str
    from_broadcaster_user_login: str
    from_broadcaster_user_name: str
    to_broadcaster_user_id: str
    to_broadcaster_user_login: str
    to_broadcaster_user_name: str
    viewers: int


class ChannelRaidEventSub(msgspec.Struct):
    subscription: ChannelRaidSubscription
    event: ChannelRaidEvent
