from typing import Literal

import msgspec


class ChannelModerateSubscription(msgspec.Struct):
    type: Literal["channel.moderate"]


class ChannelModerateEvent(msgspec.Struct):
    broadcaster_user_id: str
    # A plain str, not the Literal of every action Twitch documents: the handler
    # compares it to one value, and an action added to Twitch's list would
    # otherwise fail validation and spend the subscription's failure budget.
    action: str


class ChannelModerateEventSub(msgspec.Struct):
    subscription: ChannelModerateSubscription
    event: ChannelModerateEvent
