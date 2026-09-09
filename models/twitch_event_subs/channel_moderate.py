from typing import Literal

from pydantic import BaseModel


class ChannelModerateSubscription(BaseModel):
    type: Literal["channel.moderate"]


class ChannelModerateEvent(BaseModel):
    broadcaster_user_id: str
    # A plain str, not the Literal of every action Twitch documents: the handler
    # compares it to one value, and an action added to Twitch's list would
    # otherwise fail validation and spend the subscription's failure budget.
    action: str


class ChannelModerateEventSub(BaseModel):
    subscription: ChannelModerateSubscription
    event: ChannelModerateEvent
