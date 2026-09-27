from typing import Literal

from pydantic import BaseModel


class ChannelPointsCustomRewardRedemptionAddSubscription(BaseModel):
    type: Literal["channel.channel_points_custom_reward_redemption.add"]


class ChannelPointsCustomRewardRedemptionAddEvent(BaseModel):
    broadcaster_user_id: str
    # The id matches the autoshoutout list, the login is what a `!so` line
    # needs; user_name is the display name and is not modelled, because
    # nothing here shows it.
    user_id: str
    user_login: str


class ChannelPointsCustomRewardRedemptionAddEventSub(BaseModel):
    subscription: ChannelPointsCustomRewardRedemptionAddSubscription
    event: ChannelPointsCustomRewardRedemptionAddEvent
