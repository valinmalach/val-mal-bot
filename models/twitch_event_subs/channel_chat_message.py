from typing import Literal

from pydantic import BaseModel

from .common import Badge, Message, Subscription


class ChannelChatMessageCondition(BaseModel):
    broadcaster_user_id: str
    user_id: str


class ChannelChatMessageSubscription(Subscription):
    type: Literal["channel.chat.message"]
    condition: ChannelChatMessageCondition


class ChannelChatMessageEvent(BaseModel):
    broadcaster_user_id: str
    broadcaster_user_login: str
    broadcaster_user_name: str
    chatter_user_name: str
    message: Message
    badges: list[Badge]
    # Set only for a line relayed from another channel's shared chat, which the
    # dispatcher drops; absent on an ordinary message rather than equal to the
    # broadcaster.
    source_broadcaster_user_id: str | None = None


class ChannelChatMessageEventSub(BaseModel):
    subscription: ChannelChatMessageSubscription
    event: ChannelChatMessageEvent
