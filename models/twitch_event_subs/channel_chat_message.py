from typing import Literal

from pydantic import BaseModel

from .common.badge import Badge
from .common.message import Message


class ChannelChatMessageSubscription(BaseModel):
    type: Literal["channel.chat.message"]


class ChannelChatMessageEvent(BaseModel):
    broadcaster_user_id: str
    broadcaster_user_login: str
    broadcaster_user_name: str
    # The id matches the autoshoutout list, because a rename cannot move it;
    # the login is what a `!so` line needs. chatter_user_name is the display
    # name, which for some locales is a different string from the login
    # entirely, so it is for showing and never for matching.
    chatter_user_id: str
    chatter_user_login: str
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
