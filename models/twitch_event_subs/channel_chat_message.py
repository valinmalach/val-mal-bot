from typing import Literal, Optional

from pydantic import BaseModel

from .common import Badge, Message, Subscription


class ChannelChatMessageCondition(BaseModel):
    broadcaster_user_id: str
    user_id: str


class ChannelChatMessageSubscription(Subscription):
    condition: ChannelChatMessageCondition


class Cheer(BaseModel):
    bits: int


class Reply(BaseModel):
    parent_message_id: str
    parent_message_body: str
    parent_user_id: str
    parent_user_name: str
    parent_user_login: str
    thread_message_id: str
    thread_user_id: str
    thread_user_name: str
    thread_user_login: str


class ChannelChatMessageEvent(BaseModel):
    broadcaster_user_id: str
    broadcaster_user_login: str
    broadcaster_user_name: str
    chatter_user_id: str
    chatter_user_login: str
    chatter_user_name: str
    message_id: str
    message: Message
    message_type: Literal[
        "text",
        "channel_points_highlighted",
        "channel_points_sub_only",
        "user_intro",
        "power_ups_message_effect",
        "power_ups_gigantified_emote",
    ]
    badges: list[Badge]
    cheer: Optional[Cheer]
    color: Optional[str]
    reply: Optional[Reply]
    channel_points_custom_reward_id: Optional[str]
    source_broadcaster_user_id: Optional[str]
    source_broadcaster_user_name: Optional[str]
    source_broadcaster_user_login: Optional[str]
    source_message_id: Optional[str]
    source_badges: Optional[list[Badge]]
    is_source_only: Optional[bool]


class ChannelChatMessageEventSub(BaseModel):
    subscription: ChannelChatMessageSubscription
    event: ChannelChatMessageEvent
