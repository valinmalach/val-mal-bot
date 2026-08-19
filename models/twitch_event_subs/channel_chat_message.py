from typing import Literal

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
    cheer: Cheer | None
    color: str | None
    reply: Reply | None
    channel_points_custom_reward_id: str | None
    source_broadcaster_user_id: str | None
    source_broadcaster_user_name: str | None
    source_broadcaster_user_login: str | None
    source_message_id: str | None
    source_badges: list[Badge] | None
    is_source_only: bool | None


class ChannelChatMessageEventSub(BaseModel):
    subscription: ChannelChatMessageSubscription
    event: ChannelChatMessageEvent
