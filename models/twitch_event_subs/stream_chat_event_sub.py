from typing import Literal, Optional

from pydantic import BaseModel


class StreamChatEventCondition(BaseModel):
    broadcaster_user_id: str
    user_id: str


class StreamChatEventSubscription(BaseModel):
    id: str
    type: str
    version: str
    status: str
    cost: int
    condition: StreamChatEventCondition
    created_at: str


class Cheermote(BaseModel):
    prefix: str
    bits: int
    tier: int


class Emote(BaseModel):
    id: str
    emote_set_id: str
    owner_id: str
    format: list[Literal["static", "animated"]]


class Mention(BaseModel):
    user_id: str
    user_name: str
    user_login: str


class Fragment(BaseModel):
    type: Literal["text", "cheermote", "emote", "mention"]
    text: str
    cheermote: Optional[Cheermote]
    emote: Optional[Emote]
    mention: Optional[Mention]


class ChatMessage(BaseModel):
    text: str
    fragments: list[Fragment]


class Badge(BaseModel):
    set_id: str
    id: str
    info: str


class Cheer(BaseModel):
    bits: int


class ChatReply(BaseModel):
    parent_message_id: str
    parent_message_body: str
    parent_user_id: str
    parent_user_name: str
    parent_user_login: str
    thread_message_id: str
    thread_user_id: str
    thread_user_name: str
    thread_user_login: str


class StreamChatEvent(BaseModel):
    broadcaster_user_id: str
    broadcaster_user_login: str
    broadcaster_user_name: str
    chatter_user_id: str
    chatter_user_login: str
    chatter_user_name: str
    message_id: str
    message: ChatMessage
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
    reply: Optional[ChatReply]
    channel_points_custom_reward_id: Optional[str]
    source_broadcaster_user_id: Optional[str]
    source_broadcaster_user_name: Optional[str]
    source_broadcaster_user_login: Optional[str]
    source_message_id: Optional[str]
    source_badges: Optional[list[Badge]]
    is_source_only: Optional[bool]


class StreamChatEventSub(BaseModel):
    subscription: StreamChatEventSubscription
    event: StreamChatEvent
