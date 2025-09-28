from typing import Literal, Optional

from pydantic import BaseModel

from .common import Badge, Message, Subscription


class ChannelChatNotificationCondition(BaseModel):
    broadcaster_user_id: str
    user_id: str


class ChannelChatNotificationSubscription(Subscription):
    condition: ChannelChatNotificationCondition


class Sub(BaseModel):
    sub_tier: Literal["1000", "2000", "3000"]
    is_prime: Optional[bool]
    duration_months: int


class Resub(Sub):
    cumulative_months: int
    streak_months: int
    is_gift: bool
    gifter_is_anonymous: Optional[bool]
    gifter_user_id: Optional[str]
    gifter_user_name: Optional[str]
    gifter_user_login: Optional[str]


class SubGift(Sub):
    cumulative_total: Optional[int]
    recipient_user_id: str
    recipient_user_name: str
    recipient_user_login: str
    community_gift_id: Optional[str]


class CommunitySubGift(BaseModel):
    id: str
    total: int
    sub_tier: Literal["1000", "2000", "3000"]
    cumulative_total: Optional[int]


class GiftPaidUpgrade(BaseModel):
    gifter_is_anonymous: bool
    gifter_user_id: Optional[str]
    gifter_user_name: Optional[str]


class PrimePaidUpgrade(BaseModel):
    sub_tier: Literal["1000", "2000", "3000"]


class PayItForward(GiftPaidUpgrade):
    gifter_user_login: Optional[str]


class Raid(BaseModel):
    user_id: str
    user_name: str
    user_login: str
    viewer_count: int
    profile_image_url: str


class Unraid(BaseModel):
    pass


class Announcement(BaseModel):
    color: str


class BitsBadgeTier(BaseModel):
    tier: int


class Amount(BaseModel):
    value: int
    decimal_place: int
    currency: str


class CharityDonation(BaseModel):
    charity_name: str
    amount: Amount


class ChannelChatNotificationEvent(BaseModel):
    broadcaster_user_id: str
    broadcaster_user_name: str
    broadcaster_user_login: str
    chatter_user_id: str
    chatter_user_name: str
    chatter_is_anonymous: bool
    color: str
    badges: list[Badge]
    system_message: str
    message_id: str
    message: Message
    notice_type: Literal[
        "sub",
        "resub",
        "sub_gift",
        "community_sub_gift",
        "gift_paid_upgrade",
        "prime_paid_upgrade",
        "raid",
        "unraid",
        "pay_it_forward",
        "announcement",
        "bits_badge_tier",
        "charity_donation",
        "shared_chat_sub",
        "shared_chat_resub",
        "shared_chat_sub_gift",
        "shared_chat_community_sub_gift",
        "shared_chat_gift_paid_upgrade",
        "shared_chat_prime_paid_upgrade",
        "shared_chat_raid",
        "shared_chat_pay_it_forward",
        "shared_chat_announcement",
    ]
    sub: Optional[Sub]
    resub: Optional[Resub]
    sub_gift: Optional[SubGift]
    community_sub_gift: Optional[CommunitySubGift]
    gift_paid_upgrade: Optional[GiftPaidUpgrade]
    prime_paid_upgrade: Optional[PrimePaidUpgrade]
    pay_it_forward: Optional[PayItForward]
    raid: Optional[Raid]
    unraid: Optional[Unraid]
    announcement: Optional[Announcement]
    bits_badge_tier: Optional[BitsBadgeTier]
    charity_donation: Optional[CharityDonation]
    source_broadcaster_user_id: Optional[str]
    source_broadcaster_user_name: Optional[str]
    source_broadcaster_user_login: Optional[str]
    source_message_id: Optional[str]
    source_badges: Optional[list[Badge]]
    shared_chat_sub: Optional[Sub]
    shared_chat_resub: Optional[Resub]
    shared_chat_sub_gift: Optional[SubGift]
    shared_chat_community_sub_gift: Optional[CommunitySubGift]
    shared_chat_gift_paid_upgrade: Optional[GiftPaidUpgrade]
    shared_chat_prime_paid_upgrade: Optional[PrimePaidUpgrade]
    shared_chat_pay_it_forward: Optional[PayItForward]
    shared_chat_raid: Optional[Raid]
    shared_chat_announcement: Optional[Announcement]


class ChannelChatNotificationEventSub(BaseModel):
    subscription: ChannelChatNotificationSubscription
    event: ChannelChatNotificationEvent
