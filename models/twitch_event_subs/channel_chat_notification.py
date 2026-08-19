from typing import Literal

from pydantic import BaseModel

from .common import Badge, Message, Subscription


class ChannelChatNotificationCondition(BaseModel):
    broadcaster_user_id: str
    user_id: str


class ChannelChatNotificationSubscription(Subscription):
    condition: ChannelChatNotificationCondition


class Sub(BaseModel):
    sub_tier: Literal["1000", "2000", "3000"]
    is_prime: bool | None
    duration_months: int


class Resub(Sub):
    cumulative_months: int
    streak_months: int
    is_gift: bool
    gifter_is_anonymous: bool | None
    gifter_user_id: str | None
    gifter_user_name: str | None
    gifter_user_login: str | None


class SubGift(Sub):
    cumulative_total: int | None
    recipient_user_id: str
    recipient_user_name: str
    recipient_user_login: str
    community_gift_id: str | None


class CommunitySubGift(BaseModel):
    id: str
    total: int
    sub_tier: Literal["1000", "2000", "3000"]
    cumulative_total: int | None


class GiftPaidUpgrade(BaseModel):
    gifter_is_anonymous: bool
    gifter_user_id: str | None
    gifter_user_name: str | None


class PrimePaidUpgrade(BaseModel):
    sub_tier: Literal["1000", "2000", "3000"]


class PayItForward(GiftPaidUpgrade):
    gifter_user_login: str | None


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
    sub: Sub | None
    resub: Resub | None
    sub_gift: SubGift | None
    community_sub_gift: CommunitySubGift | None
    gift_paid_upgrade: GiftPaidUpgrade | None
    prime_paid_upgrade: PrimePaidUpgrade | None
    pay_it_forward: PayItForward | None
    raid: Raid | None
    unraid: Unraid | None
    announcement: Announcement | None
    bits_badge_tier: BitsBadgeTier | None
    charity_donation: CharityDonation | None
    source_broadcaster_user_id: str | None
    source_broadcaster_user_name: str | None
    source_broadcaster_user_login: str | None
    source_message_id: str | None
    source_badges: list[Badge] | None
    shared_chat_sub: Sub | None
    shared_chat_resub: Resub | None
    shared_chat_sub_gift: SubGift | None
    shared_chat_community_sub_gift: CommunitySubGift | None
    shared_chat_gift_paid_upgrade: GiftPaidUpgrade | None
    shared_chat_prime_paid_upgrade: PrimePaidUpgrade | None
    shared_chat_pay_it_forward: PayItForward | None
    shared_chat_raid: Raid | None
    shared_chat_announcement: Announcement | None


class ChannelChatNotificationEventSub(BaseModel):
    subscription: ChannelChatNotificationSubscription
    event: ChannelChatNotificationEvent
