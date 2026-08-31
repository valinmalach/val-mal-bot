"""Modelled ahead of its route: nothing subscribes to channel.chat.notification
yet and no endpoint serves it. Deliberate, not an oversight.
"""

from typing import Literal, Self

from pydantic import BaseModel, model_validator

from .common import Badge, Message, Subscription


class ChannelChatNotificationCondition(BaseModel):
    broadcaster_user_id: str
    user_id: str


class ChannelChatNotificationSubscription(Subscription):
    type: Literal["channel.chat.notification"]
    condition: ChannelChatNotificationCondition


class Sub(BaseModel):
    sub_tier: Literal["1000", "2000", "3000"]
    is_prime: bool | None = None
    duration_months: int


class Resub(Sub):
    cumulative_months: int
    streak_months: int
    is_gift: bool
    gifter_is_anonymous: bool | None = None
    gifter_user_id: str | None = None
    gifter_user_name: str | None = None
    gifter_user_login: str | None = None


class SubGift(Sub):
    cumulative_total: int | None = None
    recipient_user_id: str
    recipient_user_name: str
    recipient_user_login: str
    community_gift_id: str | None = None


class CommunitySubGift(BaseModel):
    id: str
    total: int
    sub_tier: Literal["1000", "2000", "3000"]
    cumulative_total: int | None = None


class GiftPaidUpgrade(BaseModel):
    gifter_is_anonymous: bool
    gifter_user_id: str | None = None
    gifter_user_name: str | None = None


class PrimePaidUpgrade(BaseModel):
    sub_tier: Literal["1000", "2000", "3000"]


class PayItForward(GiftPaidUpgrade):
    gifter_user_login: str | None = None


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


# Twitch documents unraid as "Returns an empty payload if notice_type is not
# unraid, otherwise returns null" — the one type whose field is empty exactly when
# it applies, inverting the rule the validator below asserts. The docs do not say
# which way is real, so nothing is asserted for it either way.
#
# The Literal below also carries 21 of the 25 types Twitch documents. watch_streak,
# modiversary and shared_chat_modiversary are unmodelled, and "unknown" names no
# field at all, so completing the list needs entries here as well as fields.
_NO_DETAIL = {"unraid"}


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
    sub: Sub | None = None
    resub: Resub | None = None
    sub_gift: SubGift | None = None
    community_sub_gift: CommunitySubGift | None = None
    gift_paid_upgrade: GiftPaidUpgrade | None = None
    prime_paid_upgrade: PrimePaidUpgrade | None = None
    pay_it_forward: PayItForward | None = None
    raid: Raid | None = None
    unraid: Unraid | None = None
    announcement: Announcement | None = None
    bits_badge_tier: BitsBadgeTier | None = None
    charity_donation: CharityDonation | None = None
    source_broadcaster_user_id: str | None = None
    source_broadcaster_user_name: str | None = None
    source_broadcaster_user_login: str | None = None
    source_message_id: str | None = None
    source_badges: list[Badge] | None = None
    shared_chat_sub: Sub | None = None
    shared_chat_resub: Resub | None = None
    shared_chat_sub_gift: SubGift | None = None
    shared_chat_community_sub_gift: CommunitySubGift | None = None
    shared_chat_gift_paid_upgrade: GiftPaidUpgrade | None = None
    shared_chat_prime_paid_upgrade: PrimePaidUpgrade | None = None
    shared_chat_pay_it_forward: PayItForward | None = None
    shared_chat_raid: Raid | None = None
    shared_chat_announcement: Announcement | None = None

    @model_validator(mode="after")
    def _detail_for_notice(self) -> Self:
        """Twitch fills the field its notice_type names, where one is named.

        Not every type names one, so this asserts nothing about those rather than
        assuming a rule that does not hold for all of them.
        """
        if self.notice_type in _NO_DETAIL:
            return self
        # Default, not a bare lookup: a notice type added to the Literal without
        # its field should fail as a validation error, not an AttributeError.
        if getattr(self, self.notice_type, None) is None:
            raise ValueError(f"notice_type {self.notice_type!r} carries no detail")
        return self


class ChannelChatNotificationEventSub(BaseModel):
    subscription: ChannelChatNotificationSubscription
    event: ChannelChatNotificationEvent
