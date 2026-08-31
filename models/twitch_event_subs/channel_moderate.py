from typing import Literal

from pydantic import BaseModel

from .common import Subscription


class ChannelModerateCondition(BaseModel):
    broadcaster_user_id: str
    moderator_user_id: str


class ChannelModerateSubscription(Subscription):
    type: Literal["channel.moderate"]
    condition: ChannelModerateCondition


class Followers(BaseModel):
    follow_duration_minutes: int


class Slow(BaseModel):
    wait_time_seconds: int


class User(BaseModel):
    user_id: str
    user_login: str
    user_name: str


class Ban(User):
    reason: str | None = None


class Timeout(User):
    reason: str | None = None
    expires_at: str


class Raid(User):
    viewer_count: int


class Delete(User):
    message_id: str
    message_body: str


class AutomodTerms(BaseModel):
    action: Literal["add", "remove"]
    list: Literal["blocked", "permitted"]
    terms: list[str]
    from_automod: bool


class UnbanRequest(User):
    is_approved: bool
    moderator_message: str | None = None


class Warn(User):
    reason: str | None = None
    chat_rules_cited: list[str] | None = None


class ChannelModerateEvent(BaseModel):
    broadcaster_user_id: str
    broadcaster_user_login: str
    broadcaster_user_name: str
    source_broadcaster_user_id: str | None = None
    source_broadcaster_user_login: str | None = None
    source_broadcaster_user_name: str | None = None
    moderator_user_id: str
    moderator_user_login: str
    moderator_user_name: str
    action: Literal[
        "ban",
        "timeout",
        "unban",
        "untimeout",
        "clear",
        "emoteonly",
        "emoteonlyoff",
        "followers",
        "followersoff",
        "uniquechat",
        "uniquechatoff",
        "slow",
        "slowoff",
        "subscribers",
        "subscribersoff",
        "unraid",
        "delete",
        "unvip",
        "vip",
        "raid",
        "add_blocked_term",
        "add_permitted_term",
        "remove_blocked_term",
        "remove_permitted_term",
        "mod",
        "unmod",
        "approve_unban_request",
        "deny_unban_request",
        "warn",
        "shared_chat_ban",
        "shared_chat_timeout",
        "shared_chat_unban",
        "shared_chat_untimeout",
        "shared_chat_delete",
    ]
    followers: Followers | None = None
    slow: Slow | None = None
    vip: User | None = None
    unvip: User | None = None
    mod: User | None = None
    unmod: User | None = None
    ban: Ban | None = None
    unban: User | None = None
    timeout: Timeout | None = None
    untimeout: User | None = None
    raid: Raid | None = None
    unraid: User | None = None
    delete: Delete | None = None
    automod_terms: AutomodTerms | None = None
    unban_request: UnbanRequest | None = None
    warn: Warn | None = None
    shared_chat_ban: Ban | None = None
    shared_chat_unban: User | None = None
    shared_chat_timeout: Timeout | None = None
    shared_chat_untimeout: User | None = None
    shared_chat_delete: Delete | None = None


class ChannelModerateEventSub(BaseModel):
    subscription: ChannelModerateSubscription
    event: ChannelModerateEvent
