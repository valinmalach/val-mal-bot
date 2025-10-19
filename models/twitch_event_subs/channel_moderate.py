from typing import Literal, Optional

from pydantic import BaseModel

from .common import Subscription


class ChannelModerateCondition(BaseModel):
    broadcaster_user_id: str
    user_id: str


class ChannelModerateSubscription(Subscription):
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
    reason: Optional[str]


class Timeout(User):
    reason: Optional[str]
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
    moderator_message: Optional[str]


class Warn(User):
    reason: Optional[str]
    chat_rules_cited: Optional[list[str]]


class ChannelModerateEvent(BaseModel):
    broadcaster_user_id: str
    broadcaster_user_login: str
    broadcaster_user_name: str
    source_broadcaster_user_id: str
    source_broadcaster_user_login: str
    source_broadcaster_user_name: str
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
    followers: Optional[Followers]
    slow: Optional[Slow]
    vip: Optional[User]
    unvip: Optional[User]
    mod: Optional[User]
    unmod: Optional[User]
    ban: Optional[Ban]
    unban: Optional[User]
    timeout: Optional[Timeout]
    untimeout: Optional[User]
    raid: Optional[Raid]
    unraid: Optional[User]
    delete: Optional[Delete]
    automod_terms: Optional[AutomodTerms]
    unban_request: Optional[UnbanRequest]
    warn: Optional[Warn]
    shared_chat_ban: Optional[Ban]
    shared_chat_unban: Optional[User]
    shared_chat_timeout: Optional[Timeout]
    shared_chat_untimeout: Optional[User]
    shared_chat_delete: Optional[Delete]


class ChannelModerateEventSub(BaseModel):
    subscription: ChannelModerateSubscription
    event: ChannelModerateEvent
