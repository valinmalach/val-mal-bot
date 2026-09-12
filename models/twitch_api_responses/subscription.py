from pydantic import BaseModel, ConfigDict

from .pagination import Pagination


class SubscriptionCondition(BaseModel):
    # Undeclared keys are kept rather than dropped, because a condition is
    # round-tripped to recreate a subscription at a new callback. The five below
    # cover the eight subscriptions in use, so declaring them is what lets the
    # rest of the code read one by name; anything Twitch adds -- a reward_id on a
    # redemption, say -- would otherwise vanish silently on the way through and
    # recreate a subscription broader than the one it replaced.
    #
    # What this model keeps and what the recreate sends are not the same set:
    # migrate.condition_of drops any value that is "", because Twitch returns ""
    # for the unset half of a channel.raid condition and refuses a create that
    # carries both halves. Making that round-trip faithful again would delete
    # both raid subscriptions and fail to recreate either.
    model_config = ConfigDict(extra="allow")

    broadcaster_user_id: str | None = None
    # A raid is keyed on the two ends rather than one broadcaster, and a
    # moderate subscription carries the moderator as well.
    to_broadcaster_user_id: str | None = None
    from_broadcaster_user_id: str | None = None
    moderator_user_id: str | None = None
    user_id: str | None = None


class SubscriptionTransport(BaseModel):
    method: str | None = None
    callback: str | None = None
    session_id: str | None = None
    connected_at: str | None = None
    disconnected_at: str | None = None


class Subscription(BaseModel):
    id: str
    status: str
    type: str
    version: str
    condition: SubscriptionCondition
    created_at: str
    transport: SubscriptionTransport
    cost: int


class SubscriptionResponse(BaseModel):
    data: list[Subscription]
    total: int
    total_cost: int
    max_total_cost: int
    pagination: Pagination
