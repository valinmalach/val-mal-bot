from pydantic import BaseModel


class SubscriptionCondition(BaseModel):
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


class Pagination(BaseModel):
    cursor: str | None = None


class SubscriptionResponse(BaseModel):
    data: list[Subscription]
    total: int
    total_cost: int
    max_total_cost: int
    pagination: Pagination
