import msgspec

from valmal.twitch.models.api.pagination import Pagination


class SubscriptionTransport(msgspec.Struct):
    method: str | None = None
    callback: str | None = None
    session_id: str | None = None
    connected_at: str | None = None
    disconnected_at: str | None = None


class Subscription(msgspec.Struct):
    id: str
    status: str
    type: str
    version: str
    # A plain dict, because a condition is round-tripped to recreate a
    # subscription at a new callback, and a key Twitch adds -- a reward_id on a
    # redemption, say -- must come through or the recreate is broader than the
    # one it replaced. Values are object, not str: one null or number would
    # otherwise fail the whole listing, and the migration's dump with it. Read a
    # key with .get: which ones are present depends on the type.
    #
    # What this keeps and what the recreate sends are not the same set:
    # migrate_plan.condition_of drops any value that is "", because Twitch
    # returns "" for the unset half of a channel.raid condition and refuses a
    # create that carries both halves. Making that round-trip faithful again
    # would delete both raid subscriptions and fail to recreate either.
    condition: dict[str, object]
    created_at: str
    transport: SubscriptionTransport
    cost: int


class SubscriptionResponse(msgspec.Struct):
    data: list[Subscription]
    total: int
    total_cost: int
    max_total_cost: int
    pagination: Pagination
