from services.twitch.migrate_plan import Action, condition_of, decide
from valmal.twitch.models.api.subscription import (
    Subscription,
    SubscriptionCondition,
    SubscriptionTransport,
)

ROUTES = {"stream.online": "/webhook/twitch/stream-online"}
CURRENT = "https://bot.example/webhook/twitch/stream-online"


def _sub(
    type_: str = "stream.online",
    callback: str = CURRENT,
    status: str = "enabled",
    **condition: str | int,
) -> Subscription:
    return Subscription(
        id="1",
        status=status,
        type=type_,
        version="1",
        condition=SubscriptionCondition(**condition),  # pyright: ignore[reportArgumentType]
        created_at="2026-01-01T00:00:00Z",
        transport=SubscriptionTransport(method="webhook", callback=callback),
        cost=1,
    )


def test_matching_callback_is_kept() -> None:
    assert decide(_sub(), ROUTES) is Action.KEEP


def test_other_callback_is_repointed() -> None:
    old = "https://old.example/webhook/twitch/stream-online"

    assert decide(_sub(callback=old), ROUTES) is Action.REPOINT


def test_type_without_a_route_is_left_alone() -> None:
    assert decide(_sub("channel.follow"), ROUTES) is Action.SKIP


def test_status_does_not_decide() -> None:
    pending = _sub(status="webhook_callback_verification_pending")

    assert decide(pending, ROUTES) is Action.KEEP


def test_condition_drops_the_empty_half_of_a_raid() -> None:
    raid = _sub("channel.raid", from_broadcaster_user_id="1", to_broadcaster_user_id="")

    assert condition_of(raid) == {"from_broadcaster_user_id": "1"}


def test_condition_keeps_an_undeclared_falsy_value() -> None:
    assert condition_of(_sub(broadcaster_user_id="1", reward_id=0)) == {
        "broadcaster_user_id": "1",
        "reward_id": 0,
    }
