import pytest

from valmal.twitch.eventsub.migrate_plan import Action, condition_of, decide
from valmal.twitch.models.api.subscription import (
    Subscription,
    SubscriptionTransport,
)

ROUTES = {"stream.online": "/webhook/twitch/stream-online"}
CURRENT = "https://bot.example/webhook/twitch/stream-online"


def _sub(
    type_: str = "stream.online",
    callback: str = CURRENT,
    status: str = "enabled",
    **condition: object,
) -> Subscription:
    return Subscription(
        id="1",
        status=status,
        type=type_,
        version="1",
        condition=condition,
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


@pytest.mark.parametrize("value", [5, True, ["1"]])
def test_a_condition_value_that_is_not_a_string_is_left_alone(value: object) -> None:
    """Its recreate would likely be refused, after the delete."""
    assert decide(_sub(callback="https://old.example/x", user_id=value), ROUTES) is (
        Action.MALFORMED
    )


def test_a_null_condition_value_is_left_alone_too() -> None:
    """In a field the type requires, the recreate would go without it."""
    old = "https://old.example/x"
    null = _sub(callback=old, broadcaster_user_id=None)

    assert decide(null, ROUTES) is Action.MALFORMED


def test_a_malformed_condition_already_current_is_simply_kept() -> None:
    assert decide(_sub(broadcaster_user_id=None), ROUTES) is Action.KEEP


def test_force_recreates_one_already_current() -> None:
    assert decide(_sub(), ROUTES, force=True) is Action.REPOINT


@pytest.mark.parametrize(
    "status", ["notification_failures_exceeded", "webhook_callback_verification_failed"]
)
def test_force_recreates_one_that_failed_on_the_old_secret(status: str) -> None:
    assert decide(_sub(status=status), ROUTES, force=True) is Action.REPOINT


@pytest.mark.parametrize(
    "status", ["authorization_revoked", "user_removed", "moderator_removed"]
)
def test_force_keeps_one_whose_permission_is_gone(status: str) -> None:
    """Twitch would refuse the recreate, so forcing it would only delete it."""
    assert decide(_sub(status=status), ROUTES, force=True) is Action.KEEP


def test_force_still_leaves_unrouted_and_malformed_ones_alone() -> None:
    assert decide(_sub("channel.follow"), ROUTES, force=True) is Action.SKIP
    assert decide(_sub(user_id=None), ROUTES, force=True) is Action.MALFORMED


def test_status_does_not_decide() -> None:
    pending = _sub(status="webhook_callback_verification_pending")

    assert decide(pending, ROUTES) is Action.KEEP


def test_condition_drops_the_empty_half_of_a_raid() -> None:
    raid = _sub("channel.raid", from_broadcaster_user_id="1", to_broadcaster_user_id="")

    assert condition_of(raid) == {"from_broadcaster_user_id": "1"}


def test_condition_keeps_a_key_twitch_added() -> None:
    assert condition_of(_sub(broadcaster_user_id="1", reward_id="abc")) == {
        "broadcaster_user_id": "1",
        "reward_id": "abc",
    }
