"""Which of the subscriptions Twitch holds for this deployment can still deliver."""

from valmal.twitch.client.api import callback_prefix, get_subscriptions
from valmal.twitch.models.api.subscription import Subscription


async def broken_subscriptions() -> dict[str, str]:
    """Every subscription that will not reach the bot, by identity and reason.

    Computes, says nothing: the startup check reports whatever it finds, while
    the loop reports only what changed, and neither wants the other's rule.
    """
    return {
        f"{subscription.type} ({subscription_target(subscription)})": reason
        for subscription in await get_subscriptions()
        if (reason := undeliverable(subscription)) is not None
    }


def subscription_target(subscription: Subscription) -> str:
    """Whichever id identifies this subscription, since the field varies by type."""
    condition = subscription.condition
    return next(
        (
            f"{label} {value}"
            for label, value in (
                ("broadcaster", condition.broadcaster_user_id),
                ("to broadcaster", condition.to_broadcaster_user_id),
                ("from broadcaster", condition.from_broadcaster_user_id),
                ("user", condition.user_id),
            )
            if value
        ),
        f"id {subscription.id}",
    )


def undeliverable(subscription: Subscription) -> str | None:
    """Why this subscription will not reach the bot, or None when it will.

    Two things are checkable without knowing which event types the bot expects,
    and only two: Twitch having disabled it, and a callback pointing elsewhere.
    Matching whole URLs instead would call every event type the bot does not
    create itself — chat, follows, raids — undeliverable on every start.

    The prefix has to end at a path boundary. A bare ``startswith`` also accepts
    ``/webhook/twitching`` and ``/webhook/twitch-old``, which are different paths
    that FastAPI does not route. What is left uncovered is an unrouted segment
    *under* the prefix, and that fails its deliveries until Twitch disables it,
    which the status check above then catches.
    """
    if subscription.status != "enabled":
        return subscription.status

    callback = subscription.transport.callback or ""
    prefix = callback_prefix()
    if callback != prefix and not callback.startswith(f"{prefix}/"):
        return f"calling back on {callback or 'nothing'}"
    return None
