"""Repointing every EventSub subscription at this deployment's callback host.

Works off the live subscription list and never a fixed one. There are eight
subscription *types* but more than eight subscriptions: ``stream.online`` and
``stream.offline`` exist once per subscribed broadcaster, and nothing in this
repo knows how many that is -- Twitch is the only authority on it, so anything
working from a list written here would silently leave the rest behind.

Twitch's uniqueness key is the type and the condition alone, not the transport,
so a second subscription for one event at a new callback is a 409. Repointing is
therefore delete-then-create, which is destructive in a way this module is built
around: the dump goes out before anything is deleted, one failure does not
abandon the rest, and a dry run is what you get unless you ask otherwise.
"""

import json
import logging
from collections.abc import Mapping
from dataclasses import dataclass, field
from enum import StrEnum
from typing import Any

from errors import notify, notify_file
from models.twitch_api_responses.subscription import Subscription
from services.twitch.api import (
    callback_url,
    create_subscription,
    delete_subscription,
    get_subscriptions,
    get_users,
    subscription_target,
)
from services.twitch.helix import HelixError

logger = logging.getLogger(__name__)


class Action(StrEnum):
    """What this deployment should do with one existing subscription."""

    REPOINT = "repoint"
    KEEP = "already current"
    # Something a person created deliberately that this deployment has no route
    # for. Reported and left alone: deleting it would destroy it for good, and a
    # recreate this end cannot address would not bring it back.
    SKIP = "no route for this type"


def decide(subscription: Subscription, routes: Mapping[str, str]) -> Action:
    """What to do with this subscription. Pure, and the whole rule.

    KEEP is status-aware because a subscription Twitch disabled delivers nothing
    however right its callback looks, and leaving one behind is the failure this
    command exists to end. A disabled subscription on the correct callback is
    still repointed, which recreates it enabled.
    """
    path = routes.get(subscription.type)
    if path is None:
        return Action.SKIP
    if subscription.status == "enabled" and subscription.transport.callback == (
        callback_url(path)
    ):
        return Action.KEEP
    return Action.REPOINT


@dataclass
class Outcome:
    """What one migration pass found, and then did."""

    repoint: list[Subscription] = field(default_factory=list)
    keep: list[Subscription] = field(default_factory=list)
    skip: list[Subscription] = field(default_factory=list)
    migrated: list[Subscription] = field(default_factory=list)
    # Subscriptions that are now gone: the delete landed and the create did not.
    lost: list[tuple[Subscription, str]] = field(default_factory=list)
    dumped: bool = False
    # Carried so the caller names people rather than ids, without asking Helix
    # the same question a second time.
    logins: dict[str, str] = field(default_factory=dict)


_CONDITION_ID_FIELDS = (
    "broadcaster_user_id",
    "to_broadcaster_user_id",
    "from_broadcaster_user_id",
    "moderator_user_id",
    "user_id",
)


def condition_of(subscription: Subscription) -> dict[str, Any]:
    """The condition as Twitch sent it, including keys the model does not declare.

    ``exclude_none`` rather than a field list: the five declared keys default to
    None and only some are set per type, while extras carry no default at all.
    """
    return subscription.condition.model_dump(exclude_none=True)


def _condition_ids(subscriptions: list[Subscription]) -> list[str]:
    """Every distinct Twitch user id named by any of these conditions."""
    ids = {
        value
        for subscription in subscriptions
        for name in _CONDITION_ID_FIELDS
        if isinstance(value := getattr(subscription.condition, name, None), str)
    }
    return sorted(ids)


async def _logins(subscriptions: list[Subscription]) -> dict[str, str]:
    """Twitch id to login for everyone these subscriptions name.

    A dump without this is not enough to recover by hand: a subscription stores
    a user id, while ``/subscribe`` takes a login. A lookup that fails costs the
    names and not the dump, which is the part that cannot be reconstructed.
    """
    ids = _condition_ids(subscriptions)
    if not ids:
        return {}
    try:
        return {user.id: user.login for user in await get_users(ids)}
    except HelixError as e:
        await notify(
            f"Could not resolve {len(ids)} Twitch id(s) to logins for the"
            f" subscription dump, so it names ids only: {e}",
            key="migrate-dump-logins",
        )
        return {}


def render_dump(
    subscriptions: list[Subscription],
    routes: Mapping[str, str],
    logins: Mapping[str, str],
) -> str:
    """The complete definition of every subscription, as JSON somebody can act on.

    Everything needed to recreate one by hand is here -- type, version, the full
    condition, and the login behind each id -- because after the delete this is
    the only record that it existed.
    """
    return json.dumps(
        [
            {
                "id": subscription.id,
                "type": subscription.type,
                "version": subscription.version,
                "status": subscription.status,
                "condition": condition_of(subscription),
                "logins": {
                    value: logins[value]
                    for name in _CONDITION_ID_FIELDS
                    if isinstance(
                        value := getattr(subscription.condition, name, None), str
                    )
                    and value in logins
                },
                "callback_now": subscription.transport.callback,
                "callback_after": (
                    callback_url(path)
                    if (path := routes.get(subscription.type))
                    else None
                ),
                "action": decide(subscription, routes).value,
            }
            for subscription in subscriptions
        ],
        indent=2,
    )


def describe(subscription: Subscription, logins: Mapping[str, str]) -> str:
    """One subscription, named the way a person would look for it.

    Matched on the id subscription_target actually chose, and on the whole of it:
    a Twitch id is a run of digits, so one is a substring of another often enough
    that searching the rendered label would confidently name the wrong person.
    """
    target = subscription_target(subscription)
    login = next(
        (
            logins[value]
            for name in _CONDITION_ID_FIELDS
            if isinstance(value := getattr(subscription.condition, name, None), str)
            and target.endswith(f" {value}")
            and value in logins
        ),
        None,
    )
    return f"{subscription.type} ({target}{f' = {login}' if login else ''})"


# Discord refuses a message over 2000 characters. The summary is the one reply
# whose length follows how many subscriptions exist, so it is the one that can
# reach it; the dump it points at is a file and has no such limit.
_DISCORD_MESSAGE_LIMIT = 1990


def _listed(
    heading: str, subscriptions: list[Subscription], logins: Mapping[str, str]
) -> list[str]:
    return [heading, *(f"- {describe(s, logins)}" for s in subscriptions)]


def summary(outcome: Outcome, *, confirm: bool) -> str:
    """What the pass found, and did. Rendered here rather than by the caller, so
    the thing that knows what an Outcome means is what says it out loud."""
    if not outcome.repoint:
        left_alone = f", {len(outcome.skip)} left alone" if outcome.skip else ""
        return (
            f"Nothing to migrate: {len(outcome.keep)} subscription(s) already"
            f" call back on this deployment{left_alone}."
        )

    lines: list[str] = []
    if confirm:
        lines.append(f"Repointed {len(outcome.migrated)}/{len(outcome.repoint)}.")
        if outcome.lost:
            lines += _listed(
                "**Not back in place - act on these:**",
                [subscription for subscription, _ in outcome.lost],
                outcome.logins,
            )
        if not outcome.dumped:
            lines.append("**Abandoned:** the dump could not be delivered.")
    else:
        lines += _listed(
            f"**Dry run.** Would repoint {len(outcome.repoint)}:",
            outcome.repoint,
            outcome.logins,
        )
        lines += ["", "Run it again with `confirm: True` to do it."]

    if outcome.keep:
        lines.append(f"{len(outcome.keep)} already current.")
    if outcome.skip:
        lines += _listed(
            "Left alone, no route for the type:", outcome.skip, outcome.logins
        )
    if outcome.dumped:
        lines.append(
            "The full definition of every subscription is in the admin channel."
        )

    return _within_limit(lines)


def _within_limit(lines: list[str]) -> str:
    """The lines that fit, whole, and a pointer to the rest.

    Slicing the joined text instead cuts mid-line, and the line most likely to
    be cut is the last of the ones naming what is now missing -- which is the
    one worth reading. Dropping whole lines keeps every line that survives
    readable, and the dump has all of them anyway.
    """
    text = "\n".join(lines)
    if len(text) <= _DISCORD_MESSAGE_LIMIT:
        return text

    more = "... and more. Every subscription is in the admin channel dump."
    kept: list[str] = []
    budget = _DISCORD_MESSAGE_LIMIT - len(more) - 1
    for line in lines:
        if sum(len(k) + 1 for k in kept) + len(line) > budget:
            break
        kept.append(line)
    return "\n".join([*kept, more])


async def _repoint(
    subscription: Subscription, path: str, outcome: Outcome, logins: Mapping[str, str]
) -> None:
    """Move one subscription, or say precisely what is now missing.

    The delete has to come first -- see the module docstring -- so a create that
    fails afterwards leaves nothing behind. That is what ``lost`` records, and
    why it is reported per subscription rather than only in the summary.
    """
    identity = describe(subscription, logins)
    try:
        await delete_subscription(subscription.id)
    except HelixError as e:
        # Nothing was destroyed: the old subscription is still whatever it was.
        outcome.lost.append((subscription, f"not deleted, left as it was: {e}"))
        await notify(
            f"Could not delete {identity} while repointing it, so it still"
            f" calls back on {subscription.transport.callback}: {e}",
            key=f"migrate-delete:{subscription.id}",
        )
        return

    try:
        await create_subscription(
            subscription.type,
            subscription.version,
            condition_of(subscription),
            callback_url(path),
        )
    except HelixError as e:
        outcome.lost.append((subscription, f"deleted, not recreated: {e}"))
        await notify(
            f"{identity} is now GONE: it was deleted and could not be recreated"
            f" ({e}). Definition: version {subscription.version}, condition"
            f" {json.dumps(condition_of(subscription))}.",
            key=f"migrate-create:{subscription.id}",
        )
        return

    outcome.migrated.append(subscription)


async def migrate(routes: Mapping[str, str], *, confirm: bool) -> Outcome:
    """Repoint every subscription this deployment has a route for.

    A failed Helix listing raises, because a migration that cannot see the
    subscriptions has nothing to work from. Everything after that point is
    per-subscription: one that will not move must not abandon the ones behind it.
    """
    subscriptions = await get_subscriptions()
    outcome = Outcome()
    for subscription in subscriptions:
        match decide(subscription, routes):
            case Action.REPOINT:
                outcome.repoint.append(subscription)
            case Action.KEEP:
                outcome.keep.append(subscription)
            case Action.SKIP:
                outcome.skip.append(subscription)

    if not outcome.repoint:
        return outcome

    logins = await _logins(subscriptions)
    outcome.logins = dict(logins)

    # Every subscription rather than the ones about to move, and on a dry run as
    # well as a real one: the dry run is how somebody gets this file before
    # committing to the destruction, and after the delete it is the only record
    # that a lost subscription existed at all.
    outcome.dumped = await notify_file(
        f"{'Repointing' if confirm else 'Dry run for'} "
        f"{len(outcome.repoint)} EventSub subscription(s). This is every"
        f" subscription as it stands, which is the only record of the ones that"
        f" delete-then-create is about to put at risk.",
        "eventsub-subscriptions.json",
        render_dump(subscriptions, routes, logins),
    )

    if not confirm:
        return outcome

    if not outcome.dumped:
        # Refusing is the whole safety property: a delete whose record did not
        # land is one nobody can undo.
        await notify(
            "Migration abandoned before touching anything: the subscription dump"
            " could not be delivered, and deleting without it is unrecoverable."
        )
        return outcome

    for subscription in outcome.repoint:
        await _repoint(subscription, routes[subscription.type], outcome, logins)

    if outcome.lost:
        await notify(
            f"Migration finished with {len(outcome.lost)} subscription(s) not"
            f" back in place:\n"
            + "\n".join(
                f"- {describe(subscription, logins)}: {reason}"
                for subscription, reason in outcome.lost
            )
        )
    return outcome
