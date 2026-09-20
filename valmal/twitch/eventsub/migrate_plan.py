"""The pure rule for one EventSub subscription, and the rendering of it.

``decide`` reads a subscription's callback against this deployment's routes
and answers repoint/keep/skip; ``Outcome`` is what one migration pass
collects; and ``describe``/``render_dump``/``summary`` say either back out
loud. Nothing here touches Helix or Discord -- ``services/twitch/migrate.py``
is the only thing that acts on the answer, and imports this module, never the
other way around.
"""

import json
from collections.abc import Mapping
from dataclasses import dataclass, field
from enum import StrEnum
from typing import Any

from valmal.twitch.client.api import callback_url
from valmal.twitch.models.api.subscription import Subscription


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

    The callback decides, and the status deliberately does not. This used to
    repoint anything not ``enabled`` even when its callback was already right,
    on the reasoning that a disabled subscription delivers nothing whatever its
    callback says. True, and not this command's problem: that reasoning bought a
    delete-then-create on a subscription already pointing where it should.

    Which is worse than doing nothing in every case it reaches.
    ``webhook_callback_verification_pending`` is a few-second transient on the
    way to ``enabled``, so catching one mid-verification destroys something that
    was about to arrive by itself. ``authorization_revoked``, ``user_removed``
    and ``moderator_removed`` are not things recreating repairs, so the delete
    turns a subscription that is at least visible in ``get_subscriptions()`` into
    one that is gone -- and six of the eight types cannot be recreated here.

    A subscription that is on the right callback and still not delivering is
    already ``undeliverable``, which ``recheck_subscriptions`` reports hourly.
    That check owns the problem; this one owns the callback.
    """
    path = routes.get(subscription.type)
    if path is None:
        return Action.SKIP
    if subscription.transport.callback == callback_url(path):
        return Action.KEEP
    return Action.REPOINT


@dataclass
class Outcome:
    """What one migration pass found, and then did."""

    repoint: list[Subscription] = field(default_factory=list)
    keep: list[Subscription] = field(default_factory=list)
    skip: list[Subscription] = field(default_factory=list)
    migrated: list[Subscription] = field(default_factory=list)
    # Two failure modes, kept apart because they call for opposite responses and
    # only one of them is an emergency. `stuck` did not move and is still
    # whatever it was, on the old callback -- a retry is the whole remedy.
    # `lost` is gone: the delete landed and the create did not, so for six of
    # the eight types the dump is the only way back. Reporting them as one list
    # made a working subscription and a destroyed one read identically.
    stuck: list[tuple[Subscription, str]] = field(default_factory=list)
    lost: list[tuple[Subscription, str]] = field(default_factory=list)
    dumped: bool = False
    # True when a confirmed run was already in flight, so this one did nothing.
    busy: bool = False
    # Carried so the caller names people rather than ids, without asking Helix
    # the same question a second time.
    logins: dict[str, str] = field(default_factory=dict)


# Every condition field that carries a Twitch user id, with how to say it. The
# labels match subscription_target's wording, which names only one of them.
_CONDITION_ID_FIELDS = (
    ("broadcaster_user_id", "broadcaster"),
    ("to_broadcaster_user_id", "to broadcaster"),
    ("from_broadcaster_user_id", "from broadcaster"),
    ("moderator_user_id", "moderator"),
    ("user_id", "user"),
)


def _named_ids(subscription: Subscription) -> list[tuple[str, str]]:
    """Every (label, value) an id field in this condition actually names.

    The one place that reads ``_CONDITION_ID_FIELDS`` off a live condition, so
    that filtering what counts as "named" only has to be right once. A field
    Twitch left as ``""`` -- its way of saying "not set" on the unused half of a
    ``channel.raid`` condition -- is skipped here for the same reason
    ``condition_of`` drops it from the recreate: a value that names nobody is
    not a value to look up, print, or resolve to a login.

    Before this existed, three call sites read the raw attribute themselves and
    only one of them remembered to check for ``""``. The other two put an empty
    login lookup through Helix every run and printed "to broadcaster " with
    nothing after it -- correct by the field's own logic, since ``""`` really is
    what Twitch sent, and wrong by every reader's, since it never named anyone.
    """
    return [
        (label, value)
        for name, label in _CONDITION_ID_FIELDS
        if isinstance(value := getattr(subscription.condition, name, None), str)
        and value != ""
    ]


def condition_of(subscription: Subscription) -> dict[str, Any]:
    """The condition as Twitch sent it, minus the keys it sent empty.

    ``exclude_none`` rather than a field list: the five declared keys default to
    None and only some are set per type, while extras carry no default at all.

    The empty strings go for a harder reason than tidiness. Twitch answers with
    ``""`` for the half of a ``channel.raid`` condition that is not set, and of
    that pair its create endpoint says: "Set either the from_broadcaster_user_id
    or to_broadcaster_user_id condition parameter but not both. If you pass both
    parameters, the subscription request fails." Round-tripping the ``""`` back
    therefore fails every raid recreate -- *after* its delete, which is the one
    unrecoverable outcome this module has, on one of the six types nothing here
    can rebuild.

    Dropping it is not lossy: ``""`` is how Twitch says "not set" on the way out
    and absence is how it requires the same thing on the way in. Compared against
    ``""`` rather than tested for falsity, because ``0`` and ``False`` in an
    undeclared key are values Twitch chose to send and are not this to decide.
    """
    return {
        key: value
        for key, value in subscription.condition.model_dump(exclude_none=True).items()
        if value != ""
    }


def condition_ids(subscriptions: list[Subscription]) -> list[str]:
    """Every distinct value these conditions name in an id field.

    Not necessarily an id: what a condition holds is Twitch's to decide, and
    ``_usable`` (in ``services.twitch.migrate``, the only caller) is what
    decides whether it can be looked up. ``""`` is already excluded by
    ``_named_ids`` -- it is not a value anybody sent to be resolved, it is
    Twitch declining to fill in the other half of a raid condition, and a
    lookup for it produced a false "cannot be a Twitch user id" notice on every
    run that had one.
    """
    ids = {
        value for subscription in subscriptions for _, value in _named_ids(subscription)
    }
    return sorted(ids)


def render_dump(
    subscriptions: list[Subscription],
    routes: Mapping[str, str],
    logins: Mapping[str, str],
) -> str:
    """Every subscription as JSON somebody can act on, not as Twitch phrased it.

    Everything needed to recreate one by hand is here -- type, version,
    condition, and the login behind each id -- because after the delete this is
    the only record that it existed.

    The condition is ``condition_of``'s, so a value Twitch sent as ``""`` is
    absent here. That is the difference between a transcript and something
    somebody can act on: pasting the raid condition back as Twitch phrased it is
    refused, and this file exists to be pasted back. ``callback_now`` and
    ``callback_after`` are likewise the migration's own reading rather than
    fields Twitch returns.
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
                    for _, value in _named_ids(subscription)
                    if value in logins
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

    Every id the condition carries, not only the first. Twitch's uniqueness key
    is the type and the condition, so two subscriptions of one type against one
    broadcaster are legitimate as long as another id differs -- two
    channel.moderate for different moderators, or two channel.chat.message for
    different reading users. Naming the broadcaster alone rendered those
    identically, and a failure list is exactly where telling them apart matters.

    Hence not ``subscription_target``, which answers with one id by design and is
    right for the undeliverable summaries it serves. Building on it here meant
    matching its rendered text to find out which id it had picked, which is a
    thing to get wrong for no gain.
    """
    named = [
        f"{label} {value}" + (f" = {logins[value]}" if value in logins else "")
        for label, value in _named_ids(subscription)
    ]
    # A condition naming nobody is not one of the eight, but it still has to be
    # reportable: the id is all Twitch gives that is certain to identify it.
    inside = ", ".join(named) or f"id {subscription.id}"
    return f"{subscription.type} ({inside})"


# Discord refuses a message over 2000 characters. The summary is the one reply
# whose length follows how many subscriptions exist, so it is the one that can
# reach it; the dump it points at is a file and has no such limit.
_DISCORD_MESSAGE_LIMIT = 1990


def _listed(
    heading: str, subscriptions: list[Subscription], logins: Mapping[str, str]
) -> list[str]:
    return [heading, *(f"- {describe(s, logins)}" for s in subscriptions)]


def _with_reasons(
    heading: str,
    failures: list[tuple[Subscription, str]],
    logins: Mapping[str, str],
) -> list[str]:
    """A failure list that keeps its reasons.

    The reason is the difference between a subscription somebody has to go and
    recreate by hand and one they can leave alone, and dropping it made those
    two read identically in the one message the operator sees first.
    """
    return [
        heading,
        *(f"- {describe(s, logins)}: {reason}" for s, reason in failures),
    ]


def summary(outcome: Outcome, *, confirm: bool) -> str:
    """What the pass found, and did. Rendered here rather than by the caller, so
    the thing that knows what an Outcome means is what says it out loud."""
    if outcome.busy:
        return (
            "A confirmed migration is already running. This one did nothing --"
            " two at once would race each other's deletes."
        )

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
            lines += _with_reasons(
                "**GONE - recreate these from the dump:**",
                outcome.lost,
                outcome.logins,
            )
        if outcome.stuck:
            lines += _with_reasons(
                "Not moved, still on the old callback - safe to retry:",
                outcome.stuck,
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
    used = 0
    for line in lines:
        if used + len(line) > budget:
            break
        kept.append(line)
        used += len(line) + 1
    return "\n".join([*kept, more])
