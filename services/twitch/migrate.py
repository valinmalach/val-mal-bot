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


def _condition_ids(subscriptions: list[Subscription]) -> list[str]:
    """Every distinct value these conditions carry in an id field.

    Not necessarily an id: what a condition holds is Twitch's to decide, and
    ``_usable`` is what decides whether it can be looked up.
    """
    ids = {
        value
        for subscription in subscriptions
        for name, _ in _CONDITION_ID_FIELDS
        if isinstance(value := getattr(subscription.condition, name, None), str)
    }
    return sorted(ids)


def _usable(value: str) -> bool:
    """Whether this could be a Twitch user id at all.

    A run of ASCII digits. ``isascii`` as well as ``isdigit`` because the latter
    is true of Arabic-Indic digits and of superscripts, neither of which Helix
    parses as a number.

    Necessary and not sufficient, deliberately: what Helix's own validator
    accepts is not published, so a value that is all digits and still refused
    would pass here. Bounding the length would be inventing a rule about
    somebody else's id format, and rejecting a real id is worse than sending one
    Twitch declines -- which is why the failure below names what it sent.
    """
    return value.isascii() and value.isdigit()


# How many values a notice names before it stops and says how many are left. The
# lists here are as long as the subscription count and the dump carries all of
# them, so the message only has to be enough to start looking.
_NAMED_LIMIT = 10


def _some(values: list[str]) -> str:
    """The first few values, saying how many were not named.

    The remainder is stated rather than implied: a count that does not match the
    list reads as a bug in the report rather than a cap on it.
    """
    shown = ", ".join(repr(value) for value in values[:_NAMED_LIMIT])
    rest = len(values) - _NAMED_LIMIT
    return f"{shown} and {rest} more" if rest > 0 else shown


async def _logins(subscriptions: list[Subscription]) -> dict[str, str]:
    """Twitch id to login for everyone these subscriptions name.

    A dump without this is not enough to recover by hand: a subscription stores
    a user id, while ``/subscribe`` takes a login. A lookup that fails costs the
    names and not the dump, which is the part that cannot be reconstructed.

    Anything that cannot be a user id is dropped before the call rather than
    sent. Helix answers one malformed identifier with a 400 for the whole
    request, and it ignores ids that merely do not exist, so a 400 from that
    endpoint is about the shape of something rather than a deleted account --
    and it cost every login in the batch, not just the bad one. Both notices
    name the values involved, because the filter cannot be complete: a value
    Helix refuses for a reason not visible from here has to leave behind enough
    for somebody to find it.

    Each key carries the values it is about. Keying on the notice alone held back
    a *different* set inside the fifteen-minute window, in the one path that
    exists to make these visible -- and the run that follows a dry run is well
    inside it.

    ``json.dumps`` rather than joining on a comma, because ``unusable`` is by
    construction whatever failed the digit test, so a value holding a comma is
    the shape most likely to be in it -- and joining made ``['a,b']`` and
    ``['a', 'b']`` the same key, which is the same suppression bug again by a
    narrower route.
    """
    ids = _condition_ids(subscriptions)
    usable = [value for value in ids if _usable(value)]
    if unusable := [value for value in ids if not _usable(value)]:
        await notify(
            f"{len(unusable)} subscription condition value(s) cannot be a Twitch"
            f" user id, so they were left out of the login lookup:"
            f" {_some(unusable)}."
            f" The dump still carries every condition in full.",
            key=f"migrate-unusable-ids:{json.dumps(unusable)}",
        )

    if not usable:
        return {}
    try:
        return {user.id: user.login for user in await get_users(usable)}
    except HelixError as e:
        await notify(
            f"Could not resolve {len(usable)} Twitch id(s) to logins for the"
            f" subscription dump, so it names ids only: {e}."
            f" The ids sent were: {_some(usable)}."
            f" A 400 here means one of them is a shape Helix will not take.",
            key=f"migrate-dump-logins:{json.dumps(usable)}",
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
                    for name, _ in _CONDITION_ID_FIELDS
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
        for name, label in _CONDITION_ID_FIELDS
        if isinstance(value := getattr(subscription.condition, name, None), str)
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


async def _exists_at(subscription: Subscription, callback: str) -> bool:
    """Whether Twitch already has this subscription on this callback.

    Asked only after a 409, and the reason it has to be asked is that
    ``create_subscription`` is ``repeatable``. A POST whose reply is lost is
    re-sent, and Twitch answers the retry with a 409 because the first attempt
    did create it -- so the 409 says the subscription exists at least as often as
    it says somebody else got there first. Reporting it as destroyed would send
    somebody to recreate, by hand, a subscription that is already in place, and
    for the six types provisioned outside this repo that attempt 409s too.

    A lookup that itself fails answers False: unproven is not the same as
    present, and over-reporting a loss costs an unnecessary check while
    under-reporting one costs a subscription nobody knows is missing.

    ``version`` is compared too, though Twitch documents its 409 as being about
    the type and condition alone. If that is the whole key the clause is
    redundant and costs nothing; if version is part of it, leaving the clause out
    would let a subscription of another version for the same type and condition
    be mistaken for the one just recreated. Redundant beats wrong, and a stricter
    match errs toward reporting a loss, which is the direction chosen above.
    """
    try:
        return any(
            other.type == subscription.type
            and other.version == subscription.version
            and other.condition == subscription.condition
            and other.transport.callback == callback
            for other in await get_subscriptions()
        )
    except HelixError:
        return False


async def _repoint(
    subscription: Subscription, path: str, outcome: Outcome, logins: Mapping[str, str]
) -> None:
    """Move one subscription, or say precisely what became of it.

    The delete has to come first -- see the module docstring -- so a create that
    fails afterwards leaves nothing behind. ``lost`` is that case and only that
    case; a delete that failed leaves the subscription untouched and goes to
    ``stuck``, because the two need opposite things done about them.
    """
    identity = describe(subscription, logins)
    callback = callback_url(path)
    try:
        await delete_subscription(subscription.id)
    except HelixError as e:
        # Nothing was destroyed: the old subscription is still whatever it was.
        outcome.stuck.append((subscription, f"delete failed, untouched: {e}"))
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
            callback,
        )
    except HelixError as e:
        if e.status == 409 and await _exists_at(subscription, callback):
            outcome.migrated.append(subscription)
            logger.info(
                "409 on recreating %s, and it is present on %s: a retried POST"
                " whose first attempt had already landed",
                subscription.type,
                callback,
            )
            return
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

    A second confirmed run refuses while one is in flight. Two of them would
    interleave deletes and creates over the same subscriptions, where the loser
    of a delete race is reported untouched when it was destroyed, and the loser
    of a create race gets a 409 that no longer means what it usually does.
    Refusing rather than queueing, because a run that waits and then finds
    nothing left to do looks exactly like one that was not needed.

    A dry run is not guarded: it changes nothing, so two of them cost two
    listings.
    """
    global _confirming
    if confirm and _confirming:
        return Outcome(busy=True)
    if confirm:
        # Taken here, with no await between the check above and this line, for
        # the reason controller/twitch.py _claim is one step: an await in the
        # gap would let two runs both pass a check that only looked.
        _confirming = True
    try:
        return await _migrate(routes, confirm=confirm)
    finally:
        if confirm:
            _confirming = False


# Whether a confirmed migration is mid-flight. A plain flag rather than a lock:
# what is wanted is refusal, and asyncio.Lock queues.
_confirming = False


async def _migrate(routes: Mapping[str, str], *, confirm: bool) -> Outcome:
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
            f"Migration destroyed {len(outcome.lost)} subscription(s) it could"
            f" not recreate. Recreate these from the dump:\n"
            + "\n".join(
                f"- {describe(subscription, logins)}: {reason}"
                for subscription, reason in outcome.lost
            )
        )
    if outcome.stuck:
        # Separately, and not as an emergency: these are still delivering on the
        # old callback, so the remedy is running the command again.
        await notify(
            f"Migration left {len(outcome.stuck)} subscription(s) on the old"
            f" callback, untouched. Re-running moves them:\n"
            + "\n".join(
                f"- {describe(subscription, logins)}: {reason}"
                for subscription, reason in outcome.stuck
            )
        )
    return outcome
