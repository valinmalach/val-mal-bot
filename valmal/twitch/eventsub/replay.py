"""What makes a signed delivery one the bot has not already handled: is it fresh, and
has its id already reached a handler."""

import time
from collections import OrderedDict

import pendulum

from valmal.core.errors import notify

# Twitch's own guidance for replay: a delivery whose timestamp is far from now
# is not one Twitch is sending, it is one somebody kept. Applied in both
# directions, since a clock ahead is as wrong as a clock behind.
MESSAGE_WINDOW_SECONDS = 600

# Ids of deliveries that reached a handler. Only those, so a delivery Twitch
# retries because this end failed still gets through, while a retry of one that
# worked does not run it twice. Process-local is enough and not a compromise:
# the Discord gateway connection lives in this process, so a second replica
# would double every bot action.
#
# Twice the freshness window, and that is provably enough rather than a guess:
# freshness is measured on the wall clock and this on the monotonic one, so a
# host clock behind Twitch's would leave a gap where a delivery is still fresh
# but no longer remembered - except that a clock more than one window out
# refuses everything as stale anyway, so the usable skew cannot exceed a window.
_HANDLED_TTL_SECONDS = MESSAGE_WINDOW_SECONDS * 2

# A safety valve, not the eviction policy; the TTL is. The chat route claims one
# id per chat line, so the old 1024 was reached at under two lines a second and
# then forgot ids inside their own window, which is precisely a redelivery being
# handled twice. Reaching even this says the traffic broke the assumption, so it
# is counted and said out loud rather than silently dropping the oldest.
_HANDLED_LIMIT = 20_000
_handled: OrderedDict[str, float] = OrderedDict()
_forgotten_early = 0


def is_stale(sent: pendulum.DateTime) -> bool:
    return abs((pendulum.now("UTC") - sent).total_seconds()) > MESSAGE_WINDOW_SECONDS


def claim(message_id: str) -> bool:
    """Take this delivery, or say that something already has it.

    Checking and taking are one step so that adding an await between them later
    cannot let two copies of one delivery both past a check that only looked.
    """
    global _forgotten_early
    cutoff = time.monotonic() - _HANDLED_TTL_SECONDS
    while _handled and next(iter(_handled.values())) < cutoff:
        _handled.popitem(last=False)
    if message_id in _handled:
        return False
    _handled[message_id] = time.monotonic()
    while len(_handled) > _HANDLED_LIMIT:
        _handled.popitem(last=False)
        _forgotten_early += 1
    return True


async def report_forgotten(endpoint: str) -> None:
    """Say so when the cap bit, because then a redelivery can be handled twice."""
    global _forgotten_early
    if not _forgotten_early:
        return
    dropped, _forgotten_early = _forgotten_early, 0
    await notify(
        f"Replay cache full on {endpoint}: {dropped} delivery id(s) forgotten"
        f" inside their window, so a redelivery of one could run twice.",
        key="replay-cache-full",
    )


def release(message_id: str) -> None:
    """Give a claim back, so a delivery this end failed can still be retried."""
    _handled.pop(message_id, None)
