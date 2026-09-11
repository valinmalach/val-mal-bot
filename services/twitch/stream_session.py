"""The main broadcaster being live: at most one at a time.

Owns every piece of state that lasts exactly one stream — what the shoutout
queue is holding and the ad-break warning — and is the single answer to whether
the bot is live. A live alert is deliberately not here: one exists per
broadcaster and is kept in Postgres, because it names a Discord message that has
to outlive a redeploy.

Only Helix confirming the broadcaster gone ends a session. Beginning one is a
different transition, and resets rather than tears down; see
``docs/adr/0004-the-stream-session-ends-itself.md``.
"""

import asyncio
import logging

import pendulum

from background import fire_and_forget
from errors import notify, report
from models.twitch_api_responses.stream import Stream
from services.config import config
from services.twitch.api import get_ad_schedule, get_stream
from services.twitch.chat import say_template
from services.twitch.helix import HelixError
from services.twitch.shoutout_queue import shoutout_queue

logger = logging.getLogger(__name__)

# The stream this session is for, or None when nobody is live. It holds the
# Stream rather than a flag because an offline check that took a moment has to
# tell "the stream I asked about is gone" from "a different one began while I
# was asking".
_stream: Stream | None = None

_ad_break_task: asyncio.Task | None = None

# Twitch user ids this session has already resolved: each is either **spent**,
# meaning they had their autoshoutout, or was looked up and found not to be on
# the **autoshoutout list**. One set rather than two because nothing needs the
# halves apart - what both mean here is "do not ask about this chatter again
# until the next stream", which is what keeps the list out of the hot path.
_settled: set[int] = set()


def is_main_broadcaster(broadcaster_id: str | int) -> bool:
    return str(broadcaster_id) == config.setting("twitch_broadcaster_id")


def is_live() -> bool:
    """Whether the main broadcaster is streaming, and the only answer to that."""
    return _stream is not None


def _start(stream: Stream) -> None:
    """Take up a stream, discarding whatever a previous session left behind.

    A reset, not a teardown. It is also the recovery for a session nothing was
    ever in a position to end: a stream.offline that never arrived leaves one
    standing, and this is what stops that outliving the gap between two streams.

    Taking up the stream already held does nothing, because resetting is only
    right for a stream that is over. `resume()` runs on every gateway
    reconnect, not just at startup, so without this a reconnect halfway
    through a stream would empty the queue of shoutouts already promised in
    chat and cancel the pending ad-break warning. `live_alert._start` guards
    the same reconnect for the same reason.
    """
    global _stream

    if _stream is not None and _stream.id == stream.id:
        return

    _stream = stream
    shoutout_queue.clear()
    cancel_ad_break_warning()
    _settled.clear()


def _end() -> None:
    """Stand the session down. Only a confirmed-gone stream reaches here."""
    global _stream

    _stream = None
    shoutout_queue.clear()
    cancel_ad_break_warning()
    _settled.clear()


def is_settled(twitch_user_id: int) -> bool:
    """Whether this session has already resolved this Twitch user."""
    return twitch_user_id in _settled


def settle(twitch_user_id: int) -> None:
    """Record that this session need not ask about this Twitch user again.

    Called for a list member who has had their autoshoutout and for a chatter
    found not to be on the list, because the session does nothing further for
    either. Ignored when nobody is live: there is no session to remember it.
    """
    if is_live():
        _settled.add(twitch_user_id)


async def began(broadcaster_id: int, stream: Stream) -> None:
    """Greet chat and bring the session up. Does not raise.

    Each line stands alone, and neither can fail the caller: the live alert is
    posted after this, and a greeting Twitch refused is not worth losing it over.
    """
    if not is_main_broadcaster(broadcaster_id):
        return

    _start(stream)
    await say_template(broadcaster_id, "twitch_stream_greeting")
    await say_template(
        broadcaster_id,
        "twitch_stream_announce",
        name=stream.user_name,
        game=stream.game_name,
        title=stream.title,
    )


async def resume() -> None:
    """Bring the session up for a stream that was already running. Does not raise.

    Not began(): nothing is greeted, because the stream did not just start. That
    difference is the whole reason this is not a second call to began().
    """
    try:
        # These three are exhaustive, which is what lets the docstring promise
        # not to raise: config.setting is a dict lookup with a default, int()
        # fails only as TypeError or ValueError, and helix raises HelixError
        # and nothing else by its own stated contract. Named rather than a
        # bare except so a broadcaster id that is missing or not a number
        # stays distinguishable from Twitch being unreachable, rather than
        # reaching the startup reporter as one undifferentiated failure.
        stream = await get_stream(int(config.setting("twitch_broadcaster_id")))
    except (HelixError, TypeError, ValueError) as e:
        await report(e, "Could not check whether the broadcaster is live at startup")
        return

    if stream and stream.type == "live":
        _start(stream)


async def wake(broadcaster_id: str | int) -> None:
    """Re-check a session that a stream.offline says may be over. Does not raise.

    The webhook is a prompt, not the answer. Twitch sends one for a connection
    that dropped as readily as for a stream that is finished, and standing the
    session down on the webhook alone would dump the queue for a broadcaster who
    is back thirty seconds later. Helix decides.
    """
    if not is_main_broadcaster(broadcaster_id):
        return

    asked_about = _stream
    if asked_about is None:
        return

    try:
        stream = await get_stream(int(broadcaster_id))
    except HelixError as e:
        # helix.request has already retried a GET, so reaching here means Twitch
        # could not be reached at all rather than that it was slow. The session
        # is left standing: the next stream starting resets it, which is the
        # same bound ADR 0004 accepts for an undeliverable subscription.
        await notify(
            f"Could not check whether broadcaster {broadcaster_id} is still live,"
            f" so the stream session is left up: {e}",
            key=f"session-wake:{broadcaster_id}",
        )
        return

    if stream is not None and stream.type == "live":
        return

    if _stream is None or _stream.id != asked_about.id:
        # Superseded. A stream that came straight back began while this check
        # was in flight, and the answer above is about the one before it.
        logger.info(
            "Offline check for stream %s is stale; session now holds %s",
            asked_about.id,
            _stream.id if _stream else None,
        )
        return

    _end()


def cancel_ad_break_warning() -> None:
    global _ad_break_task

    if _ad_break_task and not _ad_break_task.done():
        _ad_break_task.cancel()
    _ad_break_task = None


def schedule_ad_break_warning(broadcaster_id: str) -> None:
    """Replace the pending warning with one for the next ad break."""
    global _ad_break_task

    # A session is one broadcaster, so there is one warning. Guarded rather than
    # keyed: an ad break on any other channel would otherwise stand down the
    # main broadcaster's warning by taking its place.
    if not is_main_broadcaster(broadcaster_id):
        return

    cancel_ad_break_warning()
    _ad_break_task = fire_and_forget(
        _warn_before_next_ad(broadcaster_id), name=f"ad-break-warning-{broadcaster_id}"
    )


async def _warn_before_next_ad(broadcaster_id: str) -> None:
    try:
        ad_schedule = await get_ad_schedule(int(broadcaster_id))
        if not ad_schedule:
            return

        notify_time = pendulum.from_timestamp(ad_schedule.next_ad_at).subtract(
            minutes=5
        )
        wait_seconds = (notify_time - pendulum.now(tz=pendulum.UTC)).total_seconds()
        if wait_seconds > 0:
            await asyncio.sleep(wait_seconds)
            if not is_live():
                # The sleep is most of an ad cycle, and cancellation only
                # reaches a session that ended in a way something noticed. A
                # warning about ads nobody is watching is worse than silence.
                return
            await say_template(broadcaster_id, "twitch_ad_break_warning")
    except asyncio.CancelledError:
        logger.info(
            "Cancelled ad break notification task for broadcaster_id=%s", broadcaster_id
        )
        raise
    except Exception as e:  # noqa: BLE001
        await report(e, "Error scheduling next ad break notification")
