"""Everything the bot says about itself, in one place.

Neither function raises. Both run inside somebody's ``except`` block, where an
escaping exception would replace the one being reported, and a lost report is
worse than an ugly one.

Every distinct failure reaches the admin channel at least once. Repeats of one
already delivered are held back for ``_WINDOW_SECONDS`` and counted, and the
next one through says how many it stood in for, so a sustained outage costs a
few messages rather than one a minute. Suppression follows a delivery and only
a delivery: a notice that did not land opens no window, so the first report of
anything is never the one that goes missing. The windows live in memory, so a
restart says everything again.
"""

import asyncio
import io
import logging
import time
import traceback
from dataclasses import dataclass

import discord

logger = logging.getLogger(__name__)

_ADMIN_CHANNEL = "bot_admin"

# Discord rejects a message over 2000 characters, and a rejected report is a
# lost one. The traceback goes as a file, so only the summary is at risk.
_MAX_CONTENT = 1900

# How long one delivered message stands in for its own repeats.
_WINDOW_SECONDS = 15 * 60

# Only reached if a key turns out to vary per event, which is the bug this
# would otherwise hide as unbounded memory.
_MAX_TRACKED = 512


@dataclass
class _Window:
    until: float
    suppressed: int


_windows: dict[str, _Window] = {}


def _prune(now: float) -> None:
    for key in [k for k, w in _windows.items() if now - w.until > _WINDOW_SECONDS]:
        del _windows[key]
    # Down to one below the cap, because the caller is about to add one.
    if len(_windows) >= _MAX_TRACKED:
        # Closest to expiry first: they are the ones with the least left to say.
        for key in sorted(_windows, key=lambda k: _windows[k].until)[
            : len(_windows) - _MAX_TRACKED + 1
        ]:
            del _windows[key]


async def _send_once(key: str, text: str, trace: str | None) -> bool:
    """Deliver unless an identical message already did, inside the window."""
    now = time.monotonic()
    _prune(now)

    window = _windows.get(key)
    if window is not None and now < window.until:
        window.suppressed += 1
        logger.info("Held back (%d since the last report): %s", window.suppressed, text)
        return True

    # Claimed before the send, and nothing between here and it yields: two tasks
    # racing on one key must not both deliver.
    held = window.suppressed if window is not None else 0
    window = _Window(until=now + _WINDOW_SECONDS, suppressed=0)
    _windows[key] = window

    if held:
        # Leading, because _deliver truncates the tail. "went unreported" rather
        # than "since the last report": these are the occurrences nobody saw a
        # message for, which is true both of ones held back behind a delivery
        # and of ones whose own delivery failed.
        text = f"[{held} more went unreported] {text}"

    sent = False
    try:
        sent = await _deliver(text, trace)
    finally:
        # In a finally because a _deliver that raises has delivered nothing
        # either, and leaving its window standing would suppress the retry —
        # the one way this could swallow a failure whole.
        if not sent and _windows.get(key) is window:
            window.until = now
            window.suppressed += held + 1
    return sent


async def report(exc: Exception, context: str, *, key: str | None = None) -> None:
    """Log an exception, then deliver it and its traceback to the admin channel."""
    try:
        summary = (
            f"{context} - Type: {type(exc).__name__}, Message: {exc}, Args: {exc.args}"
        )
        # format_exception, not format_exc: an exception collected from
        # asyncio.gather(return_exceptions=True) is not the one being handled,
        # and format_exc would describe nothing.
        trace = "".join(traceback.format_exception(exc))
        logger.error("%s\nTraceback:\n%s", summary, trace)
        # Context names the thing that failed; the type keeps two different
        # failures reported from one place from standing in for each other.
        await _send_once(key or f"{context}\x00{type(exc).__name__}", summary, trace)
    except Exception:
        # Describing an exception can itself fail: a __str__ that raises, or a
        # services import that never completed.
        logger.exception("Reporting failed for: %s", context)


async def notify(text: str, *, key: str | None = None) -> bool:
    """Deliver a notice: something the admin channel should see that is not an exception.

    ``key`` is what two occurrences must share to count as the same notice.
    Give one wherever the text carries a detail that varies between repeats of
    the same problem, or every repeat is a new notice and nothing is held back.

    Returns whether the admin channel has the news — true when this call
    delivered it, and true when a message inside the window already did.
    """
    try:
        # Callers that have a severity log it themselves; this is the record
        # that a notice was raised at all.
        logger.info(text)
        return await _send_once(key or text, text, None)
    except Exception:
        logger.exception("Notifying failed for: %s", text)
        return False


def notify_soon(text: str, *, key: str | None = None) -> None:
    """Notify from a caller that is not async, on the running loop if there is one.

    For the two places that render text out of the database and are ordinary
    functions. Without this they can only log, and a template that is missing or
    will not format is a message the viewer sees as wrong with nothing anywhere
    saying why.
    """
    logger.warning(text)
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        # No loop: configuration is being read outside the bot, and the log
        # line is all there can be.
        return

    # Deferred both ways: background reaches back into this module for report.
    from background import fire_and_forget

    fire_and_forget(notify(text, key=key), name="notify")


async def _deliver(text: str, trace: str | None) -> bool:
    # Deferred: importing services at module scope runs the whole package, and
    # main.py reports cog-load failures before any of it is up.
    from services.config import config

    if not config.loaded:
        logger.warning("Undelivered, no configuration loaded: %s", text)
        return False

    from services.helper.helper import send_message

    file = (
        discord.File(io.BytesIO(trace.encode("utf-8")), filename="traceback.txt")
        if trace is not None
        else None
    )
    # quiet: send_message announces a channel it cannot resolve, and announcing
    # this one goes through here again.
    sent = await send_message(
        text[:_MAX_CONTENT],
        config.channel(_ADMIN_CHANNEL),
        file=file,
        quiet=True,
        # Nothing here is ever meant to ping. Reports and notices relay text
        # from Twitch, Discord and the database, and any of it could carry a
        # mention that nobody chose to send.
        allowed_mentions=discord.AllowedMentions.none(),
    )
    if sent is None:
        logger.warning("Undelivered, admin channel unavailable: %s", text)
        return False
    return True
