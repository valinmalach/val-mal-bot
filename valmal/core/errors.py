"""Everything the bot says about itself, in one place.

None of these raise. They run inside somebody's ``except`` block, where an
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
# lost one. A notice can grow with what went wrong (a line per undeliverable
# subscription), so it is cut to fit under that.
_MAX_CONTENT = 1900

# What replaces the lines that did not fit, so a cut notice does not read as one
# that ended there.
_OVERFLOW_NOTE = "... the rest is attached."

# The note plus the newline joining it to the last kept line, reserved by
# _shortened and by _deliver when it caps the outage prefix.
_OVERFLOW_RESERVED = len(_OVERFLOW_NOTE) + 1

# How long one delivered message stands in for its own repeats.
_WINDOW_SECONDS = 15 * 60

# A backstop for a key that varies per event, which would otherwise grow the
# table without bound.
_MAX_TRACKED = 512

# Messages that reached nobody, counted across keys: a window only reports its
# own key's repeats, so with the channel unreachable a key that never fires
# again would say nothing. Reported when the channel comes back.
_undelivered = 0

# Every log line below carries relayed text, so all use %r: it shows whitespace
# and quoting that %s would hide.


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


async def _send_once(key: str, text: str, attachment: tuple[str, str] | None) -> bool:
    """Deliver unless an identical message already did, inside the window."""
    now = time.monotonic()
    _prune(now)

    window = _windows.get(key)
    if window is not None and now < window.until:
        window.suppressed += 1
        logger.info("Held back (%d since the last report): %r", window.suppressed, text)
        return True

    # Claimed before the send, and nothing between here and it yields: two tasks
    # racing on one key must not both deliver.
    held = window.suppressed if window is not None else 0
    window = _Window(until=now + _WINDOW_SECONDS, suppressed=0)
    _windows[key] = window

    if held:
        # Leading, because _deliver truncates the tail. "Unreported" covers both
        # occurrences held back behind a delivery and ones whose delivery failed.
        text = f"[{held} more went unreported] {text}"

    sent = False
    try:
        sent = await _deliver(text, attachment)
    finally:
        # In a finally: a _deliver that raises has delivered nothing, and its
        # window left standing would suppress the retry.
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
        logger.error("%r", summary, exc_info=exc)
        # Context names the thing that failed; the type keeps two different
        # failures reported from one place from standing in for each other.
        await _send_once(
            key or f"{context}\x00{type(exc).__name__}",
            summary,
            ("traceback.txt", trace),
        )
    except Exception:
        # Describing an exception can itself fail (a __str__ that raises).
        logger.exception("Reporting failed for: %r", context)


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
        logger.info("%r", text)
        return await _send_once(key or text, text, None)
    except Exception:
        logger.exception("Notifying failed for: %r", text)
        return False


async def notify_file(text: str, filename: str, content: str) -> bool:
    """Deliver a notice with a file attached, never held back as a repeat.

    The window is deliberately skipped rather than keyed around. This carries a
    record somebody needs in order to undo what the bot is about to do, and two
    of them inside one window are two different records -- suppressing the
    second would leave the destruction it precedes with nothing to reverse it.
    """
    try:
        logger.info("%r (attached %r, %d characters)", text, filename, len(content))
        return await _deliver(text, (filename, content))
    except Exception:
        logger.exception("Notifying failed for: %r", text)
        return False


def notify_soon(text: str, *, key: str | None = None) -> None:
    """Notify from a caller that is not async, on the running loop if there is one.

    For the places that render text out of the database and are ordinary
    functions. Without this they can only log, and a template that is missing,
    will not format, or names a channel/role with no row is a message the
    viewer sees as wrong with nothing anywhere saying why.
    """
    logger.warning("%r", text)
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        # No loop: configuration is being read outside the bot, and the log
        # line is all there can be.
        return

    # Deferred both ways: background reaches back into this module for report.
    from valmal.core.background import fire_and_forget

    fire_and_forget(notify(text, key=key), name="notify")


def _shortened(text: str, limit: int = _MAX_CONTENT) -> str:
    """The whole lines that fit, and a note saying the rest is attached.

    Whole lines, because a cut mid-line reads as the notice ending rather than
    as truncated. A line longer than the whole budget can never fit, so it keeps
    its head in whatever room is left: a report's summary says what was being
    attempted and its traceback does not, so dropping it would leave the admin
    channel with no idea what failed. The note that follows says it was cut.
    """
    budget = limit - _OVERFLOW_RESERVED
    kept: list[str] = []
    used = 0
    for line in text.split("\n"):
        if used + len(line) > budget:
            if len(line) > budget and used < budget:
                kept.append(line[: budget - used])
            break
        kept.append(line)
        used += len(line) + 1
    return "\n".join([*kept, _OVERFLOW_NOTE])


async def _deliver(text: str, attachment: tuple[str, str] | None) -> bool:
    # Deferred: config imports this module.
    from valmal.core.config import config

    global _undelivered

    if not config.loaded:
        _undelivered += 1
        logger.warning("Undelivered, no configuration loaded: %r", text)
        return False

    from valmal.bot.send import send_message

    # Leading, because the tail is what gets cut. Prepended here so every path
    # through the admin channel carries it, and cleared only once something has
    # arrived. Capped on its own: _undelivered is unbounded, and a huge count
    # could leave no room for _shortened's overflow note.
    prefix = (
        f"[{_undelivered} message(s) reached nobody while this channel was"
        f" unreachable]\n"
        if _undelivered
        else ""
    )[: _MAX_CONTENT - _OVERFLOW_RESERVED]
    if len(prefix) + len(text) > _MAX_CONTENT:
        # The whole notice goes as a file, unless a report's traceback already
        # holds the message's one attachment; the summary is then shortened,
        # never silently sliced.
        if attachment is None:
            attachment = ("notice.txt", prefix + text)
        # Shortened without the prefix, given the room it takes, so the summary
        # competes for space first; together, the prefix could push it out whole.
        text = prefix + _shortened(text, _MAX_CONTENT - len(prefix))
    else:
        text = prefix + text

    file = None
    if attachment is not None:
        filename, content = attachment
        file = discord.File(io.BytesIO(content.encode("utf-8")), filename=filename)

    # Counted before the attempt: send_message raises on a channel it resolved
    # but could not post to, and that reached nobody too. quiet, because
    # announcing an unresolvable channel would come back through here.
    _undelivered += 1
    sent = await send_message(
        text,
        config.channel(_ADMIN_CHANNEL),
        file=file,
        quiet=True,
        # Relayed text could carry a mention nobody chose to send.
        allowed_mentions=discord.AllowedMentions.none(),
    )
    if sent is None:
        logger.warning("Undelivered, admin channel unavailable: %r", text)
        return False

    _undelivered = 0
    return True
