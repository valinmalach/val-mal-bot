"""Everything the bot says about itself, in one place.

Neither function raises. A handler that can fail replaces the exception it was
called to describe, and a lost report is worse than an ugly one.
"""

import io
import logging
import traceback

import discord

logger = logging.getLogger(__name__)

_ADMIN_CHANNEL = "bot_admin"

# Discord rejects a message over 2000 characters, and a rejected report is a
# lost one. The traceback goes as a file, so only the summary is at risk.
_MAX_CONTENT = 1900


async def report(exc: Exception, context: str) -> None:
    """Log an exception, then deliver it and its traceback to the admin channel."""
    summary = (
        f"{context} - Type: {type(exc).__name__}, Message: {exc}, Args: {exc.args}"
    )
    # format_exception, not format_exc: an exception collected from
    # asyncio.gather(return_exceptions=True) is not the one being handled, and
    # format_exc would describe nothing.
    trace = "".join(traceback.format_exception(exc))
    logger.error("%s\nTraceback:\n%s", summary, trace)
    await _deliver(summary, trace)


async def notify(text: str) -> None:
    """Deliver a notice: something the admin channel should see that is not an exception."""
    logger.warning(text)
    await _deliver(text, None)


async def _deliver(text: str, trace: str | None) -> None:
    # Deferred: importing services at module scope runs the whole package, and
    # main.py reports cog-load failures before any of it is up.
    from services.config import config

    if not config.loaded:
        logger.warning("Undelivered, no configuration loaded: %s", text)
        return

    try:
        from services.helper.helper import send_message

        file = (
            discord.File(io.BytesIO(trace.encode("utf-8")), filename="traceback.txt")
            if trace is not None
            else None
        )
        sent = await send_message(
            text[:_MAX_CONTENT], config.channel(_ADMIN_CHANNEL), file=file
        )
        if sent is None:
            logger.warning("Undelivered, admin channel unavailable: %s", text)
    except Exception:
        logger.exception("Undelivered, admin channel raised: %s", text)
