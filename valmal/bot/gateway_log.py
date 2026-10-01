"""Why a gateway session dropped, at INFO.

discord.py logs a RESUME at INFO but the reason for it at DEBUG, beside a line
for every gateway payload. This lifts the reasons alone: Discord asking for a
reconnect, a receive timeout, or the socket closing with a code.
"""

import logging

# discord.py's own format strings, matched exactly; a test checks that the
# installed discord.py still logs each one.
REASONS = frozenset(
    {
        "Received RECONNECT opcode.",
        "Timed out receiving packet. Attempting a reconnect.",
        "Websocket closed with %s, attempting a reconnect.",
        "Websocket closed with %s, cannot reconnect.",
    }
)


def _reasons_only(record: logging.LogRecord) -> bool:
    if record.levelno > logging.DEBUG:
        return True
    if record.msg not in REASONS:
        return False
    record.levelno, record.levelname = logging.INFO, "INFO"
    return True


def install() -> None:
    """Pass discord.gateway's reconnect reasons through, and none of its other
    DEBUG lines."""
    gateway = logging.getLogger("discord.gateway")
    gateway.addFilter(_reasons_only)
    gateway.setLevel(logging.DEBUG)
