"""The one place the bot reads the time, so a test can stop it: datetime.now is
on a C type and cannot be patched, and this can."""

from datetime import UTC, datetime


def now() -> datetime:
    return datetime.now(UTC)
