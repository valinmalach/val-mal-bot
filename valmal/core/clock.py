"""The one place the bot reads the time, so a test can stop it: datetime.now is
on a C type and cannot be patched, and this can."""

from datetime import UTC, datetime


def now() -> datetime:
    return datetime.now(UTC)


def as_utc_if_naive(moment: datetime) -> datetime:
    """A naive datetime read as UTC, as pendulum.instance read it; an aware one as is.

    Every column is timestamptz and every Discord time is aware, so a naive value
    should never arrive, but the rules that took one before still take one.
    """
    return moment if moment.tzinfo else moment.replace(tzinfo=UTC)
