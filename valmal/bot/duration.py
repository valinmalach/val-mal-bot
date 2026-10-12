"""How long ago something was, in words."""

from calendar import monthrange
from datetime import datetime

from valmal.core import clock


def _unit(value: int, name: str) -> str:
    return f"{value} {name}{'' if value == 1 else 's'}"


def _add_months(moment: datetime, months: int) -> datetime:
    """Calendar months later, on the same day or the month's last when it is shorter."""
    year, month = divmod(moment.month - 1 + months, 12)
    year += moment.year
    day = min(moment.day, monthrange(year, month + 1)[1])
    return moment.replace(year=year, month=month + 1, day=day)


def _whole_months(start: datetime, end: datetime) -> int:
    months = (end.year - start.year) * 12 + end.month - start.month
    return months - 1 if _add_months(start, months) > end else months


def get_age(date_time: datetime, limit_units: int = -1) -> str:
    """The distance from now, largest unit first, e.g. "2 months, 3 days".

    Anything a year or a month old is said in years, months, weeks and days;
    below that, weeks down to seconds. Zero of a unit is dropped, so the string
    never leads with "0 hours" -- except when everything is zero, which is
    "0 seconds".
    """
    start, end = sorted((date_time, clock.now()))
    months = _whole_months(start, end)
    rest = end - _add_months(start, months)
    weeks, days = divmod(rest.days, 7)

    if months:
        units = [
            (months // 12, "year"),
            (months % 12, "month"),
            (weeks, "week"),
            (days, "day"),
        ]
    else:
        hours, seconds = divmod(rest.seconds, 3600)
        units = [
            (weeks, "week"),
            (days, "day"),
            (hours, "hour"),
            (seconds // 60, "minute"),
            (seconds % 60, "second"),
        ]

    parts = [_unit(value, name) for value, name in units if value]
    if not parts:
        return _unit(0, "second")
    return ", ".join(parts[:limit_units] if limit_units > 0 else parts)


def get_ordinal_suffix(n: int) -> str:
    if 10 <= n % 100 <= 13:
        return f"{n}th"
    return f"{n}{('th', 'st', 'nd', 'rd')[n % 10] if n % 10 < 4 else 'th'}"
