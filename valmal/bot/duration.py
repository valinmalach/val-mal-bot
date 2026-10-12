"""How long ago something was, in words."""

from calendar import monthrange
from datetime import UTC, datetime

from valmal.core import clock


def _unit(value: int, name: str) -> str:
    return f"{value} {name}{'' if value == 1 else 's'}"


def _precise_diff(
    start: datetime, end: datetime
) -> tuple[int, int, int, int, int, int]:
    """Years, months, days, hours, minutes and seconds from start to end.

    pendulum's precise_diff to the letter, because every age the bot has said was
    worded by it. Its day borrow counts a shortfall equal to the gap between two
    month lengths as a whole month, so a second short of three months is "3
    months", where counting calendar months says "2 months, 4 weeks, 2 days".
    """
    seconds = end.second - start.second - (end.microsecond < start.microsecond)
    minutes = end.minute - start.minute
    hours = end.hour - start.hour
    days = end.day - start.day
    if seconds < 0:
        seconds += 60
        minutes -= 1
    if minutes < 0:
        minutes += 60
        hours -= 1
    if hours < 0:
        hours += 24
        days -= 1

    years = end.year - start.year
    months = end.month - start.month
    if days < 0:
        previous = (end.year, end.month - 1) if end.month > 1 else (end.year - 1, 12)
        last_month_days = monthrange(*previous)[1]
        shortfall = monthrange(end.year, end.month)[1] - last_month_days
        if days < shortfall:
            days += max(start.day, last_month_days)
        elif days == shortfall:
            days = 0
            months += 1
        else:
            days += last_month_days
        months -= 1
    if months < 0:
        months += 12
        years -= 1
    return years, months, days, hours, minutes, seconds


def get_age(date_time: datetime, limit_units: int = -1) -> str:
    """The distance from now, largest unit first, e.g. "2 months, 3 days".

    Anything a year or a month old is said in years, months, weeks and days;
    below that, weeks down to seconds. Zero of a unit is dropped, so the string
    never leads with "0 hours" -- except when everything is zero, which is
    "0 seconds".
    """
    moment = clock.as_utc_if_naive(date_time)
    start, end = sorted((moment.astimezone(UTC), clock.now().astimezone(UTC)))
    years, months, days, hours, minutes, seconds = _precise_diff(start, end)
    weeks, days = divmod(days, 7)

    if years or months:
        units = [(years, "year"), (months, "month"), (weeks, "week"), (days, "day")]
    else:
        units = [
            (weeks, "week"),
            (days, "day"),
            (hours, "hour"),
            (minutes, "minute"),
            (seconds, "second"),
        ]

    parts = [_unit(value, name) for value, name in units if value]
    if not parts:
        return _unit(0, "second")
    return ", ".join(parts[:limit_units] if limit_units > 0 else parts)


def get_ordinal_suffix(n: int) -> str:
    if 10 <= n % 100 <= 13:
        return f"{n}th"
    return f"{n}{('th', 'st', 'nd', 'rd')[n % 10] if n % 10 < 4 else 'th'}"
