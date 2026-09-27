"""How long ago something was, in words."""

import pendulum
from pendulum import DateTime


def _unit(value: int, name: str) -> str:
    return f"{value} {name}{'' if value == 1 else 's'}"


def get_age(date_time: DateTime, limit_units: int = -1) -> str:
    """The distance from now, largest unit first, e.g. "2 months, 3 days".

    Anything a year or a month old is said in years, months, weeks and days;
    below that, weeks down to seconds. Zero of a unit is dropped, so the string
    never leads with "0 hours" -- except when everything is zero, which is
    "0 seconds".

    weeks is asked for explicitly because remaining_days is the remainder after
    them: a three-week-old thing has remaining_days == 0, and reading only that
    called it "0 seconds".
    """
    now = pendulum.now("UTC")
    age = now - date_time if date_time <= now else date_time - now

    if age.years or age.months:
        units = [
            (age.years, "year"),
            (age.months, "month"),
            (age.weeks, "week"),
            (age.remaining_days, "day"),
        ]
    else:
        units = [
            (age.weeks, "week"),
            (age.remaining_days, "day"),
            (age.hours, "hour"),
            (age.minutes, "minute"),
            (age.remaining_seconds, "second"),
        ]

    parts = [_unit(value, name) for value, name in units if value]
    if not parts:
        return _unit(0, "second")
    return ", ".join(parts[:limit_units] if limit_units > 0 else parts)


def get_ordinal_suffix(n: int) -> str:
    if 10 <= n % 100 <= 13:
        return f"{n}th"
    return f"{n}{('th', 'st', 'nd', 'rd')[n % 10] if n % 10 < 4 else 'th'}"
