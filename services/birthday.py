"""When a birthday next falls.

One rule answers that, asked two ways: next_birthday_on when it is being set and
the timezone is still known, next_birthday when it is being rolled forward and
it is not. Two implementations of this disagreed once and wrote dates that had
already passed.
"""

from datetime import datetime
from functools import cache

import pendulum
from pendulum import DateTime

from constants import Months


@cache
def _is_leap_year(year: int) -> bool:
    return (year % 400 == 0) or (year % 100 != 0) and (year % 4 == 0)


@cache
def _next_leap_year(year: int) -> int:
    while not _is_leap_year(year):
        year += 1
    return year


def is_leap_day(month: Months, day: int) -> bool:
    """Whether a birthday falls on 29 February, and so exists only some years.

    Derived here and nowhere else: the answer picks the year to store the
    birthday in and is also stored beside it, and those two must not disagree.
    """
    return month == Months.February and day == 29


def next_birthday(birthday: datetime, is_leap: bool, after: DateTime) -> DateTime:
    """The next occurrence of a stored birthday, strictly after ``after``.

    A stored birthday is a UTC instant for one particular year, so advancing it
    is a year bump; 29 February has to land on a leap year to exist at all.
    """
    moment = pendulum.instance(birthday)
    if moment > after:
        return moment

    # The flag is the authority, but a 29 February instant cannot be replaced
    # into a common year whatever the flag says.
    if is_leap or (moment.month == 2 and moment.day == 29):
        candidate = moment.replace(year=_next_leap_year(after.year))
        if candidate > after:
            return candidate
        return moment.replace(year=_next_leap_year(after.year + 1))

    candidate = moment.replace(year=after.year)
    return candidate if candidate > after else moment.replace(year=after.year + 1)


def next_birthday_on(
    month: Months, day: int, timezone: str, after: DateTime
) -> DateTime:
    """The start of the next ``month``/``day`` in ``timezone``, after ``after``.

    Local midnight, or the first instant of that day in the zones that spring
    forward across it and have no midnight on that date.
    """
    zone = pendulum.timezone(timezone)
    leap = is_leap_day(month, day)
    # Their year, not UTC's: east of UTC the two disagree for part of every day,
    # and starting from UTC's leaves both candidates in the past.
    local_year = after.in_tz(zone).year
    year = _next_leap_year(local_year) if leap else local_year
    midnight = DateTime(year, month.value, day, tzinfo=zone).in_tz("UTC")
    if midnight > after:
        return midnight

    year = _next_leap_year(year + 1) if leap else year + 1
    return DateTime(year, month.value, day, tzinfo=zone).in_tz("UTC")
