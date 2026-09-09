"""When a birthday next falls.

One rule answers that, asked two ways: next_birthday_on from the parts, when a
birthday is being set, and next_birthday from a stored instant, when one is
being rolled forward. Two implementations of this disagreed once and wrote dates
that had already passed, so the second asks the first wherever it can.
"""

from calendar import isleap
from datetime import datetime
from functools import cache

import pendulum
from pendulum import DateTime

from constants import Months


@cache
def _next_leap_year(year: int) -> int:
    while not isleap(year):
        year += 1
    return year


def is_leap_day(month: Months, day: int) -> bool:
    """Whether a birthday falls on 29 February, and so exists only some years.

    Derived here and nowhere else: the answer picks the year to store the
    birthday in and is also stored beside it, and those two must not disagree.
    """
    return month == Months.February and day == 29


def next_birthday(
    birthday: datetime, is_leap: bool, after: DateTime, timezone: str | None = None
) -> DateTime:
    """The next occurrence of a stored birthday, strictly after ``after``.

    With the zone it was set in, the local date is read back off the instant and
    ``next_birthday_on`` answers from the parts, as it did when the birthday was
    set. Without one there is nothing to construct a local date in, so advancing
    a UTC instant is a year bump - which preserves neither the local day nor
    local midnight across a zone's transitions, and is why the column exists
    (issue #12). ``None`` is every row written before it did.
    """
    moment = pendulum.instance(birthday)
    if moment > after:
        return moment

    if timezone is not None:
        local = moment.in_tz(timezone)
        return next_birthday_on(Months(local.month), local.day, timezone, after)

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
