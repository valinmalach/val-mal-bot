import pendulum

from constants import Months
from services.birthday import is_leap_day, next_birthday, next_birthday_on


def test_leap_day_is_only_29_february() -> None:
    assert is_leap_day(Months.February, 29)
    assert not is_leap_day(Months.February, 28)
    assert not is_leap_day(Months.March, 29)


def test_next_birthday_on_is_strictly_after() -> None:
    after = pendulum.datetime(2026, 3, 5)

    assert next_birthday_on(Months.March, 5, "UTC", after) == pendulum.datetime(
        2027, 3, 5
    )
    assert next_birthday_on(Months.March, 6, "UTC", after) == pendulum.datetime(
        2026, 3, 6
    )


def test_leap_day_waits_for_a_leap_year() -> None:
    after = pendulum.datetime(2026, 1, 1)

    assert next_birthday_on(Months.February, 29, "UTC", after) == pendulum.datetime(
        2028, 2, 29
    )


def test_local_year_not_utc_year_east_of_utc() -> None:
    # 05:00 on 1 January in Tokyo is still 31 December in UTC. Starting from UTC's
    # year would leave both candidates in the past.
    after = pendulum.datetime(2026, 12, 31, 20)

    got = next_birthday_on(Months.January, 1, "Asia/Tokyo", after)

    assert got == pendulum.datetime(2027, 12, 31, 15)


def test_stored_instant_in_the_future_is_returned_as_is() -> None:
    stored = pendulum.datetime(2027, 6, 1)

    assert next_birthday(stored, False, pendulum.datetime(2026, 1, 1)) == stored


def test_stored_instant_rolls_forward_in_its_zone() -> None:
    stored = pendulum.datetime(2026, 1, 1, 5)  # 2026-01-01T00:00 in New York
    after = pendulum.datetime(2026, 6, 1)

    got = next_birthday(stored, False, after, "America/New_York")

    assert got == pendulum.datetime(2027, 1, 1, 5)
