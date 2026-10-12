from datetime import UTC, datetime

from valmal.bot.birthday import Months, is_leap_day, next_birthday, next_birthday_on


def test_leap_day_is_only_29_february() -> None:
    assert is_leap_day(Months.February, 29)
    assert not is_leap_day(Months.February, 28)
    assert not is_leap_day(Months.March, 29)


def test_next_birthday_on_is_strictly_after() -> None:
    after = datetime(2026, 3, 5, tzinfo=UTC)

    assert next_birthday_on(Months.March, 5, "UTC", after) == datetime(
        2027, 3, 5, tzinfo=UTC
    )
    assert next_birthday_on(Months.March, 6, "UTC", after) == datetime(
        2026, 3, 6, tzinfo=UTC
    )


def test_leap_day_waits_for_a_leap_year() -> None:
    after = datetime(2026, 1, 1, tzinfo=UTC)

    assert next_birthday_on(Months.February, 29, "UTC", after) == datetime(
        2028, 2, 29, tzinfo=UTC
    )


def test_local_year_not_utc_year_east_of_utc() -> None:
    # 05:00 on 1 January in Tokyo is still 31 December in UTC. Starting from UTC's
    # year would leave both candidates in the past.
    after = datetime(2026, 12, 31, 20, tzinfo=UTC)

    got = next_birthday_on(Months.January, 1, "Asia/Tokyo", after)

    assert got == datetime(2027, 12, 31, 15, tzinfo=UTC)


def test_stored_instant_in_the_future_is_returned_as_is() -> None:
    stored = datetime(2027, 6, 1, tzinfo=UTC)

    assert next_birthday(stored, False, datetime(2026, 1, 1, tzinfo=UTC)) == stored


def test_stored_instant_rolls_forward_in_its_zone() -> None:
    stored = datetime(2026, 1, 1, 5, tzinfo=UTC)  # 2026-01-01T00:00 in New York
    after = datetime(2026, 6, 1, tzinfo=UTC)

    got = next_birthday(stored, False, after, "America/New_York")

    assert got == datetime(2027, 1, 1, 5, tzinfo=UTC)
