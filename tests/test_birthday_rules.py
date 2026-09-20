from datetime import UTC, datetime

import pendulum
import pytest
from pendulum import DateTime

from constants import Months
from services.birthday import (
    _next_leap_year,
    is_leap_day,
    next_birthday,
    next_birthday_on,
)

AFTER = pendulum.datetime(2026, 6, 15, 12)


def utc(year: int, month: int, day: int, hour: int = 0) -> datetime:
    return datetime(year, month, day, hour, tzinfo=UTC)


class TestLeapDay:
    @pytest.mark.parametrize("month", list(Months))
    def test_only_february_has_one(self, month: Months) -> None:
        assert is_leap_day(month, 29) is (month is Months.February)

    @pytest.mark.parametrize("day", [1, 27, 28, 30])
    def test_no_other_february_day_is_one(self, day: int) -> None:
        assert not is_leap_day(Months.February, day)

    @pytest.mark.parametrize(
        ("year", "leap"),
        [
            (2026, 2028),
            (2028, 2028),
            (2029, 2032),
            (2097, 2104),
            (2100, 2104),
            (2000, 2000),
        ],
    )
    def test_the_next_leap_year_follows_the_century_rule(
        self, year: int, leap: int
    ) -> None:
        assert _next_leap_year(year) == leap


class TestNextBirthdayOn:
    def test_a_date_later_this_year_is_this_year(self) -> None:
        assert next_birthday_on(Months.July, 4, "UTC", AFTER) == pendulum.datetime(
            2026, 7, 4
        )

    def test_a_date_already_gone_by_is_next_year(self) -> None:
        assert next_birthday_on(Months.January, 4, "UTC", AFTER) == pendulum.datetime(
            2027, 1, 4
        )

    def test_today_after_midnight_has_passed_is_next_year(self) -> None:
        assert next_birthday_on(Months.June, 15, "UTC", AFTER) == pendulum.datetime(
            2027, 6, 15
        )

    def test_the_first_instant_of_the_day_is_when_it_starts_so_it_is_not_after_itself(
        self,
    ) -> None:
        """Auckland's 16 June begins at 15 June 12:00 UTC, which is exactly AFTER."""
        assert next_birthday_on(
            Months.June, 16, "Pacific/Auckland", AFTER
        ) == pendulum.datetime(2027, 6, 15, 12)

    def test_the_result_is_always_utc(self) -> None:
        got = next_birthday_on(Months.July, 4, "Asia/Tokyo", AFTER)

        assert got.timezone_name == "UTC"

    def test_local_midnight_in_a_zone_west_of_utc_is_later_in_utc(self) -> None:
        assert next_birthday_on(
            Months.July, 4, "America/Los_Angeles", AFTER
        ) == pendulum.datetime(2026, 7, 4, 7)

    def test_the_same_local_date_is_a_different_instant_across_a_dst_change(
        self,
    ) -> None:
        winter = next_birthday_on(
            Months.January, 15, "America/New_York", pendulum.datetime(2026, 1, 1)
        )
        summer = next_birthday_on(
            Months.July, 15, "America/New_York", pendulum.datetime(2026, 1, 1)
        )

        assert (winter.hour, summer.hour) == (5, 4)

    def test_a_zone_that_springs_forward_across_midnight_starts_at_the_first_instant(
        self,
    ) -> None:
        """Havana loses 00:00 to 01:00 on 8 March 2026, so that day has no midnight."""
        got = next_birthday_on(
            Months.March, 8, "America/Havana", pendulum.datetime(2026, 1, 1)
        )

        assert got == pendulum.datetime(2026, 3, 8, 5)

    def test_east_of_the_date_line_the_local_year_can_be_a_year_ahead(self) -> None:
        """It is already 2027 in Kiritimati while UTC is still on 2026's last day."""
        after = pendulum.datetime(2026, 12, 31, 12)

        assert next_birthday_on(
            Months.January, 1, "Pacific/Kiritimati", after
        ) == pendulum.datetime(2027, 12, 31, 10)

    def test_a_leap_day_waits_for_the_next_leap_year(self) -> None:
        assert next_birthday_on(Months.February, 29, "UTC", AFTER) == (
            pendulum.datetime(2028, 2, 29)
        )

    def test_a_leap_day_that_just_passed_waits_for_the_one_after(self) -> None:
        after = pendulum.datetime(2028, 3, 1)

        assert next_birthday_on(Months.February, 29, "UTC", after) == (
            pendulum.datetime(2032, 2, 29)
        )

    def test_a_leap_day_across_the_century_that_is_not_a_leap_year(self) -> None:
        after = pendulum.datetime(2097, 1, 1)

        assert next_birthday_on(Months.February, 29, "UTC", after) == (
            pendulum.datetime(2104, 2, 29)
        )

    def test_the_leap_day_itself_is_not_after_itself(self) -> None:
        after = pendulum.datetime(2028, 2, 29)

        assert next_birthday_on(Months.February, 29, "UTC", after) == (
            pendulum.datetime(2032, 2, 29)
        )

    def test_the_last_day_of_the_year(self) -> None:
        assert next_birthday_on(Months.December, 31, "UTC", AFTER) == (
            pendulum.datetime(2026, 12, 31)
        )

    def test_a_zone_the_tz_database_does_not_have_is_refused(self) -> None:
        with pytest.raises(ValueError, match="Mars/Base"):
            next_birthday_on(Months.July, 4, "Mars/Base", AFTER)

    def test_the_answer_is_always_strictly_after(self) -> None:
        for zone in ("UTC", "Asia/Tokyo", "America/Havana", "Pacific/Kiritimati"):
            for month, day in (
                (Months.January, 1),
                (Months.June, 15),
                (Months.December, 31),
            ):
                got = next_birthday_on(month, day, zone, AFTER)

                assert got > AFTER, (zone, month, day)
                assert got < AFTER.add(years=1, days=2), (zone, month, day)


class TestNextBirthdayFromTheStoredInstant:
    def test_a_stored_instant_still_ahead_is_kept_whatever_the_flags(self) -> None:
        stored = utc(2027, 6, 1)

        assert next_birthday(stored, False, AFTER) == stored
        assert next_birthday(stored, True, AFTER, "Asia/Tokyo") == stored

    def test_an_instant_exactly_now_is_not_ahead_and_rolls_forward(self) -> None:
        got = next_birthday(datetime(2026, 6, 15, 12, tzinfo=UTC), False, AFTER, "UTC")

        assert got == pendulum.datetime(2027, 6, 15)

    def test_the_result_is_a_pendulum_datetime(self) -> None:
        assert isinstance(next_birthday(utc(2025, 7, 4), False, AFTER), DateTime)

    def test_the_local_day_is_read_back_not_the_utc_day(self) -> None:
        """20:00 UTC on 4 July is already 5 July in Auckland."""
        got = next_birthday(utc(2025, 7, 4, 20), False, AFTER, "Pacific/Auckland")

        assert got == pendulum.datetime(2026, 7, 4, 12)

    def test_a_dst_zone_keeps_local_midnight_across_the_roll(self) -> None:
        stored = pendulum.datetime(2025, 7, 4, 4)  # midnight in New York, EDT

        got = next_birthday(
            stored, False, pendulum.datetime(2026, 8, 1), "America/New_York"
        )

        assert got == pendulum.datetime(2027, 7, 4, 4)

    def test_a_leap_day_stays_a_leap_day_through_the_zone_path(self) -> None:
        stored = pendulum.datetime(2028, 2, 29)

        got = next_birthday(stored, True, pendulum.datetime(2028, 6, 1), "UTC")

        assert got == pendulum.datetime(2032, 2, 29)

    def test_a_zone_the_tz_database_has_dropped_raises_for_the_caller_to_clear(
        self,
    ) -> None:
        with pytest.raises(ValueError, match="Gone/Away"):
            next_birthday(utc(2025, 7, 4), False, AFTER, "Gone/Away")


class TestRowsWrittenBeforeTheZoneColumnExisted:
    """No zone to read the local date in, so the year is simply bumped (issue #12)."""

    def test_a_birthday_later_this_year(self) -> None:
        assert next_birthday(utc(2025, 7, 4), False, AFTER) == pendulum.datetime(
            2026, 7, 4
        )

    def test_a_birthday_already_passed_this_year_goes_to_next_year(self) -> None:
        assert next_birthday(utc(2025, 1, 4), False, AFTER) == pendulum.datetime(
            2027, 1, 4
        )

    def test_the_time_of_day_is_kept(self) -> None:
        assert next_birthday(utc(2025, 7, 4, 15), False, AFTER) == pendulum.datetime(
            2026, 7, 4, 15
        )

    def test_a_stored_year_far_behind_catches_up_in_one_step(self) -> None:
        assert next_birthday(utc(2001, 7, 4), False, AFTER) == pendulum.datetime(
            2026, 7, 4
        )

    def test_the_same_day_earlier_than_now_goes_to_next_year(self) -> None:
        got = next_birthday(utc(2025, 6, 15, 6), False, AFTER)

        assert got == pendulum.datetime(2027, 6, 15, 6)

    def test_a_flagged_leap_day_goes_to_the_next_leap_year(self) -> None:
        assert next_birthday(utc(2024, 2, 29), True, AFTER) == pendulum.datetime(
            2028, 2, 29
        )

    def test_a_leap_day_without_the_flag_is_still_a_leap_day(self) -> None:
        """A 29 February instant cannot be placed in a common year whatever the flag says."""
        assert next_birthday(utc(2024, 2, 29), False, AFTER) == pendulum.datetime(
            2028, 2, 29
        )

    def test_a_leap_day_that_has_just_passed_goes_four_years_on(self) -> None:
        after = pendulum.datetime(2028, 3, 1)

        assert next_birthday(utc(2028, 2, 29), True, after) == pendulum.datetime(
            2032, 2, 29
        )

    def test_a_leap_day_still_ahead_in_a_leap_year_is_this_year(self) -> None:
        after = pendulum.datetime(2028, 1, 1)

        assert next_birthday(utc(2024, 2, 29), True, after) == pendulum.datetime(
            2028, 2, 29
        )

    def test_the_first_of_march_is_an_ordinary_birthday(self) -> None:
        got = next_birthday(utc(2027, 3, 1), False, pendulum.datetime(2027, 3, 1))

        assert got == pendulum.datetime(2028, 3, 1)

    @pytest.mark.parametrize("month", range(1, 13))
    def test_every_month_lands_after_now_and_within_a_year(self, month: int) -> None:
        got = next_birthday(utc(2025, month, 10), False, AFTER)

        assert AFTER < got <= AFTER.add(years=1)
