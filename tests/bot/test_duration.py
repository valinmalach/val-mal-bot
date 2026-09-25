import pendulum
import pytest

from valmal.bot.duration import get_age, get_ordinal_suffix

NOW = pendulum.datetime(2026, 6, 15, 12)


@pytest.fixture(autouse=True)
def _frozen_clock(monkeypatch: pytest.MonkeyPatch) -> None:
    """get_age reads the clock itself; a real one makes every expectation drift."""
    monkeypatch.setattr(pendulum, "now", lambda tz=None: NOW)


@pytest.mark.parametrize(
    ("delta", "expected"),
    [
        ({}, "0 seconds"),
        ({"seconds": 1}, "1 second"),
        ({"seconds": 2}, "2 seconds"),
        ({"seconds": 61}, "1 minute, 1 second"),
        ({"minutes": 59, "seconds": 59}, "59 minutes, 59 seconds"),
        ({"hours": 1}, "1 hour"),
        ({"hours": 3, "minutes": 2}, "3 hours, 2 minutes"),
        ({"days": 1}, "1 day"),
        ({"days": 6, "hours": 23}, "6 days, 23 hours"),
        ({"days": 29}, "4 weeks, 1 day"),
        ({"days": 40}, "1 month, 1 week, 2 days"),
        ({"years": 1}, "1 year"),
        ({"years": 1, "days": 2}, "1 year, 2 days"),
        ({"years": 2, "months": 3, "days": 4}, "2 years, 3 months, 4 days"),
    ],
)
def test_age_names_the_units_largest_first(
    delta: dict[str, int], expected: str
) -> None:
    assert get_age(NOW.subtract(**delta)) == expected


@pytest.mark.parametrize("weeks", [1, 2, 3])
def test_a_whole_number_of_weeks_is_not_zero_seconds(weeks: int) -> None:
    """remaining_days is the remainder after the weeks, so reading only that
    called a three-week-old thing "0 seconds"."""
    expected = f"{weeks} week" + ("" if weeks == 1 else "s")

    assert get_age(NOW.subtract(weeks=weeks)) == expected


def test_a_future_moment_reads_the_same_as_the_past_one() -> None:
    assert get_age(NOW.add(hours=5)) == get_age(NOW.subtract(hours=5)) == "5 hours"
    assert get_age(NOW.add(days=2)) == "2 days"


def test_hours_and_below_are_dropped_once_a_month_has_passed() -> None:
    """Documented: past a month it is said in years, months, weeks and days."""
    assert get_age(NOW.subtract(years=1, hours=5)) == "1 year"


@pytest.mark.parametrize(
    ("limit", "expected"),
    [
        (1, "2 years"),
        (2, "2 years, 3 months"),
        (3, "2 years, 3 months, 4 days"),
        (10, "2 years, 3 months, 4 days"),
        (0, "2 years, 3 months, 4 days"),
        (-1, "2 years, 3 months, 4 days"),
    ],
)
def test_limit_units_keeps_the_leading_units_and_zero_means_all(
    limit: int, expected: str
) -> None:
    then = NOW.subtract(years=2, months=3, days=4)

    assert get_age(then, limit) == expected


def test_limiting_to_one_unit_still_says_zero_seconds_for_now() -> None:
    assert get_age(NOW, 1) == "0 seconds"


@pytest.mark.parametrize(
    ("n", "expected"),
    [
        (0, "0th"),
        (1, "1st"),
        (2, "2nd"),
        (3, "3rd"),
        (4, "4th"),
        (9, "9th"),
        (10, "10th"),
        (11, "11th"),
        (12, "12th"),
        (13, "13th"),
        (14, "14th"),
        (20, "20th"),
        (21, "21st"),
        (22, "22nd"),
        (23, "23rd"),
        (24, "24th"),
        (100, "100th"),
        (101, "101st"),
        (111, "111th"),
        (112, "112th"),
        (113, "113th"),
        (121, "121st"),
        (1000, "1000th"),
        (1011, "1011th"),
    ],
)
def test_ordinal_suffix(n: int, expected: str) -> None:
    assert get_ordinal_suffix(n) == expected


def test_every_day_of_a_month_gets_a_suffix() -> None:
    """A birthday is announced by day of month; none of 1-31 may come out odd."""
    suffixes = {1: "st", 2: "nd", 3: "rd", 21: "st", 22: "nd", 23: "rd", 31: "st"}

    assert [get_ordinal_suffix(day) for day in range(1, 32)] == [
        f"{day}{suffixes.get(day, 'th')}" for day in range(1, 32)
    ]
