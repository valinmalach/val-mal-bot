import pytest

from valmal.bot.birthday import MAX_DAYS, Months


def test_every_month_is_numbered_in_calendar_order() -> None:
    assert [m.value for m in Months] == list(range(1, 13))
    assert Months.January.value == 1 and Months.December.value == 12


@pytest.mark.parametrize(
    ("month", "days"),
    [
        (Months.January, 31),
        # A leap year is passed in on purpose: 29 February is a valid birthday.
        (Months.February, 29),
        (Months.March, 31),
        (Months.April, 30),
        (Months.May, 31),
        (Months.June, 30),
        (Months.July, 31),
        (Months.August, 31),
        (Months.September, 30),
        (Months.October, 31),
        (Months.November, 30),
        (Months.December, 31),
    ],
)
def test_max_days_is_the_longest_the_month_can_be(month: Months, days: int) -> None:
    assert MAX_DAYS[month] == days


def test_max_days_covers_every_month_and_nothing_else() -> None:
    assert set(MAX_DAYS) == set(Months)
