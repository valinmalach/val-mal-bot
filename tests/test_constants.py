import importlib.util
from pathlib import Path

import pytest

import constants
from constants import COGS, MAX_DAYS, Months, TokenType

ROOT = Path(__file__).resolve().parent.parent


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


def test_token_types_are_the_strings_stored_in_the_database() -> None:
    assert [t.value for t in TokenType] == ["app", "user", "broadcaster"]
    assert TokenType("app") is TokenType.App
    # A (str, Enum): str() and equality with the bare string both matter to callers.
    assert TokenType.User == "user"


def test_the_twitch_header_names_are_the_ones_twitch_sends() -> None:
    assert constants.TWITCH_MESSAGE_ID == "Twitch-Eventsub-Message-Id"
    assert constants.TWITCH_MESSAGE_TYPE == "Twitch-Eventsub-Message-Type"
    assert constants.TWITCH_MESSAGE_TIMESTAMP == "Twitch-Eventsub-Message-Timestamp"
    assert constants.TWITCH_MESSAGE_SIGNATURE == "Twitch-Eventsub-Message-Signature"
    assert constants.HMAC_PREFIX == "sha256="


def test_every_cog_module_is_registered() -> None:
    """Nothing auto-discovers a cog: one missing from COGS is never loaded."""
    modules = {
        f"cogs.{path.stem}"
        for path in (ROOT / "cogs").glob("*.py")
        if path.stem != "__init__"
    }

    assert modules == set(COGS)


def test_every_registered_cog_names_a_module_that_exists() -> None:
    for name in COGS:
        assert importlib.util.find_spec(name) is not None, name


def test_no_cog_is_registered_twice() -> None:
    assert len(COGS) == len(set(COGS))
