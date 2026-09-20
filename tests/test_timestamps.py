import pendulum
import pytest

from services.twitch.timestamps import parse_rfc3339


def _arabic_indic(text: str) -> str:
    """The same text in non-ASCII digits: the shape regex's digit class matches
    them, so only pendulum stands between them and a parsed timestamp."""
    return "".join(chr(0x0660 + int(c)) if c.isdigit() else c for c in text)


@pytest.mark.parametrize(
    ("text", "iso"),
    [
        ("2025-05-31T12:34:56Z", "2025-05-31T12:34:56+00:00"),
        ("2025-05-31T12:34:56.5Z", "2025-05-31T12:34:56.500000+00:00"),
        ("2025-05-31T12:34:56.123456Z", "2025-05-31T12:34:56.123456+00:00"),
        # Twitch sends nanoseconds; pendulum keeps microseconds.
        ("2025-05-31T12:34:56.123456789Z", "2025-05-31T12:34:56.123456+00:00"),
        ("2025-05-31T12:34:56+05:30", "2025-05-31T12:34:56+05:30"),
        ("2025-05-31T12:34:56-08:00", "2025-05-31T12:34:56-08:00"),
        ("2024-02-29T00:00:00Z", "2024-02-29T00:00:00+00:00"),
    ],
)
def test_accepts_rfc3339(text: str, iso: str) -> None:
    parsed = parse_rfc3339(text)

    assert isinstance(parsed, pendulum.DateTime)
    assert parsed.tzinfo is not None
    assert parsed.isoformat() == iso


def test_the_offset_is_kept_but_the_instant_is_the_same() -> None:
    assert parse_rfc3339("2025-05-31T12:00:00+02:00") == parse_rfc3339(
        "2025-05-31T10:00:00Z"
    )


@pytest.mark.parametrize(
    "text",
    [
        "",
        " ",
        "not a date",
        # Shape: everything pendulum would take that RFC3339 does not.
        "2025-05-31 12:34:56Z",
        "2025-05-31t12:34:56Z",
        "2025-05-31T12:34:56z",
        "2025-05-31T12:34:56",
        "2025-05-31",
        "12:34:56Z",
        "20250531T123456Z",
        "P1D",
        "PT1H",
        "2025-05-31T12:34:56Z/2025-06-01T12:34:56Z",
        "2025-05-31T12:34:56.Z",
        "2025-05-31T12:34:56.1234567890Z",
        "2025-05-31T12:34:56+0530",
        " 2025-05-31T12:34:56Z",
        "2025-05-31T12:34:56Z ",
        "2025-05-31T12:34:56Z\n",
        # The shape holds and the value cannot exist.
        "2025-13-45T99:99:99Z",
        "2025-02-30T12:34:56Z",
        "2023-02-29T00:00:00Z",
        "2025-05-31T24:00:00Z",
        "2025-05-31T12:60:00Z",
        # Non-ASCII digits.
        _arabic_indic("2025-05-31T12:34:56Z"),
    ],
)
def test_rejects_anything_that_is_not_rfc3339_with_one_error_type(text: str) -> None:
    with pytest.raises(ValueError, match="Not an RFC3339 timestamp"):
        parse_rfc3339(text)


def test_the_error_echoes_at_most_forty_characters_of_the_input() -> None:
    """The input is a header off an unverified request."""
    with pytest.raises(ValueError) as caught:
        parse_rfc3339("A" * 500)

    assert "A" * 40 in str(caught.value)
    assert "A" * 41 not in str(caught.value)


def test_an_unparseable_value_chains_the_underlying_error() -> None:
    with pytest.raises(ValueError) as caught:
        parse_rfc3339("2025-13-45T99:99:99Z")

    assert caught.value.__cause__ is not None
