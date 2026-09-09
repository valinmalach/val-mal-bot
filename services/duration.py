"""How long ago something was, in words."""

from functools import cache

import pendulum
from pendulum import DateTime


def get_age(date_time: DateTime, limit_units: int = -1) -> str:
    now = pendulum.now("UTC")
    age = now - date_time if date_time <= now else date_time - now

    years, months, days, hours, minutes, seconds = (
        age.years,
        age.months,
        age.remaining_days,
        age.hours,
        age.minutes,
        age.remaining_seconds,
    )

    parts = _get_age_parts(years, months, days, hours, minutes, seconds)
    parts = parts[:limit_units] if limit_units > 0 else parts
    return ", ".join(parts)


def _get_age_parts(
    years: int, months: int, days: int, hours: int, minutes: int, seconds: int
) -> list[str]:
    """Extract age parts based on the time units."""
    if years > 0 or months > 0:
        return _get_large_time_units(years, months, days)
    return _get_small_time_units(days, hours, minutes, seconds)


def _get_large_time_units(years: int, months: int, days: int) -> list[str]:
    """Get age parts for years, months, and days."""
    parts = []
    if years:
        parts.append(format_unit(years, "year"))
    if months:
        parts.append(format_unit(months, "month"))
    if days:
        parts.append(format_unit(days, "day"))
    return parts


def _get_small_time_units(
    days: int, hours: int, minutes: int, seconds: int
) -> list[str]:
    """Get age parts for smaller time units (weeks, days, hours, minutes, seconds)."""
    parts = []

    weeks, remaining_days = divmod(days, 7)
    if weeks:
        parts.append(format_unit(weeks, "week"))
    if remaining_days:
        parts.append(format_unit(remaining_days, "day"))
    if hours:
        parts.append(format_unit(hours, "hour"))
    if minutes:
        parts.append(format_unit(minutes, "minute"))
    if seconds or not parts:
        parts.append(format_unit(seconds, "second"))

    return parts


@cache
def get_ordinal_suffix(n: int) -> str:
    if 10 <= n % 100 <= 13:
        return f"{n}th"
    last_digit = n % 10
    if last_digit == 1:
        return f"{n}st"
    elif last_digit == 2:
        return f"{n}nd"
    elif last_digit == 3:
        return f"{n}rd"
    return f"{n}th"


@cache
def format_unit(value: int, unit: str) -> str:
    return f"{value} {f'{unit}s' if value != 1 else unit}"
