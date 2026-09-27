"""Parsing the timestamps Twitch sends, for webhook deliveries and streams alike."""

import re

import pendulum
from pendulum import DateTime

# Exactly what gets through: pendulum takes a space for the T and would accept
# it, and rejects a lowercase t or z, so neither belongs in the shape.
_RFC3339 = re.compile(
    r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d{1,9})?(Z|[+-]\d{2}:\d{2})"
)


def parse_rfc3339(date_str: str) -> DateTime:
    """An RFC3339 timestamp (e.g. '2025-05-31T12:34:56Z') as an aware DateTime.

    Shape-checked first: pendulum.parse on its own also accepts durations,
    intervals, and zone-less times that it silently calls UTC.
    """
    # One error type for the caller to catch. The shape check alone still lets
    # through 2025-13-45T99:99:99Z, which pendulum throws its own type at.
    rejected = ValueError(f"Not an RFC3339 timestamp: {date_str[:40]!r}")
    if not _RFC3339.fullmatch(date_str):
        raise rejected
    try:
        parsed = pendulum.parse(date_str)
    except Exception as unparseable:
        raise rejected from unparseable
    if not isinstance(parsed, DateTime) or parsed.tzinfo is None:
        raise rejected
    return parsed
