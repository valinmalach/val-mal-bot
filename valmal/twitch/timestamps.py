"""Parsing the timestamps Twitch sends, for webhook deliveries and streams alike."""

import re
from datetime import datetime

# Exactly what gets through. fromisoformat also takes a space or a lowercase t for
# the T, a lowercase z, an offset without its colon, and 24:00 as the next day's
# midnight, and none of those is RFC3339 as Twitch sends it.
_RFC3339 = re.compile(
    r"\d{4}-\d{2}-\d{2}T([01]\d|2[0-3]):\d{2}:\d{2}(\.\d{1,9})?(Z|[+-]\d{2}:\d{2})",
    re.ASCII,
)


def parse_rfc3339(date_str: str) -> datetime:
    """An RFC3339 timestamp (e.g. '2025-05-31T12:34:56Z') as an aware datetime.

    Shape-checked first, and the shape has a zone: fromisoformat on its own also
    accepts a date alone and a time with none, which it hands back naive.
    """
    # One error type for the caller to catch. The shape check alone still lets
    # through 2025-13-45T99:99:99Z, which fromisoformat refuses.
    rejected = ValueError(f"Not an RFC3339 timestamp: {date_str[:40]!r}")
    if not _RFC3339.fullmatch(date_str):
        raise rejected
    try:
        parsed = datetime.fromisoformat(date_str)
    except ValueError as unparseable:
        raise rejected from unparseable
    return parsed
