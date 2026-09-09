"""Proving a webhook came from Twitch, and reading the time it says it was sent.

Here rather than among the Discord helpers it used to live beside: none of it is
about Discord, and all of it is about a Twitch delivery.
"""

import hashlib
import hmac
import re

import pendulum
from pendulum import DateTime


def get_hmac_message(
    twitch_message_id: str, twitch_message_timestamp: str, body: str
) -> str:
    """Not cached: every argument comes from an unverified request."""
    return twitch_message_id + twitch_message_timestamp + body


def get_hmac(secret: str, message: str) -> str:
    return hmac.new(
        secret.encode("utf-8"), message.encode("utf-8"), hashlib.sha256
    ).hexdigest()


def verify_message(hmac_str: str, verify_signature: str) -> bool:
    """Whether a presented signature matches, in constant time.

    The presented one arrives in a header, which decodes as latin-1, and
    compare_digest raises on non-ASCII rather than returning False.
    """
    if not verify_signature.isascii():
        return False
    return hmac.compare_digest(hmac_str, verify_signature)


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
