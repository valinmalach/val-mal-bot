"""Signing and delivering EventSub notifications, independently of the code under test.

The HMAC is computed here with the standard library rather than by calling
services.twitch.signature: a test that signs with the function it is checking
can only ever agree with it.
"""

import hashlib
import hmac
import json
from typing import Any

import pendulum

SECRET = "test"
NOW = pendulum.datetime(2026, 6, 15, 12)
NOW_ISO = "2026-06-15T12:00:00Z"


def iso(delta_seconds: int = 0) -> str:
    return NOW.add(seconds=delta_seconds).to_iso8601_string()


def sign(message_id: str, timestamp: str, body: str | bytes) -> str:
    raw = body if isinstance(body, bytes) else body.encode()
    digest = hmac.new(
        SECRET.encode(), message_id.encode() + timestamp.encode() + raw, hashlib.sha256
    ).hexdigest()
    return f"sha256={digest}"


def delivery(
    body: dict[str, Any] | str | bytes,
    *,
    message_id: str = "msg-1",
    timestamp: str = NOW_ISO,
    message_type: str = "notification",
    signature: str | None = None,
) -> tuple[bytes, dict[str, str]]:
    """The body and headers Twitch would send, signed unless `signature` overrides it."""
    if isinstance(body, dict):
        raw = json.dumps(body).encode()
    else:
        raw = body if isinstance(body, bytes) else body.encode()
    headers = {
        "Twitch-Eventsub-Message-Id": message_id,
        "Twitch-Eventsub-Message-Timestamp": timestamp,
        "Twitch-Eventsub-Message-Type": message_type,
        "Twitch-Eventsub-Message-Signature": (
            signature if signature is not None else sign(message_id, timestamp, raw)
        ),
        "Content-Type": "application/json",
    }
    return raw, headers


def stream_online_payload(stream_id: str = "10") -> dict[str, Any]:
    return {
        "subscription": {"type": "stream.online"},
        "event": {
            "id": stream_id,
            "broadcaster_user_id": "1",
            "broadcaster_user_login": "bob",
            "broadcaster_user_name": "Bob",
            "type": "live",
            "started_at": "2026-06-15T11:59:00Z",
        },
    }
