"""Proving a webhook came from Twitch."""

import hashlib
import hmac


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
