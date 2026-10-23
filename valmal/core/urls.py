"""What counts as a well-formed URL, for the ones that reach Twitch, asyncpg or Discord.

Parsed rather than pattern-matched: urlsplit takes the scheme, host and port apart,
and each check below says which of them this bot accepts.
"""

import ipaddress
import re
from urllib.parse import SplitResult, urlsplit

# RFC 3986's characters, and percent escapes for anything but a control character.
# Checked before urlsplit, which drops a tab or newline silently rather than refusing
# it, and asyncpg would decode an escaped one into the connection it then fails.
_URL_CHARS = re.compile(
    r"(?:[A-Za-z0-9\-._~:/?#\[\]@!$&'()*+,;=]|%(?![01][0-9A-Fa-f]|7[Ff])[0-9A-Fa-f]{2})+"
)
_LABEL = re.compile(r"[A-Za-z0-9](?:[A-Za-z0-9-]{0,61}[A-Za-z0-9])?")

# The schemes valmal/db/config.py rewrites for asyncpg and Alembic.
POSTGRES_SCHEMES = frozenset(
    {
        "postgresql+asyncpg",
        "postgresql+psycopg2",
        "postgresql+psycopg",
        "postgresql",
        "postgres",
    }
)


def _is_host(host: str | None) -> bool:
    if not host:
        return False
    try:
        ipaddress.ip_address(host)
    except ValueError:
        return len(host) <= 253 and all(_LABEL.fullmatch(p) for p in host.split("."))
    return True


def _parts(url: str) -> SplitResult | None:
    if not _URL_CHARS.fullmatch(url):
        return None
    try:
        parts = urlsplit(url)
        port = parts.port  # raises for a port that is not a number up to 65535
    except ValueError:
        return None
    # A colon with no port after it is not "no port": callback_url would keep it.
    if port == 0 or parts.netloc.endswith(":") or not _is_host(parts.hostname):
        return None
    return parts


def is_app_url(url: str) -> bool:
    """What Twitch accepts as a callback root: https on the standard port, no path.

    Plain http only for localhost, for local development. The scheme must be
    lowercase as written, because callbacks are compared as text with what Twitch
    stores.
    """
    parts = _parts(url)
    if parts is None or parts.path not in ("", "/") or "?" in url or "#" in url:
        return False
    if parts.username is not None:
        return False
    if url.startswith("https://"):
        return parts.port in (None, 443) and "." in (parts.hostname or "")
    return url.startswith("http://") and parts.hostname in {"localhost", "127.0.0.1"}


def is_database_url(url: str) -> bool:
    """A postgres URL naming one database, as asyncpg and Alembic both need."""
    parts = _parts(url)
    # The path as written, less its one leading slash: get_dsn forwards it unchanged,
    # so "db/" would reach asyncpg as a database called "db/".
    database = parts.path.removeprefix("/") if parts else ""
    return (
        parts is not None
        and parts.scheme in POSTGRES_SCHEMES
        and bool(database)
        and "/" not in database
        and "#" not in url
    )


def is_https_url(url: str) -> bool:
    """An absolute https URL on a real host, which Discord will fetch as an image."""
    parts = _parts(url)
    return (
        parts is not None
        and url.startswith("https://")
        and parts.username is None
        and "." in (parts.hostname or "")
    )
