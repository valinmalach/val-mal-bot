"""Database connection settings resolved from the environment.

The connection string is a secret, so it stays in ``.env`` as ``DATABASE_URL``.
"""

import os
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

from dotenv import load_dotenv

load_dotenv()

ASYNC_DRIVER = "postgresql+asyncpg"
SYNC_DRIVER = "postgresql+psycopg2"

_POSTGRES_SCHEMES = (
    "postgresql+asyncpg",
    "postgresql+psycopg2",
    "postgresql+psycopg",
    "postgresql",
    "postgres",
)

# asyncpg.connect() raises TypeError on these libpq-only options, and SQLAlchemy
# passes them through uncoerced, so they are dropped rather than translated.
_LIBPQ_ONLY_PARAMS = frozenset(
    {
        "sslmode",
        "channel_binding",
        "target_session_attrs",
        "connect_timeout",
        "options",
    }
)
_SSLMODE_TO_ASYNCPG_SSL = {
    "disable": "disable",
    "allow": "prefer",
    "prefer": "prefer",
    "require": "require",
    "verify-ca": "verify-ca",
    "verify-full": "verify-full",
}


def normalize_database_url(url: str, *, driver: str = ASYNC_DRIVER) -> str:
    """Point a Postgres URL at an explicit SQLAlchemy driver.

    Railway's ``postgresql://`` names no DBAPI and carries options asyncpg rejects.
    """
    parts = urlsplit(url)
    if parts.scheme not in _POSTGRES_SCHEMES:
        return url

    query = _translate_query(parts.query, driver=driver)
    return urlunsplit((driver, parts.netloc, parts.path, query, parts.fragment))


def _translate_query(query: str, *, driver: str) -> str:
    if not query:
        return query

    params = parse_qsl(query, keep_blank_values=True)
    if driver != ASYNC_DRIVER:
        return urlencode(params)

    translated = [
        (key, value) for key, value in params if key not in _LIBPQ_ONLY_PARAMS
    ]
    sslmode = next(
        (value for key, value in params if key == "sslmode"),
        None,
    )
    if sslmode and not any(key == "ssl" for key, _ in translated):
        translated.append(("ssl", _SSLMODE_TO_ASYNCPG_SSL.get(sslmode, sslmode)))
    return urlencode(translated)


def get_database_url(*, driver: str = ASYNC_DRIVER) -> str:
    """Return the configured database URL, raising if ``DATABASE_URL`` is unset."""
    raw = os.getenv("DATABASE_URL", "").strip()
    if not raw:
        raise RuntimeError(
            "DATABASE_URL is not set. Copy the Postgres connection string from "
            "Railway into .env as DATABASE_URL."
        )
    return normalize_database_url(raw, driver=driver)


def database_url_configured() -> bool:
    """Whether a database URL is available, without raising if it is not."""
    return bool(os.getenv("DATABASE_URL", "").strip())
