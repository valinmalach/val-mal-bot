"""Database connection settings resolved from the environment.

The connection string is a secret, so it stays in ``.env`` as ``DATABASE_URL``.
"""

from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

from config import settings

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
    """Return the configured database URL, ready for ``create_engine``."""
    return normalize_database_url(settings.database_url, driver=driver)
