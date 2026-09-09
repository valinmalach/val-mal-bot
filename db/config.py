"""Database connection settings resolved from the environment.

The connection string is a secret, so it stays in ``.env`` as ``DATABASE_URL``.
"""

from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

from config import settings

ASYNC_DRIVER = "postgresql+asyncpg"

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


def _translate_query(query: str) -> str:
    if not query:
        return query

    params = parse_qsl(query, keep_blank_values=True)
    translated = [
        (key, value) for key, value in params if key not in _LIBPQ_ONLY_PARAMS
    ]
    sslmode = next(
        (value for key, value in params if key == "sslmode"),
        None,
    )
    if sslmode and all(key != "ssl" for key, _ in translated):
        translated.append(("ssl", _SSLMODE_TO_ASYNCPG_SSL.get(sslmode, sslmode)))
    return urlencode(translated)


def get_database_url() -> str:
    """The configured database URL, pointed at asyncpg and ready for an engine.

    Railway's ``postgresql://`` names no DBAPI and carries options asyncpg
    rejects. Alembic runs on the same async engine, so there is one driver here
    and no caller has ever asked for another.
    """
    parts = urlsplit(settings.database_url)
    if parts.scheme not in _POSTGRES_SCHEMES:
        return settings.database_url

    return urlunsplit(
        (
            ASYNC_DRIVER,
            parts.netloc,
            parts.path,
            _translate_query(parts.query),
            parts.fragment,
        )
    )
