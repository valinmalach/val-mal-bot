"""Database connection settings resolved from the environment.

The connection string is a secret, so it stays in ``.env`` as ``DATABASE_URL``.
"""

from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

from valmal.core.settings import settings

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


# asyncpg's own DSN parser reads sslmode and target_session_attrs, but sends any
# other query parameter to the server as a setting, and these are not settings.
_NOT_FOR_ASYNCPG = frozenset({"channel_binding", "connect_timeout", "options"})


def get_dsn() -> str:
    """The configured database URL in the form ``asyncpg.connect()`` reads."""
    parts = urlsplit(settings.database_url)
    if parts.scheme not in _POSTGRES_SCHEMES:
        return settings.database_url

    query = urlencode(
        [
            (key, value)
            for key, value in parse_qsl(parts.query, keep_blank_values=True)
            if key not in _NOT_FOR_ASYNCPG
        ]
    )
    return urlunsplit(("postgresql", parts.netloc, parts.path, query, parts.fragment))


def get_database_url() -> str:
    """The configured database URL as Alembic's SQLAlchemy engine reads it.

    Railway's ``postgresql://`` names no DBAPI and carries options asyncpg
    rejects. The bot's own pool reads ``get_dsn()`` instead.
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
