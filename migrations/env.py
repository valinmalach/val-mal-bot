"""Alembic environment for the bot's Postgres database.

The URL comes from ``DATABASE_URL``, not from ``alembic.ini``.
"""

import asyncio
from logging.config import fileConfig
from typing import Any, Literal

from alembic import context
from alembic.autogenerate.api import AutogenContext
from sqlalchemy import CheckConstraint, pool
from sqlalchemy.engine import Connection
from sqlalchemy.ext.asyncio import async_engine_from_config
from sqlmodel.sql.sqltypes import AutoString

from db.config import get_database_url

# Importing db.models is what registers every table on the metadata it exports.
from db.models import metadata

config = context.config

if config.config_file_name is not None:
    fileConfig(config.config_file_name)

target_metadata = metadata


def _configure_url() -> str:
    """Resolve the database URL and hand it to Alembic's config."""
    url = get_database_url()
    # escape '%' so ConfigParser interpolation leaves passwords intact
    config.set_main_option("sqlalchemy.url", url.replace("%", "%%"))
    return url


def _render_item(
    type_: str, obj: Any, autogen_context: AutogenContext
) -> str | Literal[False]:
    """Render SQLModel's ``AutoString`` as a plain ``sa.String``.

    Identical DDL here, and it keeps revisions free of a ``sqlmodel`` import.
    """
    if type_ == "type" and isinstance(obj, AutoString):
        autogen_context.imports.add("import sqlalchemy as sa")
        length = getattr(obj, "length", None)
        return f"sa.String(length={length})" if length else "sa.String()"
    return False


# Names of the CHECK constraints SQLAlchemy generates for enum columns.
_ENUM_CHECKS = frozenset(
    constraint.name
    for table in target_metadata.tables.values()
    for constraint in table.constraints
    if isinstance(constraint, CheckConstraint)
    and getattr(constraint, "_type_bound", False)
    and constraint.name
)


def _include_object(
    obj: Any, name: str | None, type_: str, reflected: bool, compare_to: Any
) -> bool:
    """Hide enum CHECK constraints from the comparison.

    Autogenerate reflects them but skips them on the metadata side, so every
    revision would otherwise open with a spurious drop_constraint.
    """
    return not (type_ == "check_constraint" and name in _ENUM_CHECKS)


def _context_options() -> dict:
    return {
        "target_metadata": target_metadata,
        # Without this, column type changes are silently skipped.
        "compare_type": True,
        "compare_server_default": True,
        "render_item": _render_item,
        "include_object": _include_object,
    }


def run_migrations_offline() -> None:
    """Emit SQL to stdout instead of running it against a database."""
    context.configure(
        url=_configure_url(),
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
        **_context_options(),
    )

    with context.begin_transaction():
        context.run_migrations()


def do_run_migrations(connection: Connection) -> None:
    context.configure(connection=connection, **_context_options())

    with context.begin_transaction():
        context.run_migrations()


async def run_async_migrations() -> None:
    _configure_url()
    connectable = async_engine_from_config(
        config.get_section(config.config_ini_section, {}),
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )

    async with connectable.connect() as connection:
        await connection.run_sync(do_run_migrations)

    await connectable.dispose()


def run_migrations_online() -> None:
    asyncio.run(run_async_migrations())


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
