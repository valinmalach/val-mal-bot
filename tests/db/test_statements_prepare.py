"""Every statement the bot runs is valid against the real, migrated schema.

Skipped unless ``PREPARE_DATABASE_URL`` names a database: the suite never talks
to Postgres. CI's ``migrations`` job sets it after ``alembic upgrade head``, so a
column renamed in a revision but not in a statement fails there, not in
production. ``prepare()`` makes Postgres resolve every table, column and
parameter type without running anything.
"""

import os

import asyncpg
import pytest

from tests.db.support import STATEMENTS

pytestmark = [
    pytest.mark.anyio,
    pytest.mark.skipif(
        not os.environ.get("PREPARE_DATABASE_URL"),
        reason="needs PREPARE_DATABASE_URL, set by CI's migrations job",
    ),
]


@pytest.mark.parametrize("name", sorted(STATEMENTS))
async def test_postgres_accepts_the_statement(name: str) -> None:
    connection = await asyncpg.connect(os.environ["PREPARE_DATABASE_URL"])
    try:
        await connection.prepare(STATEMENTS[name])
    finally:
        await connection.close()
