"""Rules every statement the bot runs must keep (ADR 0005)."""

import dataclasses
import re

import pytest

from tests.db.support import STATEMENTS
from valmal.db import models, rows

# Each table's row, by table name.
ROWS = {
    getattr(models, name).__tablename__: getattr(rows, name)
    for name in models.__all__
    if hasattr(getattr(models, name), "__table__")
}
HAS_UPDATED_AT = {
    table.name for table in models.metadata.tables.values() if "updated_at" in table.c
}


def _table(statement: str) -> str:
    match = re.search(r"\b(?:FROM|INTO|UPDATE)\s+(\w+)", statement)
    assert match, statement
    return match[1]


def _writes_an_update(statement: str) -> bool:
    return bool(re.search(r"^\s*UPDATE\b|\bDO UPDATE\b", statement))


UPDATES = sorted(name for name, sql in STATEMENTS.items() if _writes_an_update(sql))
ROW_READS = sorted(
    name
    for name, sql in STATEMENTS.items()
    if sql.lstrip().startswith("SELECT") and not sql.lstrip().startswith("SELECT 1")
)


def test_there_are_statements_to_check() -> None:
    assert UPDATES and ROW_READS


@pytest.mark.parametrize("name", UPDATES)
def test_every_update_sets_updated_at_itself(name: str) -> None:
    """SQLAlchemy's onupdate set it before; nothing in Postgres does."""
    statement = STATEMENTS[name]
    if _table(statement) not in HAS_UPDATED_AT:
        pytest.skip("this table has no updated_at")

    assert "updated_at = now()" in statement


def test_the_update_rule_catches_a_statement_that_forgets() -> None:
    forgot = "UPDATE discord_user SET username = $2 WHERE id = $1"

    assert _writes_an_update(forgot)
    assert "updated_at = now()" not in forgot


@pytest.mark.parametrize("name", ROW_READS)
def test_every_read_selects_exactly_its_rows_fields(name: str) -> None:
    """A read missing a column would fail building the row only at runtime."""
    statement = STATEMENTS[name]
    selected = re.search(r"SELECT(.*?)FROM", statement, re.S)
    assert selected, statement
    columns = [column.strip() for column in selected[1].split(",")]
    row = ROWS[_table(statement)]

    assert sorted(columns) == sorted(f.name for f in dataclasses.fields(row))
