"""The rows the bot reads match the models Alembic builds the schema from (ADR 0005)."""

import dataclasses
import types
import typing
from typing import Any

import pytest
from sqlalchemy import Table

from valmal.db import models, rows

MODELS: dict[str, Table] = {
    name: getattr(models, name).__table__
    for name in models.__all__
    if hasattr(getattr(models, name), "__table__")
}


def _nullable(annotation: Any) -> bool:
    return isinstance(
        annotation, types.UnionType
    ) and types.NoneType in typing.get_args(annotation)


def test_every_table_has_a_row_and_every_row_a_table() -> None:
    assert sorted(MODELS) == sorted(rows.__all__)


@pytest.mark.parametrize("name", sorted(MODELS))
def test_a_row_has_exactly_its_tables_columns(name: str) -> None:
    fields = {f.name for f in dataclasses.fields(getattr(rows, name))}

    assert fields == {column.name for column in MODELS[name].columns}


@pytest.mark.parametrize("name", sorted(MODELS))
def test_a_row_field_may_be_none_exactly_where_its_column_may_be_null(
    name: str,
) -> None:
    hints = typing.get_type_hints(getattr(rows, name))
    nullable = {f: _nullable(hint) for f, hint in hints.items()}

    assert nullable == {c.name: bool(c.nullable) for c in MODELS[name].columns}


def test_a_row_cannot_be_changed_after_it_is_read() -> None:
    user = rows.DiscordUser(id=1, username="someone")

    with pytest.raises(dataclasses.FrozenInstanceError):
        user.username = "someone else"  # pyright: ignore[reportAttributeAccessIssue]
