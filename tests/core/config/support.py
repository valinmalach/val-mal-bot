"""Fakes and row builders shared by the ConfigCache tests."""

from types import SimpleNamespace
from typing import Any

from valmal.db.models import AppSetting, DiscordRole, SettingValueType


class FakeSession:
    """Answers `select(Model)` with the rows it was given for that model."""

    def __init__(self, rows: dict[type, list[Any]]) -> None:
        self.rows = rows
        self.statements: list[Any] = []

    async def execute(self, statement: Any) -> Any:
        self.statements.append(statement)
        model = statement.column_descriptions[0]["entity"]
        rows = list(self.rows.get(model, []))
        return SimpleNamespace(scalars=lambda: SimpleNamespace(all=lambda: rows))


class DatabaseDown:
    """A session scope that cannot be entered. A class, not a generator: a tool
    reads the `yield` after the `raise` as unreachable and removes it, and the
    function then stops being an async context manager at all."""

    async def __aenter__(self) -> None:
        raise ConnectionError("database is down")

    async def __aexit__(self, *exc: object) -> None:
        return None


def role(key: str, role_id: int, **fields: Any) -> DiscordRole:
    return DiscordRole(key=key, role_id=role_id, name=key.title(), **fields)


def setting(key: str, value: str | None, kind: SettingValueType) -> AppSetting:
    return AppSetting(key=key, value=value, value_type=kind)
