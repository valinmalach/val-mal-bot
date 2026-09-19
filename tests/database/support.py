from types import TracebackType
from typing import Any

from sqlalchemy.dialects import postgresql
from sqlalchemy.sql import ClauseElement


def sql(statement: ClauseElement) -> str:
    """The statement as Postgres would receive it, whitespace made uniform."""
    return " ".join(str(statement.compile(dialect=postgresql.dialect())).split())


def params(statement: ClauseElement) -> dict[str, Any]:
    return {**(statement.compile(dialect=postgresql.dialect()).params or {})}


class Result:
    def __init__(self, rows: list[Any]) -> None:
        self.rows = rows

    def scalars(self) -> Result:
        return self

    def all(self) -> list[Any]:
        return self.rows

    def scalar_one_or_none(self) -> Any:
        return self.rows[0] if self.rows else None


class Database:
    """What a repository function sent, and what the database would have answered."""

    def __init__(self) -> None:
        self.statements: list[ClauseElement] = []
        self.answers: list[list[Any]] = []
        self.rows: dict[tuple[type, int], Any] = {}
        self.gets: list[tuple[type, int]] = []
        self.scopes = 0
        self.failure: Exception | None = None

    @property
    def only(self) -> ClauseElement:
        assert len(self.statements) == 1, self.statements
        return self.statements[0]

    async def execute(self, statement: ClauseElement) -> Result:
        if self.failure is not None:
            raise self.failure
        self.statements.append(statement)
        return Result(self.answers.pop(0) if self.answers else [])

    async def get(self, model: type, key: int) -> Any:
        self.gets.append((model, key))
        return self.rows.get((model, key))

    def scope(self) -> Scope:
        self.scopes += 1
        return Scope(self)


class Scope:
    def __init__(self, database: Database) -> None:
        self.database = database

    async def __aenter__(self) -> Database:
        return self.database

    async def __aexit__(
        self,
        kind: type[BaseException] | None,
        error: BaseException | None,
        trace: TracebackType | None,
    ) -> None:
        return None


def assigned(statement: ClauseElement) -> list[str]:
    """The columns an ``ON CONFLICT DO UPDATE`` overwrites, in order."""
    text = sql(statement)
    _, _, update = text.partition("DO UPDATE SET ")
    return [part.split(" = ")[0] for part in update.split(", ")]
