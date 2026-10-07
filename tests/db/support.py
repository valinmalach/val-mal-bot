"""A fake asyncpg connection, and every statement the bot can run."""

from typing import Any

from valmal.db import configuration, repository

# Every statement constant, by name: what CI prepares against the real schema.
STATEMENTS: dict[str, str] = {
    name: value
    for module in (repository, configuration)
    for name, value in vars(module).items()
    if name.isupper() and isinstance(value, str)
}

Call = tuple[str, str, tuple[Any, ...]]


class Database:
    """What a repository function sent, and what the database would have answered.

    Each call takes the next answer from `answers`; with none left, the method's
    empty answer: no rows, no record, no value. `failure` makes every call raise.
    """

    def __init__(self) -> None:
        self.calls: list[Call] = []
        self.answers: list[Any] = []
        self.transactions = 0
        self.failure: Exception | None = None

    @property
    def only(self) -> Call:
        assert len(self.calls) == 1, self.calls
        return self.calls[0]

    def _answer(self, method: str, statement: str, args: tuple[Any, ...]) -> Any:
        if self.failure is not None:
            raise self.failure
        self.calls.append((method, statement, args))
        if self.answers:
            return self.answers.pop(0)
        empty: dict[str, Any] = {"execute": "OK", "fetch": []}
        return empty.get(method)

    async def execute(self, statement: str, *args: Any) -> Any:
        return self._answer("execute", statement, args)

    async def fetch(self, statement: str, *args: Any) -> Any:
        return self._answer("fetch", statement, args)

    async def fetchrow(self, statement: str, *args: Any) -> Any:
        return self._answer("fetchrow", statement, args)

    async def fetchval(self, statement: str, *args: Any) -> Any:
        return self._answer("fetchval", statement, args)


class Transaction:
    """What ``pool.transaction()`` yields, counted. A class, not a generator: a
    tool reads a `yield` after a `raise` as unreachable and deletes it."""

    def __init__(self, database: Database) -> None:
        self.database = database

    async def __aenter__(self) -> Database:
        self.database.transactions += 1
        return self.database

    async def __aexit__(self, *exc: object) -> None:
        return None
