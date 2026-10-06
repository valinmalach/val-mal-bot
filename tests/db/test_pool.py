import asyncio
import logging
from collections.abc import Callable
from types import SimpleNamespace, TracebackType
from typing import Any

import asyncpg
import pytest

from valmal.core.settings import settings
from valmal.db import pool

pytestmark = pytest.mark.anyio


class Transaction:
    def __init__(self, connection: Connection) -> None:
        self.connection = connection

    async def __aenter__(self) -> None:
        self.connection.events.append("begin")

    async def __aexit__(
        self,
        kind: type[BaseException] | None,
        error: BaseException | None,
        trace: TracebackType | None,
    ) -> None:
        self.connection.events.append("rollback" if error else "commit")


class Connection:
    def __init__(self, name: str, ping_error: Exception | None = None) -> None:
        self.name = name
        self.ping_error = ping_error
        self.events: list[str] = []
        self.codecs: list[tuple[str, str]] = []
        self.loggers: list[Callable[..., None]] = []

    async def execute(self, query: str) -> str:
        self.events.append(query)
        if self.ping_error is not None:
            raise self.ping_error
        return "SELECT 1"

    def transaction(self) -> Transaction:
        return Transaction(self)

    def terminate(self) -> None:
        self.events.append("terminate")

    async def set_type_codec(self, kind: str, **options: Any) -> None:
        self.codecs.append((kind, options["schema"]))

    def add_query_logger(self, callback: Callable[..., None]) -> None:
        self.loggers.append(callback)


class Pool:
    def __init__(self, connections: list[Connection]) -> None:
        self.connections = connections
        self.released: list[str] = []
        self.closed = False

    async def acquire(self) -> Connection:
        return self.connections.pop(0)

    async def release(self, connection: Connection) -> None:
        self.released.append(connection.name)

    async def close(self) -> None:
        self.closed = True


class Created:
    def __init__(self, result: Pool) -> None:
        self.result = result
        self.calls: list[tuple[tuple[Any, ...], dict[str, Any]]] = []

    async def __call__(self, *args: Any, **kwargs: Any) -> Pool:
        self.calls.append((args, kwargs))
        await asyncio.sleep(0)
        return self.result


@pytest.fixture(autouse=True)
def _no_pool(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(pool, "_pool", None)


def install(monkeypatch: pytest.MonkeyPatch, *connections: Connection) -> Created:
    created = Created(Pool(list(connections)))
    monkeypatch.setattr(pool.asyncpg, "create_pool", created)
    return created


async def run(statement: str = "work") -> None:
    async with pool.transaction() as connection:
        await connection.execute(statement)


class TestCreation:
    def test_importing_the_module_connects_to_nothing(self) -> None:
        assert pool._pool is None

    async def test_two_first_callers_at_once_create_one_pool(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        created = install(monkeypatch, Connection("a"), Connection("b"))

        await asyncio.gather(run(), run())

        assert len(created.calls) == 1

    async def test_it_is_created_to_match_the_sqlalchemy_pool_it_replaced(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Five plus five overflow before; idle connections never retired."""
        created = install(monkeypatch, Connection("a"))

        await run()

        ((args, kwargs),) = created.calls
        assert args == (pool.get_dsn(),)
        assert kwargs == {
            "min_size": 1,
            "max_size": 10,
            "max_inactive_connection_lifetime": 0,
            "init": pool._init,
        }


class TestTransaction:
    async def test_it_pings_then_runs_the_block_in_a_transaction_and_releases(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        connection = Connection("a")
        created = install(monkeypatch, connection)

        await run()

        assert connection.events == ["SELECT 1", "begin", "work", "commit"]
        assert created.result.released == ["a"]

    async def test_an_error_in_the_block_rolls_back_releases_and_propagates(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        connection = Connection("a")
        created = install(monkeypatch, connection)

        with pytest.raises(LookupError):
            async with pool.transaction():
                raise LookupError

        assert connection.events[-1] == "rollback"
        assert created.result.released == ["a"]

    @pytest.mark.parametrize(
        "dead",
        [
            ConnectionResetError(),
            asyncpg.InterfaceError("closed"),
            asyncpg.ConnectionDoesNotExistError("gone"),
        ],
    )
    async def test_a_dead_connection_is_replaced_before_anything_runs(
        self, dead: Exception, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """What pool_pre_ping did: a Postgres restart costs no failed statement."""
        stale, fresh = Connection("stale", dead), Connection("fresh")
        created = install(monkeypatch, stale, fresh)

        await run()

        assert stale.events == ["SELECT 1", "terminate"]
        assert fresh.events == ["SELECT 1", "begin", "work", "commit"]
        assert created.result.released == ["stale", "fresh"]

    async def test_a_second_dead_connection_is_raised(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        created = install(
            monkeypatch,
            Connection("a", ConnectionResetError()),
            Connection("b", ConnectionResetError()),
        )

        with pytest.raises(ConnectionResetError):
            await run()

        assert created.result.released == ["a", "b"]

    async def test_a_statement_that_fails_after_the_ping_is_not_retried(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Only the ping is safe to repeat: after it, work may have happened."""
        created = install(monkeypatch, Connection("a"), Connection("b"))

        with pytest.raises(ConnectionResetError):
            async with pool.transaction():
                raise ConnectionResetError

        assert created.result.released == ["a"]
        assert [c.name for c in created.result.connections] == ["b"]


class TestEachConnection:
    async def test_json_columns_come_back_as_python_values(self) -> None:
        connection = Connection("a")

        await pool._init(connection)  # pyright: ignore[reportArgumentType]

        assert connection.codecs == [("json", "pg_catalog"), ("jsonb", "pg_catalog")]

    @pytest.mark.parametrize("echo", [True, False])
    async def test_statements_are_logged_only_when_db_echo_is_set(
        self, echo: bool, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(settings, "db_echo", echo)
        connection = Connection("a")

        await pool._init(connection)  # pyright: ignore[reportArgumentType]

        assert connection.loggers == ([pool._log_query] if echo else [])

    def test_a_logged_statement_carries_its_sql_and_time_but_not_its_arguments(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        caplog.set_level(logging.INFO, logger=pool.logger.name)
        query = SimpleNamespace(
            query="SELECT $1", args=("secret-token",), elapsed=0.0012345
        )

        pool._log_query(query)  # pyright: ignore[reportArgumentType]

        (record,) = caplog.records
        assert record.__dict__["sql"] == "SELECT $1"
        assert record.__dict__["elapsed"] == 0.001234
        assert "secret-token" not in str(record.__dict__)


class TestClosing:
    async def test_it_closes_the_pool_and_a_later_call_makes_a_new_one(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        created = install(monkeypatch, Connection("a"), Connection("b"))
        await run()

        await pool.close_pool()

        assert created.result.closed
        assert pool._pool is None
        await run()
        assert len(created.calls) == 2

    async def test_closing_with_no_pool_does_nothing(self) -> None:
        await pool.close_pool()

        assert pool._pool is None
