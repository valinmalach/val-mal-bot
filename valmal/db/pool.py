"""The process's one asyncpg pool, and the transaction every statement runs in.

Created on first use, so importing this connects to nothing. Only the repository
reaches the database, through ``transaction()``.
"""

import asyncio
import contextlib
import json
import logging
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from typing import TypeAlias

import asyncpg
from asyncpg.pool import PoolConnectionProxy

from valmal.core.settings import settings
from valmal.db.config import get_dsn

logger = logging.getLogger(__name__)

# What the repository is handed: a pooled connection, used like a plain one.
# Not a PEP 695 `type` statement, hence the UP040: Sourcery silently skips its
# custom rules in a file that has one (see AGENTS.md).
Connection: TypeAlias = "PoolConnectionProxy[asyncpg.Record]"  # noqa: UP040

_pool: asyncpg.Pool | None = None
_creating = asyncio.Lock()
# Far longer than any statement here takes.
_CLOSE_TIMEOUT = 5

# What a dead connection raises when it is used, rather than a statement failing.
_DEAD = (OSError, asyncpg.InterfaceError, asyncpg.PostgresConnectionError)


def _log_query(query: asyncpg.connection.LoggedQuery) -> None:
    # Not the arguments: an OAuth token is one.
    logger.info("SQL", extra={"sql": query.query, "elapsed": round(query.elapsed, 6)})


async def _init(connection: asyncpg.Connection) -> None:
    for kind in ("json", "jsonb"):
        await connection.set_type_codec(
            kind, encoder=json.dumps, decoder=json.loads, schema="pg_catalog"
        )
    if settings.db_echo:
        connection.add_query_logger(_log_query)


async def _pool_now() -> asyncpg.Pool:
    global _pool
    async with _creating:
        if _pool is None:
            _pool = await asyncpg.create_pool(
                get_dsn(),
                min_size=1,
                max_size=10,
                # The database is on Railway's private network, which drops
                # nothing idle; retiring idle connections only cost a fresh login
                # on most calls. The ping below covers a Postgres restart.
                max_inactive_connection_lifetime=0,
                init=_init,
            )
        return _pool


@asynccontextmanager
async def transaction() -> AsyncGenerator[Connection]:
    """A pooled connection inside a transaction, committed when the block exits.

    The connection is pinged first and replaced once if it is dead, so a Postgres
    restart costs no failed statement. Retrying is safe there because nothing has
    run on it yet.
    """
    pool = await _pool_now()
    retried = False
    while True:
        connection = await pool.acquire()
        try:
            await connection.execute("SELECT 1")
            break
        except _DEAD:
            # A connection asyncpg saw drop is already back in the pool, and its
            # proxy refuses every call, terminate() included. Otherwise terminate()
            # hands it back itself; either way no release() is needed.
            with contextlib.suppress(asyncpg.InterfaceError):
                connection.terminate()
            if retried:
                raise
            retried = True
        except BaseException:
            # Cancelled, or an error that leaves the connection open: give it back,
            # or ten of these would leave nothing to acquire.
            await pool.release(connection)
            raise
    try:
        async with connection.transaction():
            yield connection
    finally:
        await pool.release(connection)


async def close_pool() -> None:
    """Close every pooled connection. Call this on shutdown."""
    global _pool
    # Under the lock, so a pool still being created is closed rather than left open.
    async with _creating:
        if _pool is None:
            return
        # close() waits for every connection to come back, so one stuck in a
        # stalled call would hold shutdown forever. Cancelled by the timeout, it
        # terminates the pool itself.
        try:
            await asyncio.wait_for(_pool.close(), _CLOSE_TIMEOUT)
        except TimeoutError:
            # The bot has closed its Discord connection by now, so only the log can say.
            logger.warning(
                "Terminated the database pool: a connection was not released "
                "within %s seconds of shutdown",
                _CLOSE_TIMEOUT,
            )
        _pool = None
