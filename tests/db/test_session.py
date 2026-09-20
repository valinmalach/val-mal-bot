from types import SimpleNamespace, TracebackType
from typing import Any

import pytest

from valmal.core.settings import settings
from valmal.db import session

pytestmark = pytest.mark.anyio


@pytest.fixture(autouse=True)
def _fresh(monkeypatch: pytest.MonkeyPatch) -> None:
    """Each test starts with no engine, and leaves none behind."""
    monkeypatch.setattr(session, "_engine", None)
    monkeypatch.setattr(session, "_session_factory", None)


class TestTheEngineIsLazy:
    def test_importing_the_module_created_nothing(self) -> None:
        assert session._engine is None and session._session_factory is None

    def test_the_first_call_builds_one_and_later_calls_share_it(self) -> None:
        first = session.get_engine()

        assert session.get_engine() is first

    def test_it_speaks_asyncpg_whatever_scheme_the_url_used(self) -> None:
        """DATABASE_URL in the test environment is a plain postgresql:// one."""
        assert session.get_engine().url.drivername == "postgresql+asyncpg"

    def test_it_verifies_a_connection_before_handing_it_out_and_retires_old_ones(
        self,
    ) -> None:
        """Railway's proxy drops idle connections."""
        pool = session.get_engine().pool

        assert pool._pre_ping is True  # pyright: ignore[reportAttributeAccessIssue]
        assert pool._recycle == 300  # pyright: ignore[reportAttributeAccessIssue]

    def test_the_pool_is_five_with_five_overflow(self) -> None:
        pool = session.get_engine().pool

        assert pool.size() == 5  # pyright: ignore[reportAttributeAccessIssue]
        assert pool._max_overflow == 5  # pyright: ignore[reportAttributeAccessIssue]

    @pytest.mark.parametrize("echo", [True, False])
    def test_echo_follows_the_setting(
        self, echo: bool, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(settings, "db_echo", echo)

        assert session.get_engine().echo is echo

    def test_building_one_opens_no_connection(self) -> None:
        engine = session.get_engine()

        assert engine.pool.checkedout() == 0  # pyright: ignore[reportAttributeAccessIssue]


class TestTheFactory:
    def test_is_built_once_on_the_engine(self) -> None:
        factory = session.get_session_factory()

        assert session.get_session_factory() is factory
        assert factory.kw["bind"] is session.get_engine()

    def test_objects_are_not_expired_on_commit_so_a_row_can_be_read_after_the_scope(
        self,
    ) -> None:
        factory = session.get_session_factory()

        assert factory.kw["expire_on_commit"] is False

    def test_nothing_is_flushed_behind_the_callers_back(self) -> None:
        assert session.get_session_factory().kw["autoflush"] is False


class FakeSession:
    def __init__(self, calls: list[str]) -> None:
        self.calls = calls

    async def __aenter__(self) -> FakeSession:
        return self

    async def __aexit__(
        self,
        kind: type[BaseException] | None,
        error: BaseException | None,
        trace: TracebackType | None,
    ) -> None:
        self.calls.append("close")

    async def commit(self) -> None:
        self.calls.append("commit")

    async def rollback(self) -> None:
        self.calls.append("rollback")


@pytest.fixture
def calls(monkeypatch: pytest.MonkeyPatch) -> list[str]:
    made: list[str] = []
    monkeypatch.setattr(
        session, "get_session_factory", lambda: lambda: FakeSession(made)
    )
    return made


class TestSessionScope:
    async def test_commits_when_the_body_finishes(self, calls: list[str]) -> None:
        async with session.session_scope():
            calls.append("work")

        assert calls == ["work", "commit", "close"]

    async def test_yields_the_session_it_committed(self, calls: list[str]) -> None:
        async with session.session_scope() as scoped:
            assert isinstance(scoped, FakeSession)

    async def test_rolls_back_and_re_raises_when_the_body_fails(
        self, calls: list[str]
    ) -> None:
        with pytest.raises(ValueError, match="boom"):
            async with session.session_scope():
                raise ValueError("boom")

        assert calls == ["rollback", "close"]

    async def test_a_failure_at_commit_is_rolled_back_too(
        self, calls: list[str], monkeypatch: pytest.MonkeyPatch
    ) -> None:
        async def refuse(self: FakeSession) -> None:
            self.calls.append("commit")
            raise ConnectionError("lost")

        monkeypatch.setattr(FakeSession, "commit", refuse)

        with pytest.raises(ConnectionError):
            async with session.session_scope():
                pass

        assert calls == ["commit", "rollback", "close"]

    async def test_the_original_error_is_the_one_that_propagates(
        self, calls: list[str]
    ) -> None:
        with pytest.raises(KeyError):
            async with session.session_scope():
                raise KeyError("first")

    async def test_a_body_that_returns_early_still_commits(
        self, calls: list[str]
    ) -> None:
        async def read() -> int:
            async with session.session_scope():
                return 1

        assert await read() == 1
        assert calls == ["commit", "close"]

    async def test_two_scopes_are_two_sessions(self, calls: list[str]) -> None:
        async with session.session_scope():
            pass
        async with session.session_scope():
            pass

        assert calls == ["commit", "close", "commit", "close"]


class TestDisposeEngine:
    async def test_closes_the_pool_and_forgets_both_objects(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        disposed: list[str] = []

        async def dispose() -> None:
            disposed.append("engine")

        monkeypatch.setattr(session, "_engine", SimpleNamespace(dispose=dispose))
        monkeypatch.setattr(session, "_session_factory", object())

        await session.dispose_engine()

        assert disposed == ["engine"]
        assert session._engine is None and session._session_factory is None

    async def test_with_nothing_built_it_does_nothing(self) -> None:
        await session.dispose_engine()

        assert session._engine is None

    async def test_a_real_unconnected_engine_disposes_cleanly(self) -> None:
        session.get_session_factory()

        await session.dispose_engine()

        assert session._engine is None and session._session_factory is None

    async def test_a_new_engine_is_built_after_disposal(self) -> None:
        first = session.get_engine()
        await session.dispose_engine()

        assert session.get_engine() is not first


def test_the_public_surface_is_exactly_what_all_says() -> None:
    names: list[Any] = sorted(session.__all__)

    assert names == [
        "dispose_engine",
        "get_engine",
        "get_session_factory",
        "session_scope",
    ]
