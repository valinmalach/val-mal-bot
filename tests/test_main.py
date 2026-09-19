import logging
import sys
from collections.abc import AsyncGenerator, Coroutine, Iterator
from types import ModuleType
from typing import Any
from unittest.mock import AsyncMock

import httpx
import pytest

from config import settings
from constants import COGS
from controller.twitch import WEBHOOK_PATHS
from logging_json import JsonFormatter

pytestmark = pytest.mark.anyio


@pytest.fixture(scope="module")
def entry() -> Iterator[ModuleType]:
    """main, imported with the logging it installs undone afterwards.

    Importing it points the root logger at stdout through the JSON formatter, which
    is right for the process and would leak into every other test.
    """
    root = logging.getLogger()
    handlers, level = list(root.handlers), root.level
    httpx_level = logging.getLogger("httpx").level
    import main

    yield main
    root.handlers[:] = handlers
    root.setLevel(level)
    logging.getLogger("httpx").setLevel(httpx_level)


class Process:
    def __init__(self) -> None:
        self.reported: list[str] = []
        self.fired: list[tuple[str | None, Coroutine[Any, Any, None]]] = []
        self.order: list[str] = []
        self.loaded: list[str] = []
        self.load_errors: dict[str, BaseException] = {}
        self.start_error: Exception | None = None
        self.started_with: list[str] = []
        self.removed: list[str] = []
        self.closed = 0


@pytest.fixture
def process(entry: ModuleType, monkeypatch: pytest.MonkeyPatch) -> Iterator[Process]:
    process = Process()

    async def load_extension(name: str) -> None:
        process.order.append(f"load {name}")
        if name in process.load_errors:
            raise process.load_errors[name]
        process.loaded.append(name)

    async def start(token: str) -> None:
        process.order.append("start")
        process.started_with.append(token)
        if process.start_error is not None:
            raise process.start_error

    async def report(exc: Exception, context: str, **_: object) -> None:
        process.reported.append(context)

    def fire_and_forget(
        coro: Coroutine[Any, Any, None], *, name: str | None = None
    ) -> None:
        process.fired.append((name, coro))

    async def aclose() -> None:
        process.closed += 1

    monkeypatch.setattr(entry.bot, "load_extension", load_extension)
    monkeypatch.setattr(entry.bot, "start", start)
    monkeypatch.setattr(
        entry.bot, "remove_command", lambda name: process.removed.append(name)
    )
    monkeypatch.setattr(entry, "report", report)
    monkeypatch.setattr(entry, "fire_and_forget", fire_and_forget)
    monkeypatch.setattr(entry.http_client, "aclose", aclose)
    yield process
    for _, coro in process.fired:
        coro.close()


class TestMain:
    async def test_removes_the_default_help_before_anything_loads(
        self, entry: ModuleType, process: Process
    ) -> None:
        await entry.main()

        assert process.removed == ["help"]

    async def test_loads_every_cog_then_starts_with_the_active_token(
        self, entry: ModuleType, process: Process
    ) -> None:
        await entry.main()

        assert sorted(process.loaded) == sorted(COGS)
        assert process.order[-1] == "start"
        assert process.started_with == [settings.active_discord_token]

    async def test_every_cog_is_attempted_before_the_bot_starts(
        self, entry: ModuleType, process: Process
    ) -> None:
        await entry.main()

        assert process.order.index("start") == len(COGS)

    async def test_no_failures_schedules_no_report(
        self, entry: ModuleType, process: Process
    ) -> None:
        await entry.main()

        assert process.fired == []

    async def test_a_cog_that_will_not_load_does_not_stop_the_others_or_the_bot(
        self,
        entry: ModuleType,
        process: Process,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        process.load_errors[COGS[2]] = ImportError("broken")

        with caplog.at_level(logging.ERROR, logger=entry.logger.name):
            await entry.main()

        assert sorted(process.loaded) == sorted(c for c in COGS if c != COGS[2])
        assert process.started_with != []
        assert f"Failed to load extension {COGS[2]}" in caplog.text

    async def test_the_failure_is_reported_later_by_a_task_named_for_it(
        self, entry: ModuleType, process: Process, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Nothing said during loading could reach a channel that config has not loaded."""
        process.load_errors[COGS[0]] = ImportError("broken")
        ready = AsyncMock()
        monkeypatch.setattr(entry.bot, "wait_until_ready", ready)

        await entry.main()
        ((name, coro),) = process.fired
        await coro

        assert name == "cog-failures"
        ready.assert_awaited_once()
        assert process.reported == [f"Failed to load extension {COGS[0]}"]

    async def test_each_failure_is_paired_with_its_own_cog(
        self, entry: ModuleType, process: Process, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        process.load_errors[COGS[1]] = ImportError("a")
        process.load_errors[COGS[3]] = ImportError("b")
        monkeypatch.setattr(entry.bot, "wait_until_ready", AsyncMock())

        await entry.main()
        await process.fired[0][1]

        assert process.reported == [
            f"Failed to load extension {COGS[1]}",
            f"Failed to load extension {COGS[3]}",
        ]

    async def test_a_failing_start_is_reported_and_never_raised(
        self, entry: ModuleType, process: Process
    ) -> None:
        process.start_error = RuntimeError("bad token")

        await entry.main()

        assert process.reported == ["Unhandled exception in main"]


class TestLifespan:
    async def test_starts_the_bot_in_the_background_and_closes_the_client_on_shutdown(
        self,
        entry: ModuleType,
        process: Process,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        async def fake_main() -> None:
            return None

        monkeypatch.setattr(entry, "main", fake_main)

        async with entry.lifespan(entry.app):
            assert [name for name, _ in process.fired] == ["bot"]
            assert process.closed == 0

        assert process.closed == 1

    async def test_the_client_is_closed_only_after_the_app_stops(
        self,
        entry: ModuleType,
        process: Process,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        async def fake_main() -> None:
            return None

        monkeypatch.setattr(entry, "main", fake_main)

        async with entry.lifespan(entry.app):
            during = process.closed

        assert (during, process.closed) == (0, 1)


class TestRoutes:
    @pytest.fixture
    async def http(self, entry: ModuleType) -> AsyncGenerator[httpx.AsyncClient]:
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=entry.app), base_url="http://test"
        ) as client:
            yield client

    async def test_the_root_names_the_bot_in_plain_text(
        self, http: httpx.AsyncClient
    ) -> None:
        response = await http.get("/")

        assert response.status_code == 200
        assert response.text == "Valin Malach Bot"
        assert response.headers["content-type"].startswith("text/plain")

    async def test_the_health_check_says_healthy(self, http: httpx.AsyncClient) -> None:
        response = await http.get("/health")

        assert (response.status_code, response.text) == (200, "Healthy")

    async def test_the_health_check_does_not_depend_on_the_bot_or_database(
        self, http: httpx.AsyncClient
    ) -> None:
        """Railway restarts a container that fails it, so it must not reach anything."""
        assert (await http.get("/health")).status_code == 200

    async def test_an_unknown_path_is_a_404(self, http: httpx.AsyncClient) -> None:
        assert (await http.get("/nope")).status_code == 404

    async def test_both_routers_are_mounted(self, entry: ModuleType) -> None:
        paths = set(entry.app.openapi()["paths"])

        assert "/twitch/oauth/start/{identity}" in paths
        assert "/twitch/oauth/callback" in paths
        assert any(path.startswith("/webhook/twitch") for path in paths)

    async def test_the_webhook_refuses_an_unsigned_request(
        self, http: httpx.AsyncClient
    ) -> None:
        path = next(iter(WEBHOOK_PATHS.values()))

        response = await http.post(path, content=b"{}")

        assert response.status_code == 403

    async def test_the_app_owns_the_lifespan_that_starts_the_bot(
        self, entry: ModuleType
    ) -> None:
        assert entry.app.router.lifespan_context is not None


class TestLogging:
    def test_the_process_logs_json_to_stdout_through_the_shared_formatter(
        self, entry: ModuleType
    ) -> None:
        assert isinstance(entry._handler.formatter, JsonFormatter)
        assert entry._handler.stream is not sys.stderr

    def test_httpx_is_quietened_because_it_logs_every_request_at_info(
        self, entry: ModuleType
    ) -> None:
        assert logging.getLogger("httpx").level == logging.WARNING
