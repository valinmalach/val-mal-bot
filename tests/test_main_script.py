"""main run as a script or imported fresh, each in a process of its own."""

import json
import logging
from typing import Any

import pytest

from tests.support import run_python
from valmal.core.settings import settings

# Run in a process of its own: importing main configures the root logger, which
# pytest has already put handlers on, so basicConfig would do nothing in here and a
# check made in here would pass whatever main asked for.
DESCRIBE = """
import json, logging, sys
import main
root = logging.getLogger()
print(json.dumps({
    "level": root.level,
    "handlers": [
        [type(h).__name__, type(h.formatter).__name__, h.stream is sys.stdout]
        for h in root.handlers
    ],
    "gateway": [logging.getLogger("discord.gateway").level, len(logging.getLogger("discord.gateway").filters)],
}))
"""


# Server.run replaced, so running main as a script only records how it would have
# started the server.
SERVE = """
import json, runpy
from uvicorn.server import Server
captured = {}
def run(self, sockets=None):
    c = self.config
    captured.update(app=type(c.app).__name__, host=c.host, port=c.port,
                    log_level=c.log_level, access_log=c.access_log,
                    log_config=c.log_config, ws=c.ws)
    self.started = True
Server.run = run
runpy.run_path("main.py", run_name="__main__")
print(json.dumps(captured))
"""


# As SERVE, in the lazy mode production runs in, after `before`, printing whether
# uvicorn's CLI and the click it is built on were loaded (#123).
def started_lazily(before: str = "") -> str:
    lines = [
        "import json, runpy, sys",
        "from uvicorn.server import Server",
        "def run(self, sockets=None): self.started = True",
        "Server.run = run",
        before,
        'runpy.run_path("main.py", run_name="__main__")',
        'print(json.dumps(sorted({"click", "uvicorn.main"} & set(sys.modules))))',
    ]
    return chr(10).join(lines)


# What the running bot must not load: the database layer it does not use (ADR 0005),
# and pydantic, which SQLModel keeps installed (#124). Alembic and the tests keep
# both; the bot reads rows over asyncpg and parses Twitch with msgspec.
UNUSED = {
    "sqlalchemy",
    "sqlmodel",
    "alembic",
    "greenlet",
    "pydantic",
    "pydantic_core",
    "pydantic_settings",
}


def loaded(before: str = "") -> str:
    """A script importing main and every cog, after `before`, that prints which of
    UNUSED loaded. The cogs too: main does not import them, the bot loads them by
    name at startup, and they are where the database is used."""
    found = f"sorted({{m.split('.')[0] for m in sys.modules}} & {UNUSED!r})"
    lines = [
        "import importlib, json, sys",
        before,
        "import main",
        "from valmal.bot.cogs import COGS",
        "for cog in COGS: importlib.import_module(cog)",
        f"print(json.dumps({found}))",
    ]
    return chr(10).join(lines)


def run_in_a_fresh_process(script: str, *flags: str) -> Any:
    """The last line the script prints, as JSON, from a process of its own."""
    done = run_python(*flags, "-c", script)
    return json.loads(done.stdout.strip().splitlines()[-1])


@pytest.fixture(scope="module")
def logging_of_a_fresh_process() -> dict[str, Any]:
    return run_in_a_fresh_process(DESCRIBE)


@pytest.fixture(scope="module")
def how_the_server_starts() -> dict[str, Any]:
    return run_in_a_fresh_process(SERVE)


# As SERVE, with the bot having stopped by itself while the server ran.
STOPPED = """
import json, runpy, sys
from uvicorn.server import Server
def run(self, sockets=None):
    self.started = True
    sys.modules["__main__"]._bot_stopped = True
Server.run = run
try:
    runpy.run_path("main.py", run_name="__main__")
except SystemExit as e:
    print(json.dumps({"exit": e.code}))
else:
    print(json.dumps({"exit": None}))
"""


class TestLogging:
    def test_the_root_logger_has_one_handler_writing_json_to_stdout(
        self, logging_of_a_fresh_process: dict[str, Any]
    ) -> None:
        """Railway colours a line by its stream unless the line is JSON with a level."""
        assert logging_of_a_fresh_process["handlers"] == [
            ["StreamHandler", "JsonFormatter", True]
        ]

    def test_it_logs_from_info_up(
        self, logging_of_a_fresh_process: dict[str, Any]
    ) -> None:
        assert logging_of_a_fresh_process["level"] == logging.INFO

    def test_the_gateway_reconnect_reasons_are_let_through(
        self, logging_of_a_fresh_process: dict[str, Any]
    ) -> None:
        assert logging_of_a_fresh_process["gateway"] == [logging.DEBUG, 1]


class TestRunningItAsAScript:
    def test_exits_as_failed_once_the_bot_has_stopped_by_itself(self) -> None:
        """As PID 1 in a container the re-raised SIGTERM does nothing, so this is
        what makes Railway restart it."""
        assert run_in_a_fresh_process(STOPPED) == {"exit": 1}

    def test_serves_the_app_on_every_interface_at_the_configured_port(
        self, how_the_server_starts: dict[str, Any]
    ) -> None:
        """Railway reaches the container on its external interface."""
        assert how_the_server_starts["app"] == "Starlette"
        # Asserted, not bound: the point is that main binds every interface.
        assert how_the_server_starts["host"] == "0.0.0.0"  # noqa: S104
        assert how_the_server_starts["port"] == settings.port

    def test_leaves_logging_to_the_json_formatter_by_giving_uvicorn_no_config(
        self, how_the_server_starts: dict[str, Any]
    ) -> None:
        """Without this uvicorn installs its own handlers, which are not JSON, and
        Railway then colours a line by the stream it arrived on."""
        assert how_the_server_starts["log_config"] is None
        assert "log_config" in how_the_server_starts

    def test_loads_no_websocket_stack(
        self, how_the_server_starts: dict[str, Any]
    ) -> None:
        assert how_the_server_starts["ws"] == "none"

    def test_logs_requests_at_info(self, how_the_server_starts: dict[str, Any]) -> None:
        assert how_the_server_starts["log_level"] == "info"
        assert how_the_server_starts["access_log"] is True

    def test_starting_the_server_loads_no_click_under_lazy_imports(self) -> None:
        """uvicorn.run would: it lives beside the CLI, about 1 MiB for nothing."""
        lazy = ("-X", "lazy_imports=all")

        assert run_in_a_fresh_process(started_lazily(), *lazy) == []

    def test_the_check_would_see_click_if_uvicorn_run_were_touched(self) -> None:
        lazy = ("-X", "lazy_imports=all")
        touched = started_lazily("import uvicorn; uvicorn.run")

        assert run_in_a_fresh_process(touched, *lazy) == ["click", "uvicorn.main"]


class TestWhatTheBotLoads:
    def test_importing_main_loads_no_sqlalchemy_or_sqlmodel(self) -> None:
        """About 20 MiB the bot only paid for at import; Alembic runs apart."""
        assert run_in_a_fresh_process(loaded()) == []

    def test_the_check_would_see_them_if_they_were_loaded(self) -> None:
        assert "sqlalchemy" in run_in_a_fresh_process(loaded("import sqlalchemy"))

    def test_importing_main_loads_no_pydantic(self) -> None:
        """About 11 MiB, settings and Twitch models included (#124)."""
        assert not {"pydantic", "pydantic_settings"} & set(
            run_in_a_fresh_process(loaded())
        )

    def test_the_check_would_see_pydantic_if_it_were_loaded(self) -> None:
        assert "pydantic" in run_in_a_fresh_process(loaded("import pydantic"))
