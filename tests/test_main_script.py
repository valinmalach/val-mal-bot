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


# uvicorn.run replaced, so running main as a script only records how it would have
# started the server.
SERVE = """
import json, runpy
import uvicorn
captured = {}
uvicorn.run = lambda app, **kwargs: captured.update(kwargs, app=type(app).__name__)
runpy.run_path("main.py", run_name="__main__")
print(json.dumps(captured))
"""


# What the running bot loads of the database layer it does not use (ADR 0005).
# Alembic and the tests keep SQLAlchemy; the bot reads rows over asyncpg.
UNUSED = {"sqlalchemy", "sqlmodel", "alembic", "greenlet"}


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


def run_in_a_fresh_process(script: str) -> dict[str, Any]:
    """The last line the script prints, as JSON, from a process of its own."""
    done = run_python("-c", script)
    return json.loads(done.stdout.strip().splitlines()[-1])


@pytest.fixture(scope="module")
def logging_of_a_fresh_process() -> dict[str, Any]:
    return run_in_a_fresh_process(DESCRIBE)


@pytest.fixture(scope="module")
def how_the_server_starts() -> dict[str, Any]:
    return run_in_a_fresh_process(SERVE)


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


class TestWhatTheBotLoads:
    def test_importing_main_loads_no_sqlalchemy_or_sqlmodel(self) -> None:
        """About 20 MiB the bot only paid for at import; Alembic runs apart."""
        assert run_in_a_fresh_process(loaded()) == []

    def test_the_check_would_see_them_if_they_were_loaded(self) -> None:
        assert "sqlalchemy" in run_in_a_fresh_process(loaded("import sqlalchemy"))
