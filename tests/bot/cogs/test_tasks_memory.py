import logging
from typing import Any
from unittest.mock import MagicMock

import pytest

from valmal.bot.cogs import tasks
from valmal.bot.cogs.tasks import Tasks

pytestmark = pytest.mark.anyio


async def log_once() -> None:
    loop: Any = Tasks.log_memory
    await loop.coro(Tasks(MagicMock()))


async def test_logs_the_snapshot_as_extra_fields(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    """Each extra= field is a top-level JSON key, which test_logging_json covers."""

    def snapshot() -> dict[str, int]:
        return {"rss_anon_kb": 83748, "heap_free_bytes": 1774032}

    monkeypatch.setattr(tasks.memory, "snapshot", snapshot)
    caplog.set_level(logging.INFO, logger=tasks.logger.name)

    await log_once()

    (record,) = [r for r in caplog.records if r.name == tasks.logger.name]
    assert record.getMessage() == "Memory"
    assert record.__dict__["rss_anon_kb"] == 83748
    assert record.__dict__["heap_free_bytes"] == 1774032


async def test_a_failed_read_is_reported_and_the_loop_survives(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    reported: list[str] = []

    def snapshot() -> dict[str, int]:
        raise OSError("no /proc")

    async def report(exc: Exception, context: str, **_: object) -> None:
        reported.append(context)

    monkeypatch.setattr(tasks.memory, "snapshot", snapshot)
    monkeypatch.setattr(tasks, "report", report)

    await log_once()

    assert reported == ["Could not read the bot's memory"]
