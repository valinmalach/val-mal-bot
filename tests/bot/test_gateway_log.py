import inspect
import logging
from collections.abc import Iterator

import discord.gateway
import pytest

from valmal.bot import gateway_log


class Kept(logging.Handler):
    def __init__(self) -> None:
        super().__init__()
        self.records: list[logging.LogRecord] = []

    def emit(self, record: logging.LogRecord) -> None:
        self.records.append(record)


@pytest.fixture
def kept(monkeypatch: pytest.MonkeyPatch) -> Iterator[Kept]:
    """discord.gateway with the filter installed and a handler of its own; the
    logger's level, filters and handlers are put back afterwards. The level goes
    back through setLevel, which also clears logging's cached isEnabledFor."""
    gateway = logging.getLogger("discord.gateway")
    monkeypatch.setattr(gateway, "filters", [])
    monkeypatch.setattr(gateway, "handlers", [])
    monkeypatch.setattr(gateway, "propagate", False)
    level = gateway.level
    gateway_log.install()
    handler = Kept()
    gateway.addHandler(handler)
    yield handler
    gateway.setLevel(level)


@pytest.mark.parametrize("reason", sorted(gateway_log.REASONS))
def test_a_reconnect_reason_is_raised_to_info(reason: str, kept: Kept) -> None:
    args = (4000,) if "%s" in reason else ()

    logging.getLogger("discord.gateway").debug(reason, *args)

    (record,) = kept.records
    assert (record.levelno, record.levelname) == (logging.INFO, "INFO")
    assert record.getMessage() == (reason % args if args else reason)


def test_every_other_debug_line_is_dropped(kept: Kept) -> None:
    gateway = logging.getLogger("discord.gateway")

    gateway.debug("For Shard ID %s: WebSocket Event: %s", None, {"op": 0})
    gateway.debug("Shard ID %s has sent the RESUME payload.", None)

    assert kept.records == []


def test_info_and_above_pass_unchanged(kept: Kept) -> None:
    gateway = logging.getLogger("discord.gateway")

    gateway.info("Shard ID %s has successfully RESUMED session %s.", None, "abc")
    gateway.warning("Shard ID %s has stopped responding to the gateway.", None)

    assert [r.levelno for r in kept.records] == [logging.INFO, logging.WARNING]


@pytest.mark.parametrize("reason", sorted(gateway_log.REASONS))
def test_the_installed_discord_py_still_logs_each_reason(reason: str) -> None:
    """The match is on discord.py's own text; a reworded release would make the
    filter drop the line silently, so this fails instead."""
    assert f"'{reason}'" in inspect.getsource(discord.gateway)
