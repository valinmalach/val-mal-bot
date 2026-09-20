"""Fixtures for the error-reporting tests, which are split across files so that
each is short enough to be reviewed whole."""

from types import SimpleNamespace
from typing import Any

import pytest

from tests.error_reporting.support import Channel, Clock
from valmal.core import errors
from valmal.core.config import config


@pytest.fixture
def clock(monkeypatch: pytest.MonkeyPatch) -> Clock:
    fake = Clock()
    monkeypatch.setattr(errors, "time", SimpleNamespace(monotonic=fake.monotonic))
    return fake


@pytest.fixture(autouse=True)
def _fresh_state(monkeypatch: pytest.MonkeyPatch) -> None:
    """The suppression windows and the undelivered count are module globals."""
    monkeypatch.setattr(errors, "_windows", {})
    monkeypatch.setattr(errors, "_undelivered", 0)


@pytest.fixture
def delivered(monkeypatch: pytest.MonkeyPatch) -> list[tuple[str, Any]]:
    """Replace the send with a recorder, for the tests that are about deciding
    whether to send rather than about the sending."""
    sent: list[tuple[str, Any]] = []

    async def deliver(text: str, attachment: tuple[str, str] | None) -> bool:
        sent.append((text, attachment))
        return True

    monkeypatch.setattr(errors, "_deliver", deliver)
    return sent


@pytest.fixture
def admin(monkeypatch: pytest.MonkeyPatch) -> Channel:
    """A loaded configuration whose admin channel is 4242, and a fake send."""
    import services.send

    channel = Channel()
    monkeypatch.setattr(config, "_loaded", True)
    monkeypatch.setattr(config, "_channels", {"bot_admin": 4242})
    monkeypatch.setattr(services.send, "send_message", channel.send_message)
    return channel
