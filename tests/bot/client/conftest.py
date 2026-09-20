import pytest

from tests.bot.client.support import Errors
from valmal.bot import client as bot_init
from valmal.core.config import config


@pytest.fixture
def errors(monkeypatch: pytest.MonkeyPatch) -> Errors:
    errors = Errors()

    async def report(exc: Exception, context: str, **_: object) -> None:
        errors.calls.append("report")
        errors.reported.append((exc, context))

    async def notify(text: str, *, key: str | None = None) -> bool:
        errors.notified.append(text)
        return errors.notify_result

    def template(key: str, **_: object) -> str:
        return errors.templates[key]

    monkeypatch.setattr(bot_init, "report", report)
    monkeypatch.setattr("valmal.core.errors.report", report)
    monkeypatch.setattr(bot_init, "notify", notify)
    monkeypatch.setattr(config, "template", template)
    monkeypatch.setattr(bot_init, "_started", False)
    monkeypatch.setattr(bot_init, "_announced", False)
    return errors
