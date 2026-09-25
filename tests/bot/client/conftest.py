import pytest

from tests.bot.client.support import Errors
from valmal.bot import client as bot_client
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

    monkeypatch.setattr(bot_client, "report", report)
    monkeypatch.setattr("valmal.core.errors.report", report)
    monkeypatch.setattr(bot_client, "notify", notify)
    monkeypatch.setattr(config, "template", template)
    monkeypatch.setattr(bot_client, "_started", False)
    monkeypatch.setattr(bot_client, "_announced", False)
    return errors
