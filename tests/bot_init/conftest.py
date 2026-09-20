import pytest

from init import bot_init
from services.config import config
from tests.bot_init.support import Errors


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
    monkeypatch.setattr("errors.report", report)
    monkeypatch.setattr(bot_init, "notify", notify)
    monkeypatch.setattr(config, "template", template)
    monkeypatch.setattr(bot_init, "_started", False)
    monkeypatch.setattr(bot_init, "_announced", False)
    return errors
