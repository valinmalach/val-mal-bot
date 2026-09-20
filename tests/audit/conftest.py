import discord
import pytest

from tests.audit.support import (
    AUDIT_CHANNEL,
    COLORS,
    TEMPLATES,
    Audit,
)
from valmal.bot import audit
from valmal.core.config import config


@pytest.fixture
def log(monkeypatch: pytest.MonkeyPatch) -> Audit:
    log = Audit()

    async def send_embed(embed: discord.Embed, channel_id: int, **_: object) -> int:
        log.sent.append((embed, channel_id))
        return 1

    monkeypatch.setattr(audit, "send_embed", send_embed)
    monkeypatch.setattr(audit, "get_age", lambda when: "2 days")
    monkeypatch.setattr(config, "_channels", {"audit_logs": AUDIT_CHANNEL})
    monkeypatch.setattr(config, "_templates", dict(TEMPLATES))
    monkeypatch.setattr(config, "_settings", dict(COLORS))
    return log
