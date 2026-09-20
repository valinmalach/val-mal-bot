from collections.abc import AsyncGenerator, Callable

import httpx
import pytest
from fastapi import FastAPI

from tests.oauth_controller.support import (
    BOT_ID,
    BROADCASTER_ID,
    SCOPES,
    Said,
    Stored,
)
from tests.twitch.support import Script
from valmal.core.config import config
from valmal.twitch.oauth import grants as oauth
from valmal.twitch.oauth import router as twitch_oauth


@pytest.fixture(autouse=True)
def _configuration(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(oauth, "_pending_authorizations", {})
    monkeypatch.setattr(
        config,
        "_settings",
        {
            "twitch_app_scopes": list(SCOPES),
            "twitch_bot_user_id": BOT_ID,
            "twitch_broadcaster_id": BROADCASTER_ID,
        },
    )


@pytest.fixture
def stored(monkeypatch: pytest.MonkeyPatch) -> Stored:
    stored = Stored()
    monkeypatch.setattr(twitch_oauth, "token_manager", stored)
    return stored


@pytest.fixture
def said(monkeypatch: pytest.MonkeyPatch) -> Said:
    said = Said()
    monkeypatch.setattr(twitch_oauth, "notify", said.notify)
    monkeypatch.setattr(twitch_oauth, "report", said.report)
    return said


@pytest.fixture
def twitch(monkeypatch: pytest.MonkeyPatch) -> Callable[..., Script]:
    """Point the controller at a scripted id.twitch.tv: exchange first, then validate."""

    def install(*outcomes: httpx.Response | Exception) -> Script:
        script = Script(*outcomes)
        http = script.client()
        monkeypatch.setattr(twitch_oauth, "client", lambda: http)
        return script

    return install


@pytest.fixture
async def http(stored: Stored, said: Said) -> AsyncGenerator[httpx.AsyncClient]:
    app = FastAPI()
    app.include_router(twitch_oauth.twitch_oauth_router)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as http:
        yield http
