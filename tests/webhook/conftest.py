"""Fixtures for the webhook controller tests, split across files so each is short
enough to be reviewed whole."""

import asyncio
from collections import OrderedDict
from collections.abc import AsyncGenerator, Callable, Coroutine
from types import SimpleNamespace
from typing import Any

import httpx
import pendulum
import pytest
from fastapi import FastAPI, Request, Response

import controller.twitch as ctl
from models.twitch_event_subs.stream_online import StreamOnlineEventSub
from tests.webhook.support import NOW


class Hooks:
    """What the controller announced, and what it dispatched."""

    def __init__(self) -> None:
        self.notified: list[tuple[str, str | None]] = []
        self.reported: list[str] = []
        self.dispatched: list[tuple[str | None, Coroutine[Any, Any, None]]] = []
        self.events: list[StreamOnlineEventSub] = []
        self.clock = 1000.0
        self.fail_dispatch: Exception | None = None
        self.handler_error: Exception | None = None

    async def run_dispatched(self) -> None:
        for _, coro in self.dispatched:
            await coro
        self.dispatched.clear()


@pytest.fixture
def hooks(monkeypatch: pytest.MonkeyPatch) -> Hooks:
    hooks = Hooks()

    async def notify(text: str, *, key: str | None = None) -> bool:
        hooks.notified.append((text, key))
        return True

    async def report(exc: Exception, context: str, **_: object) -> None:
        hooks.reported.append(context)

    def fire_and_forget(
        coro: Coroutine[Any, Any, None], name: str | None = None
    ) -> asyncio.Future[None]:
        if hooks.fail_dispatch is not None:
            coro.close()
            raise hooks.fail_dispatch
        hooks.dispatched.append((name, coro))
        return asyncio.get_running_loop().create_future()

    monkeypatch.setattr(ctl, "notify", notify)
    monkeypatch.setattr(ctl, "report", report)
    monkeypatch.setattr(ctl, "fire_and_forget", fire_and_forget)
    monkeypatch.setattr(ctl, "time", SimpleNamespace(monotonic=lambda: hooks.clock))
    monkeypatch.setattr(pendulum, "now", lambda tz=None: NOW)
    monkeypatch.setattr(ctl, "_handled", OrderedDict())
    monkeypatch.setattr(ctl, "_forgotten_early", 0)
    return hooks


@pytest.fixture
async def unawaited(hooks: Hooks) -> AsyncGenerator[None]:
    """A dispatched handler nobody ran is a coroutine never awaited; close them."""
    yield
    for _, coro in hooks.dispatched:
        coro.close()


@pytest.fixture
def app(hooks: Hooks) -> FastAPI:
    """process_webhook behind one route whose handler records what it was given."""
    app = FastAPI()

    async def handler(event: StreamOnlineEventSub) -> None:
        if hooks.handler_error is not None:
            raise hooks.handler_error
        hooks.events.append(event)

    @app.post("/t")
    async def route(request: Request) -> Response:
        return await ctl.process_webhook(request, "/t", StreamOnlineEventSub, handler)

    return app


@pytest.fixture
async def client(app: FastAPI, unawaited: None) -> AsyncGenerator[httpx.AsyncClient]:
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as http:
        yield http


Post = Callable[..., Coroutine[Any, Any, httpx.Response]]
