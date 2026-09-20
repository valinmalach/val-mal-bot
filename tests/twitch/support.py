"""Fakes for the Twitch layer: a token manager, and a scripted HTTP transport."""

import json
from collections.abc import Callable
from types import SimpleNamespace
from typing import Any

import httpx
import pendulum

from constants import TokenType

# The two callbacks a subscribe/unsubscribe test cares about, shared so the
# online and conflict-replacement tests agree on what this deployment answers on.
ONLINE = "https://bot.example/webhook/twitch"
OFFLINE = "https://bot.example/webhook/twitch/offline"

# Shared by the actions and subscribe-conflict tests, each of which patches this
# into config._settings themselves rather than through the directory's autouse
# `scopes` fixture -- neither wants the app scopes that one seeds.
BOT_SETTINGS = {"twitch_bot_user_id": "999", "twitch_broadcaster_id": "111"}


class FakeTokens:
    """The interface helix uses from the real token manager."""

    def __init__(self) -> None:
        self.tokens: dict[TokenType, str] = {
            TokenType.App: "app-token",
            TokenType.User: "user-token",
            TokenType.Broadcaster: "broadcaster-token",
        }
        self.stale: set[TokenType] = set()
        self.refresh_result = True
        self.refresh_error: Exception | None = None
        self.refreshed: list[TokenType] = []

    def token(self, token_type: TokenType) -> str:
        return self.tokens.get(token_type, "")

    def needs_refresh(self, token_type: TokenType) -> bool:
        return token_type in self.stale

    async def refresh(self, token_type: TokenType) -> bool:
        self.refreshed.append(token_type)
        if self.refresh_error is not None:
            raise self.refresh_error
        if self.refresh_result:
            self.tokens[token_type] = f"fresh-{len(self.refreshed)}"
            self.stale.discard(token_type)
        return self.refresh_result


def reply(
    status: int = 200, body: Any = None, text: str | None = None
) -> httpx.Response:
    """A canned response: JSON when `body` is given, otherwise raw `text`."""
    if body is not None:
        return httpx.Response(status, json=body)
    return httpx.Response(status, text=text or "")


class Script:
    """A transport handler that plays back outcomes in order, then repeats the last.

    An outcome is a Response, or an Exception to raise. Every request it saw is
    kept, so a test can say what was sent as well as what came back.
    """

    def __init__(self, *outcomes: httpx.Response | Exception) -> None:
        self.outcomes = list(outcomes)
        self.requests: list[httpx.Request] = []

    def __call__(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        index = min(len(self.requests) - 1, len(self.outcomes) - 1)
        outcome = self.outcomes[index]
        if isinstance(outcome, Exception):
            raise outcome
        return outcome

    def client(self) -> httpx.AsyncClient:
        return httpx.AsyncClient(transport=httpx.MockTransport(self))


def steps(script: Script) -> list[tuple[str, str]]:
    """Every request a Script saw, as (method, path) in order."""
    return [(r.method, r.url.path) for r in script.requests]


def body(request: httpx.Request) -> dict[str, Any]:
    return json.loads(request.content)


def user_json(id: str = "1", login: str = "bob", **overrides: Any) -> dict[str, Any]:
    return {
        "id": id,
        "login": login,
        "display_name": login.title(),
        "type": "",
        "broadcaster_type": "",
        "description": "",
        "profile_image_url": "https://cdn/p.png",
        "offline_image_url": "",
        "created_at": "2020-01-01T00:00:00Z",
    } | overrides


def stream_json(id: str = "10", user_id: str = "1", **overrides: Any) -> dict[str, Any]:
    return {
        "id": id,
        "user_id": user_id,
        "user_login": "bob",
        "user_name": "Bob",
        "game_id": "g1",
        "game_name": "A Game",
        "type": "live",
        "title": "playing",
        "tags": ["English"],
        "viewer_count": 5,
        "started_at": "2026-01-01T00:00:00Z",
        "language": "en",
        "thumbnail_url": "https://cdn/{width}x{height}.jpg",
        "is_mature": False,
    } | overrides


def video_json(
    id: str = "v1", stream_id: str | None = "10", **overrides: Any
) -> dict[str, Any]:
    return {
        "id": id,
        "stream_id": stream_id,
        "user_id": "1",
        "user_login": "bob",
        "user_name": "Bob",
        "title": "vod",
        "description": "",
        "created_at": "2026-01-01T00:00:00Z",
        "published_at": "2026-01-01T00:00:00Z",
        "url": f"https://twitch.tv/videos/{id}",
        "thumbnail_url": "",
        "viewable": "public",
        "view_count": 1,
        "language": "en",
        "type": "archive",
        "duration": "1h2m3s",
    } | overrides


def subscription_json(
    id: str = "s1",
    type: str = "stream.online",
    status: str = "enabled",
    callback: str | None = "https://bot.example/webhook/twitch",
    **condition: str,
) -> dict[str, Any]:
    return {
        "id": id,
        "status": status,
        "type": type,
        "version": "1",
        "condition": condition or {"broadcaster_user_id": "1"},
        "created_at": "2026-01-01T00:00:00Z",
        "transport": {"method": "webhook", "callback": callback},
        "cost": 1,
    }


def page(data: list[dict[str, Any]], cursor: str | None = None) -> dict[str, Any]:
    """A Helix list reply, with the fields every list carries."""
    return {
        "data": data,
        "total": len(data),
        "total_cost": 0,
        "max_total_cost": 10,
        "pagination": {"cursor": cursor} if cursor else {},
    }


class TokenDb:
    """What the token manager asks the database: rows for a select, anything else
    is recorded (the upsert), and `error` makes every statement raise."""

    def __init__(self) -> None:
        self.rows: list[Any] = []
        self.statements: list[Any] = []
        self.error: Exception | None = None

    async def execute(self, statement: Any) -> Any:
        self.statements.append(statement)
        if self.error is not None:
            raise self.error
        rows = list(self.rows)
        return SimpleNamespace(scalars=lambda: SimpleNamespace(all=lambda: rows))


class Scope:
    """An async context manager over a TokenDb. A class, not a generator: a tool
    reads a `yield` after a `raise` as unreachable and deletes it."""

    def __init__(self, db: TokenDb) -> None:
        self.db = db

    async def __aenter__(self) -> TokenDb:
        return self.db

    async def __aexit__(self, *exc: object) -> None:
        return None


# Shared by the token manager's refresh and concurrency tests, which otherwise
# each defined their own identical copies.
NOW = pendulum.datetime(2026, 6, 15, 12)
TOKEN_URL = "https://id.twitch.tv/oauth2/token"
Http = Callable[..., Script]
Notices = list[tuple[str, str | None]]

APP_OK = {"access_token": "new-app", "expires_in": 3600, "token_type": "bearer"}
USER_OK = {
    "access_token": "new-access",
    "refresh_token": "new-refresh",
    "expires_in": 14000,
    "scope": ["chat:read"],
    "token_type": "bearer",
}


def chat_event(
    text: str = "hello",
    *,
    chatter_id: str = "3",
    chatter_login: str = "chatter",
    chatter_name: str = "Chatter",
    broadcaster_id: str = "111",
    broadcaster_login: str = "bob",
    badges: tuple[str, ...] = (),
    source_broadcaster_id: str | None = None,
) -> dict[str, Any]:
    """A channel.chat.message notification body, as Twitch delivers it."""
    event: dict[str, Any] = {
        "broadcaster_user_id": broadcaster_id,
        "broadcaster_user_login": broadcaster_login,
        "broadcaster_user_name": broadcaster_login.title(),
        "chatter_user_id": chatter_id,
        "chatter_user_login": chatter_login,
        "chatter_user_name": chatter_name,
        "message": {"text": text, "fragments": []},
        "badges": [{"set_id": b, "id": "1", "info": ""} for b in badges],
    }
    if source_broadcaster_id is not None:
        event["source_broadcaster_user_id"] = source_broadcaster_id
    return {"subscription": {"type": "channel.chat.message"}, "event": event}


def redemption_event(
    user_id: str = "5", user_login: str = "redeemer", broadcaster_id: str = "111"
) -> dict[str, Any]:
    return {
        "subscription": {"type": "channel.channel_points_custom_reward_redemption.add"},
        "event": {
            "broadcaster_user_id": broadcaster_id,
            "user_id": user_id,
            "user_login": user_login,
        },
    }


def channel_json(
    broadcaster_id: str = "1",
    login: str = "bob",
    name: str = "Bob",
    game: str = "A Game",
) -> dict[str, Any]:
    return {
        "broadcaster_id": broadcaster_id,
        "broadcaster_login": login,
        "broadcaster_name": name,
        "broadcaster_language": "en",
        "game_name": game,
        "game_id": "g1",
        "title": "t",
        "delay": 0,
        "tags": [],
        "content_classification_labels": [],
        "is_branded_content": False,
    }
