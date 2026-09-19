"""Fakes for the Twitch layer: a token manager, and a scripted HTTP transport."""

from typing import Any

import httpx

from constants import TokenType


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
