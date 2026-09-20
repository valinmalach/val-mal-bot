from typing import Any
from urllib.parse import parse_qs

import httpx

from constants import TokenType
from models.auth.auth_response import RefreshResponse
from services.twitch import oauth
from tests.credentials import CLIENT_ID

SCOPES = ["chat:read", "moderator:manage:shoutouts"]
BOT_ID = "999"
BROADCASTER_ID = "111"

USER_CALLBACK = "/twitch/oauth/callback"
BROADCASTER_CALLBACK = "/twitch/oauth/callback/broadcaster"

TOKEN_URL = "https://id.twitch.tv/oauth2/token"
VALIDATE_URL = "https://id.twitch.tv/oauth2/validate"


def token_body(**overrides: Any) -> dict[str, Any]:
    return {
        "access_token": "access-1",
        "expires_in": 3600,
        "refresh_token": "refresh-1",
        "scope": list(SCOPES),
        "token_type": "bearer",
    } | overrides


def validation_body(user_id: str = BOT_ID, **overrides: Any) -> dict[str, Any]:
    return {
        "client_id": CLIENT_ID,
        "login": "somebody",
        "scopes": list(SCOPES),
        "user_id": user_id,
        "expires_in": 3600,
    } | overrides


def new_state(token_type: TokenType) -> str:
    """A live state for this identity, as /twitch-auth would have issued one."""
    url = oauth.create_authorization_start_url(token_type)
    return parse_qs(url.split("?", 1)[1])["state"][0]


def form(request: httpx.Request) -> dict[str, str]:
    return {k: v[0] for k, v in parse_qs(request.content.decode()).items()}


class Stored:
    """What the token manager was asked to keep, and whether it should refuse."""

    def __init__(self) -> None:
        self.user: list[RefreshResponse] = []
        self.broadcaster: list[RefreshResponse] = []
        self.error: Exception | None = None

    async def set_user_access_token(self, response: RefreshResponse) -> None:
        if self.error is not None:
            raise self.error
        self.user.append(response)

    async def set_broadcaster_access_token(self, response: RefreshResponse) -> None:
        if self.error is not None:
            raise self.error
        self.broadcaster.append(response)


class Said:
    """The admin channel, as the controller reached it."""

    def __init__(self) -> None:
        self.notified: list[tuple[str, str | None]] = []
        self.reported: list[str] = []

    async def notify(self, text: str, *, key: str | None = None) -> bool:
        self.notified.append((text, key))
        return True

    async def report(self, exc: Exception, context: str, **_: object) -> None:
        self.reported.append(context)

    @property
    def texts(self) -> list[str]:
        return [text for text, _ in self.notified]
