"""Owner-initiated Twitch authorization-code grants."""

import secrets
import time
from dataclasses import dataclass
from urllib.parse import urlencode

from config import settings
from constants import TokenType
from services.config import config

_AUTHORIZATION_TTL_SECONDS = 10 * 60
_AUTHORIZATION_ENDPOINT = "https://id.twitch.tv/oauth2/authorize"


@dataclass(frozen=True)
class PendingAuthorization:
    token_type: TokenType
    expires_at: float


_pending_authorizations: dict[str, PendingAuthorization] = {}


def _require_user_identity(token_type: TokenType) -> None:
    if token_type not in {TokenType.User, TokenType.Broadcaster}:
        raise ValueError(f"OAuth authorization is not available for {token_type.value}")


def _prune_expired_authorizations() -> None:
    now = time.monotonic()
    expired = [
        state
        for state, authorization in _pending_authorizations.items()
        if authorization.expires_at <= now
    ]
    for state in expired:
        del _pending_authorizations[state]


def configured_scopes() -> list[str]:
    scopes = config.setting("twitch_app_scopes")
    if not isinstance(scopes, list) or not scopes:
        raise RuntimeError("twitch_app_scopes must be a non-empty JSON list")
    if not all(isinstance(scope, str) and scope for scope in scopes):
        raise RuntimeError("twitch_app_scopes contains an invalid scope")
    return scopes


def callback_uri(token_type: TokenType) -> str:
    _require_user_identity(token_type)
    path = (
        "/twitch/oauth/callback/broadcaster"
        if token_type is TokenType.Broadcaster
        else "/twitch/oauth/callback"
    )
    return f"{settings.app_url.rstrip('/')}{path}"


def expected_user_id(token_type: TokenType) -> str:
    _require_user_identity(token_type)
    setting = (
        "twitch_broadcaster_id"
        if token_type is TokenType.Broadcaster
        else "twitch_bot_user_id"
    )
    user_id = config.setting(setting)
    if not isinstance(user_id, str) or not user_id:
        raise RuntimeError(f"{setting} must be a non-empty string")
    return user_id


def create_authorization_start_url(token_type: TokenType) -> str:
    """Create a short-lived link suitable for an ephemeral Discord button."""
    _require_user_identity(token_type)
    _prune_expired_authorizations()
    state = secrets.token_urlsafe(32)
    _pending_authorizations[state] = PendingAuthorization(
        token_type=token_type,
        expires_at=time.monotonic() + _AUTHORIZATION_TTL_SECONDS,
    )
    query = urlencode({"state": state})
    return (
        f"{settings.app_url.rstrip('/')}/twitch/oauth/start/{token_type.value}?{query}"
    )


def authorization_url(token_type: TokenType, state: str) -> str:
    """Build Twitch's consent URL if state is live and bound to this identity."""
    _require_user_identity(token_type)
    _prune_expired_authorizations()
    pending = _pending_authorizations.get(state)
    if pending is None or pending.token_type is not token_type:
        raise ValueError("Invalid or expired OAuth state")

    query = urlencode(
        {
            "response_type": "code",
            "client_id": settings.twitch_client_id,
            "redirect_uri": callback_uri(token_type),
            "scope": " ".join(configured_scopes()),
            "state": state,
            "force_verify": "true",
        }
    )
    return f"{_AUTHORIZATION_ENDPOINT}?{query}"


def consume_authorization(token_type: TokenType, state: str) -> bool:
    """Consume a state once, accepting it only for the identity it was issued for."""
    _require_user_identity(token_type)
    _prune_expired_authorizations()
    pending = _pending_authorizations.get(state)
    if pending is None or pending.token_type is not token_type:
        return False
    del _pending_authorizations[state]
    return True
