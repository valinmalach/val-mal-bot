"""Twitch OAuth tokens, held in the oauth_token table."""

import asyncio
import logging
from collections.abc import Awaitable, Callable
from typing import Self, cast

import pendulum
from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert

from config import settings
from constants import TokenType
from db import OAuthToken, OAuthTokenKey
from db.session import session_scope
from models import AuthResponse, RefreshResponse
from services.config import config
from services.helper.helper import send_message
from services.helper.http_client import http_client_manager

logger = logging.getLogger(__name__)

# Treat a token as due slightly before Twitch actually expires it, so a request
# already in flight cannot cross the boundary.
_EXPIRY_MARGIN_SECONDS = 60

_KEYS = {
    TokenType.App: OAuthTokenKey.APP,
    TokenType.User: OAuthTokenKey.USER,
    TokenType.Broadcaster: OAuthTokenKey.BROADCASTER,
}


class TwitchTokenManager:
    _instance: TwitchTokenManager | None = None
    _access: dict[TokenType, str]
    _refresh: dict[TokenType, str]
    _expires_at: dict[TokenType, pendulum.DateTime]
    _locks: dict[TokenType, asyncio.Lock]

    def __new__(cls) -> Self:
        if cls._instance is None:
            instance = super().__new__(cls)
            instance._access = {}
            instance._refresh = {}
            instance._expires_at = {}
            instance._locks = {token_type: asyncio.Lock() for token_type in TokenType}
            cls._instance = instance
        return cast("Self", cls._instance)

    async def load(self) -> None:
        """Read every stored token into memory, replacing what is held."""
        async with session_scope() as session:
            rows = (await session.execute(select(OAuthToken))).scalars().all()

        access: dict[TokenType, str] = {}
        refresh: dict[TokenType, str] = {}
        expires_at: dict[TokenType, pendulum.DateTime] = {}

        by_key = {row.key: row for row in rows}
        for token_type, key in _KEYS.items():
            row = by_key.get(key)
            if row is None:
                continue
            access[token_type] = row.access_token
            if row.refresh_token:
                refresh[token_type] = row.refresh_token
            if row.expires_at:
                expires_at[token_type] = pendulum.instance(row.expires_at)

        # Swapped rather than updated in place: a row deleted or an expiry
        # cleared in the database has to disappear from memory too.
        self._access, self._refresh, self._expires_at = access, refresh, expires_at
        logger.info("Loaded %d Twitch token(s) from the database", len(self._access))

    async def _store(
        self,
        token_type: TokenType,
        access_token: str,
        refresh_token: str | None,
        expires_in: int | None,
        scopes: list[str] | None = None,
    ) -> None:
        self._access[token_type] = access_token
        if refresh_token:
            self._refresh[token_type] = refresh_token

        expires_at = None
        if expires_in is None:
            _ = self._expires_at.pop(token_type, None)
        else:
            expires_at = pendulum.now("UTC").add(seconds=expires_in)
            self._expires_at[token_type] = expires_at

        values = {
            "key": _KEYS[token_type],
            "access_token": access_token,
            "refresh_token": refresh_token or self._refresh.get(token_type),
            "expires_at": expires_at,
            "scopes": scopes or [],
        }
        statement = insert(OAuthToken).values(**values)
        updates = {k: getattr(statement.excluded, k) for k in values if k != "key"}
        async with session_scope() as session:
            await session.execute(
                statement.on_conflict_do_update(index_elements=["key"], set_=updates)
            )

    def needs_refresh(self, token_type: TokenType) -> bool:
        """Whether the token should be refreshed before it is used again.

        False when no expiry is known, which falls back to reacting to a 401.
        """
        expires_at = self._expires_at.get(token_type)
        if expires_at is None:
            return False
        return pendulum.now("UTC") >= expires_at.subtract(
            seconds=_EXPIRY_MARGIN_SECONDS
        )

    def token(self, token_type: TokenType) -> str:
        return self._access.get(token_type, "")

    @property
    def app_access_token(self) -> str:
        return self.token(TokenType.App)

    @property
    def user_access_token(self) -> str:
        return self.token(TokenType.User)

    @property
    def broadcaster_access_token(self) -> str:
        return self.token(TokenType.Broadcaster)

    async def _guarded(
        self, token_type: TokenType, refresh: Callable[[], Awaitable[bool]]
    ) -> bool:
        """Run one refresh at a time per identity, reusing a concurrent result.

        Twitch invalidates a refresh token the moment it is used, so two callers
        racing on the same one leave the loser holding a dead token.
        """
        before = self.token(token_type)
        async with self._locks[token_type]:
            current = self.token(token_type)
            # Only a token that changed while this caller waited proves someone
            # else refreshed. Testing the expiry instead made a 401 unrecoverable:
            # a revoked token still looks current, so the refresh never ran.
            if current and current != before:
                return True
            return await refresh()

    async def refresh_app_access_token(self) -> bool:
        return await self._guarded(TokenType.App, self._refresh_app_access_token)

    async def _refresh_app_access_token(self) -> bool:
        scopes = cast("list[str]", config.setting("twitch_app_scopes", []))
        params = {
            "client_id": settings.twitch_client_id,
            "client_secret": settings.twitch_client_secret,
            "grant_type": "client_credentials",
            "scope": " ".join(scopes),
        }
        response = await http_client_manager.request(
            "POST", "https://id.twitch.tv/oauth2/token", params=params
        )

        if response.status_code < 200 or response.status_code >= 300:
            logger.error(f"Token refresh failed with status={response.status_code}")
            await send_message(
                f"Failed to refresh access token: {response.status_code} {response.text}",
                config.channel("bot_admin"),
            )
            return False

        auth_response = AuthResponse.model_validate(response.json())
        if auth_response.token_type != "bearer":
            logger.error(f"Unexpected token type received: {auth_response.token_type}")
            await send_message(
                f"Unexpected token type: {auth_response.token_type}",
                config.channel("bot_admin"),
            )
            return False

        await self._store(
            TokenType.App,
            auth_response.access_token,
            None,
            auth_response.expires_in,
            scopes,
        )
        return True

    async def set_user_access_token(self, auth_response: RefreshResponse) -> None:
        await self._store_refreshed(TokenType.User, auth_response)

    async def set_broadcaster_access_token(
        self, auth_response: RefreshResponse
    ) -> None:
        await self._store_refreshed(TokenType.Broadcaster, auth_response)

    async def _store_refreshed(
        self, token_type: TokenType, auth_response: RefreshResponse
    ) -> None:
        scope = auth_response.scope
        await self._store(
            token_type,
            auth_response.access_token,
            auth_response.refresh_token,
            auth_response.expires_in,
            scope if isinstance(scope, list) else [scope],
        )

    async def refresh_user_access_token(self, broadcaster: bool = False) -> bool:
        token_type = TokenType.Broadcaster if broadcaster else TokenType.User
        return await self._guarded(
            token_type, lambda: self._refresh_user_access_token(token_type)
        )

    async def _refresh_user_access_token(self, token_type: TokenType) -> bool:
        label = "broadcaster" if token_type is TokenType.Broadcaster else "user"

        refresh_token = self._refresh.get(token_type)
        if not refresh_token:
            logger.error(f"No {label} refresh token available")
            await send_message(
                f"No {label} refresh token available", config.channel("bot_admin")
            )
            return False

        params = {
            "client_id": settings.twitch_client_id,
            "client_secret": settings.twitch_client_secret,
            "grant_type": "refresh_token",
            "refresh_token": refresh_token,
        }
        response = await http_client_manager.request(
            "POST", "https://id.twitch.tv/oauth2/token", params=params
        )

        if response.status_code < 200 or response.status_code >= 300:
            logger.error(
                f"{label} token refresh failed with status={response.status_code}"
            )
            await send_message(
                f"Failed to refresh {label} access token: "
                f"{response.status_code} {response.text}",
                config.channel("bot_admin"),
            )
            return False

        auth_response = RefreshResponse.model_validate(response.json())
        if auth_response.token_type != "bearer":
            logger.error(f"Unexpected token type received: {auth_response.token_type}")
            await send_message(
                f"Unexpected token type: {auth_response.token_type}",
                config.channel("bot_admin"),
            )
            return False

        await self._store_refreshed(token_type, auth_response)
        return True


token_manager = TwitchTokenManager()
