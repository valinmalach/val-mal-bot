"""The two Twitch authorization-code flows, on their own router.

Separate from the webhook controller because it shares nothing with it: no
signature checking, no EventSub models, a different path prefix.
"""

import logging

from discord.utils import escape_markdown
from fastapi import APIRouter, HTTPException, Response
from fastapi.responses import RedirectResponse

from config import settings
from constants import TokenType
from errors import notify, report
from models import RefreshResponse, TokenValidationResponse
from services.helper.http_client import http_client_manager
from services.twitch.oauth import (
    authorization_url,
    callback_uri,
    configured_scopes,
    consume_authorization,
    expected_user_id,
)
from services.twitch.token_manager import token_manager

logger = logging.getLogger(__name__)

twitch_oauth_router = APIRouter()

# Twitch's denial text is short; anything longer is not from Twitch.
_MAX_DENIAL_REASON = 200


async def _validate_oauth_identity(
    auth_response: RefreshResponse,
    token_type: TokenType,
    endpoint: str,
) -> TokenValidationResponse:
    response = await http_client_manager.request(
        "GET",
        "https://id.twitch.tv/oauth2/validate",
        headers={"Authorization": f"OAuth {auth_response.access_token}"},
    )
    if response.status_code < 200 or response.status_code >= 300:
        logger.error(
            "OAuth token validation failed with status=%s", response.status_code
        )
        await notify(
            f"Failed to validate the Twitch token from {endpoint}: "
            f"{response.status_code} {response.text}"
        )
        raise HTTPException(status_code=500, detail="Twitch token validation failed")

    validation = TokenValidationResponse.model_validate(response.json())
    expected_id = expected_user_id(token_type)
    missing_scopes = sorted(set(configured_scopes()) - set(validation.scopes))

    problems = []
    if validation.client_id != settings.twitch_client_id:
        problems.append("the token belongs to a different Twitch application")
    if validation.user_id != expected_id:
        problems.append(
            f"expected Twitch user ID {expected_id}, but {validation.login} "
            f"authorized as ID {validation.user_id}"
        )
    if missing_scopes:
        problems.append(f"missing scopes: {', '.join(missing_scopes)}")

    if problems:
        message = f"Rejected {token_type.value} authorization: {'; '.join(problems)}"
        logger.warning(message)
        await notify(message)
        raise HTTPException(status_code=400, detail=message)
    return validation


async def _oauth_callback_common(
    code: str | None,
    state: str,
    endpoint: str,
    token_type: TokenType,
    error: str | None,
    error_description: str | None,
) -> tuple[RefreshResponse, TokenValidationResponse]:
    if not consume_authorization(token_type, state):
        logger.warning("400: Bad request on %s. Invalid OAuth state.", endpoint)
        await notify(f"400: Bad request on {endpoint}. Invalid or expired OAuth state.")
        raise HTTPException(status_code=400, detail="Invalid or expired OAuth state")

    if error:
        # Both arrive in the callback query string, so neither is Twitch's word
        # for anything. The caller is told only that it was denied; the reason
        # goes to the admin channel, which is where it is actually read, capped
        # and with mentions stripped on the way. !r for the log, where a bare
        # newline would forge a line instead.
        reason = (error_description or error)[:_MAX_DENIAL_REASON]
        logger.warning("Twitch authorization denied on %s: %r", endpoint, reason)
        await notify(
            f"Twitch authorization denied on {endpoint}: {escape_markdown(reason)}",
            key=f"oauth-denied:{endpoint}",
        )
        raise HTTPException(status_code=400, detail="Twitch authorization denied")
    if not code:
        raise HTTPException(status_code=400, detail="Missing Twitch authorization code")

    params = {
        "client_id": settings.twitch_client_id,
        "client_secret": settings.twitch_client_secret,
        "code": code,
        "grant_type": "authorization_code",
        "redirect_uri": callback_uri(token_type),
    }
    response = await http_client_manager.request(
        "POST", "https://id.twitch.tv/oauth2/token", data=params
    )

    if response.status_code < 200 or response.status_code >= 300:
        logger.error(
            f"Token exchange failed with status={response.status_code}, response={response.text}"
        )
        await notify(
            f"Failed to exchange token: {response.status_code} {response.text}"
        )
        raise HTTPException(status_code=500)

    auth_response = RefreshResponse.model_validate(response.json())
    if auth_response.token_type != "bearer":
        logger.error(
            f"Token exchange failed: unexpected token type {auth_response.token_type}"
        )
        await notify(
            f"Failed to exchange token: unexpected token type {auth_response.token_type}"
        )
        raise HTTPException(status_code=500)
    validation = await _validate_oauth_identity(auth_response, token_type, endpoint)
    return auth_response, validation


@twitch_oauth_router.get("/twitch/oauth/start/{identity}")
async def twitch_oauth_start(identity: str, state: str) -> Response:
    try:
        token_type = TokenType(identity)
        return RedirectResponse(authorization_url(token_type, state), status_code=302)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from None


@twitch_oauth_router.get("/twitch/oauth/callback")
async def twitch_oauth_callback(
    state: str,
    code: str | None = None,
    error: str | None = None,
    error_description: str | None = None,
) -> Response:
    try:
        auth_response, validation = await _oauth_callback_common(
            code,
            state,
            "/twitch/oauth/callback",
            TokenType.User,
            error,
            error_description,
        )
        await token_manager.set_user_access_token(auth_response)
        return Response(
            f"Authorization successful for bot account {validation.login} "
            f"({validation.user_id}). You can close this tab.",
            status_code=200,
        )
    except HTTPException:
        raise
    except Exception as e:
        await report(e, "500: Internal server error on /twitch/oauth/callback")
        raise HTTPException(status_code=500) from e


@twitch_oauth_router.get("/twitch/oauth/callback/broadcaster")
async def twitch_oauth_callback_broadcaster(
    state: str,
    code: str | None = None,
    error: str | None = None,
    error_description: str | None = None,
) -> Response:
    try:
        auth_response, validation = await _oauth_callback_common(
            code,
            state,
            "/twitch/oauth/callback/broadcaster",
            TokenType.Broadcaster,
            error,
            error_description,
        )
        await token_manager.set_broadcaster_access_token(auth_response)
        return Response(
            f"Authorization successful for broadcaster account {validation.login} "
            f"({validation.user_id}). You can close this tab.",
            status_code=200,
        )
    except HTTPException:
        raise
    except Exception as e:
        await report(
            e, "500: Internal server error on /twitch/oauth/callback/broadcaster"
        )
        raise HTTPException(status_code=500) from e
