"""One way to call Helix: tokens, retry, status and parsing behind one door.

Anything that stops a call completing raises ``HelixError``. Nothing here
reports: whoever catches has the context worth reporting, and three layers each
announcing the same failure is what this replaced.
"""

import asyncio
import logging
from typing import Any, Literal

import httpx
from pydantic import BaseModel

from config import settings
from constants import TokenType
from services.helper.http_client import http_client_manager, is_transient_network_error
from services.twitch.token_manager import token_manager

logger = logging.getLogger(__name__)

_BASE_URL = "https://api.twitch.tv/helix"
_MAX_ATTEMPTS = 3
_BACKOFF_SECONDS = 1.0

Method = Literal["GET", "POST", "DELETE"]

# Repeating a read or a delete costs nothing; repeating a POST can send a second
# chat message. docs/adr/0002-helix-posts-are-not-retried.md has the why.
_REPEATABLE_METHODS = frozenset({"GET", "DELETE"})


class HelixError(Exception):
    """A Helix call that did not complete.

    ``status`` and ``response`` are set when Twitch answered, so a caller that
    cares about one code — a 429 and its headers, say — can still reach it.
    """

    def __init__(
        self,
        message: str,
        *,
        status: int | None = None,
        response: httpx.Response | None = None,
    ) -> None:
        super().__init__(message)
        self.status = status
        self.response = response


def _headers(token_type: TokenType) -> dict[str, str]:
    return {
        "Client-ID": settings.twitch_client_id,
        "Authorization": f"Bearer {token_manager.token(token_type)}",
    }


async def _ensure_token(token_type: TokenType) -> None:
    """Refresh before the call when the token is missing or due to lapse."""
    if token_manager.token(token_type) and not token_manager.needs_refresh(token_type):
        return
    if not await token_manager.refresh(token_type):
        raise HelixError(f"No {token_type.value} token available and refresh failed")


async def _send(
    method: Method,
    url: str,
    token_type: TokenType,
    params: dict[str, Any] | None,
    json: dict[str, Any] | None,
) -> httpx.Response:
    return await http_client_manager.request(
        method, url, headers=_headers(token_type), params=params, json=json
    )


async def request(
    method: Method,
    path: str,
    *,
    params: dict[str, Any] | None = None,
    json: dict[str, Any] | None = None,
    token_type: TokenType = TokenType.App,
    repeatable: bool | None = None,
) -> httpx.Response:
    """Call Helix, refreshing a rejected token once and retrying where it is safe.

    ``repeatable`` overrides the per-method default for a call that is provably
    safe to send twice.
    """
    url = f"{_BASE_URL}{path}"
    safe_to_repeat = method in _REPEATABLE_METHODS if repeatable is None else repeatable
    attempts = _MAX_ATTEMPTS if safe_to_repeat else 1

    for attempt in range(attempts):
        await _ensure_token(token_type)

        try:
            response = await _send(method, url, token_type, params, json)
        except Exception as e:
            if attempt + 1 >= attempts or not is_transient_network_error(e):
                raise HelixError(f"{method} {path} failed: {e}") from e
            await asyncio.sleep(_BACKOFF_SECONDS * 2**attempt)
            continue

        if response.status_code == 401:
            # Rejected, so the request cannot have taken effect: re-sending it
            # is safe for every method, POST included.
            logger.warning("Unauthorized %s %s, refreshing the token", method, path)
            if not await token_manager.refresh(token_type):
                raise HelixError(
                    f"{method} {path} was unauthorized and the refresh failed",
                    status=401,
                    response=response,
                )
            response = await _send(method, url, token_type, params, json)

        if 200 <= response.status_code < 300:
            return response

        if 500 <= response.status_code < 600 and attempt + 1 < attempts:
            logger.warning(
                "Server error %s on %s %s, attempt %d",
                response.status_code,
                method,
                path,
                attempt + 1,
            )
            await asyncio.sleep(_BACKOFF_SECONDS * 2**attempt)
            continue

        raise HelixError(
            f"{method} {path} returned {response.status_code}: {response.text}",
            status=response.status_code,
            response=response,
        )

    # Unreachable: the last attempt either returns or raises above.
    raise HelixError(f"{method} {path} gave up after {attempts} attempts")


async def fetch[T: BaseModel](
    model: type[T],
    method: Method,
    path: str,
    *,
    params: dict[str, Any] | None = None,
    json: dict[str, Any] | None = None,
    token_type: TokenType = TokenType.App,
    repeatable: bool | None = None,
) -> T:
    """Call Helix and parse the reply, so a body that will not parse is a failure."""
    response = await request(
        method,
        path,
        params=params,
        json=json,
        token_type=token_type,
        repeatable=repeatable,
    )
    try:
        return model.model_validate(response.json())
    except Exception as e:
        raise HelixError(
            f"{method} {path} answered with a body that would not parse: {e}",
            status=response.status_code,
            response=response,
        ) from e
