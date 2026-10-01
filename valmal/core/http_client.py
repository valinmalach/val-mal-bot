"""The one HTTP client every outbound call shares.

One session, so connections are pooled across Twitch's API, its OAuth endpoint
and everything else; created on first use rather than at import, because
importing a module must not require a network stack. aiohttp rather than a
second library because discord.py already loads it.
"""

from collections.abc import Mapping
from dataclasses import dataclass
from json import loads
from typing import Any

import aiohttp

_session: aiohttp.ClientSession | None = None

# Worth another attempt: the request never reached the server, or the connection
# died mid-reply. Not the rest of aiohttp.ClientError: an invalid URL fails again
# identically, and a malformed reply head or a redirect loop is the server's
# answer, not a lost one.
TRANSIENT = (TimeoutError, aiohttp.ClientConnectionError, aiohttp.ClientPayloadError)


@dataclass(frozen=True)
class Reply:
    """A reply that has already been read in full.

    An aiohttp response has to be read before its connection goes back to the
    pool, so ``request`` reads it and hands back this instead; nothing outside
    this module holds a live response. The headers the real client supplies
    look a name up in any case, as HTTP says they should.
    """

    status: int
    headers: Mapping[str, str]
    text: str

    def json(self) -> Any:
        return loads(self.text)


def _client() -> aiohttp.ClientSession:
    """The process-wide session, created on first use.

    Unguarded on purpose: there is no await between the test and the
    assignment, so no other task can run in between and race a second session
    into existence.
    """
    global _session
    if _session is None:
        _session = aiohttp.ClientSession(
            connector=aiohttp.TCPConnector(limit=50, keepalive_timeout=30.0),
            # aiohttp has no write timeout; connect covers waiting for the pool.
            timeout=aiohttp.ClientTimeout(
                connect=10.0, sock_connect=10.0, sock_read=30.0
            ),
        )
    return _session


async def request(
    method: str,
    url: str,
    *,
    headers: Mapping[str, str] | None = None,
    params: Mapping[str, Any] | None = None,
    json: Any = None,
    data: Mapping[str, str] | None = None,
) -> Reply:
    """Send one request and return its reply, read in full."""
    async with _client().request(
        method, url, headers=headers, params=params, json=json, data=data
    ) as response:
        # Replaced, not raised: an error page need not be UTF-8, and raising
        # would lose the status the caller decides a retry or a re-queue by.
        text = await response.text(errors="replace")
        return Reply(response.status, response.headers, text)


async def aclose() -> None:
    """Close the session and its pool. Call this on shutdown."""
    global _session
    if _session is not None:
        await _session.close()
        _session = None
