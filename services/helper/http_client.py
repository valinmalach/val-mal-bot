"""The one HTTP client every outbound call shares.

One client, so connections are pooled across Twitch's API, its OAuth endpoint
and everything else; created on first use rather than at import, because
importing a module must not require a network stack.
"""

import httpx


def is_transient_network_error(exc: BaseException) -> bool:
    """True for timeouts and connection issues where str(exc) may be empty (e.g. httpx.ConnectTimeout)."""
    msg = str(exc).lower()
    name = type(exc).__name__.lower()
    terms = (
        "connection",
        "timeout",
        "network",
        "remoteprotocolerror",
    )
    return any(t in msg or t in name for t in terms)


_client: httpx.AsyncClient | None = None


def client() -> httpx.AsyncClient:
    """The process-wide client, created on first use.

    Unguarded on purpose: there is no await between the test and the
    assignment, so no other task can run in between and race a second client
    into existence.
    """
    global _client
    if _client is None:
        _client = httpx.AsyncClient(
            limits=httpx.Limits(
                max_keepalive_connections=20,
                max_connections=50,
                keepalive_expiry=30.0,
            ),
            timeout=httpx.Timeout(connect=10.0, read=30.0, write=10.0, pool=10.0),
            http2=True,
            follow_redirects=True,
        )
    return _client


async def aclose() -> None:
    """Close the client and its pool. Call this on shutdown."""
    global _client
    if _client is not None:
        await _client.aclose()
        _client = None
