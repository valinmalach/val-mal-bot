import asyncio
import logging
from typing import Self, cast

import httpx

logger = logging.getLogger(__name__)


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


class HttpClientManager:
    _instance: HttpClientManager | None = None
    _client: httpx.AsyncClient | None = None
    _lock: asyncio.Lock = asyncio.Lock()

    def __new__(cls) -> Self:
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cast("Self", cls._instance)

    async def get_client(self) -> httpx.AsyncClient:
        """Get the global HTTP client, creating it if necessary"""
        if self._client is None:
            async with self._lock:
                if self._client is None:
                    self._client = httpx.AsyncClient(
                        limits=httpx.Limits(
                            max_keepalive_connections=20,
                            max_connections=50,
                            keepalive_expiry=30.0,
                        ),
                        timeout=httpx.Timeout(
                            connect=10.0, read=30.0, write=10.0, pool=10.0
                        ),
                        http2=True,
                        follow_redirects=True,
                    )
                    logger.info("Global HTTP client initialized")
        return self._client

    async def close(self) -> None:
        """Close the HTTP client and clean up resources"""
        if self._client is not None:
            async with self._lock:
                if self._client is not None:
                    await self._client.aclose()
                    self._client = None
                    logger.info("Global HTTP client closed")

    async def __aenter__(self):
        return await self.get_client()

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        # Don't close on context exit to maintain global connection pool
        pass

    async def request(
        self,
        method: str,
        url: str,
        *,
        headers: dict | None = None,
        params: dict | None = None,
        json: dict | None = None,
        data: dict | None = None,
        **kwargs,
    ) -> httpx.Response:
        """Make an HTTP request using the global client.

        Failures propagate unreported: this is the transport, and the caller
        that knows what the request was for is the one worth hearing from.
        """
        client = await self.get_client()
        return await client.request(
            method=method,
            url=url,
            headers=headers,
            params=params,
            json=json,
            data=data,
            **kwargs,
        )


http_client_manager = HttpClientManager()
