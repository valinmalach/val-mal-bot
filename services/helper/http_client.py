import asyncio
import logging
from typing import Optional

import httpx
import sentry_sdk

logger = logging.getLogger(__name__)


class HttpClientManager:
    _instance: Optional["HttpClientManager"] = None
    _client: Optional[httpx.AsyncClient] = None
    _lock: asyncio.Lock = asyncio.Lock()

    def __new__(cls) -> "HttpClientManager":
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

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
        headers: Optional[dict] = None,
        params: Optional[dict] = None,
        json: Optional[dict] = None,
        data: Optional[dict] = None,
        **kwargs,
    ) -> httpx.Response:
        """Make an HTTP request using the global client"""
        client = await self.get_client()
        try:
            return await client.request(
                method=method,
                url=url,
                headers=headers,
                params=params,
                json=json,
                data=data,
                **kwargs,
            )
        except Exception as e:
            logger.error(f"HTTP request failed: {method} {url} - {e}")
            sentry_sdk.capture_exception(e)
            raise


# Global instance
http_client_manager = HttpClientManager()
