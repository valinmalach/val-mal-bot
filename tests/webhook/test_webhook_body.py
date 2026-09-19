from collections.abc import AsyncIterator
from types import SimpleNamespace
from typing import Any

import pytest
from fastapi import HTTPException

import controller.twitch as ctl
from tests.webhook.conftest import Hooks

pytestmark = pytest.mark.anyio


class Streamed:
    """A request that counts how often its body was read off the wire."""

    def __init__(self, *chunks: bytes) -> None:
        self.chunks = chunks
        self.reads = 0

    async def stream(self) -> AsyncIterator[bytes]:
        self.reads += 1
        for chunk in self.chunks:
            yield chunk


async def read(request: Streamed | SimpleNamespace) -> bytes:
    return await ctl._bounded_body(request, "/t")  # pyright: ignore[reportArgumentType]


class TestBoundedBody:
    async def test_joins_the_chunks_in_order(self, hooks: Hooks) -> None:
        assert await read(Streamed(b"ab", b"cd", b"ef")) == b"abcdef"

    async def test_an_empty_body_is_empty_bytes_not_none(self, hooks: Hooks) -> None:
        assert await read(Streamed()) == b""

    async def test_reads_the_stream_once_and_caches_onto_the_request(
        self, hooks: Hooks
    ) -> None:
        """The later json() must read what was counted here, as Starlette's body() does."""
        request = Streamed(b"payload")

        first = await read(request)
        second = await read(request)

        assert first == second == b"payload"
        assert request.reads == 1
        assert request._body == b"payload"  # pyright: ignore[reportAttributeAccessIssue]

    async def test_a_body_already_cached_is_returned_without_touching_the_stream(
        self, hooks: Hooks
    ) -> None:
        request: Any = SimpleNamespace(_body=b"cached")

        assert await read(request) == b"cached"

    async def test_an_empty_cached_body_is_still_cached(self, hooks: Hooks) -> None:
        """b"" is a body that was read; only None means it was not."""
        request = Streamed(b"would-be-read")
        request._body = b""  # pyright: ignore[reportAttributeAccessIssue]

        assert await read(request) == b""
        assert request.reads == 0

    async def test_exactly_the_limit_is_accepted(self, hooks: Hooks) -> None:
        body = b"x" * ctl._MAX_BODY_BYTES

        assert await read(Streamed(body)) == body

    async def test_one_byte_over_the_limit_is_refused_and_announced(
        self, hooks: Hooks
    ) -> None:
        with pytest.raises(HTTPException) as refused:
            await read(Streamed(b"x" * (ctl._MAX_BODY_BYTES + 1)))

        assert refused.value.status_code == 413
        assert hooks.notified == [
            ("413: Oversized request on /t, refused part-read.", None)
        ]

    async def test_the_limit_counts_across_chunks_not_per_chunk(
        self, hooks: Hooks
    ) -> None:
        half = b"x" * (ctl._MAX_BODY_BYTES // 2 + 1)

        with pytest.raises(HTTPException):
            await read(Streamed(half, half))

    async def test_stops_reading_at_the_chunk_that_crosses_the_limit(
        self, hooks: Hooks
    ) -> None:
        consumed: list[int] = []

        class Endless(Streamed):
            async def stream(self) -> AsyncIterator[bytes]:
                for n in range(10_000):
                    consumed.append(n)
                    yield b"x" * 65_536

        with pytest.raises(HTTPException):
            await read(Endless())

        assert len(consumed) <= ctl._MAX_BODY_BYTES // 65_536 + 2

    async def test_a_refused_body_is_not_cached_for_the_next_reader(
        self, hooks: Hooks
    ) -> None:
        request = Streamed(b"x" * (ctl._MAX_BODY_BYTES + 1))

        with pytest.raises(HTTPException):
            await read(request)

        assert not hasattr(request, "_body")
