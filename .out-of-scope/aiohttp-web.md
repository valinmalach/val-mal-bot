# aiohttp's own web server

The bot keeps serving HTTP through uvicorn and Starlette. It does not move to
`aiohttp.web`, even though aiohttp is already loaded for discord.py and
`http_client`.

## Why this is out of scope

The only reason was memory: dropping uvicorn, h11 and Starlette was estimated at
about 0.85 MiB (#115). Measured again on 2026-10-23, after `-OO` with precompiled
bytecode (#128) and msgspec in place of pydantic (#124), the swap costs memory
instead:

- **About +0.45 MiB `RssAnon` and +0.35 MiB `RssFile`.** This was measured in an
  image built as Railway builds it and run with `-OO -X lazy_imports=all`. Both
  variants serve the same 13 routes on a real socket.
- **The cause is lazy imports.** uvicorn, h11 and Starlette now load only the 25
  modules the bot's path touches. `aiohttp.web`'s server modules (`web_urldispatcher`,
  `web_request`, `web_response`, `web_protocol` and the rest) are larger than all of
  them together. The client half of aiohttp that is already loaded shares little with
  them.

The setup and the readings are in #128's log.

The work would also have been large. Every route would move to aiohttp's request
and response types, mostly the EventSub webhook router: the signature check, replay
protection and the 400/500 split. The lifespan would become startup and cleanup
hooks, without breaking the startup order. The access log would go through
`JsonFormatter`. The tests would move from `httpx.ASGITransport` to aiohttp's test
client.

Revisit if uvicorn or Starlette start loading noticeably more at runtime, or if
aiohttp's server modules get smaller. Measure first either way.

## Prior requests

- #126: "Serve HTTP with aiohttp.web instead of uvicorn and starlette"
