# aiodns

The bot does not install `aiodns` to resolve hostnames asynchronously.

## Why this is out of scope

With `aiodns` installed, aiohttp's `DefaultResolver` becomes `AsyncResolver`
(`aiohttp/resolver.py`), so aiohttp stops handing lookups to
`loop.getaddrinfo`. The idea was that this takes the DNS threads out of the
process and saves memory.

It cannot, because those threads are not aiohttp's. `pyproject.toml`
installs uvloop on Linux, and `main.py` leaves uvicorn's `loop` at its default, so the
bot runs on uvloop. Under uvloop, `loop.getaddrinfo` runs on libuv's thread
pool: four threads by default (`UV_THREADPOOL_SIZE`, which nothing here sets),
started together on first use, alive for the life of the process. Production
showed exactly that on 2026-10-02:

```text
python main.py   Threads: 6   VmRSS: 105840 kB
  1 python            the event loop
  1 keep-alive-hand   discord.py's gateway heartbeat
  4 libuv-worker      libuv's thread pool
```

aiohttp is not the pool's only user. asyncpg connects with
`loop.create_connection(host, ...)` and the database host is a name, so its
lookup goes through the same pool. With `aiodns` the pool would still start on
the first database connection and stay, whatever its size.

So the change removes no threads and costs:

- about +1.1 MB resident, because aiohttp imports `aiodns` and `pycares` at
  import time (measured in the project environment on 2026-09-28);
- four more packages: `aiodns` itself, and `pycares`, `cffi` and `pycparser`
  beneath it;
- a third Python 3.15 blocker: `pycares` 5.0.1 has no cp315 or abi3 wheel.

Revisit only if aiohttp becomes the only thing in the process that resolves
names. Off uvloop, asyncpg would still resolve through asyncio's default
executor, which is threads too.

## Prior requests

- #103: "Resolve DNS with aiodns once aiohttp carries every call"
