# aiodns

The bot does not install `aiodns` to resolve hostnames asynchronously.

## Why this is out of scope

With `aiodns` installed, aiohttp's `DefaultResolver` becomes `AsyncResolver`
(`aiohttp/resolver.py`), so aiohttp stops handing lookups to
`loop.getaddrinfo`. The idea was that this takes the DNS threads out of the
process and saves memory.

It cannot, because those threads are not aiohttp's: they belong to the event loop,
which runs `loop.getaddrinfo` on threads of its own. Which threads depends on the loop.

- **On uvloop, when this was decided:** libuv's thread pool, four threads by default
  (`UV_THREADPOOL_SIZE`, which nothing set), started together on first use and alive
  for the life of the process. Production showed exactly that on 2026-10-02:

  ```text
  python main.py   Threads: 6   VmRSS: 105840 kB
    1 python            the event loop
    1 keep-alive-hand   discord.py's gateway heartbeat
    4 libuv-worker      libuv's thread pool
  ```

- **On plain asyncio, since #125 dropped uvloop:** the loop's default
  `ThreadPoolExecutor`, which starts a thread only when no idle one is free and
  keeps it afterwards, so the count is what concurrent lookups have needed so far.

Either way, aiohttp is not the pool's only user. asyncpg connects with
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
names. Until then, asyncpg's lookups keep the loop's resolver threads alive on
uvloop and asyncio alike.

## Prior requests

- #103: "Resolve DNS with aiodns once aiohttp carries every call"
