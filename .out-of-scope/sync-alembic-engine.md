# A synchronous Alembic engine

Migrations keep running on SQLAlchemy's asyncio engine, with `sqlalchemy[asyncio]`
and the `greenlet` it installs. They do not move to a synchronous engine over
psycopg 3.

## Why this is out of scope

The goal was to drop `greenlet`, which is the only reason `sqlalchemy[asyncio]` is
declared (`uv tree --invert --package greenlet`). But a synchronous engine needs a
synchronous driver, and asyncpg has none. The change therefore swaps one compiled
package for another; it does not remove one:

- **New Python releases.** `psycopg-binary` is also a compiled wheel, and it bundles
  libpq and OpenSSL. It is as likely as `greenlet` to be late for a future Python, so
  the risk the issue set out to remove stays where it is. Both had cp315 wheels when
  this was decided (2026-10-13): `greenlet` 3.5.6, `psycopg-binary` 3.3.6.
- **Memory.** Nothing to gain. The bot never imports SQLAlchemy (ADR 0005), so
  `greenlet` only loads in the `alembic upgrade head` process that ends before the
  bot starts.
- **Free-threaded builds,** which `greenlet` does not support yet, are not something
  this bot plans to run on.
- **The breakage that prompted the idea is already fixed.** SQLAlchemy 2.1 stopped
  installing `greenlet` by default, and declaring the `[asyncio]` extra (cb3752d,
  #113) put it back.

The one real gain would be deleting the asyncpg URL translation in
`valmal/db/config.py` (`_translate_query` and the two tables it uses), since psycopg
reads libpq parameters as written. That is not worth a new runtime dependency and
a larger image. `pg8000`, the only pure-Python option, does not read libpq
parameters either, so it would keep the translation and have nothing left to gain.

Revisit if `greenlet` is late with wheels for a Python release the project wants,
while a synchronous driver is not.

## Prior requests

- #114: "Run Alembic on a synchronous engine, so the project no longer needs greenlet"
