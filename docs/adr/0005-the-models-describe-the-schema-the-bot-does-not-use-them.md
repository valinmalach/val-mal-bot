---
status: proposed (accepted when #98 lands)
---

# The models describe the schema; the bot does not use them

The SQLModel classes in `valmal/db/models/` stay, but only as the schema's definition: Alembic's
autogenerate and `alembic check`, and `tests/test_migrations.py`, compare the database against
them, and nothing the running bot imports touches them. The bot talks to Postgres through an
asyncpg pool, with every statement in `valmal/db/repository.py`, and reads rows into frozen,
slotted dataclasses in `valmal/db/rows.py` that carry the models' names. The reason is memory,
which is what the service is billed for: measured on 3.14.8, importing SQLAlchemy and SQLModel on
top of everything else the bot loads costs about 23 MiB and 151,000 Python blocks (161 modules),
a third of what all the bot's imports cost. Alembic runs in its own process before the bot
starts, so keeping SQLAlchemy installed for it costs the bot nothing.

## Considered options

- **Keep SQLModel at runtime** — the status quo, and the 23 MiB.
- **Delete the models and treat the migrations as the only schema** — less code, but no
  autogenerate, no `alembic check`, and nothing compares the database with anything.
- **SQLAlchemy Core without the ORM** — still imports SQLAlchemy, which is most of the cost.

## Consequences

The schema is described twice, by the models and by the SQL, so drift between them is
guarded rather than impossible: a unit test holds each row dataclass's fields equal to its
model's columns, and CI's `migrations` job `prepare()`s every repository statement against the
freshly migrated database. SQLAlchemy's `onupdate` no longer runs, so every UPDATE and upsert
sets `updated_at = now()` itself, which a test checks.
