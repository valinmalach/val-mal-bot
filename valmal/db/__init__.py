"""Postgres for the bot: the schema, and the statements the bot runs against it.

The schema is ``db/models`` (for Alembic and the tests only), revisions are in
``migrations/``; the bot reads rows through ``repository`` and ``configuration``
on the pool in ``pool``. See ``valmal/db/README.md`` and ADR 0005. Import from
the module that owns the name rather than through this one.
"""
