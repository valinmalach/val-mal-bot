# Database

Postgres (Railway), described with [SQLModel](https://sqlmodel.tiangolo.com/) and
migrated with [Alembic](https://alembic.sqlalchemy.org/). The bot reads its records
through `valmal/db/repository.py` and its configuration through
`valmal/core/config.py`; the few facts that cannot be edited, such as the header names
Twitch signs a delivery with, live in code beside what uses them.

## Layout

| Path | What it is |
| --- | --- |
| `valmal/db/config.py` | Resolves `DATABASE_URL`, rewrites it onto the asyncpg driver |
| `valmal/db/session.py` | Lazily created async engine, session factory, `session_scope()` |
| `valmal/db/base.py` | Constraint naming convention, `created_at`/`updated_at` mixins |
| `valmal/db/models/` | The tables |
| `valmal/db/repository.py` | Reads and writes the records the bot keeps at runtime |
| `migrations/` | Alembic revisions; `env.py` reads `DATABASE_URL` |

Nothing connects at import time, so importing a module cannot fail on a missing
`DATABASE_URL`.

## Commands

```sh
uv run alembic upgrade head                        # apply migrations
uv run alembic upgrade head --sql                  # print the SQL, no connection
uv run alembic revision --autogenerate -m "..."    # diff models against the database
uv run alembic revision -m "..."                   # empty revision, for a data change
uv run alembic downgrade -1                        # roll back one revision
uv run alembic current                             # what is applied
```

On Windows, `--sql` needs `PYTHONIOENCODING=utf-8`: some seeded text is emoji and
the console encoding is not.

### Changing configuration

Configuration lives in migrations, so adding, editing or removing a row is a new
revision rather than an edit to a seed file. Three rules, learned the hard way:

* **Never import the models or read a data file in a revision.** A revision has
  to mean the same thing forever; if it reads something that keeps changing, the
  history stops describing what actually ran. Write the columns out as
  `sa.table()`/`sa.column()` stubs and the values as literals, as `0002` does.
* **Inserts should tolerate a conflict** (`insert(...).on_conflict_do_nothing()`),
  so a revision is a no-op against a database that already has the row. Every
  id-keyed configuration table has a unique constraint over its natural key, so
  a conflict target is not needed.
* **Guard an update with the value it replaces:**
  `UPDATE ... SET content = :new WHERE key = :key AND content = :old`. Rows still
  holding the old default move; a row somebody edited in the database is left
  alone. An unguarded UPDATE silently discards that edit.

A delete can rely on the schema: `discord_embed_field` and both
`twitch_command` child tables are `ON DELETE CASCADE`, so removing the row that
owns them takes them with it.

The checks to run before committing are in [AGENTS.md](../../AGENTS.md).

## The tables

### Records

| Table | Notes |
| --- | --- |
| `discord_user` | A guild member and their next birthday: `birthday` is a `timestamptz` holding the next occurrence in UTC, with `birthday_timezone` beside it so rolling it forward can rebuild the local date |
| `discord_message` | Cached content, for reporting edits and deletions; `attachment_urls` is a `JSONB` array |
| `live_alert` | A live-alert message still being updated, keyed by `broadcaster_id` |
| `twitch_autoshoutout` | The Twitch users due an autoshoutout, keyed by Twitch user id |

### Twitch OAuth tokens

`oauth_token` holds one row per identity (`app`, `user`, `broadcaster`), keyed to
match `TokenType` in `valmal/db/models/enums.py`.

These are the one kind of secret that cannot live in `.env`: the bot mints and
rotates them itself, so it needs somewhere it can *write*.

Two caveats:

* **Stored in plaintext for now.** Encrypting `access_token` and `refresh_token`
  under a key held in `.env` is a deliberate later step; it would mean a
  database dump alone is not enough to act as the account.
* **A copied token is dead** as soon as the running bot refreshes it, because
  Twitch issues a new refresh token on every refresh and invalidates the previous
  one. A new environment starts with empty rows: the app token needs one
  `client_credentials` call and correctly has no refresh token, and the owner-only
  `/twitch-auth` Discord command creates the user and broadcaster grants through
  `/twitch/oauth/callback` and `/twitch/oauth/callback/broadcaster`.

`expires_at` lets a token be refreshed *before* it lapses instead of after a
request comes back 401. It is nullable because Twitch does not always return
`expires_in`; null means "unknown" and falls back to refreshing reactively.

### Configuration

| Table | Holds |
| --- | --- |
| `discord_channel` | The channels the bot posts to, by slug (`audit_logs`, `welcome`) |
| `discord_role` | The roles it hands out, by slug, plus each panel button's `custom_id` |
| `discord_embed` / `discord_embed_field` | The rules and role-panel embeds |
| `discord_auto_response` | Canned replies to plain chat messages, such as `ping` → `pong` |
| `twitch_command` / `twitch_command_response` | Chat commands and the text each sends |
| `twitch_command_component` | Commands that fan out to other commands, such as `!everything` |
| `message_template` | One-off canned text: the follow thank-you, the ad-break warning, the raid-out message, the birthday wish, the startup message and the audit-entry sentences |
| `app_setting` | Scalar settings: `guild_id`, `owner_id`, `broadcaster_username`, the command prefix, the embed colour palette, the Twitch scope list and the two Twitch account IDs |

### What splits between `.env` and the database

`.env` answers *how this instance authenticates* and *where it runs*. The
database describes *what the bot does*.

| `.env` | Why it stays |
| --- | --- |
| `DISCORD_TOKEN`, `TEST_DISCORD_TOKEN` | Credential, and it selects which bot identity this instance is |
| `TWITCH_CLIENT_ID`, `TWITCH_CLIENT_SECRET` | Both halves of one Twitch API credential; the ID is sent on every Helix call and belongs with its secret |
| `TWITCH_WEBHOOK_SECRET` | Credential |
| `DATABASE_URL` | Bootstrap — it is how the database is reached in the first place |
| `APP_URL` | Environment, not configuration: the EventSub callback host differs between the test and production instances, so a shared database cannot hold one right answer |
| `PORT` | Railway injects this; not a value the bot has an opinion about |
| `USE_TEST_BOT`, `DB_ECHO` | Switches for one instance, not settings the bot acts on |

The two Twitch account IDs, `twitch_bot_user_id` and `twitch_broadcaster_id`, are
`app_setting` rows rather than `.env` values: they are not credentials and not
environment-specific, and name the accounts the bot acts *as* and acts *on*, the
same kind of fact as a Discord channel or role ID. They are strings because that
is how Helix returns them and how every call site uses them.

### Deliberately not in the database

* **Credentials and environment** — everything in the `.env` table above.
* **Facts, not configuration** — `Months`/`MAX_DAYS`, the `COGS` list, the
  Twitch EventSub header names, `HMAC_PREFIX`, and the Helix rate-limit
  constants in `shoutout_queue.py`.

## Conventions

**Placeholders.** Embed field text and message templates are stored with
placeholders resolved at render time against the other tables:

* `{channel:promo}` → `<#1378917167336001606>` (looked up by `discord_channel.key`)
* `{role:follower}` → `<@&1291769015190032435>` (looked up by `discord_role.key`)

Twitch command responses use `{chatter}`, `{target}` and `{broadcaster}`, filled
in from the chat event, and never the two above: Twitch chat cannot show a Discord
mention, so a response is not rendered against the channel and role tables.

**Handlers.** `twitch_command.handler` names a callable in the command registry
rather than an enum value, so adding a command that needs real logic does not
need a migration. `static` just sends each `twitch_command_response` in order.

**IDs are addressed by slug, never by value.** `discord_channel` and
`discord_role` are keyed on a stable `key` (`audit_logs`, `follower`) with the
snowflake in a separate, freely editable column; foreign keys point at `key`, so
nothing in the schema depends on an ID's value. Twitch IDs work the same way as
`app_setting` rows. Code must resolve `"audit_logs"` at call time and never hold
a snowflake, so an ID can be changed in the database without a redeploy.

`valmal/core/config.py` is that read path: one snapshot taken in `setup_hook()`. Nothing
calls `config.load()` again after that, so an edit takes effect on the next restart,
not before it.

**Constraint names** come from the convention in `valmal/db/base.py`. Keep it: without
it Postgres invents names and every autogenerated migration wants to recreate
constraints.

**Enums are checked `VARCHAR`, not native Postgres enums.** Adding a value to a
native enum needs `ALTER TYPE`; rewriting a CHECK constraint is an ordinary
migration. `enum_column()` in `valmal/db/base.py` builds them. `oauth_token.key` uses
`enums.TokenType` rather than a second enum of its own: the column's values
and the ones the Helix layer passes around are the same three strings.

**Two columns are deliberately unread.** `twitch_command.cooldown_seconds` and
`discord_role.assignable` each name a feature nothing implements yet — a
per-command rate limit, and a role listed on a panel without being
self-assignable. They cost nothing at runtime, and dropping them means a
migration now and another one to put them back, so they stay. Anything sweeping
the schema for dead columns will find these two; this is the answer.

**Two SQLModel typing workarounds live in `valmal/db/base.py`** and both look redundant
without knowing why:

* `UTC_TIMESTAMP` is annotated `Any` because SQLModel types `Field(sa_type=...)`
  as `type[Any]`, while a type *instance* is the only way to say "with time
  zone". SQLAlchemy type objects are safe to share across columns; `Column`
  objects are not, which is why the timestamp mixins cannot use `sa_column`.
* `TableBase.__tablename__` is `ClassVar[Any]`, not `ClassVar[str]`. SQLModel
  declares that name twice — as `ClassVar[str]` and as a `declared_attr` — so
  `str` trades one type error for an incompatible-override error.

## Not done yet

1. **Encrypt `oauth_token`.** Deliberately deferred; see the caveat above.
2. **Editing without database access** — admin commands or a frontend over the
   configuration tables, which is what the slug-keyed IDs above are for.
