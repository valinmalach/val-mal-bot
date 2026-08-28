# Database

Postgres (Railway), described with [SQLModel](https://sqlmodel.tiangolo.com/) and
migrated with [Alembic](https://alembic.sqlalchemy.org/). The parquet files and
the scattered hardcoded values have been consolidated here: the bot reads its
records through `db/repository.py` and its configuration through
`services/config.py`, and `constants.py` keeps only facts that cannot be edited.

## Layout

| Path | What it is |
| --- | --- |
| `db/config.py` | Resolves `DATABASE_URL`, rewrites it onto the asyncpg driver |
| `db/session.py` | Lazily created async engine, session factory, `session_scope()` |
| `db/base.py` | Constraint naming convention, `created_at`/`updated_at` mixins |
| `db/models/` | The tables |
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

Checks, all of which should be clean before committing:

```sh
uvx ruff check . --exclude .venv
uvx ruff format --check . --exclude .venv
uvx pyright                                        # same settings Pylance uses
```

## Where each table comes from

### Records that were in parquet

| Table | Replaces | Notes |
| --- | --- | --- |
| `discord_user` | `data/users.parquet` | `isBirthdayLeap` → `is_birthday_leap`; `birthday` becomes a real `timestamptz` holding the next occurrence in UTC |
| `discord_message` | `data/messages.parquet` | `attachment_urls` becomes a `JSONB` array instead of a JSON string |
| `live_alert` | `data/live_alerts.parquet` | the parquet `id` is the Twitch broadcaster, so it is named `broadcaster_id` |

### Twitch OAuth tokens

`oauth_token` replaces the five `data/twitch/*.txt` files, one row per identity
(`app`, `user`, `broadcaster`), keyed to match `constants.TokenType`.

These are the one kind of secret that cannot live in `.env`: the bot mints and
rotates them itself, so it needs somewhere it can *write*. Files were fine on a
single long-lived host and stop being fine on ephemeral storage.

Two caveats:

* **Stored in plaintext for now.** Encrypting `access_token` and `refresh_token`
  under a key held in `.env` is a deliberate later step; it would mean a
  database dump alone is not enough to act as the account.
* **Do not migrate the current values.** Twitch issues a new refresh token on
  every refresh and invalidates the previous one, so any copied token is dead as
  soon as the running bot refreshes. Create the rows empty and let them fill at
  cutover: the app token needs one `client_credentials` call and correctly has no
  refresh token. Run the owner-only `/twitch-auth` Discord command to create the
  user and broadcaster grants through `/twitch/oauth/callback` and
  `/twitch/oauth/callback/broadcaster`.

`expires_at` is what allows a token to be refreshed *before* it lapses instead
of after a request comes back 401. `TwitchTokenManager` already tracks this in
memory, from the `expires_in` that `AuthResponse` and `RefreshResponse` were
previously parsing and discarding; the column is where it becomes durable. It is
nullable because Twitch does not always return `expires_in`, and a null means
"unknown", which falls back to refreshing reactively.

### Configuration that was hardcoded

| Table | Replaces |
| --- | --- |
| `discord_channel` | the `*_CHANNEL` constants in `constants.py` |
| `discord_role` | the `*_ROLE` constants **and** `EMOJI_ROLE_MAP`, plus each button's `custom_id` from `views/` |
| `discord_embed` / `discord_embed_field` | `RULES_EMBED`, `PING_ROLES_EMBED`, `NSFW_ACCESS_EMBED`, `PRONOUN_ROLES_EMBED`, `OTHER_ROLES_EMBED`, `DMS_OPEN_EMBED` |
| `discord_auto_response` | the `ping` → `pong` / `plap` → `clank` replies in `cogs/events.py` |
| `twitch_command` / `twitch_command_response` | the message text in `services/twitch/commands.py` and the dispatch dict in `controller/twitch.py` |
| `twitch_command_component` | `!everything`, which fans out to other commands |
| `message_template` | one-off canned text: the follow thank-you, the ad-break warning, the raid-out message, the birthday wish, the startup message |
| `app_setting` | `GUILD_ID`, `OWNER_ID`, `BROADCASTER_USERNAME`, the `$` command prefix, the embed colour palette, the Twitch scope list in `token_manager.py`, and the two Twitch account IDs (see below) |

### What splits between `.env` and the database

`.env` answers *how this instance authenticates* and *where it runs*. The
database describes *what the bot does*. That line puts most of the old `.env`
back in `.env`:

| `.env` | Why it stays |
| --- | --- |
| `DISCORD_TOKEN`, `TEST_DISCORD_TOKEN` | Credential, and it selects which bot identity this instance is |
| `TWITCH_CLIENT_ID`, `TWITCH_CLIENT_SECRET` | Both halves of one Twitch API credential; the ID is sent on every Helix call and belongs with its secret |
| `TWITCH_WEBHOOK_SECRET` | Credential |
| `DATABASE_URL` | Bootstrap — it is how the database is reached in the first place |
| `APP_URL` | Environment, not configuration: the EventSub callback host differs between the test and production instances, so a shared database cannot hold one right answer |
| `USE_TEST_BOT`, `DB_ECHO` | Switches for one instance, not settings the bot acts on |

`start.sh` also reads `LOCLX_TOKEN`, `LOCLX_SUBDOMAIN` and `PORT` straight from
the environment. The shell consumes them before Python starts, so they are not
`Settings` fields and a missing one fails at the tunnel rather than at startup.
`LOCLX_SUBDOMAIN` must name the same host as `APP_URL`.

Two values move out of `.env` entirely. They are not credentials and not
environment-specific — they name the Twitch accounts the bot acts *as* and acts
*on*, which is the same kind of fact as a Discord channel or role ID:

| Old `.env` | `app_setting.key` | `value_type` |
| --- | --- | --- |
| `TWITCH_BOT_USER_ID` | `twitch_bot_user_id` | `string` |
| `TWITCH_BROADCASTER_ID` | `twitch_broadcaster_id` | `string` |

Both are stored as strings rather than integers because that is how Helix
returns them and how every call site already uses them.

They are gone from `.env.example`, and nothing reads them from the environment:
`config.setting("twitch_broadcaster_id")` resolves them at call time.

### Deliberately not in the database

* **Credentials and environment** — everything in the first table above. They
  stay in `.env`.
* **Facts, not configuration** — `Months`/`MAX_DAYS`, the `COGS` list, the
  Twitch EventSub header names, `HMAC_PREFIX`, and the Helix rate-limit
  constants in `shoutout_queue.py`.

## Conventions

**Placeholders.** Embed field text and message templates are stored with
placeholders resolved at render time against the other tables:

* `{channel:promo}` → `<#1378917167336001606>` (looked up by `discord_channel.key`)
* `{role:follower}` → `<@&1291769015190032435>` (looked up by `discord_role.key`)

Twitch command responses additionally use `{chatter}`, `{target}` and
`{broadcaster}`, filled in from the chat event.

**Handlers.** `twitch_command.handler` names a callable in the command registry
rather than an enum value, so adding a command that needs real logic does not
need a migration. `static` just sends each `twitch_command_response` in order.

**IDs are addressed by slug, never by value.** `discord_channel` and
`discord_role` are keyed on a stable `key` (`audit_logs`, `follower`) with the
snowflake in a separate, freely editable column; foreign keys point at `key`, so
nothing in the schema depends on an ID's value. Twitch IDs work the same way as
`app_setting` rows. Code must resolve `"audit_logs"` at call time and never hold
a snowflake, so an ID can be changed in the database without a redeploy.

`services/config.py` is that read path: one snapshot taken in `setup_hook()` and
reloadable, so an edit takes effect without a restart.

**Constraint names** come from the convention in `db/base.py`. Keep it: without
it Postgres invents names and every autogenerated migration wants to recreate
constraints.

**Enums are checked `VARCHAR`, not native Postgres enums.** Adding a value to a
native enum needs `ALTER TYPE`; rewriting a CHECK constraint is an ordinary
migration. `enum_column()` in `db/base.py` builds them.

**Two SQLModel typing workarounds live in `db/base.py`** and both look redundant
without knowing why:

* `UTC_TIMESTAMP` is annotated `Any` because SQLModel types `Field(sa_type=...)`
  as `type[Any]`, while a type *instance* is the only way to say "with time
  zone". SQLAlchemy type objects are safe to share across columns; `Column`
  objects are not, which is why the timestamp mixins cannot use `sa_column`.
* `TableBase.__tablename__` is `ClassVar[Any]`, not `ClassVar[str]`. SQLModel
  declares that name twice — as `ClassVar[str]` and as a `declared_attr` — so
  `str` trades one type error for an incompatible-override error.

## Done

1. **Seed.** Revision `0002` inserts the constants, embed text and command
   responses, so `alembic upgrade head` carries the configuration with the
   schema and `seed.py`/`seed_data.py` are gone. Each row is `INSERT ... ON
   CONFLICT DO NOTHING`, so an ID edited in the database survives the next
   deploy. Changing configuration is a data revision now: see above.
2. ~~**Backfill.**~~ Done and removed. The first deploy loaded the parquet
   records; `backfill.py` and `data/` went with it. Recover them from history if
   a re-run is ever needed.
3. **Repository layer.** `db/repository.py` reads and writes the records; the
   parquet cache is gone.
4. **Token cutover.** `TwitchTokenManager` reads and writes `oauth_token`.
5. **Configuration.** `services/config.py` serves channels, roles, settings,
   templates, embeds and Twitch commands from the database.

## Remaining

1. **Encrypt `oauth_token`.** Deliberately deferred; see the caveat above.
2. **Editing without database access** — admin commands or a frontend over the
   configuration tables, which is what the slug-keyed IDs above are for.
