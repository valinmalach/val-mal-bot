# Valin Malach Bot — agent instructions

Guidance for coding agents working in this repository. Claude Code reads this
file through `CLAUDE.md`.

## Keep this file current

Review this file whenever a major change lands and update it in the same commit.
A change is major if it would make anything below wrong or incomplete: a new
module or cog, a different startup order, a new or changed command or check, a
value moving between `.env` and the database, a new convention, or the arrival of
a test suite.

## Commands

```sh
uv sync                                            # dependencies; Python 3.14.7, pinned in .python-version
docker compose up -d                               # local Postgres on :5432
uv run alembic upgrade head                        # schema and the configuration in it
uv run main.py                                     # run the bot (uvicorn on PORT, default 8000)
```

Checks, all three clean before committing:

```sh
uvx ruff check . --exclude .venv
uvx ruff format --check . --exclude .venv
uvx pyright                                        # the [tool.pyright] settings Pylance also reads
```

Migrations — see `db/README.md` for the rules:

```sh
uv run alembic revision --autogenerate -m "..."    # diff the models against the database
uv run alembic revision -m "..."                   # empty revision, for a configuration change
uv run alembic upgrade head --sql                  # print the SQL without connecting
uv run alembic downgrade -1
uv run alembic current
```

On Windows `--sql` needs `PYTHONIOENCODING=utf-8`: some seeded text is emoji.

**There is no test suite** — no pytest, no test files, no CI workflow. Verification
is the three checks above plus running the bot, so do not describe a change as tested.

## Architecture

**One process wearing two faces.** `main.py` is a FastAPI app, and the Discord bot
is not the entrypoint: the lifespan handler starts `main()` as a background task,
which loads `constants.COGS` and calls `bot.start()`. Twitch never connects to the
bot — it delivers EventSub webhooks over HTTP to the router in `controller/twitch.py`.
Stop the web server and the bot goes with it.

**Startup order is spread across three files.** lifespan (`main.py`) → cog loading →
`MyBot.setup_hook()` (`init/bot_init.py`: `config.load()`, `token_manager.load()`,
command prefix, guild command-tree sync, persistent view registration) → `on_ready`
(background tasks; the startup announcement is guarded by a module flag because
`on_ready` fires again every time a gateway session cannot be resumed).

**Two configuration sources, one hard line.** `.env` → `config.settings` answers *how
this instance authenticates and where it runs*, validated once at import and failing
with the raw values redacted. Postgres → `ConfigCache` in `services/config.py` answers
*what the bot does*, snapshotted in `setup_hook()` and reloadable without a restart.
`db/README.md` records which side each value falls on and why; keep new values on the
right side of that line.

**IDs are addressed by slug, never by value.** Resolve at call time —
`config.channel("audit_logs")`, `config.role("follower")`,
`config.setting("twitch_broadcaster_id")`, `config.template(...)`. Never put a
snowflake in code or hold one across calls, so an ID can change in the database
without a redeploy.

**Changing configuration means writing a migration, not editing a file.** The rows
live in revisions. `db/README.md` has the three rules that matter — never import the
models or read a data file from a revision, make inserts tolerate a conflict, and
guard an UPDATE with the value it replaces — read it before touching `migrations/`.

**Nothing connects at import time.** `db/session.py` builds the engine lazily, so
importing any module is safe without a reachable `DATABASE_URL`.

**Twitch user grants start in Discord.** The owner-only `/twitch-auth` command
creates short-lived links for the `user` (the account in `twitch_bot_user_id`)
and `broadcaster` (the account in `twitch_broadcaster_id`) authorization-code
flows. Both request `twitch_app_scopes`; the callbacks validate the returned
Twitch user ID, client ID and scopes before upserting `oauth_token`. The `app`
row is separate, uses client credentials and has no refresh token.

**Two model packages with confusable names.** `models/` is Pydantic: Twitch API
responses and EventSub payloads. `db/models/` is SQLModel: the tables.

## Conventions

- **Background work goes through `background.fire_and_forget`.** asyncio holds only a
  weak reference to a running task, so a bare `create_task` can be collected mid-flight.
- **Deferred imports inside functions** are load-order management, not style:
  `init/bot_init.py` and `views/` import services lazily to break cycles. Leave them.
- **`token_manager` and `shoutout_queue` are singletons** (`__new__`). Import the
  instance; do not construct another.
- **Everything the bot says about itself goes through `errors.py`.** `report(exc,
  context)` for an exception, `notify(text)` for anything else worth the admin
  channel. Neither raises, both log locally first, and both say so when the channel
  is out of reach. The one other resolver of `config.channel("bot_admin")` is the
  startup announcement in `init/bot_init.py`, which sends directly because it marks
  itself done only once the send lands, so a missed announcement retries on the next
  reconnect.
- **Text from the database is formatted with `safe_format`**, never bare `str.format`
  — a row is not source, and one unmatched brace should not lose the whole message.
  `config.render` additionally resolves `{channel:key}` and `{role:key}`.
- **A new cog needs an entry in `constants.COGS`.** Nothing auto-discovers.
- **Comments record a non-obvious *why*, or do not exist.** Match the density in
  `db/base.py` and `db/config.py`; do not narrate what the code already says.

## Deployment

Railway, via `Dockerfile` → `start.sh`: bring up the loclx tunnel (the public host
EventSub calls back on), `alembic upgrade head`, then the app. Migrations run on every
container start.

Locally, EventSub cannot reach `localhost`, so stream alerts, follows, raids, ad breaks
and chat commands never fire without the tunnel. Outbound Helix calls still work.

## Agent skills

### Issue tracker

Issues and PRDs live as GitHub issues on `valinmalach/val-mal-bot`, driven by the `gh` CLI. See `docs/agents/issue-tracker.md`.

### Triage labels

The five canonical triage roles map 1:1 to identically-named GitHub labels. See `docs/agents/triage-labels.md`.

### Domain docs

Single-context: one `CONTEXT.md` plus `docs/adr/` at the repo root. See `docs/agents/domain.md`.
