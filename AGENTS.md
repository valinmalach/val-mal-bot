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

A fourth gate runs at commit time. `git commit` is intercepted by Verity, which
analyses the staged diff and can block the commit. It is a Claude Code hook, not a
git hook — there is nothing in `.git/hooks`, and it does not fire for other tools.
Its rules, and the narrow circumstances in which a finding may be waived, live in
`CLAUDE.md`; the Standard it enforces is `.verity/standard.yaml`, and `VERITY.md`
covers the setup. Run it by hand with `verity analyze`. Everything Verity generates
is gitignored, so a fresh clone has none of it — `VERITY.md` has the restore steps.

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

**A live alert is closed by its updater, never by a webhook.** `stream.offline`
carries no stream id, so the handler cannot tell which stream ended; it calls
`live_alert.wake()`, and the updater re-checks Helix and stops if it has been
superseded. `docs/adr/0001-alert-updater-is-the-only-closer.md` has the why. The
one rule the cycle turns on is `live_alert._decide`, which is pure — put new
conditions there, not in the surrounding I/O.

**`live_alert` and `stream_session` are different scopes.** A live alert exists
per broadcaster; a stream session is the main broadcaster being live, and owns
the shoutout queue and the ad-break warning. Only a stream Helix confirms is gone
stands a session down.

**Every Twitch chat line goes through `services/twitch/chat.py`.** `say` and
`say_template` report their own failure and never raise, so a line Twitch refused
cannot fail whatever was saying it. `api.send_chat_message` is not re-exported
from `services` — three separate rounds of guarding chat sends each missed a
different call site, so the guard moved to the one place they all pass through.

**Every Helix call goes through `services/twitch/helix.py`.** It owns the token
choice, the pre-emptive refresh, the one 401 re-send, retry, status checking and
parsing, and it raises `HelixError` rather than reporting — whoever catches has
the context worth reporting. `api.py` holds the endpoints on top of it: `None`
means Twitch has nothing, a failed call raises. Retry follows the method, not the
call site; `docs/adr/0002-helix-posts-are-not-retried.md` says why POSTs do not.

**A stored birthday is the next occurrence, not a date of birth.** One rule
answers when that is: `next_birthday_on` when it is being set, `next_birthday`
when it is being rolled forward, both in `services/helper/helper.py`. Two
implementations of it disagreed once and `/birthday set` wrote dates that had
already passed. `docs/adr/0003-birthday-holds-the-next-occurrence.md` records why
the column holds an instant, and what discarding the timezone costs — issue #12.

**Two model packages with confusable names.** `models/` is Pydantic: Twitch API
responses and EventSub payloads. `db/models/` is SQLModel: the tables.

## Conventions

- **Background work goes through `background.fire_and_forget`, always named.** asyncio
  holds only a weak reference to a running task, so a bare `create_task` can be
  collected mid-flight. It also reports a task that died without its own handler
  running, which is the last catch in the process — and the report is keyed on the
  task's name, so an unnamed task would report under a new name every time.
  A `@tasks.loop` cog task does not need it for the reference: discord.py keeps
  the task on the `Loop`, which the cog keeps, and names it. `cogs/tasks.py` is
  the example; reviewers read it as a bypass about once a round. It does still
  need its own reporting — a `Loop` that dies only logs — so wrap the body in a
  `try` that calls `report`, as `check_birthdays` does.
- **Deferred imports inside functions** are load-order management, not style:
  `init/bot_init.py` and `views/` import services lazily to break cycles. Leave them.
- **`token_manager` and `shoutout_queue` are singletons** (`__new__`). Import the
  instance; do not construct another.
- **Everything the bot says about itself goes through `errors.py`.** `report(exc,
  context)` for an exception, `notify(text)` for anything else worth the admin
  channel, `notify_soon(text)` for the two synchronous renderers that cannot
  await. None of them raise, all log locally first, and all say so when the
  channel is out of reach. `notify` returns whether the channel has the news, for
  the one caller that retries. Nothing else may resolve
  `config.channel("bot_admin")`.
- **A path that degrades or gives up says so in the admin channel.** Catching a
  `HelixError` to carry on without an avatar is fine; catching it into
  `logger.warning` alone is not. The logs are not watched and the Discord server
  is. Volume is not a reason to stay quiet: `errors.py` holds back a repeat of
  something it already delivered for fifteen minutes and says how many it stood
  in for, so the first of anything always lands and an outage still costs a
  handful of messages. Pass an explicit `key=` wherever the text carries a detail
  that varies between repeats of the same problem — a broadcaster id belongs in
  the key, the error string does not, or nothing is ever held back.
- **A retry loop stays quiet while retrying and speaks once when it gives up.**
  The give-up is not optional: a loop that stops into a `logger.warning` has
  stopped doing its job with nothing to say it. `helix.request` and
  `_wait_for_stream_info` are the shape to copy.
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
