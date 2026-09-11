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

Checks, all four clean before committing, and **in this order**:

```sh
sourcery review --fix .                            # apply what it can fix mechanically
sourcery review --check .                          # what is left needs a person
uvx ruff format . --exclude .venv
uvx ruff check . --exclude .venv
uvx pyright                                        # the [tool.pyright] settings Pylance also reads
```

**Sourcery runs first because its fixes are not guaranteed to satisfy the other
three.** `use-named-expression` rewrote an `if matched:` into a walrus whose
variable nothing then read — a ruff `F841` *and* a format violation, from a tool
that had just reported itself clean. Running ruff afterwards is what catches that;
running it first only means doing it twice.

**A finding `--fix` cannot repair is yours to address, not to skip.** Sourcery
leaves the judgement calls — a long function, a name that says its own type —
and silence from `--check` is the only clean state. If a rule is genuinely wrong
for this repo, disable it by id in `.sourcery.yaml` with the reason, so the next
run does not re-raise it.

`.sourcery.yaml` carries four custom rules for conventions the other tools cannot
see: a bare `create_task`, the audit channel outside `services/audit.py`, a chat
send outside `services/twitch/chat.py`, and `str.format` on database text. Each
excludes the one file that legitimately does the thing, and those exclusions are
relative to the config file — moving it breaks them silently. The Google style
set (`sourcery review --enable gpsg .`) is deliberately not enabled: 232 of its
272 findings here are the docstring mandate the comment convention below rejects.
It is worth running by hand occasionally for the dozen findings that are not.

**A PEP 695 parameter list blinds Sourcery to the whole file, silently.** Sourcery
1.45 returns no pattern-rule findings at all for a file containing `def f[T]`,
`class C[T]` or `type X = ...` — no parse error, no warning, and a clean report
that reads exactly like a clean file. Worse, it is partial: structural rules such
as `no-long-functions` still fire, so the output looks normal while every custom
rule above has stopped guarding that file. `controller/twitch.py` is in this state
today because `_route[E: BaseModel]` is worth more than the coverage; nothing else
should join it without knowing the trade. `has_configured_role` uses a module-level
`TypeVar` for exactly this reason — it needs the annotation and the coverage both.

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
is the four checks above plus running the bot, so do not describe a change as tested.

A fifth gate runs at commit time. `git commit` is intercepted by Verity, which
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
bot — it delivers EventSub webhooks over HTTP to the router in `controller/twitch.py`,
which verifies and parses them and hands each to `services/twitch/events.py`.
Stop the web server and the bot goes with it.

**Startup order is spread across three files.** lifespan (`main.py`) → cog loading →
`MyBot.setup_hook()` (`init/bot_init.py`: `config.load()`, `token_manager.load()`,
the shoutout drainer, command prefix, guild command-tree sync, persistent view
registration) → `on_ready` (background tasks; the startup announcement is guarded
by a module flag because `on_ready` fires again every time a gateway session
cannot be resumed). Anything wanted once per process belongs in `setup_hook` for
that reason — the drainer is there, not beside the other background tasks.

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

**Three files, three jobs, none of them over the threshold.**
`controller/twitch.py` receives a signed notification, verifies it, parses it and
hands it on. `services/twitch/events.py` says what each event makes the bot do —
it lives under `services/` because it names no HTTP type at all, and nothing
under `services/` imports `controller/`. `controller/twitch_oauth.py` carries the
two authorization-code flows on their own router; it shares nothing with the
webhook controller, not the signature check, the models, or the path prefix.

**A webhook route's model says which event it serves.** Each `*Subscription`
declares its own `type` as a `Literal`, so a payload for the wrong event fails to
parse instead of being compared against a string passed in beside it. That
`Literal` is the *whole* reason the subscription is modelled: nothing reads
`event_sub.subscription`, so there is no base class, no `condition`, and none of
the `id`/`status`/`cost` fields Twitch sends beside them. `process_webhook` is
generic in the model, which binds it to its handler: pairing
`StreamOnlineEventSub` with the follow handler stops type-checking, where before
both parameters were unannotated and so `Any`.

**An event models the fields its handler reads, and no more.** Pydantic ignores
what is not declared, so the rest is ballast that has to be maintained against
Twitch's docs and can only fail. A field is `str` rather than a `Literal` of the
values Twitch documents unless something branches on all of them: a value Twitch
adds would otherwise fail validation, and a 400 spends the subscription's failure
budget exactly as a 500 does.

**A signed delivery is also checked for freshness, and dispatched at most once.**
A timestamp more than ten minutes from now in either direction is refused with a
403 — Twitch's own guidance for replay, and a clock ahead is as wrong as one
behind. A message id that has already reached a handler answers 202 without
dispatching again, since Twitch says a notification may arrive twice and 202 is
what stops the retries. Only ids that were *dispatched* are remembered, so a
delivery Twitch retries because this end failed still gets through. The set is
process-local, and remembered for twice the freshness window: a clock further
out than one window refuses everything anyway, so the skew that could open a gap
between the two cannot exceed a window. It holds 20,000 ids, which the chat route
would otherwise fill in minutes — one id per chat line — and reaching the cap
says so in the admin channel, because past it a redelivery can run twice.

The cost of the freshness check is real and worse than "events are refused". A
403 is a failed delivery, and enough of them revoke the subscription — of which
five of the seven cannot be recreated from this repo. That is why the check sits
*below* the verification handshake and applies to notifications alone: a wrong
clock must not also refuse the resubscribe that repairs it.

**A signed request that the models reject answers 4xx, not 5xx.** A body that is
not JSON, is not a JSON object, or does not match the route's model is a 400 with
a notice naming the field that failed; only something genuinely unexpected is a
500 with a report. The reason is that a payload this end cannot read is not a
server fault, and a traceback for one is noise — *not* that it saves retries.
Twitch documents no 4xx/5xx distinction anywhere, and revocation counts anything
that is not a 2xx, so a 400 spends the failure budget exactly as a 500 does. That
wrong justification was recorded here first; do not restore it.

**Only two of the seven EventSub subscriptions can be created from this repo.**
`/subscribe` creates `stream.online` and `stream.offline`. Chat, follow, ad break,
raid and moderate are provisioned outside it, so a deployment whose public URL
changes leaves five subscriptions pointing at a dead callback with nothing here
able to recreate them. The startup check notices an undeliverable subscription;
it cannot repair these five.

**A live alert is closed by its updater, never by a webhook.** `stream.offline`
carries no stream id, so the handler cannot tell which stream ended; it calls
`live_alert.wake()`, and the updater re-checks Helix and stops if it has been
superseded. The same webhook also calls `stream_session.wake()`, which asks Helix
the same question for a different scope and is not the same decision. Both guard
against being superseded, for the same reason.
`docs/adr/0001-alert-updater-is-the-only-closer.md` has the why. The one rule the
cycle turns on is `live_alert_cycle._decide`, which is pure — put new conditions
there, not in the surrounding I/O.

**An alert's lifecycle and one of its passes are separate files.**
`live_alert.py` starts an updater, wakes it, and restores them all after a
restart; `live_alert_cycle.py` is one pass — `_decide`, and the refresh and
close that carry its answer out — and knows nothing about tasks. It answers with
an `Action`, which is why the split holds: the loop reads a conclusion rather
than watching the I/O that reached it.

**An autoshoutout is one per person per stream, and the session remembers who.**
`services/twitch/autoshoutout.py` owns the list, the rule and the three places
someone can turn up. The rule is `_decide` and is pure, like
`live_alert_cycle._decide`; the lookup is *inside* it, as `LOOK_UP`, because
the cost guarantee — one query per distinct chatter per stream — is the rule
rather than an optimisation wrapped around it. The session holds one set of
**settled** ids, not two: **spent** and "looked up and not on the list" are
both "do not ask again this stream", and nothing needs the halves apart.

It shouts nobody out itself. It posts `!so <login>`, the round trip
`channel_raid` already uses, so there is one implementation of what a shoutout
is — at the price of the wording being `!so`'s. An incoming raid is settled
rather than shouted, because the raid handler's own `!so` already gave them
one; that mark lives in `channel_raid` and not in the shoutout handler, which a
mod's manual `!so` also reaches. `!aso` is the exception that calls `shoutout`
directly, having a chat event already in hand.

**The shared-chat guard runs before anything reads a chat line.** It used to sit
below the command parse, which was harmless while a relayed line could only
produce a command. An autoshoutout is owed to someone who turned up in *this*
channel, so a line relayed from another one is dropped first.

**One drainer, for the life of the process.** A pass that fails costs that pass,
not the stream's shoutouts: the Helix failures inside handle themselves, so the
outer guard is for what nobody anticipated. The drainer no longer starts and
stops with the stream, which is what used to need a generation counter — one
told to stand down could still be inside a two-minute sleep when the next stream
started another alongside it. It asks the session each pass instead, and that is
not decoration: a target whose lookup failed re-queues itself, and a lookup that
failed as the stream ended would otherwise be shouted out into an offline chat.
`shoutout_queue` therefore imports `stream_session` inside `drain`, because the
session imports the queue to empty it.

**`stream_session` owns every transition of its own.** `began` and `resume` take
a session up, `wake` is the only thing that can bring one down, and both ends are
private to the module. `resume` is deliberately not a second `began`: nothing is
greeted, because the stream did not just start, and that difference is why the
startup path could never have been `began`. Beginning is not the opposite of
ending — it resets whatever a previous session left, which is also how a session
nothing could end recovers at the next `stream.online`.

**A subject per file, and no file holding six.** `services/helper/helper.py` used
to hold sending, presentation, durations, birthdays, roles and webhook
signatures. They are now `send.py`, `present.py`, `duration.py`, `birthday.py`,
`roles.py` and `services/twitch/signature.py` — the last where it belongs, since
none of it was ever about Discord. The `helper/` package went with them: it was
left holding one module, and a directory is not a subject.

**A name is imported from the module that owns it.** `services/__init__.py` and
`db/__init__.py` re-exported everything beneath them, which is what let the
split above land without touching a consumer — a migration convenience, and it
outlived the migration. Both are docstrings now, so `from services import
send_embed` is `from services.send import send_embed` and a reader lands on the
file that defines it. `db/models/__init__.py` still re-exports, because
importing it is what registers the tables on the metadata Alembic reads.

**Escaping depends on where the text lands, not on whether it is untrusted.**
`discord.utils.escape_markdown` escapes `*`, `_`, `~`, `|` and a backtick, and
neutralises a whole `[label](url)` through its URL awareness — but it leaves a
bare bracket alone. That is enough for a field value, where an attacker would
have to supply both brackets. It is *not* enough inside a link label, where the
bot has already supplied the opening one and `](` is all that is needed; a live
alert wraps a Twitch stream title exactly that way, so it escapes the brackets
itself. An author line and a footer are plain text to Discord and are left alone.

**`live_alert` and `stream_session` are different scopes.** A live alert exists
per broadcaster and is kept in Postgres, because it names a Discord message that
outlives a redeploy; a stream session is the main broadcaster being live, holds
the `Stream` it began for, and owns everything lasting exactly one stream — the
queue's contents and the ad-break warning — in memory. Only a stream Helix
confirms is gone stands a session down, and the alert updater is no longer what
asks: it used to call `ended()`, which made the session depend on an alert row
existing, and three ways of ending an updater never reached that call.
`docs/adr/0004-the-stream-session-ends-itself.md` has the why.

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
answers when that is: `next_birthday_on` from the parts when it is being set,
`next_birthday` from the instant when it is being rolled forward, both in
`services/birthday.py`. Two implementations of it disagreed once and
`/birthday set` wrote dates that had already passed, which is why the second now
asks the first: it reads the local date back off the instant using
`birthday_timezone` and hands it over as parts. Only a row written before that
column existed has no zone to read, and falls back to bumping the year on the
instant, and a name the tz database has dropped since is cleared with a notice
rather than raised: `next_birthday` is called one line *before* the write that
reschedules the record, so anything that raises there is a birthday that never
moves and so is never greeted again. `is_leap_day` is derived nowhere else — its answer picks the year the
instant lands in *and* is stored beside it as `is_birthday_leap`, so a second
copy could put those two out of step; the timezone path leaves it to
`next_birthday_on`, which asks the same predicate. All three birthday columns
are written together by `upsert_user`, since they describe one birthday.
`docs/adr/0003-birthday-holds-the-next-occurrence.md` records why the column
holds an instant.

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
- **Everything in the audit channel goes through `services/audit.py`.** One entry
  point per thing worth recording, each named for the event and taking the domain
  objects it happened to; the module settles the colour, the author line, how many
  embeds it takes and which channel it lands in. Nothing else may resolve
  `config.channel("audit_logs")`. It touches no database: `cogs/events.py` gathers
  the facts, including reading back a message the gateway cache has dropped, and
  passes them in. The welcome and goodbye embeds are deliberately not entries —
  they go to `welcome`, and they are announcements to members rather than a record
  for staff.
- **No audit entry can build a value Discord will reject.** Capping happens in
  `_cap`, reached from `_embed` (description, 4096), `_field` (field value, 1024
  and non-empty) and both author helpers (256), so a call site cannot produce one
  by forgetting. This is enforced rather than documented because the documented
  version was false: four separate values were outside it, and the worst — a
  member's roles at 43 mentions — cost the entry *and* the row deletion queued
  after it, leaving a departed member in Postgres for good. Footers are bounded by
  construction, being ids and fixed text.
- **What a person wrote is escaped; what the bot wrote is not.** `_said` for a
  person's words, `_named` for a name going into a description, `_quoted` for
  content that may instead be the bot's not-found marker. The distinction is not
  cosmetic: a description and a field value both render masked links, so an
  unescaped message can put a label of its choosing on a URL of its choosing in
  front of whoever reads the audit log. An **author line** and a **footer** are
  plain text to Discord and are deliberately left alone, which is why a name is
  escaped in one place on an entry and not the other.
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
