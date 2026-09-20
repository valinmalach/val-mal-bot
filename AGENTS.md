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

Checks, all clean before committing, and **in this order**:

```sh
sourcery review --fix .                            # apply what it can fix mechanically
sourcery review --check .                          # what is left needs a person
uv run ruff format . --exclude .venv
uv run ruff check . --exclude .venv
uv run pyright                                     # the [tool.pyright] settings Pylance also reads
uv run pytest --cov                                # the tests; CI runs the same and reports to Codacy
```

`sourcery` is a global `uv tool` (`uv tool install sourcery`), kept current with
`uv tool upgrade --all` along with the Ruff and analyzers `VERITY.md` describes. Ruff and
pyright are locked in `uv.lock`, so CI runs the same versions as the commands above, and a
release reaches both when the lock changes, not before. Sourcery is not in CI: its CLI needs
an account token of its own, so its four custom rules are enforced by running it here.

The linters read their rules from the repo, so the editor, the commands above and Codacy
agree: `[tool.ruff]`, `[tool.pylint]` and `[tool.bandit]` in `pyproject.toml`,
`.markdownlint.jsonc` and `.prospector.yaml`. Ruff selects the families Verity's Standard
names (F, B, S, ANN) plus BLE, I, SIM, UP, RUF and C90. `ANN401` and `UP042` are ignored
on purpose: `Any` is deliberate in the generic config accessors, and a `StrEnum` would
change `str()` of the `(str, Enum)` classes. A suppression says why in a comment above the
line and marks Ruff and Bandit together, `# noqa: S104  # nosec B104`; Bandit reads
everything after `nosec` as test ids, so no prose follows it. Ruff's formatter wraps a
line that gets long, which moves a trailing `noqa` off the line Ruff reports on, so keep
the markers short.

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
see: a bare `create_task`, the audit channel outside `valmal/bot/audit.py`, a chat
send outside `valmal/twitch/client/chat.py`, and `str.format` on database text. Each
excludes the one file that legitimately does the thing, and those exclusions are
relative to the config file — moving it breaks them silently. The Google style
set (`sourcery review --enable gpsg .`) is deliberately not enabled: 232 of its
272 findings here are the docstring mandate the comment convention below rejects.
It is worth running by hand occasionally for the dozen findings that are not.

**A PEP 695 parameter list blinds Sourcery to the whole file, silently.** Sourcery
1.45 and 1.46 (both checked, with a planted `create_task` and `print` in a file with
and without a `def f[T]`) return no pattern-rule findings at all for a file containing
`def f[T]`, `class C[T]` or `type X = ...` — no parse error, no warning, and a clean
report that reads exactly like a clean file. Worse, on 1.45 it is partial: structural rules such
as `no-long-functions` still fire, so the output looks normal while every custom
rule above has stopped guarding that file. `valmal/twitch/eventsub/router.py` and
`valmal/twitch/client/helix.py` are in this state today, because `_route[E: BaseModel]` and
`fetch[T: BaseModel]` are worth more than the coverage; nothing else should join them
without knowing the trade. `has_configured_role` uses a module-level
`TypeVar` for exactly this reason — it needs the annotation and the coverage both.

Migrations — see `valmal/db/README.md` for the rules:

```sh
uv run alembic revision --autogenerate -m "..."    # diff the models against the database
uv run alembic revision -m "..."                   # empty revision, for a configuration change
uv run alembic upgrade head --sql                  # print the SQL without connecting
uv run alembic downgrade -1
uv run alembic current
```

On Windows `--sql` needs `PYTHONIOENCODING=utf-8`: some seeded text is emoji.

**The suite covers everything that does not need a live service.** About 2,300 tests
cover 99% of the code outside `migrations/`, branches counted; what is left is `__main__`
guards, a demo, and lines that cannot be reached. Nothing runs against Discord, Twitch
or Postgres, so a real database round trip and a real gateway session are untested:
Helix and OAuth go through `httpx.MockTransport`, the FastAPI apps through
`httpx.ASGITransport`, repository statements are compiled with the Postgres dialect and
asserted, and `tests/test_migrations.py` renders every revision offline in a subprocess
to check the chain, the rules in `valmal/db/README.md`, the schema against the models, and that
every configuration key the code reads is seeded. Do not describe a change as tested
unless a test exercises it, and a bug a test finds is fixed with a regression test that
fails without the fix.

`tests/` mirrors `valmal/`, and an area with fixtures or fakes of its own has a
`conftest.py` and a `support.py`, imported by path as `tests.twitch.stream.live_alert.support`
— which is why Sourcery's
`dont-import-test-modules` is disabled by id in `.sourcery.yaml`. Keep a test file under
400 lines, which is Verity's `file_length` signal; past about 470 a review also drops the
middle of a file and says it is unchecked. `tests/conftest.py` fills the environment
`valmal.core.settings` validates at import, so it must run before a test module imports
anything that reaches `settings`. Anything that walks the repo (the seeded-key scan) skips `.claude/`, where agent worktrees
hold whole copies of it; coverage names the `valmal` package and `main` instead of walking the
tree, so those copies, `.verity/` and `.codacy/` never count.
Async tests carry `pytestmark = pytest.mark.anyio` and share one event loop for the
whole session, held open by the `_one_event_loop` fixture in `tests/conftest.py`; a loop
per test cost a socket pair each on Windows, and about one full run in twelve blocked
for good creating one. A test that leaves a task behind is therefore cleaned up by the
next one, not by its own loop, and a task that must not outlive the test is the test's
to cancel. `[tool.pytest.ini_options]` turns an unawaited coroutine into a failure with
two filters, not one: the warning is raised while the coroutine is collected, so pytest
reports it as an unraisable exception, and `error::RuntimeWarning` alone lets it
through. A 60 second `timeout` (pytest-timeout) names a test that hangs: on Linux it
fails that test, on Windows it can only dump the stacks and end the run.

Three habits that each cost a debugging session. Patch with `monkeypatch`, never by
assigning onto a module, or the fake leaks into the next test; assigning is only safe
where a fixture the test uses has already `monkeypatch.setattr`ed that name, which is
what restores it. Replace a module's own
`time` or `asyncio` name with a namespace holding the fake, never the global
`time.monotonic` or `asyncio.sleep`, which the event loop itself reads. And write a
non-ASCII or control character in a test as `chr(...)`: the editing tools turn an
escape sequence typed into a source file (a backslash then `u` and four digits, or a
backslash then `n`) into the literal character, which leaves an invisible one behind.

**CI is three jobs on every push, and a red one stops a merge.** `coverage`, in
`.github/workflows/coverage.yml`, runs `pytest --cov` and uploads `coverage.xml` to Codacy
when the `CODACY_PROJECT_TOKEN` secret is set. `.github/workflows/checks.yml` adds `lint`
(ruff format, ruff check and pyright) and `migrations`, which applies every revision to a
real Postgres 17, checks the result against the models with `alembic check`, downgrades
to base and upgrades again: the one thing the suite, which only renders SQL, cannot do.
Verity's `test_coverage` threshold is 95 and `test_quality` judges whether a test can fail;
`.verity/standard.yaml` has both, and `verity standard push` uploads a change to them.

**Merging to `master` is guarded by a ruleset**, which lives in the repository's settings
and nowhere in the tree, so this is its record. A pull request is required, with no
approval count because a sole maintainer cannot approve their own; review threads must be
resolved; the branch must be up to date with `master`, so what CI tested is what merges;
and `coverage`, `lint`, `migrations`, `Codacy Diff Coverage` and `Codacy Coverage
Variation` must pass. Nobody bypasses it, force-pushes or deletes the branch.

What is deliberately *not* required, and why. `Codacy Static Code Analysis` was red on
the last five PRs before this, every finding in test code (the fake credentials in
`tests/credentials.py`, a subprocess helper, `import_module`), so requiring it would have
blocked each until dismissed by hand; exclude tests from those tools in `.codacy.yaml`
and it can be added. `CodeQL` does not report on every PR, so a requirement on it would
leave some at "Expected" forever. `Sourcery review` is skipped on most, and the two
`Request reviews` jobs only ask for reviews. A required check has to be produced by the
branch, so a job renamed here has to be renamed in the ruleset too, or every PR sits at
"Expected".

**A full review on every push is requested by `.github/workflows/request-reviews.yml`,
because neither reviewer does one itself.** Sourcery re-reviews each commit on its own,
but only re-checks its existing comments and re-runs its security scans: the inline
comments are not regenerated, and after five re-reviews it stops. `@sourcery-ai review`
runs a full one, and Sourcery ignores comments from other bots, so the workflow posts it
with `SOURCERY_TRIGGER_TOKEN`, a fine-grained personal access token set to no expiry.
Sourcery reviews a PR when it opens, so that job skips `opened`. Codacy's
AI Reviewer posts a review only when it is triggered: with *Run reviewer* on
"Automatically (first review only)" the summary says the first review was requested and
nothing appears (seen on #79), and the setting has no every-push mode either. So that
job runs on `opened` as well, calling the `ai-reviewer/trigger` endpoint with
`CODACY_API_TOKEN`, an account token; `CODACY_PROJECT_TOKEN`, which uploads coverage,
does not authorise that call. Both also run on `ready_for_review`, because a draft is
skipped when it opens and would otherwise get no review until its next push, and on
`reopened`, so a PR brought back from closed is reviewed too. Each secret is handed to
the one step that uses it. A run with no secret (a fork, Dependabot, or one never
configured) skips that step rather than failing, while a token that exists but is
rejected still fails it, which is how a revoked one gets noticed. Two quick pushes can
cost two reviews, since nothing here withdraws a request already accepted. The Codacy call is not retried, because a trigger
Codacy accepted whose reply was lost would be sent twice; the first run answered 2xx
within a second, before Codacy had started analysing that commit, so nothing needed it.

One more gate runs at commit time. `git commit` is intercepted by Verity, which
analyses the staged diff and can block the commit. It is a Claude Code hook, not a
git hook — there is nothing in `.git/hooks`, and it does not fire for other tools. The
Verity plugin also reviews what changed at the end of every turn, which cannot be
switched off under the plugin (`VERITY.md` has the detail). Its rules, and the narrow circumstances in which a finding may be waived, live in
`CLAUDE.md`; the Standard it enforces is `.verity/standard.yaml`, and `VERITY.md`
covers the setup. Run it by hand with `verity analyze`. Everything Verity generates
is gitignored, so a fresh clone has none of it — `VERITY.md` has the restore steps.

## Layout

Everything importable lives in one package, `valmal/`, so the repository root holds
`main.py` and configuration and nothing else. A directory names one concern and holds
everything for it, rather than one directory per kind of file:

- `valmal/core/` — plumbing the rest leans on: `settings` (the `.env`), `config` (the
  database-backed configuration) and the `safe_format` it renders with, `errors`,
  `background`, `logging_json` and `http_client`.
- `valmal/db/` — Postgres: `models/` (the tables), `session` and `repository`.
  Alembic's revisions are `migrations/`, at the root.
- `valmal/bot/` — Discord: `client` (the bot and its gateway handlers), `cogs/`, `views`,
  and what says something in a channel: `audit`, `send`, `present`, `roles`,
  `birthday` and `duration`.
- `valmal/twitch/` — Twitch: `models/` (Pydantic payloads), `client/` (Helix, chat and subscription health),
  `oauth/` (the grant flow, its routes and the stored tokens), `eventsub/` (the signed
  webhook route, its replay protection, what each event makes the bot do, chat
  commands, and repointing subscriptions) and `stream/` (what a live stream means:
  the Discord alert, the session and the shoutouts).

`bot` and `twitch` lean on each other — the cogs call Twitch, and Twitch says things
through `bot.send` — and the deferred imports under Conventions are what keep that
from being a cycle at import time. `tests/` mirrors this tree.

## Architecture

**One process wearing two faces.** `main.py` is a FastAPI app, and the Discord bot
is not the entrypoint: the lifespan handler starts `main()` as a background task,
which loads `COGS` (`valmal/bot/cogs/__init__.py`) and calls `bot.start()`. Twitch never connects to the
bot — it delivers EventSub webhooks over HTTP to the router in `valmal/twitch/eventsub/router.py`,
which verifies and parses them and hands each to `valmal/twitch/eventsub/events.py`.
Stop the web server and the bot goes with it.

**Startup order is spread across three files.** lifespan (`main.py`) → cog loading →
`MyBot.setup_hook()` (`valmal/bot/client.py`: `config.load()`, `token_manager.load()`,
the shoutout drainer, command prefix, guild command-tree sync, persistent view
registration, `check_birthdays`/`recheck_subscriptions` started on the `Tasks`
cog behind their own `before_loop` wait for `bot.wait_until_ready()`) →
`on_ready`, the one gateway-connection handler left. The two loops start from
`setup_hook`, not from `Tasks.cog_load()`: `cog_load` runs before `bot.start()`
calls `login()`, too early for `Client._ready` to exist, so
`wait_until_ready()` there raises immediately and the loop dies silently,
never to run again — `setup_hook` runs inside `login()`, after the internal
setup that creates `_ready`, and like the drainer only needs to happen once.
`on_ready` fires again every time a gateway session cannot be resumed, and it
answers two different questions with two separate flags instead of blurring
them: `_started` tracks whether the Helix/DB-heavy work in
`run_background_tasks` has *succeeded*, so a reconnect can't repeat a
successful run and `live_alert._start`/`stream_session._start` no longer have
to defend against that case themselves — but `run_background_tasks` clears
`_started` on failure, so a later reconnect still retries rather than
stranding a live alert or the stream session for good; `_announced` tracks
whether the startup notice has been *delivered*, which a failed send is
likewise worth retrying on the next reconnect. Anything wanted once per
process belongs in `setup_hook` for that reason.

**A gateway listener has one floor.** `on_error` (`valmal/bot/client.py`, a function on
the `bot` instance via `@bot.event`, matching `on_ready` — not a method on `MyBot`)
is what discord.py calls when a dispatched listener — cog listeners included —
raises past it, handing over the event name it was dispatched as. Every listener in
`valmal/bot/cogs/events.py` used to wrap its own body in `except Exception: await report(e,
"Fatal error with on_X event")` for exactly that; `on_error` does it once, from
`sys.exc_info()`, and a new listener needs no boilerplate to be covered. A listener
that needs to report more than its own name keeps its own `try`/`except` and says
why — none currently do. `_safe_db_operation`'s `try`/`except` is not this: it
guards one write inside a listener so that failure doesn't abort the rest of the
listener's body, and names the write, not the event. This floor is dispatch-only:
a `discord.ui.View`/button callback that raises reaches `View.on_error` instead,
which is unrelated and still just logs — not covered here, and not something
issue #24 touched.

**Two configuration sources, one hard line.** `.env` → `valmal/core/settings.py` answers *how
this instance authenticates and where it runs*, validated once at import and failing
with the raw values redacted. Postgres → `ConfigCache` in `valmal/core/config.py` answers
*what the bot does*, snapshotted once in `setup_hook()`. `config.load()` has that one
call site and nothing else calls it again, so a database edit takes effect on the next
restart, not before. `valmal/db/README.md` records which side each value falls on and why;
keep new values on the right side of that line.

**IDs are addressed by slug, never by value.** Resolve at call time —
`config.channel("audit_logs")`, `config.role("follower")`,
`config.setting("twitch_broadcaster_id")`, `config.template(...)`. Never put a
snowflake in code or hold one across calls, so an ID can change in the database
without a redeploy.

**Changing configuration means writing a migration, not editing a file.** The rows
live in revisions. `valmal/db/README.md` has the three rules that matter — never import the
models or read a data file from a revision, make inserts tolerate a conflict, and
guard an UPDATE with the value it replaces — read it before touching `migrations/`.

**Nothing connects at import time.** `valmal/db/session.py` builds the engine lazily, so
importing any module is safe without a reachable `DATABASE_URL`.

**Twitch user grants start in Discord.** The owner-only `/twitch-auth` command
creates short-lived links for the `user` (the account in `twitch_bot_user_id`)
and `broadcaster` (the account in `twitch_broadcaster_id`) authorization-code
flows. Both request `twitch_app_scopes`; the callbacks validate the returned
Twitch user ID, client ID and scopes before upserting `oauth_token`. The `app`
row is separate, uses client credentials and has no refresh token.

**Three files, three jobs.** `valmal/twitch/eventsub/router.py` receives a signed
notification, verifies it, parses it and hands it on; whether a delivery is fresh and
has not been handled already is `valmal/twitch/eventsub/replay.py`, which the router
composes. `valmal/twitch/eventsub/events.py` says what each event makes the bot do —
it is a module of its own because it names no HTTP type at all, and only `main.py` and
the admin cog import a router. `valmal/twitch/oauth/router.py` carries the two
authorization-code flows on their own router; it shares nothing with the webhook
router, not the signature check, the models, or the path prefix.

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
six of the eight cannot be recreated from this repo. That is why the check sits
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

**Only two of the eight EventSub subscriptions can be created from nothing.**
`/subscribe` creates `stream.online` and `stream.offline`. Chat, follow, ad break,
raid, moderate and channel-point redemption are provisioned outside this repo, so
nothing here can bring one back once it is gone. **Repointing an existing one is
different, and covered:** `/migrate-subscriptions` moves all eight types to the
current `APP_URL`, which is what a deployment whose public URL changes needs. The
startup check notices an undeliverable subscription; the command is what repairs it.

**The migration works off the live list, never a fixed one.** There are eight
*types* but `6 + 2N` *subscriptions* — `stream.online`/`stream.offline` exist once
per subscribed broadcaster, and nothing here knows what N is, because that list
lives only in Twitch. `valmal/twitch/eventsub/migrate.py` therefore starts from
`get_subscriptions()`; a migration seeded from a list written in this repo would
silently leave every promo broadcaster behind.

**The rule, the `Outcome` shape and the rendering of either live apart from the
I/O.** `valmal/twitch/eventsub/migrate_plan.py` holds `Action`, `decide`, `Outcome`,
`condition_of` and everything that renders one (`describe`, `render_dump`,
`summary`) — nothing in it touches Helix or Discord. `valmal/twitch/eventsub/migrate.py`
keeps `_logins`, `_exists_at`, `_repoint`, `migrate` and `_confirming`, and
imports the plan, never the other way — the same one-way split as
`live_alert.py`/`live_alert_cycle.py`: the loop reads a conclusion rather than
watching the I/O that reached it. `decide` is pure, like `live_alert_cycle._decide`.

**`decide` reads the callback and deliberately ignores the status.** It once
repointed anything not `enabled` even when the callback was already right, on the
reasoning that a disabled subscription delivers nothing whatever its callback says
— true, and not this command's problem, because what that bought was a
delete-then-create on a subscription already pointing where it should.
`webhook_callback_verification_pending` is a seconds-long transient on the way to
`enabled`, so catching one mid-verification destroyed something about to arrive by
itself; `authorization_revoked` and the `*_removed` statuses are not things
recreating repairs, so the delete turned a subscription still visible in
`get_subscriptions()` into one that is gone. A correct callback that is not
delivering is already `undeliverable`, which `recheck_subscriptions` reports hourly
— that check owns the problem, this one owns the callback. Do not restore the
status condition.

**A 409 on the recreate is checked against Helix before it is called a loss.**
`create_subscription` is `repeatable`, so a POST whose reply was lost is re-sent
and Twitch answers the retry 409 *because the first attempt created it*. A 409
therefore says the subscription exists at least as often as it says something else
got there first, and reporting it as destroyed would send somebody to hand-recreate
a subscription already in place — where, for the six types provisioned outside this
repo, the attempt 409s too. `_exists_at` answers False when its own lookup fails:
over-reporting a loss costs a needless check, under-reporting one costs a
subscription nobody knows is missing.

**A failed delete and a failed create are different outcomes and are reported
apart.** `stuck` did not move and is still delivering on the old callback, so
re-running is the whole remedy; `lost` is destroyed, and for six of the eight types
the dump is the only way back. One list for both made a working subscription and a
destroyed one read identically in the reply an operator sees first, which is the
one moment that distinction matters.

**A second confirmed run refuses while one is in flight.** Two would interleave
deletes and creates over the same subscriptions: the loser of a delete race is
reported untouched when it was destroyed, and the loser of a create race gets a 409
that no longer means what the check above assumes. A plain module flag rather than
an `asyncio.Lock`, because what is wanted is refusal and a lock queues — and a run
that waits and then finds nothing to do looks exactly like one that was not needed.
Taken with no await between the check and the set, for the reason
`valmal/twitch/eventsub/replay.py`'s `claim` is one step. A dry run is not guarded: it changes
nothing.

Twitch's uniqueness key is the type and the condition **alone, not the transport**,
so the same event at a new callback is a 409 and repointing has to be delete-then-
create. Three consequences the module is built around, none of them optional: the
complete definition of every subscription goes to the admin channel as a file
*before* anything is deleted, since after the delete it is the only record one
existed; a failure is reported per subscription and does not abandon the ones
behind it; and a dry run is what you get unless you pass `confirm`. A type with no
route is reported and **left alone** — deleting it would destroy something created
deliberately elsewhere, and nothing here could recreate it.

`SubscriptionCondition` is the one model here that keeps what it does not declare
(`extra="allow"`), and that is the migration's doing rather than an oversight. Its
five keys cover the eight subscriptions in use, so declaring them is what lets the
rest of the code read one by name; but a condition is now *round-tripped* to
recreate a subscription, and a key outside those five — a `reward_id`, or anything
Twitch adds — would otherwise be dropped in silence and recreate a subscription
**broader than the one it replaced**, with nothing to say so. This is the opposite
of the rule for EventSub payload models above, and for the opposite reason: those
are read, this one is written back.

**What the model keeps and what the recreate sends are different sets, and the
difference is load-bearing.** `condition_of` drops every value that is `""`.
Twitch answers with `""` for the unset half of a `channel.raid` condition — both
directions are subscribed, so both arrive that way — and its create endpoint says
of that pair: *"Set either the from_broadcaster_user_id or to_broadcaster_user_id
condition parameter but not both. If you pass both parameters, the subscription
request fails."* A faithful round-trip therefore deletes both raid subscriptions
and recreates neither, on a type provisioned outside this repo that nothing here
can rebuild. Dropping `""` is not lossy: it is how Twitch says *not set* on the
way out and absence is how it requires the same thing on the way in. Compared
against `""` and not tested for falsity, so a `0` or `False` Twitch chose to send
in an undeclared key survives. **Do not make this round-trip faithful again.**

**`WEBHOOK_PATHS` is derived from the `_route` table, not written beside it.**
`valmal/twitch/eventsub/router.py` builds it as the routes register, reading each type off the
`Literal` its model already declares — a ninth list of the eight types is one more
thing to keep in step by hand. It is passed *into* `migrate`, never imported by it,
because nothing under `valmal/twitch/` imports a router; `valmal/bot/cogs/twitch_admin.py`
may, and is what hands it over.

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
`valmal/twitch/stream/autoshoutout.py` owns the list, the rule and the four places
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
directly, having a chat event already in hand. A redemption is the third way of
arriving and takes the ordinary path: any custom reward counts, including one
still queued for approval or later refunded, because the point is that they
turned up rather than what they bought.

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

**A subject per file, and no file holding six.** A `helper/helper.py` once held
sending, presentation, durations, birthdays, roles and webhook signatures; they are
now `send.py`, `present.py`, `duration.py`, `birthday.py`, `roles.py` (all in
`valmal/bot/`) and `valmal/twitch/eventsub/signature.py` — the last where it belongs,
since none of it was ever about Discord. A directory is a concern, not a place to
put whatever is left: it is the same reason `constants.py`, which held three domains'
values in one file, is gone, and each value sits with the code that uses it.

**A name is imported from the module that owns it.** No package `__init__` re-exports
what is beneath it, so `from valmal.bot.send import send_embed` lands a reader on the
file that defines it, and there is no `controller/__init__` or `init/__init__` handing
on a name from somewhere else. The package `__init__` files are docstrings that say
what the directory is for. `valmal/db/models/__init__.py` is the exception, because
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

**Every Twitch chat line goes through `valmal/twitch/client/chat.py`.** `say` and
`say_template` report their own failure and never raise, so a line Twitch refused
cannot fail whatever was saying it. `api.send_chat_message` is not re-exported
anywhere — three separate rounds of guarding chat sends each missed a
different call site, so the guard moved to the one place they all pass through.

**Every Helix call goes through `valmal/twitch/client/helix.py`.** It owns the token
choice, the pre-emptive refresh, the one 401 re-send, retry, status checking and
parsing, and it raises `HelixError` rather than reporting — whoever catches has
the context worth reporting. `api.py` holds the endpoints on top of it: `None`
means Twitch has nothing, a failed call raises. Retry follows the method, not the
call site; `docs/adr/0002-helix-posts-are-not-retried.md` says why POSTs do not.

**A stored birthday is the next occurrence, not a date of birth.** One rule
answers when that is: `next_birthday_on` from the parts when it is being set,
`next_birthday` from the instant when it is being rolled forward, both in
`valmal/bot/birthday.py`. Two implementations of it disagreed once and
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

**Two model packages, both called `models`.** `valmal/twitch/models/` is Pydantic:
Twitch API responses and EventSub payloads. `valmal/db/models/` is SQLModel: the
tables. Each sits under the concern it belongs to, so the path says which is which.

## Conventions

- **Background work goes through `background.fire_and_forget`, always named.** asyncio
  holds only a weak reference to a running task, so a bare `create_task` can be
  collected mid-flight. It also reports a task that died without its own handler
  running, which is the last catch in the process — and the report is keyed on the
  task's name, so an unnamed task would report under a new name every time.
  A `@tasks.loop` cog task does not need it for the reference: discord.py keeps
  the task on the `Loop`, which the cog keeps, and names it. `valmal/bot/cogs/tasks.py` is
  the example; reviewers read it as a bypass about once a round. It does still
  need its own reporting — a `Loop` that dies only logs — so wrap the body in a
  `try` that calls `report`, as `check_birthdays` does.
- **Deferred imports inside functions** are load-order management, not style:
  `valmal/bot/client.py` and `valmal/bot/views.py` import their collaborators lazily to
  break cycles. Leave them.
- **`token_manager` and `shoutout_queue` are singletons** (`__new__`). Import the
  instance; do not construct another.
- **Everything in the audit channel goes through `valmal/bot/audit.py`.** One entry
  point per thing worth recording, each named for the event and taking the domain
  objects it happened to; the module settles the colour, the author line, how many
  embeds it takes and which channel it lands in. Nothing else may resolve
  `config.channel("audit_logs")`. It touches no database: `valmal/bot/cogs/events.py` gathers
  the facts, including reading back a message the gateway cache has dropped, and
  passes them in. The welcome and goodbye embeds are deliberately not entries —
  they go to `welcome`, and they are announcements to members rather than a record
  for staff.
- **An audit entry's sentences are `message_template` rows; what fills them is
  code.** The sentences of the description, and a caption author that names the
  event, are `audit_*` templates, so they change by migration like the rest of the
  configuration. Everything else stays in `valmal/bot/audit.py`: field labels,
  footers, a description that is only values (a mention and a name) and so has no
  sentence in it, and the values a template is filled with — names, mentions,
  times, a guild's name as a caption, and the marker for a value that is absent
  (`UNKNOWN_USER`, `Never`). A sentence that flips on a condition — ban or unban,
  pinned or unpinned, one role or several, a deleter named or not — is one row per
  variant, not one row with a fragment passed in, because a fragment hides English
  in code and the rule is that none is left there. A new entry adds its rows in a
  migration.
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
- **Everything the bot says about itself goes through `valmal/core/errors.py`.** `report(exc,
  context)` for an exception, `notify(text)` for anything else worth the admin
  channel, `notify_soon(text)` for synchronous renderers that cannot
  await, `notify_file(text, filename, content)` for a notice carrying a record
  too long for a message. None of them raise, all log locally first, and all say
  so when the channel is out of reach. `notify` returns whether the channel has
  the news, for the one caller that retries. Nothing else may resolve
  `config.channel("bot_admin")`.
  `notify_file` is the one that deliberately skips the fifteen-minute repeat
  window: it carries what somebody needs in order to undo what the bot is about
  to do, and two inside one window are two different records, so standing the
  second in for the first would leave a destruction with nothing to reverse it.
- **A path that degrades or gives up says so in the admin channel.** Catching a
  `HelixError` to carry on without an avatar is fine; catching it into
  `logger.warning` alone is not. The logs are not watched and the Discord server
  is. Volume is not a reason to stay quiet: `valmal/core/errors.py` holds back a repeat of
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
- **The accessors that hand text to Discord resolve `{channel:key}` and `{role:key}`
  themselves.** `config.template()`, `config.embed()` and `config.auto_response()`
  return it already rendered, and `embed()` returns a `RenderedEmbed`, not the table
  rows, so a caller cannot send an unresolved placeholder by forgetting a step — it
  has no raw text to send. Nothing outside `valmal/core/config.py` calls `render`; a
  new accessor over stored text must render before it returns, as these do. An
  auto-response goes out with `AllowedMentions.none()`, because anyone can trigger
  one and a rendered `{role:key}` in a message would otherwise ping the role.
- **Twitch chat never carries those two placeholders, on purpose.** It cannot show
  a Discord mention, so no Twitch row may hold `{channel:key}` or `{role:key}`; the
  values a chat line does fill are `{chatter}`, `{target}` and `{broadcaster}`. A
  `twitch_command_response` skips `render` and would go out with the placeholder
  written literally. `say_template` goes through `config.template`, which renders,
  so a placeholder in one of its rows would go out as a raw `<#id>`. Neither is
  guarded; nothing seeded does it.
- **Every line either process emits is JSON, through `logging_json.JsonFormatter`.**
  Railway colors a line by which stream it landed on unless the line itself
  parses as JSON with a `level` key — `main.py` points the root logger's one
  handler at stdout through it, and `alembic.ini` does the same for the
  migration process, which never sees `main.py`'s setup. `extra={...}` on a
  call becomes a queryable top-level key. Nothing may call
  `logging.basicConfig` a second time, install a further handler, or `print`/
  write to stdout or stderr directly — any of those is a line Railway
  mis-levels by the stream it came in on.
- **A new cog needs an entry in `COGS` (`valmal/bot/cogs/__init__.py`).** Nothing auto-discovers.
- **Comments record a non-obvious *why*, or do not exist.** Match the density in
  `valmal/db/base.py` and `valmal/db/config.py`; do not narrate what the code already says.
- **An admin-only command needs `app_commands.checks.has_permissions`, not just
  `default_permissions`.** The latter is a Discord UI default a server admin can
  reconfigure away — discord.py's own docs call it "only a hint" — so it enforces
  nothing at runtime. `has_permissions` raises `MissingPermissions` (and
  `has_configured_role` a plain `CheckFailure`), which `bot.tree.error`
  (`valmal/bot/client.py`) answers ephemerally without reporting it as a bug, since a
  refused check is the check working; `BotMissingPermissions` and a cooldown are
  the two `CheckFailure`s it does report. Skip it only
  where a command already carries a strictly stronger runtime identity check —
  `twitch_auth` and `migrate_subscriptions` check `owner_id`, and adding
  `administrator` on top would block the owner in a guild where they hold that
  identity without also being a guild admin.

## Deployment

Railway builds with Railpack (`railway.json`), which detects `pyproject.toml` +
`uv.lock` and installs the pinned interpreter and dependencies itself — there is no
Dockerfile. `deploy.startCommand` chains `alembic upgrade head` and the app in one
shell command; Railpack runs `startCommand` through a shell, so `&&` sequences
without needing a wrapper script. Migrations run on every deploy. `APP_URL` is the
Railway domain itself now, not a tunnel in front of it — see `/migrate-subscriptions`
above for what repoints EventSub when that domain changes.

Locally, EventSub cannot reach `localhost`, so stream alerts, follows, raids, ad breaks
and chat commands never fire without a tunnel in front of it. Outbound Helix calls
still work.

## Agent skills

### Issue tracker

Issues and PRDs live as GitHub issues on `valinmalach/val-mal-bot`, driven by the `gh` CLI. See `docs/agents/issue-tracker.md`.

### Triage labels

The five canonical triage roles map 1:1 to identically-named GitHub labels. See `docs/agents/triage-labels.md`.

### Domain docs

Single-context: one `CONTEXT.md` plus `docs/adr/` at the repo root. See `docs/agents/domain.md`.
