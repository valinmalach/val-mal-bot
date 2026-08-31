@AGENTS.md

<!-- verity-memory:start -->
## Project Memory

This project has a knowledge graph maintained at `.verity/memory/`. Before starting
non-trivial work, scan `.verity/memory/index.md` for decisions, gotchas, and patterns
that may apply to the change you are about to make. Open specific node files via
the Read tool when the title or scope suggests relevance.

The graph is auto-maintained by Verity. Files at `.verity/memory/_archive/` are
superseded — ignore them unless investigating history.

## Quality gate: accepted risks

When the Verity pre-commit/pre-push gate FAILs, fix the findings — that is the
default. Use `verity waive <pattern-id> --file <path> --reason "…"` ONLY to relay
a risk a human has explicitly accepted: a named code-review finding, an ADR, or
the user saying so in this conversation. The --reason must cite that source.

Never waive on your own judgment, to get past a block, or pre-emptively. A waive
binds to the file's current bytes and voids automatically when the file changes,
and every waive is recorded in the run ledger. For a pattern-level false positive
use `verity feedback finding <run-id> <pattern-id> false_positive` instead.

> Durable, hand-curated guidance goes in the preserve region below (it survives
> regeneration) or anywhere OUTSIDE these markers. Everything else between the
> markers is tool-owned and overwritten on each run.

## Housekeeping Turns

When a turn will be pure housekeeping — pulling, installing dependencies,
rebasing, a formatting sweep you are not authoring — declare it BEFORE doing it:

```bash
verity ignore --turn --agent --reason "pulling latest before starting"
```

This skips the review for that turn, which saves the turn Verity would
otherwise spend saying it had nothing to say. Use `--for 30m` instead of
`--turn` when a single piece of housekeeping spans several turns.

**It is a claim about the turn, not a way to silence review.** The declaration
is checked against what the turn actually did: if anything is authored — by you,
by a subagent, or by a shell command that can write files — it voids, the review
runs anyway, and the broken declaration is reported. So declare housekeeping you
are about to do, never work you have already done, and never as a way to get past
a finding. Declarations are budgeted per session and every one is recorded with
its reason.

<!-- verity-memory:preserve -->
<!-- Add binding, hand-curated guidance here; it survives Verity regeneration. -->

## Verity runs before the commit, never after

`verity analyze` must come back clean **while the change is still uncommitted** —
staged or unstaged, both count — and only then `git commit`.

This is mechanical, not a preference. `getChangedFiles()` in the Verity CLI takes
the union of `git diff --name-only HEAD`, `git diff --name-only --cached` and
untracked files. Committed work is reached only through
`.verity/.last-reviewed-sha`, which does not exist in this repo, and the fallback
for that case looks back exactly one commit and only within 120 seconds of it
being made. So on a clean tree `verity analyze` answers

```json
{"gate_decision":"PASS","systemMessage":"Verity: No analyzable files changed"}
```

which is a PASS that reviewed nothing, and reads exactly like a real one.

The sequence for every commit:

```sh
git add <paths>
verity analyze     # must not be a "No analyzable files changed" PASS
git commit ...     # only after it comes back clean
```

**A PASS whose message says nothing was analyzed does not count as a gate.** That
message has two causes and they need telling apart: the change is already
committed, or nothing in it is analyzable. `ANALYZABLE_EXTENSIONS` covers `.py`
and the other source extensions but not `.md`, so a docs-only commit cannot be
gated and will always answer this way — which is fine, but it must be reported as
"not gated", never as a green gate. For anything touching `.py`, seeing this
message means the reading was taken too late.

The same applies to a whole branch: getting a real reading after the fact means
uncommitting or re-running per commit, not quoting the empty PASS.

## Post-task reflection
When a task is complete (you've created a PR, the user says "done" or "ship it",
or the work is clearly finished), **draft the reflection yourself first** — 1–3
concrete things worth remembering (a decision, a gotcha, or a pattern), each
cited with the files / PR / commands / error-signatures it came from. Skip
entirely if nothing non-obvious happened.

Then, depending on how the session is running:

- **Interactive** (a person is at the keyboard): show your draft and ask one
  question before recording —

  > "Here's my reflection draft for the project memory: \<draft\>. Anything to
  > confirm, correct, or add before I record it? (Say 'skip' to drop it.)"

  Record the final text (with their corrections) via
  `verity reflect --user-input "<final reflection>" --kind <kind>`. If they say
  "skip", do not record.

- **Autonomous** (headless / CI / cron, or `VERITY_AUTONOMOUS=1`): record your
  draft directly, never blocking —
  `verity reflect --user-input "<your draft>" --kind <kind> --autonomous`.
<!-- /verity-memory:preserve -->
<!-- verity-memory:end -->
