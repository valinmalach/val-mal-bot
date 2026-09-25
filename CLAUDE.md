@AGENTS.md

<!-- verity-memory:start -->
## Project Memory

This project has a knowledge graph maintained at `.verity/memory/`. Before starting
non-trivial work, scan `.verity/memory/index.md` for decisions, gotchas, and patterns
that may apply to the change you are about to make. Open specific node files via
the Read tool when the title or scope suggests relevance.

The graph is auto-maintained by Verity. Files at `.verity/memory/_archive/` are
superseded — ignore them unless investigating history.

Knowledge the organization's other repositories learned is mirrored outside the
repo at `~/.verity/orgs/<host>/<owner>/memory/` (`verity memory org` lists it).
Each claim says where it came from; one that is wrong here is demoted for
everyone with `verity memory demote <id> --reason "…"`.

## Quality gate: accepted risks

When the Verity pre-commit/pre-push gate FAILs, fix the findings — that is the
default. Use `verity waive <pattern-id> --file <path> --reason "…"` ONLY to relay
a risk a human has explicitly accepted: a named code-review finding, an ADR, or
the user saying so in this conversation. The --reason must cite that source.

Never waive on your own judgment, to get past a block, or pre-emptively. A waive
binds to the file's current bytes and voids automatically when the file changes,
and every waive is recorded in the run ledger. For a pattern-level false positive
use `verity feedback finding <run-id> <pattern-id> false_positive` instead.

## Post-task reflection

When a task is complete (you've created a PR, the user says "done" or "ship it",
or the work is clearly finished), **draft the reflection yourself** — 1–3
concrete things worth remembering (a decision, a gotcha, or a pattern), each
cited with the files / PR / commands / error-signatures it came from. Skip
entirely if nothing non-obvious happened — that judgement is the ONLY filter,
because nothing reviews the reflection before it lands.

Then record it straight away. There is no confirm step, in any environment:

```bash
verity reflect --user-input "<your draft>" --kind <kind>
```

Add `--confirmed` ONLY when the user authored or dictated the words. Without
it the node is stored as `source: agent` — Verity thought this, nobody checked
it. With it, `source: user` at full confidence — a person stands behind this.
Never claim the second for your own draft, however good it is.

**Name the files in the text.** Verity scopes the reflection to the paths it
cites, and a reflection that names no file in this repo is never retrieved for
a later review — it is recorded and then invisible. The command says so when it
happens; `--file-globs "<path or glob>"` is the fix when the prose cannot carry
the paths.

Then tell the user, in one line, what you recorded and where: the command
prints the node id, the path under `.verity/memory/`, and a dashboard link.
They did not agree to it in advance, so say it happened — editing or deleting
that file is how they correct it.

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

## Never report a false positive without `--file`

The generated section above says to use `verity feedback finding <run-id>
<pattern-id> false_positive` for a pattern-level false positive. Always add
`--file <path>` (and `--line`). Without it the suppression is scoped to
`**/*.py` — every Python file — and it cannot be listed or removed from the
CLI afterwards. This has already blinded `type-safety` on this repo once; the
reproduction and the consequences are in `VERITY.md`.

Before treating a Verity run as clean, read `suppressions_applied` in its JSON.
Zero findings and a blinded pattern look identical.

## Verity runs before the commit, never after

`verity analyze` must come back clean **while the change is still uncommitted** —
staged or unstaged, both count — and only then `git commit`.

This is mechanical, not a preference. `getChangedFiles()` in the Verity CLI takes
the union of `git diff --name-only HEAD`, `git diff --name-only --cached` and
untracked files. Committed work is reached only through
`.verity/.last-reviewed-sha`: a PASS or WARN writes the HEAD it was taken at
(unless the change was too large to review whole), and everything committed since
is added to the next reading. That file did not exist in this repo when this rule
was written and does now, so a reading taken after the commit is no longer always
empty. It is a batch of every commit since the last PASS rather than the one you
meant to check. Observed 2026-09-20 (Verity 0.33.1): with the file at `727670b`, 81
commits behind HEAD, the Stop review took in 104 files and reported them
unreviewed under the byte cap. A truncated review does not move the file (the write
is skipped when the delta was truncated, per the source), so once the backlog is
over the cap it stays there and every turn ends "not a clean review" until the file
is set to a commit a person accepts as reviewed. With no
such file the fallback looks back exactly one commit and only within 120 seconds of
it being made. So a clean tree with nothing analyzable since the last PASS answers

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
committed and already reviewed, or nothing in it is analyzable.
`ANALYZABLE_EXTENSIONS` covers `.py` and the other source extensions but not `.md`,
so a docs-only commit cannot be gated by `analyze` and will always answer this way
(the pre-commit hook is a separate surface and has reviewed staged markdown on this
repo) — which is fine, but `analyze` on it must be reported as "not gated", never as
a green gate.
`VERITY.md` and everything under `.verity/` and `.codacy/` (the Standard included)
are Verity's own and are never counted as changed. For anything touching `.py`,
seeing this message means the reading was taken too late.

The same applies to a whole branch: getting a real reading after the fact means
uncommitting or re-running per commit, not quoting the empty PASS.
<!-- /verity-memory:preserve -->
<!-- verity-memory:end -->
