---
schema: 1
id: n013-post-task-reflection
kind: domain
title: "Post-task reflection"
confidence: 0.6
status: active
source: extractor
created_by: seed
created_at: 2026-08-29T09:26:58.276Z
updated_at: 2026-08-29T09:26:58.276Z
---

# Post-task reflection

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

_Seeded from CLAUDE.md. Edit or archive if outdated._
