# VERITY.md — Quality Gate

> This project uses [Verity](https://verity.md) to enforce quality and security standards on AI-generated code.

**URL:** https://ofcamwrjwrkazqvdchko.supabase.co
**Project:** valinmalach/val-mal-bot
**Standard:** v1 — held on the service; `.verity/standard.yaml` is the local source

## Restoring on a new machine

Everything Verity generates is gitignored — the skills, the hooks, `.codacy/`, and
all of `.verity/`. A fresh clone has none of it, and this file is the only record of
how to get it back, so keep this section true.

```sh
npm install -g @codacy/verity-cli @codacy/analysis-cli
verity login                                # --force if a grant was added since your last login
verity init                                 # reinstalls the verity-* skills
verity hooks install --moments pre-commit   # commit-time gate only: no Stop hook, no pre-push
verity config get | python -c "import json,sys; print(json.dumps(json.load(sys.stdin)['content'], indent=2))" > .codacy/codacy.config.json
verity telemetry install                    # optional — cost and usage at /usage
```

The gate enforces the Standard version uploaded to the service, so it runs without a
local copy; `.verity/standard.yaml` is only the source you edit and push from, and
`verity standard get` returns it as JSON. The knowledge graph under `.verity/memory/`
re-syncs from the service on the next analysis.

On Windows, redo the two workarounds at the end of this file as well — no installer
applies them for you, and without the second one the gate runs no static analysis at
all while still reporting success.

## Quality Dimensions
- Comprehensibility (file length ≤ 400, complexity ≤ 15, function length ≤ 50, naming)
- Modularity (separation of concerns, shallow abstractions)
- Type Safety (ruff ANN001/ANN201; pyright `standard` is the project's authority)
- Test Adequacy (coverage threshold 0 — there is no test suite; see AGENTS.md)

## Security Patterns
- No hardcoded secrets (CWE-798)
- Input sanitization (CWE-20)
- Parameterized queries (CWE-89)
- Dependency verification (CWE-1395)
- No unsafe deserialization (CWE-502)
- Access control checks (CWE-639)
- Config file integrity (CWE-15)

## Project Patterns
These come from `AGENTS.md` and `db/README.md`, and are enforced by the AI reviewer:
- IDs are addressed by slug through `config.channel/role/setting/template`, never a literal snowflake
- Background work goes through `background.fire_and_forget`, never a bare `asyncio.create_task`
- Database text is rendered with `safe_format` / `config.render`, never bare `str.format`
- Self-reporting goes through `errors.report` / `errors.notify`; nothing else resolves `bot_admin`
- Configuration changes ship as an Alembic revision, under the rules in `db/README.md`

## How It Works
The gate runs at **commit time**. When the agent runs `git commit`, the Verity hook:
1. Runs static analysis over the staged diff via `@codacy/analysis-cli`
2. Sends results + code to the Verity service
3. An independent reviewer assesses the change against the Standard
4. Returns PASS / WARN / FAIL — a FAIL blocks the commit, capped at 2 self-healing cycles

## Analysis Tools
`Ruff`, `Trivy`, `shellcheck`, `Hadolint` — configured in `.codacy/codacy.config.json`
with explicitly enumerated pattern IDs (never `patterns: []`, which would enable
thousands of default rules).

**Tool binaries live in `~/.codacy/tools/<Tool>/<tool>.exe` and are kept at the latest
upstream release**, not the older versions the Codacy adapters pin. The adapters parse
`--version` only to report it, so a newer binary is used as-is. Upgrade by downloading
the release zip and extracting the `.exe` over the one in that directory.

To re-validate the pattern IDs after any change, point the validator at the installed
adapters — it cannot find them on Windows by itself:

```sh
CODACY_TOOLS_DIR=/c/nvm4w/nodejs/node_modules/@codacy/analysis-cli/node_modules/@codacy   node .claude/skills/verity-setup/validate-patterns.mjs
```

Note that the IDs this adapter defines carry a slug suffix
(`Ruff_ANN001_missing-type-function-argument`, not `Ruff_ANN001`). A wrong ID disables
the tool **silently**, so never hand-author one — derive it with `--list` and validate.

### Two Windows workarounds this setup depends on

Both are upstream bugs in the current releases (`@codacy/verity-cli` 0.31.1,
`@codacy/analysis-cli` 0.21.0). Re-apply them if the tooling is reinstalled.

1. **`codacy-analysis --install-dependencies` cannot install its own tools.** It shells
   out to `tar -xf C:\...`, and GNU tar reads `C:` as a remote host
   (`tar: Cannot connect to C: resolve failed`). Hence the manual binary installs above.

2. **Verity cannot spawn the analyzer.** `verity.js` calls
   `spawnSync("codacy-analysis", ...)` with no `shell: true`. On Windows npm installs
   that command only as `.cmd`/`.ps1` shims, so the bare name gives `ENOENT` and `.cmd`
   gives `EINVAL` (Node's CVE-2024-27980 fix). Without a fix the gate reports
   `spawn_failed` and runs **no** static analysis at all.

   Fixed by `~/.local/bin/codacy-analysis.exe` — a small C# forwarder to
   `node <analysis-cli>/dist/index.js`. That directory precedes the npm shim directory
   on PATH, so the `.exe` resolves first. The fix lives outside the npm packages
   deliberately: patching `verity.js` would be wiped by the next `npm update -g`.
