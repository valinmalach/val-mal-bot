# VERITY.md — Quality Gate

> This project uses [Verity](https://verity.md) to enforce quality and security standards on AI-generated code.

**URL:** <https://ofcamwrjwrkazqvdchko.supabase.co>
**Project:** valinmalach/val-mal-bot
**Standard:** v5 — held on the service; `.verity/standard.yaml` is the local source

## Restoring on a new machine

Everything Verity generates is gitignored — the hooks, `.codacy/`, and all of `.verity/`
except the Standard. A fresh clone has none of it, and this file is the only record of
how to get it back, so keep this section true.

Verity runs here as the Claude Code **plugin** `verity@verity`, not as project-local
skills and hooks: `.claude/skills/` is empty on purpose and `.claude/settings.json` is
`{}`. The plugin owns every hook (`verity hooks check` and `verity doctor` both say so).
On Windows `/plugin marketplace add codacy/verity` fails schema validation on
`plugins.0.source`; add the marketplace from the VS Code extension's GUI with the git
URL `https://github.com/codacy/verity.git` instead, then enable `verity@verity`.

```sh
npm install -g @codacy/verity-cli @codacy/analysis-cli
verity login                                # --force if a grant was added since your last login
verity init --moments pre-commit --intensity thorough   # what setup.json records; see below
verity telemetry install                    # optional — cost and usage at /usage
uv tool install ruff                        # gate tools go on PATH, not into ~/.codacy — see "Analysis Tools"
winget install --id AquaSecurity.Trivy --exact
winget install --id koalaman.shellcheck --exact
winget install --id hadolint.hadolint --exact
```

Then build `.codacy/codacy.config.json` as described next. It is not `verity config get`.

## One config file, two owners

Verity hardcodes `.codacy/codacy.config.json` (`CODACY_CONFIG_FILE` in `@codacy/verity-cli`)
and runs `codacy-analysis analyze --install-dependencies --files … --parallel-tools 3`. The
Codacy VS Code extension reads the same file for its live diagnostics, and **regenerates it
from Codacy Cloud, overwriting it and `codacy.config.baseline.json` with the same content**:

- at activation, when `metadata.source` is not `"remote"` while it holds a Codacy token
  (Verity's own config says `"local"`), with no log line and before any prompt is sent;
- whenever a file is created through VS Code's own UI (not by a shell or an agent) outside
  `.git/`, `.codacy/`, `.vscode/`, `.github/`, `node_modules/`. Nothing turns this off.

`cmp` of the config against its `.baseline.json` reports identical, because the extension
writes both together. Tell the files apart by `metadata.source` and by
`codacy-analysis analyze --inspect`, which lists the tools. Because the Cloud config holds
Semgrep and there is no win32 Opengrep, `--install-dependencies` aborts the whole run (no tool
executes, exit 0, 0 issues), so an overwritten file means the gate analyses nothing.

So the file is a **merge** of the Cloud repo config and Verity's config. The script forces
`metadata.source` to `"remote"` (which stops the activation rewrite), and the file is then
made read-only (which turns the file-creation rewrite into an `EPERM` in the extension's log):

```sh
rm -f .codacy/cloud.json                    # init refuses to overwrite it
codacy-analysis init --remote gh valinmalach val-mal-bot --config-file .codacy/cloud.json
verity config get | python -c "import json,sys; print(json.dumps(json.load(sys.stdin)['content'], indent=2))" > .codacy/verity.json
attrib -R .codacy/codacy.config.json || true   # read-only from the last merge
python .codacy/merge-config.py .codacy/cloud.json .codacy/verity.json > .codacy/merged.json &&
  mv -f .codacy/merged.json .codacy/codacy.config.json
attrib +R .codacy/codacy.config.json
```

The merge is written aside and moved into place only if the script succeeds. `>` straight onto
the config truncates it before Python runs, so a crash (a tool with no `patterns` key raises
`KeyError`) would leave the gate a 0-byte file, which the analyzer runs as 0 tools and 0 issues
and Verity records as `no_tools_ran`.

The Cloud dump is `.codacy/cloud.json`, never the default path: **never run `codacy-analysis
init` or `/configure-codacy` without `--config-file`**, or it *is* the gate's config. To change
the merged file, `attrib -R` it, re-run the merge, `attrib +R` it.

**Re-run the whole block after any change to `.codacy.yaml`, not only the merge.** `codacy-analysis
init --remote` bakes that file's `engines.*.exclude_paths` into each tool's own `exclude` when it
runs, so the merged file never sees a later edit. Merge into a scratch file and compare it with
`.codacy/codacy.config.json`, ignoring the timestamps in `metadata`, to check.

`.codacy/merge-config.py` is gitignored like the rest of `.codacy/`, so this copy is the source:

```python
import json
import sys

# No win32 Opengrep; --install-dependencies then aborts every run.
UNRUNNABLE = {"Semgrep"}
# See the F821 section below.
DROP = {"Ruff_F821_undefined-name"}
# A tool has patterns or a local config file, never both. Verity's list replaces the
# Cloud entry, flag off, for these even when Cloud sets nothing.
VERITY_WINS = {"Ruff"}


def load(path: str) -> dict:
    with open(path, encoding="utf-8") as f:
        return json.load(f)


cloud, verity = load(sys.argv[1]), load(sys.argv[2])
tools = {t["toolId"]: t for t in cloud["tools"] if t["toolId"] not in UNRUNNABLE}
for theirs in verity["tools"]:
    mine = tools.get(theirs["toolId"])
    uses_local_file = mine is not None and mine.get("useLocalConfigurationFile")
    if theirs["toolId"] in VERITY_WINS or uses_local_file:
        # With a local config file every pattern is ignored, and for Ruff the CLI
        # (0.23.1) finds pyproject.toml even when the flag is unset.
        tools[theirs["toolId"]] = {**theirs, "useLocalConfigurationFile": False}
    elif mine is None:
        tools[theirs["toolId"]] = theirs
    else:
        have = {p["patternId"] for p in mine["patterns"]}
        mine["patterns"] += [
            p for p in theirs["patterns"] if p["patternId"] not in have
        ]
for tool in tools.values():
    tool["patterns"] = [p for p in tool["patterns"] if p["patternId"] not in DROP]
excludes = set(cloud.get("exclude", [])) | set(verity.get("exclude", []))
# Anything but "remote" and the VS Code extension regenerates the file on activation.
json.dump(
    {
        "version": cloud["version"],
        "metadata": {**cloud["metadata"], "source": "remote"},
        "tools": list(tools.values()),
        "exclude": sorted(excludes),
    },
    sys.stdout,
    indent=2,
)
```

Two things the merge cannot keep at once. **Ruff:** Cloud's entry says "use `pyproject.toml`",
whose `[tool.ruff]` carries the same families and more (see AGENTS.md); Verity's is a
19-pattern list including `ANN001`/`ANN201`. Verity's wins in the merged file (as does Verity's
list for any merged tool whose Cloud entry uses a local config file, which would otherwise ignore
every pattern), and a tool has patterns or a local config file, never both, so the gate's Ruff does
not read `pyproject.toml`: its `E4`/`E7`/`E9` rules and the per-file `S101` ignore for
`valmal/core/logging_json.py` do not apply there. `S104` on `main.py`'s bind address is silenced
inline (`# noqa: S104  # nosec B104`), which every mode honours, and `S105` no longer fires on the
token-type comparison, which lower-cases the token type first. `S101` still fires 19 times in
`valmal/core/logging_json.py` when that file is touched. **Semgrep:** Cloud analyses with it (649
patterns) and the merged file cannot.

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
- Test Adequacy (coverage threshold 95 — `tests/` covers 99% of everything outside `migrations/`; tests are judged for whether they can fail; see AGENTS.md)

## Security Patterns

- No hardcoded secrets (CWE-798)
- Input sanitization (CWE-20)
- Parameterized queries (CWE-89)
- Dependency verification (CWE-1395)
- No unsafe deserialization (CWE-502)
- Access control checks (CWE-639)
- Config file integrity (CWE-15)

## Project Patterns

These come from `AGENTS.md` and `valmal/db/README.md`, and are enforced by the AI reviewer:

- IDs are addressed by slug through `config.channel/role/setting/template`, never a literal snowflake
- Background work goes through `background.fire_and_forget`, never a bare `asyncio.create_task`
- Database text is formatted with `safe_format`, never bare `str.format`; stored text reaches Discord only through `config.template`/`embed`/`auto_response`, which render it (nothing outside `valmal/core/config.py` calls `render`)
- Self-reporting goes through `errors.report` / `errors.notify`; nothing else resolves `bot_admin`
- Configuration changes ship as an Alembic revision, under the rules in `valmal/db/README.md`

## How It Works

Two things review the code, and only one of them is a choice. When the agent runs `git commit`,
the **pre-commit guard** (`verity guard`) reviews the staged diff. That is the moment
`setup.json` and `.verity/config.json` record (`pre-commit` / `commit`). The plugin also wires
a **Stop hook** (`verity analyze`) that reviews what changed each turn, and it cannot be
switched off under the plugin: `verity init` says "Turning the Stop review off is not yet
supported under the plugin — it stays on", so `verity doctor`'s "Stop (verity analyze): on
(plugin)" is accurate whatever `moments` says. Both run the same pipeline:

1. Runs static analysis over the changed files via `@codacy/analysis-cli`
2. Sends results + code to the Verity service
3. An independent reviewer assesses the change against the Standard
4. Returns PASS / WARN / FAIL — a FAIL blocks, capped at 2 self-healing cycles

A commit-only gate would need `verity init --no-plugin` (project-local hooks). Untested here:
its `--help` says "ignore any Claude Code plugin here" and does not say whether the plugin's own
Stop hook stops running.

## Analysis Tools

Thirteen tools, from the merge in "One config file, two owners": Verity's `Ruff`, `Trivy`,
`shellcheck` and `Hadolint`, plus the Cloud repo config's Bandit, Checkov, Pylint,
Prospector, Lizard, markdownlint, Agentlinter, Spectral and Jackson Linter (Semgrep left
out). Every one has explicitly enumerated pattern IDs (never `patterns: []`, which would
enable thousands of default rules). The Python tools run on 3.14 from `uv tool` installs
(`uv tool install <name>`; Checkov also needs a `~/.local/bin/checkov.exe` forwarder, as
its wheel ships no `.exe` and Node cannot spawn a `.cmd`), and every analyser is `global`
in the Install column of a run.

**All of Verity's four tools are installed on PATH, not in `~/.codacy`**: Ruff with `uv tool install
ruff`; Trivy, ShellCheck and Hadolint with winget (upstream's own release archives —
none is a Python package, and the PyPI wrappers for the last two only lag them). The
Codacy CLI classes a PATH tool as `global` and leaves it alone; a tool in
`~/.codacy/tools` or `~/.codacy/runtimes` is *managed*, and every `codacy-analysis
analyze` (with or without `--install-dependencies`) reinstalls a managed tool whose
version differs from the adapter's pin, downgrading it. The run's own table shows the
truth in its Install column (`global (newer than 0.16.0)`); `--inspect` does not run the
reinstall, so it cannot. `~/.codacy/tools` is empty now, and should stay so. Keep them
current with:

```sh
uv tool upgrade --all
winget upgrade AquaSecurity.Trivy koalaman.shellcheck hadolint.hadolint
```

Nothing in this repo has a shell script or a Dockerfile, so ShellCheck and Hadolint match
no files here. The adapters parse `--version` only to report it, so a newer binary is
used as-is.

To re-validate the pattern IDs after any change, point the validator at the installed
adapters — it cannot find them on Windows by itself:

```sh
CODACY_TOOLS_DIR=/c/nvm4w/nodejs/node_modules/@codacy/analysis-cli/node_modules/@codacy   node /c/nvm4w/nodejs/node_modules/@codacy/verity-cli/data/skills/verity-setup/validate-patterns.mjs .codacy/codacy.config.json
```

The IDs this adapter defines carry a slug suffix (`Ruff_ANN001_missing-type-function-argument`,
not `Ruff_ANN001`). A wrong ID disables the tool **silently**, so never hand-author one —
derive it with `--list` and validate.

**One failure it always reports is a false alarm: `spectral_no-$ref-siblings`.** The adapter
does define it, but the validator harvests IDs with `ID_CHAR = /[A-Za-z0-9_@./-]/`, which has
no `$`, so it reads the ID as `spectral_no-` and finds nothing. `verity doctor` shells out to
the same script, so it prints `.codacy/codacy.config.json: ⚠ ids do not resolve` and advises
`verity standard synthesize --config-only` or `verity init`. **Do neither for this:** the
config is not broken, and `init` re-pushes an analysis config that brings `Ruff_F821` back
(below). `verity init` also tries to re-derive the file and fails with `EPERM` on the read-only
bit, which is that bit doing its job; never `attrib -R` it to make `init` "succeed".
`codacy-analysis update-config` is the same hazard: its help says a remote config is "always
fully re-synced from Codacy Cloud (cloud is authoritative)". Any *other* line in the
validator's output is real. Checked 2026-09-20 against `@codacy/verity-cli` 0.33.1 and
`@codacy/analysis-cli` 0.23.1.

### Reporting a false positive: always pass `--file`

`verity feedback finding <run-id> <pattern-id> false_positive` **without
`--file` suppresses that pattern across `**/*.py`** — the whole language, not
the finding you reported. It is not obvious from the output, and there is no
CLI way to list or remove a suppression afterwards; `feedback finding ...
useful` does not reverse it and `verity config get` does not show it.

Demonstrated on 2026-09-10: three unscoped `false_positive` reports for
`type-safety` left a probe file containing a genuinely undefined name **and** a
function annotated `-> int` returning a `str` reviewing as `PASS` with zero
findings and `suppressions_applied: [{pattern_id: type-safety, file_glob:
"**/*.py", count: 2}]`. Reported upstream; until it is cleared, Verity's
`type-safety` dimension is blind on this repo and `uv run pyright` plus
`uv run ruff check` are the cover — both catch exactly what it now swallows.

So: pass `--file` (and `--line`), and before trusting a clean run, read
`suppressions_applied`. Zero findings is indistinguishable from a blinded
pattern — the same failure shape as a wrong pattern ID above.

### Why `Ruff_F821_undefined-name` is not in the pattern list

Codacy's Ruff runs below `--target-version py314`, so it reports F821 on a
class annotating itself in its own body —
`_instance: ClassVar[TwitchShoutoutQueue | None] = None` inside
`class TwitchShoutoutQueue`. On Python 3.14 that is correct code: PEP 649
compiles annotations into a lazy `__annotate__` thunk, so the class body never
evaluates the name.

`pyproject.toml` states `target-version = "py314"`, so a Ruff that reads it has no such
problem; only the gate's pattern mode, which exposes no target version, does. At py314 the
whole repo is clean; with `ruff check --select F821 --target-version py313` it reports
`valmal/twitch/stream/shoutout_queue.py:28` and `valmal/twitch/oauth/token_manager.py:29`
(this case) and three more of the same shape in `tests/db/support.py` and
`tests/db/test_session.py`. F821 was therefore removed from the list rather than
suppressed: the project's own Ruff and pyright both enforce it correctly, so nothing is lost,
and the recurring false positive no longer invites another suppression.

`verity config get` still returns it, so the merge script drops it. It returns because
`verity init` pushes the analysis config to the service as part of its own run: after the
`init` of 2026-09-18 F821 was back, and `S104` with it. So expect `verity config get` to
regress after any `verity init`, and restore from the merge, never from `verity config get`
alone. The gate runs from the local file, and the only readers of the service copy found are
`verity config get` and `verity doctor`, so it is left as is rather than pushed over again.
If the service turns out to use its copy for the review, push a Verity-only, F821-free file
with `verity config push --file`, never the merged one.

### Two Windows workarounds this setup depends on

Both are upstream bugs in the current releases (`@codacy/verity-cli` 0.33.1,
`@codacy/analysis-cli` 0.23.1, read from `dist/index.js` and `bin/verity.js` rather than
reproduced). Re-apply them if the tooling is reinstalled.

1. **`codacy-analysis --install-dependencies` cannot install its own tools.** It shells
   out to `tar -xf C:\...`, and GNU tar reads `C:` as a remote host
   (`tar: Cannot connect to C: resolve failed`). Hence the four tools come from uv and
   winget instead.

2. **Verity cannot spawn the analyzer.** `verity.js` calls
   `spawnSync(codacyAnalysisPath() ?? "codacy-analysis", ...)` with no `shell: true`. On
   Windows npm installs that command only as `.cmd`/`.ps1` shims, so the bare name gives
   `ENOENT` and `.cmd` gives `EINVAL` (Node's CVE-2024-27980 fix). The doctor finds the
   analyzer by a `PATH`/`PATHEXT` walk, but that walk still returns a `.cmd` shim, and
   spawning one still fails. Without a fix the gate reports `spawn_failed` and runs **no**
   static analysis at all.

   Fixed by `~/.local/bin/codacy-analysis.exe` — a small C# forwarder to
   `node <analysis-cli>/dist/index.js`. That directory precedes the npm shim directory
   on PATH, so the `.exe` resolves first. The fix lives outside the npm packages
   deliberately: patching `verity.js` would be wiped by the next `npm update -g`.
