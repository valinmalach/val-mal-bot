"""The revision history, checked without a database.

There is no Postgres in the test run, so what can be proved is what Alembic can
say offline: the chain is one line, each revision follows the rules in
``valmal/db/README.md``, and the SQL it renders builds the schema the models describe and
seeds every configuration key the code reads.
"""

import ast
import re
from pathlib import Path

import pytest
from alembic.config import Config
from alembic.script import ScriptDirectory

from tests.support import ROOT, run_python
from valmal.db.models import metadata

REVISIONS = sorted((ROOT / "migrations" / "versions").glob("*.py"))
# The code under test; docs, vendored trees and the agent worktrees under .claude,
# each a whole copy of the repo, are not what reads configuration.
NOT_SOURCE = {".venv", "tests", "migrations", ".verity", ".codacy", ".claude", "docs"}
# Keys are these shapes; a literal of another shape is not a configuration row.
KEY_SHAPE = re.compile(r"^(audit|admin|birthday|discord|stream|twitch|embed)_[a-z_]+$")
# Table and column names have the same shape and are seeded by nothing.
SCHEMA_NAMES = set(metadata.tables) | {
    column.name for table in metadata.tables.values() for column in table.columns
}


def keys_read_by(root: Path) -> dict[str, set[str]]:
    """Every configuration-key-shaped string literal in the source under root."""
    read: dict[str, set[str]] = {}
    for path in root.rglob("*.py"):
        if set(path.relative_to(root).parts) & NOT_SOURCE:
            continue
        for node in ast.walk(ast.parse(path.read_text(encoding="utf-8"))):
            if (
                isinstance(node, ast.Constant)
                and isinstance(node.value, str)
                and KEY_SHAPE.match(node.value)
                and node.value not in SCHEMA_NAMES
                and not node.value.endswith("_router")
            ):
                read.setdefault(node.value, set()).add(path.name)
    return read


@pytest.fixture(scope="module")
def script() -> ScriptDirectory:
    return ScriptDirectory.from_config(Config(str(ROOT / "alembic.ini")))


def offline(*arguments: str) -> str:
    """The SQL Alembic prints for these arguments, log lines removed."""
    done = run_python("-m", "alembic", *arguments, "--sql")
    return "\n".join(
        line for line in done.stdout.splitlines() if not line.startswith('{"level"')
    )


@pytest.fixture(scope="module")
def upgrade_sql() -> str:
    return offline("upgrade", "head")


@pytest.fixture(scope="module")
def downgrade_sql() -> str:
    return offline("downgrade", "head:base")


class TestHistory:
    def test_there_is_exactly_one_head(self, script: ScriptDirectory) -> None:
        """Two heads is two branches that were each valid and together are not."""
        assert len(script.get_heads()) == 1

    def test_there_is_exactly_one_base(self, script: ScriptDirectory) -> None:
        assert len(script.get_bases()) == 1

    def test_every_file_is_a_revision_and_every_revision_a_file(
        self, script: ScriptDirectory
    ) -> None:
        assert len(list(script.walk_revisions())) == len(REVISIONS)

    def test_the_chain_is_a_line(self, script: ScriptDirectory) -> None:
        parents = [r.down_revision for r in script.walk_revisions() if r.down_revision]

        assert len(parents) == len(set(parents))
        assert all(isinstance(parent, str) for parent in parents)

    def test_each_file_name_carries_its_revision_id(
        self, script: ScriptDirectory
    ) -> None:
        for revision in script.walk_revisions():
            assert revision.path is not None
            assert f"_{revision.revision}_" in Path(revision.path).name

    def test_revision_ids_are_numbered_in_file_order(
        self, script: ScriptDirectory
    ) -> None:
        ids = [r.revision for r in reversed(list(script.walk_revisions()))]

        assert ids == sorted(ids)
        assert ids == [f"{n:04d}" for n in range(1, len(ids) + 1)]

    def test_every_revision_can_both_go_up_and_come_down(
        self, script: ScriptDirectory
    ) -> None:
        for revision in script.walk_revisions():
            assert callable(getattr(revision.module, "upgrade", None))
            assert callable(getattr(revision.module, "downgrade", None))


@pytest.mark.parametrize("path", REVISIONS, ids=lambda p: p.name[:22])
class TestTheRulesInTheReadme:
    def test_never_imports_what_keeps_changing(self, path: Path) -> None:
        """A revision has to mean the same thing forever."""
        imported = set()
        for node in ast.walk(ast.parse(path.read_text(encoding="utf-8"))):
            if isinstance(node, ast.ImportFrom) and node.module:
                imported.add(node.module.split(".")[0])
            elif isinstance(node, ast.Import):
                imported |= {alias.name.split(".")[0] for alias in node.names}

        assert not imported & {"valmal"}

    def test_never_reads_a_data_file(self, path: Path) -> None:
        source = path.read_text(encoding="utf-8")

        for reader in ("open(", ".read_text(", ".read_bytes(", "json.load("):
            assert reader not in source

    def test_every_insert_tolerates_a_conflict(self, path: Path) -> None:
        source = path.read_text(encoding="utf-8")

        assert source.count("insert(") == source.count("on_conflict_do_nothing(")


class TestTheSqlTheyRender:
    def test_it_ends_at_the_head_revision(
        self, script: ScriptDirectory, upgrade_sql: str
    ) -> None:
        (head,) = script.get_heads()

        assert f"SET version_num='{head}'" in upgrade_sql

    def test_it_is_one_transaction(self, upgrade_sql: str) -> None:
        lines = [line for line in upgrade_sql.splitlines() if line]

        assert lines[0] == "BEGIN;" and lines[-1] == "COMMIT;"

    def test_it_creates_every_table_the_models_declare(self, upgrade_sql: str) -> None:
        created = set(re.findall(r"^CREATE TABLE (\w+) \(", upgrade_sql, re.MULTILINE))

        assert set(metadata.tables) <= created

    def test_it_creates_no_table_the_models_do_not_declare(
        self, upgrade_sql: str
    ) -> None:
        created = set(re.findall(r"^CREATE TABLE (\w+) \(", upgrade_sql, re.MULTILINE))

        assert created - set(metadata.tables) == {"alembic_version"}

    def test_every_column_the_models_declare_is_built_by_some_revision(
        self, upgrade_sql: str
    ) -> None:
        """What autogenerate would have caught, without a database to diff against."""
        built: dict[str, set[str]] = {
            table: set(re.findall(r"^    (\w+) ", body, re.MULTILINE))
            for table, body in re.findall(
                r"^CREATE TABLE (\w+) \(\n(.*?)\n\);",
                upgrade_sql,
                re.MULTILINE | re.DOTALL,
            )
        }
        for table, column in re.findall(
            r"^ALTER TABLE (\w+) ADD COLUMN (\w+)", upgrade_sql, re.MULTILINE
        ):
            built[table].add(column)

        missing = {
            table: sorted(set(model.columns.keys()) - built.get(table, set()))
            for table, model in metadata.tables.items()
            if set(model.columns.keys()) - built.get(table, set())
        }
        assert not missing

    def test_every_configuration_key_the_code_reads_is_seeded(
        self, upgrade_sql: str
    ) -> None:
        """A feature that adds a template and forgets the revision fails here."""
        read = keys_read_by(ROOT)

        unseeded = {k: v for k, v in read.items() if f"'{k}'" not in upgrade_sql}
        assert not unseeded
        assert len(read) > 50, "the scan stopped finding keys"

    def test_seeded_text_survives_the_round_trip_to_sql(self, upgrade_sql: str) -> None:
        """Some of it is emoji, which is what makes the console encoding matter."""
        assert any(ord(char) > 0x2000 for char in upgrade_sql)


class TestDowngrade:
    def test_it_runs_to_the_base(self, downgrade_sql: str) -> None:
        assert downgrade_sql.strip().startswith("BEGIN;")
        assert downgrade_sql.strip().endswith("COMMIT;")

    def test_it_drops_every_table_the_upgrade_created(
        self, downgrade_sql: str, upgrade_sql: str
    ) -> None:
        created = set(re.findall(r"^CREATE TABLE (\w+) \(", upgrade_sql, re.MULTILINE))
        dropped = set(re.findall(r"^DROP TABLE (\w+);", downgrade_sql, re.MULTILINE))

        assert created - {"alembic_version"} == dropped - {"alembic_version"}


class TestTheKeyScan:
    def test_finds_a_key_in_ordinary_source(self, tmp_path: Path) -> None:
        (tmp_path / "feature.py").write_text("KEY = 'twitch_new_template'")

        assert keys_read_by(tmp_path) == {"twitch_new_template": {"feature.py"}}

    @pytest.mark.parametrize("skipped", [".claude", ".venv", "tests", "docs"])
    def test_leaves_a_copy_of_the_repo_alone(
        self, skipped: str, tmp_path: Path
    ) -> None:
        """An agent worktree is a whole copy, and a sibling's new key must not fail this one."""
        nested = tmp_path / skipped / "worktrees" / "copy"
        nested.mkdir(parents=True)
        (nested / "feature.py").write_text("KEY = 'twitch_new_template'")

        assert keys_read_by(tmp_path) == {}

    def test_ignores_a_table_or_column_name_of_the_same_shape(
        self, tmp_path: Path
    ) -> None:
        (tmp_path / "db.py").write_text("TABLE = 'discord_channel'")

        assert keys_read_by(tmp_path) == {}
