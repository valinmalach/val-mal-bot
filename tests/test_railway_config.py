"""The build command in .railway/railway.py, read from the source: railway_sdk is a
global install for the railway CLI, not a project dependency, so it cannot be imported.

A broken build command is found only by a failed deploy, after `railway config apply`.
"""

import ast

from tests.support import ROOT

_CONFIG = ROOT / ".railway" / "railway.py"


def _build_command() -> str:
    tree = ast.parse(_CONFIG.read_text(encoding="utf-8"))
    constants = {
        node.targets[0].id: node.value.value
        for node in tree.body
        if isinstance(node, ast.Assign)
        and isinstance(node.targets[0], ast.Name)
        and isinstance(node.value, ast.Constant)
    }
    build = next(
        k.value
        for node in ast.walk(tree)
        if isinstance(node, ast.Call)
        for k in node.keywords
        if k.arg == "build"
    )
    # Adjacent literals and an f-string parse as one JoinedStr, whose only
    # placeholders are the module's constants.
    assert isinstance(build, ast.JoinedStr)
    parts: list[str] = []
    for part in build.values:
        if isinstance(part, ast.Constant):
            parts.append(str(part.value))
        else:
            assert isinstance(part, ast.FormattedValue)
            assert isinstance(part.value, ast.Name)
            parts.append(str(constants[part.value.id]))
    return "".join(parts)


def test_the_build_command_has_no_quotes() -> None:
    """Railpack strips them before sh -c: a quoted python -c failed the 2026-10-20 build."""
    command = _build_command()

    assert "'" not in command
    assert '"' not in command


def test_the_build_command_compiles_the_pinned_pythons_stdlib() -> None:
    version = (ROOT / ".python-version").read_text(encoding="utf-8").strip()
    major_minor = ".".join(version.split(".")[:2])

    assert f"/lib/python{major_minor} " in _build_command()
