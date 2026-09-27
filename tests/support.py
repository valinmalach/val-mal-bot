"""Shared by the tests at the repo's root, which have no per-area conftest: the
repo root, and running a fixed command in a process of its own (test_main.py
and test_migrations.py each need that, for a reason importing in-process would
not answer).
"""

import os
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent


def run_python(*args: str) -> subprocess.CompletedProcess[str]:
    """python <args>, in a process of its own.

    Fixed arguments and the interpreter running the tests; nothing here is
    external input.
    """
    done = subprocess.run(  # noqa: S603
        [sys.executable, *args],
        cwd=ROOT,
        env=os.environ | {"PYTHONIOENCODING": "utf-8"},
        capture_output=True,
        text=True,
        encoding="utf-8",
        timeout=120,
        check=False,
    )
    assert done.returncode == 0, done.stderr[-2000:]
    return done
