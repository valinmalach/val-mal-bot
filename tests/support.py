"""Shared by the tests at the repo's own root, which have no per-area conftest
to hold this: the repo root, and running a fixed command in a process of its
own -- needed by test_main.py and test_migrations.py alike, each for a
different reason importing the app itself in-process would not answer for.
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
