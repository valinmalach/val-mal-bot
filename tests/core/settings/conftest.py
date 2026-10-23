from pathlib import Path

import pytest

from tests.core.settings.support import OPTIONAL, REQUIRED


@pytest.fixture
def env(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> pytest.MonkeyPatch:
    """A clean environment in a directory with no .env, so nothing leaks in.

    Settings reads `.env` from the working directory; the developer's real one
    holds real credentials.
    """
    monkeypatch.chdir(tmp_path)
    for name in [*REQUIRED, *OPTIONAL]:
        monkeypatch.delenv(name, raising=False)
    for name, value in REQUIRED.items():
        monkeypatch.setenv(name, value)
    return monkeypatch
