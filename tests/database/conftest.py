import pytest

from db import repository
from tests.database.support import Database


@pytest.fixture
def database(monkeypatch: pytest.MonkeyPatch) -> Database:
    database = Database()
    monkeypatch.setattr(repository, "session_scope", database.scope)
    return database
