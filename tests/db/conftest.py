import pytest

from tests.db.support import Database
from valmal.db import repository


@pytest.fixture
def database(monkeypatch: pytest.MonkeyPatch) -> Database:
    database = Database()
    monkeypatch.setattr(repository, "session_scope", database.scope)
    return database
