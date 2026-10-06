import pytest

from tests.db.support import Database, Transaction
from valmal.db import configuration, repository


@pytest.fixture
def database(monkeypatch: pytest.MonkeyPatch) -> Database:
    database = Database()

    def transaction() -> Transaction:
        return Transaction(database)

    monkeypatch.setattr(repository, "transaction", transaction)
    monkeypatch.setattr(configuration, "transaction", transaction)
    return database
