import pytest

from tests.cogs.events_world import EventsWorld, install


@pytest.fixture
def ev(monkeypatch: pytest.MonkeyPatch) -> EventsWorld:
    return install(monkeypatch)
