import pytest

from tests.cogs import twitch_admin_world
from tests.cogs.events_world import EventsWorld, install


@pytest.fixture
def ev(monkeypatch: pytest.MonkeyPatch) -> EventsWorld:
    return install(monkeypatch)


@pytest.fixture
def admin(monkeypatch: pytest.MonkeyPatch) -> twitch_admin_world.Admin:
    return twitch_admin_world.install(monkeypatch)
