import pytest

from tests.twitch.eventsub.migrate.support import MigrateWorld
from valmal.twitch.eventsub import migrate


@pytest.fixture
def world(monkeypatch: pytest.MonkeyPatch) -> MigrateWorld:
    world = MigrateWorld()
    for name in (
        "get_subscriptions",
        "get_users",
        "delete_subscription",
        "create_subscription",
        "notify",
        "notify_file",
    ):
        monkeypatch.setattr(migrate, name, getattr(world, name))
    monkeypatch.setattr(migrate, "_confirming", False)
    return world
