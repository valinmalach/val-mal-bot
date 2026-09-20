import pytest

from services.twitch import migrate
from tests.twitch_migrate.support import MigrateWorld


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
