import asyncio
import json
from typing import Any

import pytest

from tests.twitch_migrate.support import (
    CURRENT,
    ROUTES,
    MigrateWorld,
    sub,
    two_online_and_a_raid,
)
from valmal.twitch.client.helix import HelixError
from valmal.twitch.eventsub import migrate

pytestmark = pytest.mark.anyio

ONLINE = "/webhook/twitch/stream/online"


class TestMigrateConfirmed:
    async def test_dumps_before_it_deletes_anything(
        self, world: MigrateWorld, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """After the delete the dump is the only record that a subscription existed."""
        world.listing = [sub()]
        order: list[str] = []

        async def notify_file(text: str, filename: str, content: str) -> bool:
            order.append("dump")
            return True

        async def delete(subscription_id: str) -> None:
            order.append("delete")

        original = world.create_subscription

        async def create(*args: Any) -> None:
            order.append("create")
            await original(*args)

        monkeypatch.setattr(migrate, "notify_file", notify_file)
        monkeypatch.setattr(migrate, "delete_subscription", delete)
        monkeypatch.setattr(migrate, "create_subscription", create)

        await migrate.migrate(ROUTES, confirm=True)

        assert order == ["dump", "delete", "create"]

    async def test_repoints_every_subscription_that_has_a_route_and_only_those(
        self, world: MigrateWorld
    ) -> None:
        world.listing = [
            sub(id="a", broadcaster_user_id="1"),
            sub(id="b", type="stream.offline", broadcaster_user_id="1"),
            sub(id="c", broadcaster_user_id="2", callback=CURRENT),
            sub(id="d", type="channel.follow"),
        ]

        outcome = await migrate.migrate(ROUTES, confirm=True)

        assert world.deletes == ["a", "b"]
        assert [c[0] for c in world.creates] == ["stream.online", "stream.offline"]
        assert (
            world.creates[1][3] == "https://bot.example/webhook/twitch/stream/offline"
        )
        assert [s.id for s in outcome.migrated] == ["a", "b"]

    async def test_a_subscription_per_broadcaster_is_moved_for_each(
        self, world: MigrateWorld
    ) -> None:
        """Working from the live list is what stops a promo broadcaster being left behind."""
        world.listing = [
            sub(id=f"s{n}", broadcaster_user_id=str(n)) for n in range(1, 6)
        ]

        outcome = await migrate.migrate(ROUTES, confirm=True)

        assert len(outcome.migrated) == 5
        assert world.deletes == ["s1", "s2", "s3", "s4", "s5"]

    async def test_one_failure_does_not_abandon_the_ones_behind_it(
        self, world: MigrateWorld
    ) -> None:
        world.listing = [
            sub(id="a", broadcaster_user_id="1"),
            sub(id="b", broadcaster_user_id="2"),
            sub(id="c", broadcaster_user_id="3"),
        ]
        world.delete_errors["a"] = HelixError("boom", status=500)

        outcome = await migrate.migrate(ROUTES, confirm=True)

        assert [s.id for s in outcome.migrated] == ["b", "c"]
        assert [s.id for s, _ in outcome.stuck] == ["a"]

    async def test_a_destroyed_subscription_and_a_stuck_one_are_reported_apart(
        self, world: MigrateWorld
    ) -> None:
        world.listing = [
            sub(id="a", broadcaster_user_id="1"),
            sub(id="b", type="stream.offline", broadcaster_user_id="1"),
        ]
        world.delete_errors["a"] = HelixError("stuck one")
        world.create_errors["stream.offline"] = HelixError("lost one", status=400)

        outcome = await migrate.migrate(ROUTES, confirm=True)

        assert [s.id for s, _ in outcome.stuck] == ["a"]
        assert [s.id for s, _ in outcome.lost] == ["b"]
        texts = [text for text, _ in world.notified]
        destroyed = next(t for t in texts if t.startswith("Migration destroyed 1"))
        untouched = next(t for t in texts if t.startswith("Migration left 1"))
        assert "Recreate these from the dump" in destroyed and "lost one" in destroyed
        assert "Re-running moves them" in untouched and "stuck one" in untouched
        assert "stuck one" not in destroyed and "lost one" not in untouched

    async def test_a_clean_run_sends_no_summary_notices(
        self, world: MigrateWorld
    ) -> None:
        world.listing = [sub()]

        await migrate.migrate(ROUTES, confirm=True)

        assert world.notified == []

    async def test_an_undeliverable_dump_abandons_the_run_before_touching_anything(
        self, world: MigrateWorld
    ) -> None:
        world.listing = [sub()]
        world.file_delivered = False

        outcome = await migrate.migrate(ROUTES, confirm=True)

        assert world.calls == []
        assert outcome.dumped is False and outcome.migrated == []
        ((text, _),) = world.notified
        assert "Migration abandoned before touching anything" in text

    async def test_a_dry_run_with_an_undeliverable_dump_says_nothing_extra(
        self, world: MigrateWorld
    ) -> None:
        world.listing = [sub()]
        world.file_delivered = False

        outcome = await migrate.migrate(ROUTES, confirm=False)

        assert outcome.dumped is False
        assert world.notified == []

    async def test_the_dump_covers_every_subscription_not_only_the_moving_ones(
        self, world: MigrateWorld
    ) -> None:
        world.listing = two_online_and_a_raid()

        await migrate.migrate(ROUTES, confirm=True)

        ((text, _, content),) = world.files
        assert text.startswith("Repointing 1 EventSub subscription(s).")
        assert len(json.loads(content)) == 3

    async def test_a_failed_listing_raises_because_there_is_nothing_to_work_from(
        self, world: MigrateWorld
    ) -> None:
        world.listing_error = HelixError("401")

        with pytest.raises(HelixError):
            await migrate.migrate(ROUTES, confirm=True)

    async def test_a_failed_listing_releases_the_in_flight_flag(
        self, world: MigrateWorld
    ) -> None:
        """Otherwise one bad listing would refuse every confirmed run until a restart."""
        world.listing_error = HelixError("401")

        with pytest.raises(HelixError):
            await migrate.migrate(ROUTES, confirm=True)

        assert migrate._confirming is False

    async def test_a_login_lookup_that_fails_does_not_stop_the_migration(
        self, world: MigrateWorld
    ) -> None:
        world.listing = [sub()]
        world.users_error = HelixError("down")

        outcome = await migrate.migrate(ROUTES, confirm=True)

        assert len(outcome.migrated) == 1
        assert outcome.logins == {}

    async def test_the_flag_is_released_after_a_normal_run(
        self, world: MigrateWorld
    ) -> None:
        world.listing = [sub()]

        await migrate.migrate(ROUTES, confirm=True)

        assert migrate._confirming is False


class TestOneConfirmedRunAtATime:
    async def test_a_second_confirmed_run_refuses_while_one_is_in_flight(
        self, world: MigrateWorld, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        world.listing = [sub()]
        release = asyncio.Event()
        inside = asyncio.Event()

        async def slow_delete(subscription_id: str) -> None:
            inside.set()
            # Bounded, because a second run that got past the guard would wait
            # here for a release only the first run's caller can give.
            async with asyncio.timeout(5):
                await release.wait()

        monkeypatch.setattr(migrate, "delete_subscription", slow_delete)
        async with asyncio.TaskGroup() as group:
            first = group.create_task(migrate.migrate(ROUTES, confirm=True))
            await inside.wait()
            second = await migrate.migrate(ROUTES, confirm=True)
            release.set()
        finished = first.result()

        assert second.busy is True
        assert second.repoint == [] and second.migrated == []
        assert finished.busy is False and len(finished.migrated) == 1

    async def test_the_refused_run_never_lists_subscriptions(
        self, world: MigrateWorld, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(migrate, "_confirming", True)

        outcome = await migrate.migrate(ROUTES, confirm=True)

        assert outcome.busy is True
        assert world.listings == 0

    async def test_the_refused_run_leaves_the_flag_for_the_run_that_holds_it(
        self, world: MigrateWorld, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(migrate, "_confirming", True)

        await migrate.migrate(ROUTES, confirm=True)

        assert migrate._confirming is True

    async def test_a_new_run_is_accepted_once_the_last_has_finished(
        self, world: MigrateWorld
    ) -> None:
        world.listing = [sub()]
        await migrate.migrate(ROUTES, confirm=True)
        world.calls.clear()

        outcome = await migrate.migrate(ROUTES, confirm=True)

        assert outcome.busy is False
