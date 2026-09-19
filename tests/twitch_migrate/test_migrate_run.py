import asyncio
import json
import logging
from typing import Any

import pytest

from models.twitch_api_responses.subscription import Subscription
from services.twitch import migrate
from services.twitch.helix import HelixError
from services.twitch.migrate_plan import Outcome
from tests.twitch_migrate.support import CURRENT, OLD, ROUTES, MigrateWorld, sub

pytestmark = pytest.mark.anyio

ONLINE = "/webhook/twitch/stream/online"


class TestRepoint:
    async def test_deletes_first_then_recreates_the_same_definition_at_the_new_callback(
        self, world: MigrateWorld
    ) -> None:
        outcome = Outcome()
        subscription = sub(id="s1", version="2", broadcaster_user_id="111")

        await migrate._repoint(subscription, ONLINE, outcome, {})

        assert world.calls == [
            ("delete", "s1"),
            ("create", "stream.online", "2", {"broadcaster_user_id": "111"}, CURRENT),
        ]
        assert outcome.migrated == [subscription]
        assert outcome.stuck == [] and outcome.lost == []

    async def test_a_raid_is_recreated_with_only_the_half_that_was_set(
        self, world: MigrateWorld
    ) -> None:
        """Both halves is a create Twitch refuses, after the delete has already landed."""
        raid = sub(
            type="channel.raid",
            from_broadcaster_user_id="",
            to_broadcaster_user_id="222",
        )

        await migrate._repoint(raid, "/webhook/twitch/raid", Outcome(), {})

        assert world.creates[0][2] == {"to_broadcaster_user_id": "222"}

    async def test_an_undeclared_condition_key_goes_back_or_the_copy_is_broader(
        self, world: MigrateWorld
    ) -> None:
        redemption = sub(broadcaster_user_id="111", reward_id="abc")

        await migrate._repoint(redemption, ONLINE, Outcome(), {})

        assert world.creates[0][2] == {
            "broadcaster_user_id": "111",
            "reward_id": "abc",
        }

    async def test_a_failed_delete_is_stuck_and_never_reaches_the_create(
        self, world: MigrateWorld
    ) -> None:
        world.delete_errors["s1"] = HelixError("500 down", status=500)
        outcome = Outcome()

        await migrate._repoint(sub(), ONLINE, outcome, {"111": "valin"})

        assert world.creates == []
        assert outcome.migrated == [] and outcome.lost == []
        ((subscription, reason),) = outcome.stuck
        assert subscription.id == "s1"
        assert reason == "delete failed, untouched: 500 down"
        ((text, key),) = world.notified
        assert "Could not delete stream.online (broadcaster 111 = valin)" in text
        assert f"still calls back on {OLD}" in text
        assert key == "migrate-delete:s1"

    async def test_a_failed_create_is_lost_and_says_what_to_recreate(
        self, world: MigrateWorld
    ) -> None:
        world.create_errors["stream.online"] = HelixError("400 bad", status=400)
        outcome = Outcome()

        await migrate._repoint(sub(version="2"), ONLINE, outcome, {})

        assert outcome.stuck == [] and outcome.migrated == []
        ((_, reason),) = outcome.lost
        assert reason == "deleted, not recreated: 400 bad"
        ((text, key),) = world.notified
        assert "is now GONE" in text
        assert "version 2" in text
        assert json.dumps({"broadcaster_user_id": "111"}) in text
        assert key == "migrate-create:s1"

    async def test_a_409_where_the_subscription_is_present_is_a_retried_post_that_landed(
        self, world: MigrateWorld, caplog: pytest.LogCaptureFixture
    ) -> None:
        world.create_errors["stream.online"] = HelixError("409", status=409)
        world.after = [sub(id="made", callback=CURRENT)]
        outcome = Outcome()
        subscription = sub()

        with caplog.at_level(logging.INFO, logger=migrate.logger.name):
            await migrate._repoint(subscription, ONLINE, outcome, {})

        assert outcome.migrated == [subscription]
        assert outcome.lost == []
        assert world.notified == []
        assert "409 on recreating stream.online" in caplog.text

    async def test_a_409_where_it_is_not_present_is_still_a_loss(
        self, world: MigrateWorld
    ) -> None:
        world.create_errors["stream.online"] = HelixError("409", status=409)
        world.after = []
        outcome = Outcome()

        await migrate._repoint(sub(), ONLINE, outcome, {})

        assert len(outcome.lost) == 1
        assert outcome.migrated == []

    async def test_a_409_whose_check_fails_is_a_loss_not_a_success(
        self, world: MigrateWorld
    ) -> None:
        world.create_errors["stream.online"] = HelixError("409", status=409)
        world.after = HelixError("down")
        outcome = Outcome()

        await migrate._repoint(sub(), ONLINE, outcome, {})

        assert len(outcome.lost) == 1

    @pytest.mark.parametrize("status", [400, 401, 403, 429, 500, None])
    async def test_only_a_409_earns_the_second_look(
        self, status: int | None, world: MigrateWorld
    ) -> None:
        world.create_errors["stream.online"] = HelixError("x", status=status)
        world.after = [sub(callback=CURRENT)]
        outcome = Outcome()

        await migrate._repoint(sub(), ONLINE, outcome, {})

        assert len(outcome.lost) == 1
        assert world.listings == 0


def two_online_and_a_raid() -> list[Subscription]:
    return [
        sub(id="a", broadcaster_user_id="1"),
        sub(id="b", broadcaster_user_id="2", callback=CURRENT),
        sub(id="c", type="channel.follow"),
    ]


class TestMigrateDryRun:
    async def test_lists_what_would_move_and_changes_nothing(
        self, world: MigrateWorld
    ) -> None:
        world.listing = two_online_and_a_raid()
        world.users = {"1": "one", "2": "two"}

        outcome = await migrate.migrate(ROUTES, confirm=False)

        assert [s.id for s in outcome.repoint] == ["a"]
        assert [s.id for s in outcome.keep] == ["b"]
        assert [s.id for s in outcome.skip] == ["c"]
        assert outcome.migrated == []
        assert world.calls == []

    async def test_still_sends_the_dump_so_somebody_has_it_before_committing(
        self, world: MigrateWorld
    ) -> None:
        world.listing = two_online_and_a_raid()

        outcome = await migrate.migrate(ROUTES, confirm=False)

        ((text, filename, content),) = world.files
        assert text.startswith("Dry run for 1 EventSub subscription(s).")
        assert filename == "eventsub-subscriptions.json"
        assert [e["id"] for e in json.loads(content)] == ["a", "b", "c"]
        assert outcome.dumped is True

    async def test_carries_the_logins_it_resolved(self, world: MigrateWorld) -> None:
        world.listing = two_online_and_a_raid()
        world.users = {"1": "one", "2": "two"}

        outcome = await migrate.migrate(ROUTES, confirm=False)

        assert outcome.logins == {"1": "one", "2": "two"}

    async def test_a_dry_run_is_not_blocked_by_a_confirmed_one_in_flight(
        self, world: MigrateWorld, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(migrate, "_confirming", True)
        world.listing = two_online_and_a_raid()

        outcome = await migrate.migrate(ROUTES, confirm=False)

        assert outcome.busy is False and len(outcome.repoint) == 1

    async def test_a_dry_run_does_not_release_a_flag_it_never_took(
        self, world: MigrateWorld, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(migrate, "_confirming", True)

        await migrate.migrate(ROUTES, confirm=False)

        assert migrate._confirming is True


class TestMigrateNothingToDo:
    async def test_everything_current_asks_for_no_logins_and_sends_no_dump(
        self, world: MigrateWorld
    ) -> None:
        world.listing = [sub(callback=CURRENT)]

        outcome = await migrate.migrate(ROUTES, confirm=True)

        assert [s.id for s in outcome.keep] == ["s1"]
        assert world.asked_for == [] and world.files == [] and world.calls == []
        assert outcome.dumped is False

    async def test_no_subscriptions_at_all_is_an_empty_outcome(
        self, world: MigrateWorld
    ) -> None:
        outcome = await migrate.migrate(ROUTES, confirm=True)

        assert outcome == Outcome()

    async def test_only_unrouted_types_are_left_completely_alone(
        self, world: MigrateWorld
    ) -> None:
        world.listing = [
            sub(type="channel.follow"),
            sub(id="z", type="channel.moderate"),
        ]

        outcome = await migrate.migrate(ROUTES, confirm=True)

        assert len(outcome.skip) == 2
        assert world.calls == []


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
