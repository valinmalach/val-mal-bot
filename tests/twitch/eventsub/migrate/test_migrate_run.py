import json
import logging

import pytest

from tests.twitch.eventsub.migrate.support import (
    CURRENT,
    OLD,
    ROUTES,
    MigrateWorld,
    sub,
    two_online_and_a_raid,
)
from valmal.twitch.client.helix import HelixError
from valmal.twitch.eventsub import migrate
from valmal.twitch.eventsub.migrate_plan import Outcome

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
