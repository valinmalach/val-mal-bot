import json
from typing import Any

import pytest

from models.twitch_api_responses.subscription import Subscription
from services.twitch import migrate_plan
from services.twitch.migrate_plan import (
    Action,
    Outcome,
    condition_ids,
    condition_of,
    decide,
    describe,
    render_dump,
    summary,
)
from tests.twitch_migrate.support import CURRENT, OLD, ROUTES, sub

RAID = {"from_broadcaster_user_id": "", "to_broadcaster_user_id": "222"}
LIMIT = migrate_plan._DISCORD_MESSAGE_LIMIT


class TestDecide:
    def test_a_callback_on_this_deployment_is_kept(self) -> None:
        assert decide(sub(callback=CURRENT), ROUTES) is Action.KEEP

    def test_a_callback_elsewhere_is_repointed(self) -> None:
        assert decide(sub(callback=OLD), ROUTES) is Action.REPOINT

    def test_a_path_that_is_only_close_is_repointed(self) -> None:
        assert decide(sub(callback=f"{CURRENT}/"), ROUTES) is Action.REPOINT
        assert decide(sub(callback=CURRENT.upper()), ROUTES) is Action.REPOINT

    def test_the_route_is_looked_up_by_type_so_the_same_callback_is_wrong_for_another_type(
        self,
    ) -> None:
        assert decide(sub(type="stream.offline", callback=CURRENT), ROUTES) is (
            Action.REPOINT
        )

    def test_a_type_with_no_route_is_skipped_whatever_its_callback(self) -> None:
        assert decide(sub(type="channel.follow", callback=OLD), ROUTES) is Action.SKIP
        assert decide(sub(type="channel.follow", callback=None), ROUTES) is Action.SKIP

    def test_nothing_routed_means_everything_is_skipped(self) -> None:
        assert decide(sub(), {}) is Action.SKIP

    @pytest.mark.parametrize(
        "status",
        [
            "enabled",
            "webhook_callback_verification_pending",
            "webhook_callback_verification_failed",
            "authorization_revoked",
            "user_removed",
            "notification_failures_exceeded",
        ],
    )
    def test_status_never_changes_the_answer(self, status: str) -> None:
        """A destroyed subscription is worse than a broken one in every status this reaches."""
        assert decide(sub(status=status, callback=CURRENT), ROUTES) is Action.KEEP
        assert decide(sub(status=status, callback=OLD), ROUTES) is Action.REPOINT

    def test_the_action_values_are_what_the_dump_prints(self) -> None:
        assert [a.value for a in Action] == [
            "repoint",
            "already current",
            "no route for this type",
        ]


class TestConditionOf:
    def test_keeps_what_is_set(self) -> None:
        assert condition_of(sub(broadcaster_user_id="1", user_id="2")) == {
            "broadcaster_user_id": "1",
            "user_id": "2",
        }

    def test_drops_the_unset_half_of_a_raid_because_twitch_refuses_both(self) -> None:
        assert condition_of(sub(type="channel.raid", **RAID)) == {
            "to_broadcaster_user_id": "222"
        }

    def test_drops_both_halves_being_empty_leaving_nothing_to_send(self) -> None:
        assert (
            condition_of(
                sub(
                    type="channel.raid",
                    from_broadcaster_user_id="",
                    to_broadcaster_user_id="",
                )
            )
            == {}
        )

    def test_keeps_a_key_it_does_not_declare_or_the_recreate_would_be_broader(
        self,
    ) -> None:
        assert condition_of(sub(broadcaster_user_id="1", reward_id="abc")) == {
            "broadcaster_user_id": "1",
            "reward_id": "abc",
        }

    def test_drops_an_undeclared_key_that_is_empty_too(self) -> None:
        assert condition_of(sub(broadcaster_user_id="1", reward_id="")) == {
            "broadcaster_user_id": "1"
        }

    @pytest.mark.parametrize("value", [0, False, [], {}])
    def test_keeps_an_undeclared_falsy_value_twitch_chose_to_send(
        self, value: object
    ) -> None:
        subscription = Subscription.model_validate(
            {
                "id": "s",
                "status": "enabled",
                "type": "x",
                "version": "1",
                "condition": {"broadcaster_user_id": "1", "extra": value},
                "created_at": "t",
                "transport": {},
                "cost": 0,
            }
        )

        assert condition_of(subscription)["extra"] == value

    def test_is_a_copy_so_editing_it_leaves_the_subscription_alone(self) -> None:
        subscription = sub(broadcaster_user_id="1")

        condition_of(subscription)["broadcaster_user_id"] = "changed"

        assert subscription.condition.broadcaster_user_id == "1"


class TestConditionIds:
    def test_every_distinct_value_sorted(self) -> None:
        subscriptions = [
            sub(id="a", broadcaster_user_id="30"),
            sub(id="b", broadcaster_user_id="4"),
            sub(id="c", broadcaster_user_id="30", moderator_user_id="5"),
        ]

        assert condition_ids(subscriptions) == ["30", "4", "5"]

    def test_sorts_as_text_because_what_is_returned_is_not_promised_to_be_a_number(
        self,
    ) -> None:
        assert condition_ids([sub(broadcaster_user_id="9"), sub(user_id="10")]) == [
            "10",
            "9",
        ]

    def test_an_empty_raid_half_is_not_a_value_to_look_up(self) -> None:
        assert condition_ids([sub(type="channel.raid", **RAID)]) == ["222"]

    def test_reads_every_field_that_names_a_user(self) -> None:
        everyone = sub(
            broadcaster_user_id="1",
            to_broadcaster_user_id="2",
            from_broadcaster_user_id="3",
            moderator_user_id="4",
            user_id="5",
        )

        assert condition_ids([everyone]) == ["1", "2", "3", "4", "5"]

    def test_an_undeclared_key_is_not_an_id_field(self) -> None:
        assert condition_ids([sub(reward_id="abc", broadcaster_user_id="1")]) == ["1"]

    def test_nothing_in_nothing_out(self) -> None:
        assert condition_ids([]) == []


class TestDescribe:
    def test_names_the_type_and_the_broadcaster(self) -> None:
        assert describe(sub(), {}) == "stream.online (broadcaster 111)"

    def test_adds_the_login_when_one_is_known(self) -> None:
        assert describe(sub(), {"111": "valin"}) == (
            "stream.online (broadcaster 111 = valin)"
        )

    def test_names_every_id_so_two_of_one_type_read_differently(self) -> None:
        moderator = sub(
            type="channel.moderate", broadcaster_user_id="1", moderator_user_id="2"
        )

        assert describe(moderator, {"2": "mod"}) == (
            "channel.moderate (broadcaster 1, moderator 2 = mod)"
        )

    def test_a_raid_is_named_only_by_the_half_that_is_set(self) -> None:
        described = describe(sub(type="channel.raid", **RAID), {})

        assert described == "channel.raid (to broadcaster 222)"
        assert "from" not in described

    def test_a_condition_naming_nobody_falls_back_to_the_subscription_id(self) -> None:
        subscription = sub(id="abc-123", reward_id="x")

        assert describe(subscription, {}) == "stream.online (id abc-123)"

    def test_a_login_for_an_id_the_condition_does_not_name_is_ignored(self) -> None:
        assert describe(sub(), {"999": "stranger"}) == "stream.online (broadcaster 111)"


class TestRenderDump:
    def entries(
        self, subscriptions: list[Subscription], logins: dict[str, str]
    ) -> list[dict[str, Any]]:
        return json.loads(render_dump(subscriptions, ROUTES, logins))

    def test_an_empty_list_is_an_empty_array(self) -> None:
        assert render_dump([], ROUTES, {}) == "[]"

    def test_carries_everything_needed_to_recreate_one_by_hand(self) -> None:
        (entry,) = self.entries([sub(id="s9", callback=OLD)], {"111": "valin"})

        assert entry == {
            "id": "s9",
            "type": "stream.online",
            "version": "1",
            "status": "enabled",
            "condition": {"broadcaster_user_id": "111"},
            "logins": {"111": "valin"},
            "callback_now": OLD,
            "callback_after": CURRENT,
            "action": "repoint",
        }

    def test_the_condition_is_the_recreatable_one_not_twitchs_transcript(self) -> None:
        (entry,) = self.entries([sub(type="channel.raid", **RAID)], {})

        assert entry["condition"] == {"to_broadcaster_user_id": "222"}

    def test_only_logins_this_subscription_names_are_listed(self) -> None:
        (entry,) = self.entries([sub()], {"111": "valin", "222": "someone"})

        assert entry["logins"] == {"111": "valin"}

    def test_an_id_with_no_known_login_is_left_out_rather_than_null(self) -> None:
        (entry,) = self.entries([sub()], {})

        assert entry["logins"] == {}

    def test_a_type_with_no_route_has_no_callback_after_and_is_marked_skipped(
        self,
    ) -> None:
        (entry,) = self.entries([sub(type="channel.follow")], {})

        assert entry["callback_after"] is None
        assert entry["action"] == "no route for this type"

    def test_a_subscription_already_current_says_so(self) -> None:
        (entry,) = self.entries([sub(callback=CURRENT)], {})

        assert entry["action"] == "already current"
        assert entry["callback_now"] == entry["callback_after"]

    def test_lists_every_subscription_in_the_order_given(self) -> None:
        entries = self.entries(
            [sub(id="a"), sub(id="b", type="stream.offline"), sub(id="c")], {}
        )

        assert [e["id"] for e in entries] == ["a", "b", "c"]

    def test_a_login_that_is_not_ascii_survives_the_round_trip(self) -> None:
        (entry,) = self.entries([sub()], {"111": "café"})

        assert entry["logins"] == {"111": "café"}

    def test_a_missing_callback_is_null_not_a_string(self) -> None:
        (entry,) = self.entries([sub(callback=None)], {})

        assert entry["callback_now"] is None


def outcome(**counts: int) -> Outcome:
    """An Outcome holding that many distinct subscriptions in each list."""
    made = Outcome()
    n = 0
    for name, count in counts.items():
        for _ in range(count):
            n += 1
            subscription = sub(id=f"s{n}", broadcaster_user_id=str(1000 + n))
            match name:
                case "stuck" | "lost":
                    getattr(made, name).append((subscription, "why"))
                case _:
                    getattr(made, name).append(subscription)
    return made


class TestSummary:
    def test_a_second_confirmed_run_says_it_did_nothing(self) -> None:
        text = summary(Outcome(busy=True), confirm=True)

        assert "already running" in text and "did nothing" in text

    def test_busy_wins_over_everything_else_in_the_outcome(self) -> None:
        busy = Outcome(busy=True, repoint=outcome(repoint=2).repoint)

        text = summary(busy, confirm=True)

        assert "already running" in text
        assert "Repointed" not in text

    def test_nothing_to_move_counts_what_is_already_current(self) -> None:
        assert summary(outcome(keep=3), confirm=False) == (
            "Nothing to migrate: 3 subscription(s) already call back on this"
            " deployment."
        )

    def test_nothing_to_move_mentions_what_was_left_alone(self) -> None:
        assert summary(outcome(keep=1, skip=2), confirm=True).endswith(
            "call back on this deployment, 2 left alone."
        )

    def test_a_dry_run_lists_what_would_move_and_says_how_to_do_it(self) -> None:
        text = summary(outcome(repoint=2), confirm=False)

        assert text.startswith("**Dry run.** Would repoint 2:")
        assert text.count("\n- stream.online (broadcaster 100") == 2
        assert "Run it again with `confirm: True` to do it." in text
        assert "Repointed" not in text

    def test_a_confirmed_run_says_how_many_of_how_many_moved(self) -> None:
        made = outcome(repoint=3)
        made.migrated = made.repoint[:2]
        made.dumped = True

        text = summary(made, confirm=True)

        assert text.startswith("Repointed 2/3.")
        assert "Dry run" not in text

    def test_lost_ones_are_marked_gone_with_their_reason(self) -> None:
        made = outcome(repoint=1, lost=1)
        made.dumped = True

        text = summary(made, confirm=True)

        assert "**GONE - recreate these from the dump:**" in text
        assert "- stream.online (broadcaster 1002): why" in text

    def test_stuck_ones_are_marked_safe_to_retry(self) -> None:
        made = outcome(repoint=1, stuck=1)
        made.dumped = True

        text = summary(made, confirm=True)

        assert "Not moved, still on the old callback - safe to retry:" in text
        assert "GONE" not in text

    def test_lost_is_listed_before_stuck_because_it_is_the_emergency(self) -> None:
        made = outcome(repoint=2, lost=1, stuck=1)
        made.dumped = True

        text = summary(made, confirm=True)

        assert text.index("GONE") < text.index("safe to retry")

    def test_an_undelivered_dump_is_called_abandoned_on_a_confirmed_run(self) -> None:
        text = summary(outcome(repoint=1), confirm=True)

        assert "**Abandoned:** the dump could not be delivered." in text

    def test_a_dry_run_does_not_call_an_undelivered_dump_abandoned(self) -> None:
        assert "Abandoned" not in summary(outcome(repoint=1), confirm=False)

    def test_a_delivered_dump_is_pointed_at(self) -> None:
        made = outcome(repoint=1)
        made.dumped = True

        assert summary(made, confirm=False).endswith(
            "The full definition of every subscription is in the admin channel."
        )

    def test_an_undelivered_dump_is_not_pointed_at(self) -> None:
        assert "in the admin channel" not in summary(outcome(repoint=1), confirm=False)

    def test_current_and_skipped_ones_are_reported_beside_the_ones_that_move(
        self,
    ) -> None:
        text = summary(outcome(repoint=1, keep=4, skip=1), confirm=False)

        assert "4 already current." in text
        assert "Left alone, no route for the type:" in text

    def test_names_come_from_the_logins_the_outcome_carries(self) -> None:
        made = outcome(repoint=1)
        made.logins = {"1001": "valin"}

        assert "broadcaster 1001 = valin" in summary(made, confirm=False)


class TestFitsInAMessage:
    def many(self, count: int) -> str:
        return summary(outcome(repoint=count), confirm=False)

    def test_a_short_summary_is_untouched(self) -> None:
        assert "and more" not in self.many(3)

    @pytest.mark.parametrize("count", [60, 100, 500])
    def test_a_long_one_stays_under_what_discord_accepts(self, count: int) -> None:
        text = self.many(count)

        assert len(text) <= LIMIT < 2000
        assert text.endswith(
            "... and more. Every subscription is in the admin channel dump."
        )

    def test_it_drops_whole_lines_and_never_cuts_one_in_half(self) -> None:
        lines = self.many(200).split("\n")

        listed = [line for line in lines if line.startswith("- ")]
        assert listed
        assert all(line.endswith(")") for line in listed)

    def test_what_survives_is_the_start_in_order(self) -> None:
        lines = self.many(200).split("\n")
        listed = [line for line in lines if line.startswith("- ")]

        assert listed == [
            f"- stream.online (broadcaster {1000 + n})"
            for n in range(1, len(listed) + 1)
        ]

    def test_a_confirmed_run_keeps_its_lost_list_ahead_of_the_truncation(self) -> None:
        made = outcome(repoint=150, lost=150)
        made.dumped = True

        text = summary(made, confirm=True)

        assert "GONE" in text and len(text) <= LIMIT

    def test_exactly_at_the_limit_is_not_truncated(self) -> None:
        line = "x" * LIMIT

        assert migrate_plan._within_limit([line]) == line

    def test_one_over_the_limit_is_truncated(self) -> None:
        assert migrate_plan._within_limit(["x" * (LIMIT + 1)]).startswith(
            "... and more"
        )

    def test_a_first_line_too_long_to_fit_leaves_only_the_pointer(self) -> None:
        text = migrate_plan._within_limit(["y" * 5000, "short"])

        assert text == "... and more. Every subscription is in the admin channel dump."

    @pytest.mark.parametrize("width", [1, 7, 60, 300])
    def test_never_over_the_limit_whatever_the_line_width(self, width: int) -> None:
        for count in (1, 50, 400):
            text = migrate_plan._within_limit(["z" * width] * count)

            assert len(text) <= LIMIT
