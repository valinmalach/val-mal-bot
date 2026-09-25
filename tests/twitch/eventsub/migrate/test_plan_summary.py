import pytest

from tests.twitch.eventsub.migrate.support import sub
from valmal.twitch.eventsub import migrate_plan
from valmal.twitch.eventsub.migrate_plan import Outcome, summary

RAID = {"from_broadcaster_user_id": "", "to_broadcaster_user_id": "222"}
LIMIT = migrate_plan._DISCORD_MESSAGE_LIMIT


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
