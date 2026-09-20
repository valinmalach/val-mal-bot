import logging
from datetime import UTC, datetime
from types import SimpleNamespace
from typing import Any
from unittest.mock import MagicMock

import pendulum
import pytest
from discord import AllowedMentions

from cogs import birthday
from cogs.birthday import Birthday
from constants import MAX_DAYS, Months
from services.config import config

pytestmark = pytest.mark.anyio

BACKSLASH = chr(92)
NOW = pendulum.datetime(2026, 6, 15, 12)


class World:
    """The database, the admin channel and the person's screen, recorded."""

    def __init__(self) -> None:
        self.written: list[tuple[Any, ...]] = []
        self.write_error: Exception | None = None
        self.user_row: Any = None
        self.read_error: Exception | None = None
        self.sent: list[tuple[str, str, dict[str, Any]]] = []
        self.notified: list[str] = []
        self.reported: list[tuple[str, str | None]] = []
        self.done = False
        self.send_error: Exception | None = None
        self.guild: Any = None

    def interaction(self, name: str = "val_mal", user_id: int = 7) -> Any:
        async def send_message(text: str, **kwargs: Any) -> None:
            if self.send_error is not None:
                raise self.send_error
            self.sent.append(("response", text, kwargs))

        async def followup(text: str, **kwargs: Any) -> None:
            self.sent.append(("followup", text, kwargs))

        return SimpleNamespace(
            user=SimpleNamespace(id=user_id, name=name),
            guild=self.guild,
            response=SimpleNamespace(
                send_message=send_message, is_done=lambda: self.done
            ),
            followup=SimpleNamespace(send=followup),
        )

    @property
    def texts(self) -> list[str]:
        return [text for _, text, _ in self.sent]


@pytest.fixture
def world(monkeypatch: pytest.MonkeyPatch) -> World:
    world = World()

    async def upsert_user(*args: Any) -> None:
        if world.write_error is not None:
            raise world.write_error
        world.written.append(args)

    async def get_user(user_id: int) -> Any:
        if world.read_error is not None:
            raise world.read_error
        return world.user_row

    async def notify(text: str, *, key: str | None = None) -> bool:
        world.notified.append(text)
        return True

    async def report(exc: Exception, context: str, *, key: str | None = None) -> None:
        world.reported.append((context, key))

    monkeypatch.setattr(birthday.repository, "upsert_user", upsert_user)
    monkeypatch.setattr(birthday.repository, "get_user", get_user)
    monkeypatch.setattr(birthday, "notify", notify)
    monkeypatch.setattr(birthday, "report", report)
    monkeypatch.setattr(pendulum, "now", lambda tz=None: NOW)
    monkeypatch.setattr(
        config,
        "_templates",
        {
            "birthday_set": "set",
            "birthday_set_leap": "set leap",
            "birthday_bad_timezone": "bad zone {timezone}",
            "birthday_bad_day": "bad day in {month}",
            "birthday_removed": "removed",
            "birthday_none_to_remove": "nothing to remove",
            "birthday_remove_failed": "no record",
            "birthday_operation_failed": "{action} failed {mention}",
        },
    )
    monkeypatch.setattr(config, "_settings", {"owner_id": 99})
    return world


def cog() -> Birthday:
    return Birthday(MagicMock())


async def set_birthday(
    world: World, month: Months, day: int, timezone: str = "UTC"
) -> None:
    command: Any = Birthday.set_birthday
    await command.callback(cog(), world.interaction(), month, day, timezone)


async def remove_birthday(world: World) -> None:
    command: Any = Birthday.remove_birthday
    await command.callback(cog(), world.interaction())


class TestSet:
    async def test_stores_the_next_occurrence_with_the_flags_it_was_built_from(
        self, world: World
    ) -> None:
        await set_birthday(world, Months.July, 4, "Asia/Tokyo")

        ((user_id, name, moment, leap, zone),) = world.written
        assert (user_id, name, leap, zone) == (7, "val_mal", False, "Asia/Tokyo")
        assert moment == pendulum.datetime(2026, 7, 3, 15)
        assert world.texts == ["set"]

    async def test_the_zone_defaults_to_utc(self, world: World) -> None:
        await set_birthday(world, Months.July, 4)

        assert world.written[0][4] == "UTC"
        assert world.written[0][2] == pendulum.datetime(2026, 7, 4)

    async def test_a_birthday_already_gone_this_year_is_stored_for_next_year(
        self, world: World
    ) -> None:
        await set_birthday(world, Months.January, 4)

        assert world.written[0][2] == pendulum.datetime(2027, 1, 4)

    async def test_a_leap_day_is_flagged_and_stored_in_a_leap_year(
        self, world: World
    ) -> None:
        await set_birthday(world, Months.February, 29)

        ((_, _, moment, leap, _),) = world.written
        assert leap is True
        assert moment == pendulum.datetime(2028, 2, 29)
        assert world.texts == ["set leap"]

    async def test_no_other_date_is_flagged_as_a_leap_day(self, world: World) -> None:
        await set_birthday(world, Months.February, 28)

        assert world.written[0][3] is False

    async def test_answers_exactly_once(self, world: World) -> None:
        await set_birthday(world, Months.July, 4)

        assert len(world.sent) == 1 and world.sent[0][0] == "response"

    @pytest.mark.parametrize("month", list(Months))
    async def test_the_last_day_of_every_month_is_accepted(
        self, month: Months, world: World
    ) -> None:
        await set_birthday(world, month, MAX_DAYS[month])

        assert len(world.written) == 1

    @pytest.mark.parametrize("month", [m for m in Months if MAX_DAYS[m] < 31])
    async def test_one_day_past_the_end_of_a_short_month_is_refused_by_name(
        self, month: Months, world: World, caplog: pytest.LogCaptureFixture
    ) -> None:
        with caplog.at_level(logging.WARNING, logger=birthday.logger.name):
            await set_birthday(world, month, MAX_DAYS[month] + 1)

        assert world.written == []
        assert world.texts == [f"bad day in {month.name}"]
        assert (
            f"Invalid day {MAX_DAYS[month] + 1} for month {month.name}" in caplog.text
        )

    async def test_the_thirtieth_of_february_is_refused(self, world: World) -> None:
        await set_birthday(world, Months.February, 30)

        assert world.written == [] and world.texts == ["bad day in February"]

    async def test_the_twenty_ninth_of_february_is_not(self, world: World) -> None:
        """It exists in leap years, which is what the stored year accounts for."""
        assert MAX_DAYS[Months.February] == 29


class TestABadTimezone:
    @pytest.mark.parametrize("zone", ["Mars/Base", "utc", "", "GMT+5", "Europe/london"])
    async def test_a_name_not_in_the_tz_database_is_refused_without_storing(
        self, zone: str, world: World
    ) -> None:
        await set_birthday(world, Months.July, 4, zone)

        assert world.written == []
        assert world.texts == [f"bad zone {zone}"]

    async def test_the_names_are_case_sensitive_as_the_database_is(
        self, world: World
    ) -> None:
        await set_birthday(world, Months.July, 4, "europe/london")

        assert world.written == []

    async def test_what_was_typed_is_escaped_and_cannot_mention_anyone(
        self, world: World
    ) -> None:
        await set_birthday(world, Months.July, 4, "*@everyone*")

        (_, text, kwargs) = world.sent[0]
        assert text == f"bad zone {BACKSLASH}*@everyone{BACKSLASH}*"
        mentions: AllowedMentions = kwargs["allowed_mentions"]
        assert (mentions.everyone, mentions.users, mentions.roles) == (
            False,
            False,
            False,
        )

    async def test_the_log_carries_the_value_as_a_repr_so_a_newline_cannot_forge_a_line(
        self, world: World, caplog: pytest.LogCaptureFixture
    ) -> None:
        with caplog.at_level(logging.WARNING, logger=birthday.logger.name):
            await set_birthday(world, Months.July, 4, f"a{chr(10)}b")

        assert f"Invalid timezone provided: 'a{BACKSLASH}nb'" in caplog.text

    async def test_the_zone_is_checked_before_the_day_and_only_one_answer_goes_out(
        self, world: World
    ) -> None:
        await set_birthday(world, Months.February, 30, "Mars/Base")

        assert world.texts == ["bad zone Mars/Base"]


class TestSetFails:
    async def test_a_database_failure_is_apologised_for_and_reported(
        self, world: World
    ) -> None:
        world.write_error = ConnectionError("down")

        await set_birthday(world, Months.July, 4)

        assert world.texts == ["set failed <@99>"]
        assert world.reported == [
            ("Failed to set birthday for val\\_mal (ID: 7)", None)
        ]

    async def test_the_guild_owner_is_named_when_there_is_one(
        self, world: World
    ) -> None:
        world.write_error = ConnectionError("down")
        world.guild = SimpleNamespace(owner=SimpleNamespace(mention="<@1>"))

        await set_birthday(world, Months.July, 4)

        assert world.texts == ["set failed <@1>"]

    async def test_a_guild_with_no_cached_owner_falls_back_to_the_configured_one(
        self, world: World
    ) -> None:
        world.write_error = ConnectionError("down")
        world.guild = SimpleNamespace(owner=None)

        await set_birthday(world, Months.July, 4)

        assert world.texts == ["set failed <@99>"]

    async def test_an_answer_that_cannot_be_sent_is_reported_too_and_never_raised(
        self, world: World
    ) -> None:
        world.write_error = ConnectionError("down")
        world.send_error = RuntimeError("discord down")

        await set_birthday(world, Months.July, 4)

        assert [c for c, _ in world.reported] == [
            f"Could not tell val{BACKSLASH}_mal that their birthday set failed",
            "Failed to set birthday for val\\_mal (ID: 7)",
        ]
        assert world.reported[0][1] == "birthday-unanswerable:7:set"

    async def test_an_interaction_already_answered_gets_a_followup(
        self, world: World
    ) -> None:
        world.write_error = ConnectionError("down")
        world.done = True

        await set_birthday(world, Months.July, 4)

        assert world.sent[0][0] == "followup"

    async def test_a_name_that_is_markdown_is_escaped_in_the_report(
        self, world: World
    ) -> None:
        world.write_error = ConnectionError("down")
        command: Any = Birthday.set_birthday

        await command.callback(
            cog(), world.interaction(name="**x**"), Months.July, 4, "UTC"
        )

        assert f"{BACKSLASH}*" in world.reported[-1][0]


class TestRemove:
    async def test_clears_all_three_columns_of_a_stored_birthday(
        self, world: World
    ) -> None:
        world.user_row = SimpleNamespace(birthday=datetime(2026, 7, 4, tzinfo=UTC))

        await remove_birthday(world)

        assert world.written == [(7, "val_mal", None, None, None)]
        assert world.texts == ["removed"]

    async def test_a_user_with_no_birthday_is_told_there_was_nothing_to_remove(
        self, world: World
    ) -> None:
        world.user_row = SimpleNamespace(birthday=None)

        await remove_birthday(world)

        assert world.texts == ["nothing to remove"]

    async def test_a_user_with_no_record_is_told_so_and_the_admins_are_too(
        self, world: World
    ) -> None:
        world.user_row = None

        await remove_birthday(world)

        assert world.written == []
        assert world.texts == ["no record"]
        assert world.notified == [
            f"User val{BACKSLASH}_mal (7) attempted to remove a birthday but had "
            "no record."
        ]

    async def test_a_database_failure_says_the_action_was_forget(
        self, world: World
    ) -> None:
        world.read_error = ConnectionError("down")

        await remove_birthday(world)

        assert world.texts == ["forget failed <@99>"]
        assert (
            world.reported[-1][0] == "Failed to forget birthday for val\\_mal (ID: 7)"
        )

    async def test_a_failed_write_is_reported_and_not_confirmed(
        self, world: World
    ) -> None:
        world.user_row = SimpleNamespace(birthday=datetime(2026, 7, 4, tzinfo=UTC))
        world.write_error = ConnectionError("down")

        await remove_birthday(world)

        assert world.texts == ["forget failed <@99>"]


class TestTheCommandsThemselves:
    def test_the_group_is_called_birthday(self) -> None:
        assert Birthday.__cog_group_name__ == "birthday"

    def test_both_commands_need_the_follower_role(self) -> None:
        for command in (Birthday.set_birthday, Birthday.remove_birthday):
            assert len(command.checks) == 1  # pyright: ignore[reportAttributeAccessIssue]

    def test_the_day_is_bounded_to_a_month_and_the_zone_defaults_to_utc(self) -> None:
        parameters = {p.name: p for p in Birthday.set_birthday.parameters}  # pyright: ignore[reportAttributeAccessIssue]

        assert (parameters["day"].min_value, parameters["day"].max_value) == (1, 31)
        assert parameters["timezone"].default == "UTC"
        assert parameters["timezone"].autocomplete is True

    def test_the_month_offers_all_twelve(self) -> None:
        parameters = {p.name: p for p in Birthday.set_birthday.parameters}  # pyright: ignore[reportAttributeAccessIssue]

        assert [c.value for c in parameters["month"].choices] == list(range(1, 13))
