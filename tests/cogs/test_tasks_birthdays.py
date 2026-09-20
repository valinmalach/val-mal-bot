import logging
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from typing import Any
from unittest.mock import MagicMock

import pendulum
import pytest

from cogs import tasks
from cogs.tasks import Tasks
from valmal.core.config import config

pytestmark = pytest.mark.anyio

BACKSLASH = chr(92)
NOW = pendulum.datetime(2026, 6, 15, 12, 15, 3)
DUE = datetime(2026, 6, 15, 12, 15, tzinfo=UTC)


def record(**overrides: Any) -> Any:
    return SimpleNamespace(
        **{
            "id": 7,
            "username": "val_mal",
            "birthday": DUE,
            "is_birthday_leap": False,
            "birthday_timezone": None,
        }
        | overrides
    )


class World:
    def __init__(self) -> None:
        self.due: list[Any] = []
        self.queried: list[datetime] = []
        self.query_error: Exception | None = None
        self.written: list[tuple[Any, ...]] = []
        self.write_error: Exception | None = None
        self.order: list[str] = []
        self.notified: list[tuple[str, str | None]] = []
        self.reported: list[str] = []
        self.sent: list[tuple[str, int]] = []
        self.users: dict[int, Any] = {7: SimpleNamespace(mention="<@7>")}


@pytest.fixture
def world(monkeypatch: pytest.MonkeyPatch) -> World:
    world = World()

    async def users_due_birthday(moment: datetime) -> list[Any]:
        world.queried.append(moment)
        if world.query_error is not None:
            raise world.query_error
        return world.due

    async def upsert_user(*args: Any) -> None:
        world.order.append("write")
        if world.write_error is not None:
            raise world.write_error
        world.written.append(args)

    async def notify(text: str, *, key: str | None = None) -> bool:
        world.order.append("notify")
        world.notified.append((text, key))
        return True

    async def report(exc: Exception, context: str, **_: object) -> None:
        world.reported.append(context)

    async def send_message(text: str, channel_id: int) -> int:
        world.order.append("send")
        world.sent.append((text, channel_id))
        return 1

    monkeypatch.setattr(tasks.repository, "users_due_birthday", users_due_birthday)
    monkeypatch.setattr(tasks.repository, "upsert_user", upsert_user)
    monkeypatch.setattr(tasks, "notify", notify)
    monkeypatch.setattr(tasks, "report", report)
    monkeypatch.setattr(tasks, "send_message", send_message)
    monkeypatch.setattr(pendulum, "now", lambda tz=None: NOW)
    monkeypatch.setattr(config, "_channels", {"shoutouts": 8080})
    monkeypatch.setattr(
        config, "_templates", {"discord_birthday": "Happy birthday {mention}!"}
    )
    return world


def cog(world: World) -> Tasks:
    bot = MagicMock()
    bot.get_user = lambda user_id: world.users.get(user_id)
    return Tasks(bot)


class TestAnnouncing:
    async def test_greets_the_person_in_the_shoutouts_channel(
        self, world: World
    ) -> None:
        await cog(world)._announce_birthday(record())

        assert world.sent == [("Happy birthday <@7>!", 8080)]

    async def test_someone_the_cache_does_not_hold_is_logged_and_told_to_the_admins(
        self, world: World, caplog: pytest.LogCaptureFixture
    ) -> None:
        world.users.clear()

        with caplog.at_level(logging.WARNING, logger=tasks.logger.name):
            await cog(world)._announce_birthday(record())

        assert world.sent == []
        assert world.notified == [
            ("_announce_birthday: User with ID 7 not found.", None)
        ]
        assert "Discord user ID 7 not found in guild cache" in caplog.text


class TestProcessingOneBirthday:
    async def test_a_birthday_with_no_date_is_left_alone(self, world: World) -> None:
        await cog(world)._process_birthday(record(birthday=None), NOW)

        assert world.order == []

    async def test_reschedules_then_greets_in_that_order(self, world: World) -> None:
        """A write that failed after a greeting would repeat it every quarter hour."""
        await cog(world)._process_birthday(record(), NOW)

        assert world.order == ["write", "send"]

    async def test_moves_the_birthday_to_the_next_occurrence_keeping_its_flags(
        self, world: World
    ) -> None:
        await cog(world)._process_birthday(record(), NOW)

        assert world.written == [
            (7, "val_mal", pendulum.datetime(2027, 6, 15, 12, 15), False, None)
        ]

    async def test_a_missing_leap_flag_is_stored_as_false_not_none(
        self, world: World
    ) -> None:
        await cog(world)._process_birthday(record(is_birthday_leap=None), NOW)

        assert world.written[0][3] is False

    async def test_a_leap_day_moves_to_the_next_leap_year(self, world: World) -> None:
        leap = datetime(2028, 2, 29, tzinfo=UTC)
        now = pendulum.datetime(2028, 2, 29, 0, 3)

        await cog(world)._process_birthday(
            record(birthday=leap, is_birthday_leap=True), now
        )

        assert world.written[0][2] == pendulum.datetime(2032, 2, 29)
        assert world.written[0][3] is True

    async def test_a_zone_is_used_to_keep_the_local_day(self, world: World) -> None:
        stored = datetime(2026, 6, 14, 15, tzinfo=UTC)  # midnight 15 June in Tokyo
        now = pendulum.datetime(2026, 6, 14, 15, 3)

        await cog(world)._process_birthday(
            record(birthday=stored, birthday_timezone="Asia/Tokyo"), now
        )

        assert world.written[0][2] == pendulum.datetime(2027, 6, 14, 15)
        assert world.written[0][4] == "Asia/Tokyo"

    async def test_a_failed_write_means_no_greeting(self, world: World) -> None:
        world.write_error = ConnectionError("down")

        with pytest.raises(ConnectionError):
            await cog(world)._process_birthday(record(), NOW)

        assert world.sent == []


class TestHowLateIsTooLate:
    @pytest.mark.parametrize(
        ("late", "greeted"),
        [
            (timedelta(0), True),
            (timedelta(hours=23, minutes=59), True),
            (timedelta(hours=24), True),
            (timedelta(hours=24, seconds=1), False),
            (timedelta(days=5), False),
        ],
    )
    async def test_a_birthday_is_greeted_up_to_and_including_a_day_late(
        self, late: timedelta, greeted: bool, world: World
    ) -> None:
        due = NOW - late
        await cog(world)._process_birthday(record(birthday=due), NOW)

        assert bool(world.sent) is greeted

    async def test_a_stale_one_is_moved_on_and_the_admins_told_not_greeted(
        self, world: World
    ) -> None:
        stale = datetime(2026, 6, 1, tzinfo=UTC)

        await cog(world)._process_birthday(record(birthday=stale), NOW)

        assert world.order == ["write", "notify"]
        ((text, key),) = world.notified
        assert "is too stale to announce, so nobody greeted them" in text
        assert f"val{BACKSLASH}_mal (ID: 7)" in text
        assert key == "birthday-stale:7"

    async def test_the_stale_notice_says_when_it_was_due_and_when_it_moves_to(
        self, world: World
    ) -> None:
        stale = datetime(2026, 6, 1, tzinfo=UTC)

        await cog(world)._process_birthday(record(birthday=stale), NOW)

        text = world.notified[0][0]
        assert str(stale) in text and "rescheduled to 2027-06-01" in text

    @pytest.mark.parametrize(
        ("moment", "within"),
        [
            (NOW, True),
            (NOW - timedelta(hours=24), True),
            (NOW - timedelta(hours=24, microseconds=1), False),
            (NOW + timedelta(hours=1), True),
        ],
    )
    def test_the_grace_is_a_day_and_a_future_date_is_within_it(
        self, moment: datetime, within: bool
    ) -> None:
        assert Tasks._within_announce_grace(moment, NOW) is within


class TestATimezoneThatWentAway:
    async def test_none_and_a_real_zone_are_kept_without_comment(
        self, world: World
    ) -> None:
        assert await Tasks._usable_timezone(record()) is None
        assert (
            await Tasks._usable_timezone(record(birthday_timezone="Europe/London"))
            == "Europe/London"
        )
        assert world.notified == []

    async def test_a_dropped_name_is_cleared_and_the_admins_told_once_by_key(
        self, world: World
    ) -> None:
        zone = "US/Pacific-New"

        assert await Tasks._usable_timezone(record(birthday_timezone=zone)) is None

        ((text, key),) = world.notified
        assert "is not a timezone any more" in text
        assert "US/Pacific-New" in text and f"val{BACKSLASH}_mal" in text
        assert key == "birthday-bad-zone:7"

    async def test_the_name_is_escaped_in_the_notice(self, world: World) -> None:
        await Tasks._usable_timezone(record(birthday_timezone="*x*"))

        assert f"{BACKSLASH}*x{BACKSLASH}*" in world.notified[0][0]

    async def test_the_birthday_still_moves_on_and_is_written_without_the_zone(
        self, world: World
    ) -> None:
        """A raise here would leave a birthday that never moves and is never greeted."""
        await cog(world)._process_birthday(record(birthday_timezone="Gone/Away"), NOW)

        assert world.written == [
            (7, "val_mal", pendulum.datetime(2027, 6, 15, 12, 15), False, None)
        ]
        assert world.sent == [("Happy birthday <@7>!", 8080)]


class TestProcessingEveryDueBirthday:
    async def test_handles_each_in_turn(self, world: World) -> None:
        await cog(world)._process_birthday_records(
            [record(id=1), record(id=2), record(id=3)]
        )

        assert [w[0] for w in world.written] == [1, 2, 3]

    async def test_one_that_fails_does_not_stop_the_rest(
        self, world: World, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        world.users |= {n: SimpleNamespace(mention=f"<@{n}>") for n in (1, 2, 3)}
        original = tasks.repository.upsert_user

        async def flaky(user_id: int, *args: Any) -> None:
            if user_id == 2:
                raise ConnectionError("db")
            await original(user_id, *args)

        monkeypatch.setattr(tasks.repository, "upsert_user", flaky)

        await cog(world)._process_birthday_records(
            [record(id=1), record(id=2), record(id=3)]
        )

        assert [text for text, _ in world.sent] == [
            "Happy birthday <@1>!",
            "Happy birthday <@3>!",
        ]
        assert world.reported == [
            f"Failed to process birthday for user val{BACKSLASH}_mal (ID: 2)"
        ]

    async def test_nobody_due_does_nothing(self, world: World) -> None:
        await cog(world)._process_birthday_records([])

        assert world.order == []


class TestTheLoop:
    async def test_asks_for_everyone_due_up_to_this_minute(self, world: World) -> None:
        """The seconds are dropped so a tick a hair after :15 still matches :15."""
        loop: Any = Tasks.check_birthdays
        await loop.coro(cog(world))

        assert world.queried == [pendulum.datetime(2026, 6, 15, 12, 15)]

    async def test_processes_what_it_found(self, world: World) -> None:
        world.due = [record()]
        loop: Any = Tasks.check_birthdays

        await loop.coro(cog(world))

        assert world.sent == [("Happy birthday <@7>!", 8080)]

    async def test_a_failure_of_any_kind_is_reported_and_never_ends_the_loop(
        self, world: World
    ) -> None:
        """A loop that raises stops for good and nothing says so."""
        world.query_error = ConnectionError("db")
        loop: Any = Tasks.check_birthdays

        await loop.coro(cog(world))

        assert world.reported == ["Fatal error during birthday check task"]
