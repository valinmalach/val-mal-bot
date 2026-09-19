from datetime import UTC, datetime
from types import SimpleNamespace

import pytest

from db import repository
from db.models import DiscordMessage, DiscordUser, LiveAlert, TwitchAutoShoutout
from tests.database.support import Database, params, sql

pytestmark = pytest.mark.anyio

MOMENT = datetime(2026, 6, 15, 11, 0, tzinfo=UTC)


class TestGetters:
    async def test_get_user_looks_the_row_up_by_primary_key(
        self, database: Database
    ) -> None:
        row = SimpleNamespace(id=7)
        database.rows[(DiscordUser, 7)] = row

        assert await repository.get_user(7) is row
        assert database.gets == [(DiscordUser, 7)]

    async def test_a_user_who_is_not_there_is_none(self, database: Database) -> None:
        assert await repository.get_user(7) is None

    async def test_get_message(self, database: Database) -> None:
        row = SimpleNamespace(id=5)
        database.rows[(DiscordMessage, 5)] = row

        assert await repository.get_message(5) is row
        assert await repository.get_message(6) is None

    async def test_get_live_alert(self, database: Database) -> None:
        row = SimpleNamespace(broadcaster_id=1)
        database.rows[(LiveAlert, 1)] = row

        assert await repository.get_live_alert(1) is row
        assert await repository.get_live_alert(2) is None

    async def test_each_read_uses_its_own_session_scope(
        self, database: Database
    ) -> None:
        await repository.get_user(1)
        await repository.get_message(1)
        await repository.get_live_alert(1)

        assert database.scopes == 3


class TestUsersDueBirthday:
    async def test_is_a_range_so_a_late_tick_cannot_skip_a_birthday_for_good(
        self, database: Database
    ) -> None:
        await repository.users_due_birthday(MOMENT)

        statement = database.only
        assert "discord_user.birthday <= %(birthday_1)s" in sql(statement)
        assert " = %(birthday" not in sql(statement)
        assert params(statement) == {"birthday_1": MOMENT}

    async def test_returns_the_rows_as_a_list(self, database: Database) -> None:
        a, b = SimpleNamespace(id=1), SimpleNamespace(id=2)
        database.answers = [[a, b]]

        assert await repository.users_due_birthday(MOMENT) == [a, b]

    async def test_nobody_due_is_an_empty_list_not_none(
        self, database: Database
    ) -> None:
        assert await repository.users_due_birthday(MOMENT) == []

    async def test_a_null_birthday_is_never_due(self, database: Database) -> None:
        """SQL's NULL <= x is not true, which is what keeps unset users out."""
        await repository.users_due_birthday(MOMENT)

        assert "IS NULL" not in sql(database.only)


class TestListLiveAlerts:
    async def test_selects_every_row_without_a_filter(self, database: Database) -> None:
        await repository.list_live_alerts()

        assert " WHERE " not in sql(database.only)
        assert sql(database.only).startswith("SELECT live_alert.")

    async def test_returns_them_as_a_list(self, database: Database) -> None:
        rows = [SimpleNamespace(broadcaster_id=1), SimpleNamespace(broadcaster_id=2)]
        database.answers = [rows]

        assert await repository.list_live_alerts() == rows

    async def test_none_stored_is_an_empty_list(self, database: Database) -> None:
        assert await repository.list_live_alerts() == []


class TestIsAutoshoutout:
    async def test_true_when_the_user_is_on_the_list(self, database: Database) -> None:
        database.rows[(TwitchAutoShoutout, 5)] = SimpleNamespace(twitch_user_id=5)

        assert await repository.is_autoshoutout(5) is True

    async def test_false_when_they_are_not(self, database: Database) -> None:
        assert await repository.is_autoshoutout(5) is False

    async def test_a_stored_row_that_is_falsy_still_counts(
        self, database: Database
    ) -> None:
        """The answer is is-not-None, not truthiness of the row."""
        database.rows[(TwitchAutoShoutout, 0)] = 0

        assert await repository.is_autoshoutout(0) is True


class TestTheModuleSurface:
    def test_every_name_in_all_exists(self) -> None:
        assert all(hasattr(repository, name) for name in repository.__all__)

    def test_every_public_function_is_exported(self) -> None:
        public = {
            name
            for name, value in vars(repository).items()
            if callable(value)
            and not name.startswith("_")
            and getattr(value, "__module__", "") == repository.__name__
        }

        assert public == set(repository.__all__)
