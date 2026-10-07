"""Users and the message cache."""

import inspect
from datetime import UTC, datetime
from typing import Any

import pytest

from tests.db.support import Database
from valmal.db import repository
from valmal.db.rows import DiscordMessage, DiscordUser

pytestmark = pytest.mark.anyio

MOMENT = datetime(2026, 6, 15, 11, 0, tzinfo=UTC)
STAMP = datetime(2026, 1, 1, tzinfo=UTC)


def user_record(**fields: Any) -> dict[str, Any]:
    return {
        "id": 7,
        "username": "someone",
        "birthday": None,
        "is_birthday_leap": None,
        "birthday_timezone": None,
        "created_at": STAMP,
        "updated_at": STAMP,
    } | fields


class TestGetUser:
    async def test_looks_the_user_up_by_id_and_returns_a_row(
        self, database: Database
    ) -> None:
        database.answers = [user_record(birthday=MOMENT)]

        user = await repository.get_user(7)

        assert database.only == ("fetchrow", repository.GET_USER, (7,))
        assert user == DiscordUser(**user_record(birthday=MOMENT))

    async def test_a_user_who_is_not_there_is_none(self, database: Database) -> None:
        assert await repository.get_user(7) is None

    async def test_each_read_runs_in_its_own_transaction(
        self, database: Database
    ) -> None:
        await repository.get_user(1)
        await repository.get_message(1)
        await repository.get_live_alert(1)

        assert database.transactions == 3


class TestUsersDueBirthday:
    async def test_is_a_range_so_a_late_tick_cannot_skip_a_birthday_for_good(
        self, database: Database
    ) -> None:
        await repository.users_due_birthday(MOMENT)

        assert database.only == ("fetch", repository.USERS_DUE_BIRTHDAY, (MOMENT,))
        assert "birthday <= $1" in repository.USERS_DUE_BIRTHDAY

    async def test_a_null_birthday_is_never_due(self) -> None:
        """SQL's NULL <= x is not true, which is what keeps unset users out."""
        assert "NULL" not in repository.USERS_DUE_BIRTHDAY

    async def test_returns_every_due_user_as_rows(self, database: Database) -> None:
        database.answers = [[user_record(id=1), user_record(id=2)]]

        due = await repository.users_due_birthday(MOMENT)

        assert [u.id for u in due] == [1, 2]
        assert all(isinstance(u, DiscordUser) for u in due)

    async def test_nobody_due_is_an_empty_list(self, database: Database) -> None:
        assert await repository.users_due_birthday(MOMENT) == []


class TestUpsertUser:
    async def test_writes_the_user_and_all_three_birthday_columns_together(
        self, database: Database
    ) -> None:
        await repository.upsert_user(7, "someone", MOMENT, True, "Asia/Singapore")

        assert database.only == (
            "execute",
            repository.UPSERT_USER,
            (7, "someone", MOMENT, True, "Asia/Singapore"),
        )

    def test_a_conflict_overwrites_every_column_but_the_id_and_created_at(
        self,
    ) -> None:
        _, _, updates = repository.UPSERT_USER.partition("DO UPDATE SET")

        for column in ("username", "birthday", "is_birthday_leap", "birthday_timezone"):
            assert f"{column} = EXCLUDED.{column}" in updates
        assert "id =" not in updates
        assert "created_at" not in updates

    def test_every_birthday_column_is_required(self) -> None:
        """No default: a caller that leaves one out erases a birthday by accident."""
        parameters = inspect.signature(repository.upsert_user).parameters.values()

        assert all(p.default is inspect.Parameter.empty for p in parameters)


class TestClearBirthday:
    async def test_nulls_all_three_columns_of_that_user(
        self, database: Database
    ) -> None:
        await repository.clear_birthday(7)

        assert database.only == ("execute", repository.CLEAR_BIRTHDAY, (7,))
        for column in ("birthday", "is_birthday_leap", "birthday_timezone"):
            assert f"{column} = NULL" in repository.CLEAR_BIRTHDAY
        assert "WHERE id = $1" in repository.CLEAR_BIRTHDAY


class TestUpsertUsername:
    async def test_writes_only_the_name(self, database: Database) -> None:
        await repository.upsert_username(7, "renamed")

        assert database.only == ("execute", repository.UPSERT_USERNAME, (7, "renamed"))

    def test_leaves_every_birthday_column_out_of_the_statement(self) -> None:
        """A rename must not erase a birthday the user already has."""
        assert "birthday" not in repository.UPSERT_USERNAME


class TestDeleteUser:
    async def test_is_by_id(self, database: Database) -> None:
        await repository.delete_user(7)

        assert database.only == ("execute", repository.DELETE_USER, (7,))


class TestMessages:
    def message_record(self, **fields: Any) -> dict[str, Any]:
        return {
            "id": 5,
            "contents": "hi",
            "guild_id": 1,
            "author_id": 2,
            "channel_id": 3,
            "attachment_urls": ["https://a", "https://b"],
            "created_at": STAMP,
        } | fields

    async def test_a_cached_message_comes_back_with_its_attachments(
        self, database: Database
    ) -> None:
        database.answers = [self.message_record()]

        message = await repository.get_message(5)

        assert database.only == ("fetchrow", repository.GET_MESSAGE, (5,))
        assert message == DiscordMessage(
            **self.message_record(attachment_urls=("https://a", "https://b"))
        )

    async def test_a_message_that_is_not_cached_is_none(
        self, database: Database
    ) -> None:
        assert await repository.get_message(5) is None

    async def test_stores_the_message_and_its_attachments(
        self, database: Database
    ) -> None:
        await repository.upsert_message(5, None, 1, 2, 3, ["https://a"])

        assert database.only == (
            "execute",
            repository.UPSERT_MESSAGE,
            (5, None, 1, 2, 3, ["https://a"]),
        )

    def test_an_edit_overwrites_the_content_on_the_same_id(self) -> None:
        assert "ON CONFLICT (id) DO UPDATE" in repository.UPSERT_MESSAGE
        assert "contents = EXCLUDED.contents" in repository.UPSERT_MESSAGE

    async def test_delete_message_is_by_id(self, database: Database) -> None:
        await repository.delete_message(5)

        assert database.only == ("execute", repository.DELETE_MESSAGE, (5,))

    async def test_a_bulk_delete_is_one_statement_for_the_whole_batch(
        self, database: Database
    ) -> None:
        await repository.delete_messages({1, 2, 3})

        method, statement, (ids,) = database.only
        assert (method, statement) == ("execute", repository.DELETE_MESSAGES)
        assert sorted(ids) == [1, 2, 3]

    async def test_deleting_no_messages_sends_nothing(self, database: Database) -> None:
        await repository.delete_messages([])

        assert database.calls == []
        assert database.transactions == 0
