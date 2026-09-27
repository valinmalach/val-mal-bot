from datetime import UTC, datetime

import pytest

from tests.db.support import Database, assigned, params, sql
from valmal.db import repository

pytestmark = pytest.mark.anyio

MOMENT = datetime(2026, 6, 15, 11, 0, tzinfo=UTC)


class TestUpsertUser:
    async def test_writes_the_user_and_all_three_birthday_columns_together(
        self, database: Database
    ) -> None:
        await repository.upsert_user(7, "val", MOMENT, True, "Europe/London")

        statement = database.only
        assert sql(statement).startswith("INSERT INTO discord_user")
        assert params(statement) | {"created_at": 0, "updated_at": 0} == {
            "created_at": 0,
            "updated_at": 0,
            "id": 7,
            "username": "val",
            "birthday": MOMENT,
            "is_birthday_leap": True,
            "birthday_timezone": "Europe/London",
        }

    async def test_a_conflict_on_the_id_overwrites_everything_but_the_id(
        self, database: Database
    ) -> None:
        await repository.upsert_user(7, "val", MOMENT, False, "UTC")

        assert "ON CONFLICT (id) DO UPDATE" in sql(database.only)
        assert assigned(database.only) == [
            "updated_at",
            "username",
            "birthday",
            "is_birthday_leap",
            "birthday_timezone",
        ]

    async def test_no_birthday_clears_all_three_rather_than_leaving_a_stale_one(
        self, database: Database
    ) -> None:
        """The three describe one birthday; writing two would leave the third wrong."""
        await repository.upsert_user(7, "val")

        written = params(database.only)
        assert (
            written["birthday"],
            written["is_birthday_leap"],
            written["birthday_timezone"],
        ) == (None, None, None)
        assert "excluded.birthday," in sql(database.only)

    async def test_updated_at_is_set_by_the_database_clock_not_by_the_row_being_kept(
        self, database: Database
    ) -> None:
        """The column's onupdate does not fire for ON CONFLICT DO UPDATE."""
        await repository.upsert_user(7, "val")

        assert "updated_at = now()" in sql(database.only)

    async def test_created_at_is_never_overwritten(self, database: Database) -> None:
        await repository.upsert_user(7, "val")

        assert "created_at" not in assigned(database.only)


class TestUpsertUsername:
    async def test_leaves_every_birthday_column_out_of_the_statement(
        self, database: Database
    ) -> None:
        """A rename or a first sighting must not wipe a birthday somebody set."""
        await repository.upsert_username(7, "val")

        statement = database.only
        assert assigned(statement) == ["updated_at", "username"]
        assert "birthday" not in sql(statement)
        assert "is_birthday_leap" not in sql(statement)

    async def test_still_writes_the_username(self, database: Database) -> None:
        await repository.upsert_username(7, "new-name")

        assert params(database.only)["username"] == "new-name"
        assert params(database.only)["id"] == 7


class TestUpsertMessage:
    async def test_stores_the_message_and_its_attachments_as_json(
        self, database: Database
    ) -> None:
        await repository.upsert_message(5, "hi", 1, 2, 3, ["https://a", "https://b"])

        statement = database.only
        assert sql(statement).startswith("INSERT INTO discord_message")
        assert params(statement)["attachment_urls"] == ["https://a", "https://b"]
        assert "::JSONB" in sql(statement)

    async def test_an_edit_overwrites_the_content_on_the_same_id(
        self, database: Database
    ) -> None:
        await repository.upsert_message(5, "edited", 1, 2, 3, [])

        assert "ON CONFLICT (id) DO UPDATE" in sql(database.only)
        assert assigned(database.only) == [
            "contents",
            "guild_id",
            "author_id",
            "channel_id",
            "attachment_urls",
        ]

    async def test_a_message_table_without_updated_at_gets_no_stamp(
        self, database: Database
    ) -> None:
        await repository.upsert_message(5, "hi", 1, 2, 3, [])

        assert "updated_at" not in sql(database.only)

    async def test_no_text_is_stored_as_null(self, database: Database) -> None:
        await repository.upsert_message(5, None, 1, 2, 3, ["https://a"])

        assert params(database.only)["contents"] is None


class TestUpsertLiveAlert:
    async def test_is_keyed_on_the_broadcaster(self, database: Database) -> None:
        await repository.upsert_live_alert(1, 2, 3, 4, MOMENT)

        statement = database.only
        assert "ON CONFLICT (broadcaster_id) DO UPDATE" in sql(statement)
        assert assigned(statement) == [
            "updated_at",
            "channel_id",
            "message_id",
            "stream_id",
            "stream_started_at",
        ]
        written = params(statement)
        assert (written["broadcaster_id"], written["stream_started_at"]) == (1, MOMENT)


class TestDeletes:
    async def test_delete_user_is_by_id_only(self, database: Database) -> None:
        await repository.delete_user(7)

        assert sql(database.only) == (
            "DELETE FROM discord_user WHERE discord_user.id = %(id_1)s"
        )
        assert params(database.only) == {"id_1": 7}

    async def test_delete_message_is_by_id_only(self, database: Database) -> None:
        await repository.delete_message(5)

        assert sql(database.only) == (
            "DELETE FROM discord_message WHERE discord_message.id = %(id_1)s"
        )

    async def test_delete_live_alert_by_broadcaster_takes_whatever_alert_is_there(
        self, database: Database
    ) -> None:
        await repository.delete_live_alert(1)

        assert sql(database.only) == (
            "DELETE FROM live_alert WHERE live_alert.broadcaster_id = "
            "%(broadcaster_id_1)s"
        )

    async def test_a_message_id_scopes_the_delete_so_a_newer_alert_survives(
        self, database: Database
    ) -> None:
        """A late cleanup for the previous stream must not remove this stream's row."""
        await repository.delete_live_alert(1, message_id=900)

        statement = database.only
        assert "live_alert.message_id = %(message_id_1)s" in sql(statement)
        assert " AND " in sql(statement)
        assert params(statement) == {"broadcaster_id_1": 1, "message_id_1": 900}

    async def test_a_message_id_of_zero_still_scopes_the_delete(
        self, database: Database
    ) -> None:
        """Only None means unscoped; a falsy id is still an id."""
        await repository.delete_live_alert(1, message_id=0)

        assert "message_id" in sql(database.only)


class TestAutoshoutoutList:
    async def test_adding_says_true_when_the_insert_took_the_row(
        self, database: Database
    ) -> None:
        database.answers = [[5]]

        assert await repository.add_autoshoutout(5, "bob") is True

        statement = database.only
        assert "ON CONFLICT (twitch_user_id) DO NOTHING" in sql(statement)
        assert "RETURNING twitch_autoshoutout.twitch_user_id" in sql(statement)
        assert params(statement)["login"] == "bob"

    async def test_adding_says_false_when_the_conflict_skipped_it(
        self, database: Database
    ) -> None:
        """Postgres says whether the row was taken; a read first would say whether it was."""
        database.answers = [[]]

        assert await repository.add_autoshoutout(5, "bob") is False

    async def test_a_user_id_of_zero_that_was_inserted_still_counts_as_added(
        self, database: Database
    ) -> None:
        database.answers = [[0]]

        assert await repository.add_autoshoutout(0, "zero") is True

    async def test_adding_makes_one_statement_with_no_read_beforehand(
        self, database: Database
    ) -> None:
        await repository.add_autoshoutout(5, "bob")

        assert len(database.statements) == 1 and database.gets == []

    async def test_removing_says_true_when_a_row_went(self, database: Database) -> None:
        database.answers = [[5]]

        assert await repository.remove_autoshoutout(5) is True

        assert "RETURNING twitch_autoshoutout.twitch_user_id" in sql(database.only)

    async def test_removing_says_false_when_they_were_not_on_it(
        self, database: Database
    ) -> None:
        database.answers = [[]]

        assert await repository.remove_autoshoutout(5) is False

    async def test_removal_is_by_user_id_not_login(self, database: Database) -> None:
        await repository.remove_autoshoutout(5)

        assert params(database.only) == {"twitch_user_id_1": 5}

    async def test_a_database_failure_reaches_the_caller(
        self, database: Database
    ) -> None:
        database.failure = ConnectionError("down")

        with pytest.raises(ConnectionError):
            await repository.add_autoshoutout(5, "bob")
