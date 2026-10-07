"""Live alerts, the autoshoutout list, OAuth tokens, and the module's surface."""

from datetime import UTC, datetime
from typing import Any

import pytest

from tests.db.support import Database
from valmal.db import repository
from valmal.db.enums import TokenType
from valmal.db.rows import LiveAlert, OAuthToken

pytestmark = pytest.mark.anyio

STAMP = datetime(2026, 1, 1, tzinfo=UTC)
STARTED = datetime(2026, 6, 15, 11, 0, tzinfo=UTC)


def alert_record(**fields: Any) -> dict[str, Any]:
    return {
        "broadcaster_id": 1,
        "channel_id": 2,
        "message_id": 3,
        "stream_id": 4,
        "stream_started_at": STARTED,
        "created_at": STAMP,
        "updated_at": STAMP,
    } | fields


class TestLiveAlerts:
    async def test_get_is_by_broadcaster_and_returns_a_row(
        self, database: Database
    ) -> None:
        database.answers = [alert_record()]

        alert = await repository.get_live_alert(1)

        assert database.only == ("fetchrow", repository.GET_LIVE_ALERT, (1,))
        assert alert == LiveAlert(**alert_record())

    async def test_get_of_none_stored_is_none(self, database: Database) -> None:
        assert await repository.get_live_alert(1) is None

    async def test_list_reads_every_row_without_a_filter(
        self, database: Database
    ) -> None:
        database.answers = [[alert_record(), alert_record(broadcaster_id=9)]]

        alerts = await repository.list_live_alerts()

        assert database.only == ("fetch", repository.LIST_LIVE_ALERTS, ())
        assert "WHERE" not in repository.LIST_LIVE_ALERTS
        assert [a.broadcaster_id for a in alerts] == [1, 9]

    async def test_none_stored_is_an_empty_list(self, database: Database) -> None:
        assert await repository.list_live_alerts() == []

    async def test_upsert_is_keyed_on_the_broadcaster(self, database: Database) -> None:
        await repository.upsert_live_alert(1, 2, 3, 4, STARTED)

        assert database.only == (
            "execute",
            repository.UPSERT_LIVE_ALERT,
            (1, 2, 3, 4, STARTED),
        )
        assert "ON CONFLICT (broadcaster_id)" in repository.UPSERT_LIVE_ALERT

    async def test_delete_by_broadcaster_takes_whatever_alert_is_there(
        self, database: Database
    ) -> None:
        await repository.delete_live_alert(1)

        assert database.only == ("execute", repository.DELETE_LIVE_ALERT, (1,))

    @pytest.mark.parametrize("message_id", [3, 0])
    async def test_a_message_id_scopes_the_delete_so_a_newer_alert_survives(
        self, message_id: int, database: Database
    ) -> None:
        """Zero included: the scope is "given", not "truthy"."""
        await repository.delete_live_alert(1, message_id=message_id)

        assert database.only == (
            "execute",
            repository.DELETE_LIVE_ALERT_FOR_MESSAGE,
            (1, message_id),
        )


class TestAutoshoutoutList:
    @pytest.mark.parametrize(("answer", "on_list"), [(1, True), (None, False)])
    async def test_is_on_the_list_when_a_row_comes_back(
        self, answer: int | None, on_list: bool, database: Database
    ) -> None:
        database.answers = [answer]

        assert await repository.is_autoshoutout(5) is on_list
        assert database.only == ("fetchval", repository.IS_AUTOSHOUTOUT, (5,))

    @pytest.mark.parametrize(("taken", "added"), [(5, True), (0, True), (None, False)])
    async def test_adding_says_whether_the_insert_took_the_row(
        self, taken: int | None, added: bool, database: Database
    ) -> None:
        """Zero included: the answer is is-not-None, not truthiness."""
        database.answers = [taken]

        assert await repository.add_autoshoutout(5, "someone") is added

    async def test_adding_is_one_statement_with_no_read_beforehand(
        self, database: Database
    ) -> None:
        """A read first would race a second mod adding the same channel."""
        await repository.add_autoshoutout(5, "someone")

        assert database.only == (
            "fetchval",
            repository.ADD_AUTOSHOUTOUT,
            (5, "someone"),
        )
        assert "DO NOTHING" in repository.ADD_AUTOSHOUTOUT
        assert "RETURNING" in repository.ADD_AUTOSHOUTOUT

    @pytest.mark.parametrize(("removed", "answer"), [(5, True), (None, False)])
    async def test_removing_says_whether_a_row_went(
        self, removed: int | None, answer: bool, database: Database
    ) -> None:
        database.answers = [removed]

        assert await repository.remove_autoshoutout(5) is answer
        assert database.only == ("fetchval", repository.REMOVE_AUTOSHOUTOUT, (5,))

    async def test_a_database_failure_reaches_the_caller(
        self, database: Database
    ) -> None:
        database.failure = ConnectionError("database is down")

        with pytest.raises(ConnectionError):
            await repository.add_autoshoutout(5, "someone")


class TestOAuthTokens:
    async def test_each_stored_token_comes_back_typed(self, database: Database) -> None:
        database.answers = [
            [
                {
                    "key": "broadcaster",
                    "access_token": "a",
                    "refresh_token": "r",
                    "expires_at": STARTED,
                    "scopes": ["chat:read", "chat:edit"],
                    "created_at": STAMP,
                    "updated_at": STAMP,
                }
            ]
        ]

        (token,) = await repository.list_oauth_tokens()

        assert token == OAuthToken(
            key=TokenType.Broadcaster,
            access_token="a",
            refresh_token="r",
            expires_at=STARTED,
            scopes=("chat:read", "chat:edit"),
            created_at=STAMP,
            updated_at=STAMP,
        )

    async def test_the_upsert_stores_the_keys_value_not_the_enum(
        self, database: Database
    ) -> None:
        await repository.upsert_oauth_token(TokenType.User, "a", None, None, ["x"])

        assert database.only == (
            "execute",
            repository.UPSERT_OAUTH_TOKEN,
            ("user", "a", None, None, ["x"]),
        )

    def test_a_conflict_overwrites_everything_but_the_key(self) -> None:
        _, _, updates = repository.UPSERT_OAUTH_TOKEN.partition("DO UPDATE SET")

        assert "ON CONFLICT (key)" in repository.UPSERT_OAUTH_TOKEN
        for column in ("access_token", "refresh_token", "expires_at", "scopes"):
            assert f"{column} = EXCLUDED.{column}" in updates
        assert "key =" not in updates


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
