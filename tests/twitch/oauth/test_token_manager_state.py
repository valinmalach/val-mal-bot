from datetime import UTC, datetime, timedelta

import pendulum
import pytest
from sqlalchemy.dialects import postgresql

from tests.twitch.support import TokenDb
from valmal.db.models import OAuthToken
from valmal.db.models.enums import TokenType
from valmal.twitch.models.auth import RefreshResponse
from valmal.twitch.oauth.token_manager import TwitchTokenManager

pytestmark = pytest.mark.anyio

NOW = pendulum.datetime(2026, 6, 15, 12)


def upsert_of(db: TokenDb) -> tuple[str, dict[str, object]]:
    """The SQL and bound values of the one non-select statement issued."""
    (statement,) = [s for s in db.statements if not hasattr(s, "column_descriptions")]
    compiled = statement.compile(dialect=postgresql.dialect())
    return str(compiled), dict(compiled.params)


class TestSingleton:
    def test_constructing_twice_gives_one_manager(
        self, manager: TwitchTokenManager
    ) -> None:
        assert TwitchTokenManager() is manager

    def test_every_identity_has_its_own_lock(self, manager: TwitchTokenManager) -> None:
        assert set(manager._locks) == set(TokenType)
        assert len({id(lock) for lock in manager._locks.values()}) == len(TokenType)

    def test_an_identity_with_no_token_reads_as_empty(
        self, manager: TwitchTokenManager
    ) -> None:
        assert all(manager.token(t) == "" for t in TokenType)


class TestLoad:
    async def test_reads_each_identity_into_memory(
        self, manager: TwitchTokenManager, token_db: TokenDb
    ) -> None:
        token_db.rows = [
            OAuthToken(
                key=TokenType.User,
                access_token="u-access",
                refresh_token="u-refresh",
                expires_at=datetime(2026, 7, 1, tzinfo=UTC),
            ),
            OAuthToken(key=TokenType.App, access_token="a-access"),
        ]

        await manager.load()

        assert manager.token(TokenType.User) == "u-access"
        assert manager.token(TokenType.App) == "a-access"
        assert manager.token(TokenType.Broadcaster) == ""
        assert manager._refresh == {TokenType.User: "u-refresh"}
        assert manager._expires_at == {TokenType.User: pendulum.datetime(2026, 7, 1)}

    async def test_a_row_without_a_refresh_token_or_expiry_records_neither(
        self, manager: TwitchTokenManager, token_db: TokenDb
    ) -> None:
        token_db.rows = [
            OAuthToken(
                key=TokenType.App,
                access_token="a",
                refresh_token="",
                expires_at=None,
            )
        ]

        await manager.load()

        assert manager._refresh == {}
        assert manager._expires_at == {}

    async def test_loading_replaces_rather_than_merges(
        self, manager: TwitchTokenManager, token_db: TokenDb
    ) -> None:
        """A row deleted or an expiry cleared in the database must disappear from
        memory too."""
        token_db.rows = [
            OAuthToken(
                key=TokenType.User,
                access_token="old",
                refresh_token="r",
                expires_at=datetime(2026, 7, 1, tzinfo=UTC),
            )
        ]
        await manager.load()

        token_db.rows = [OAuthToken(key=TokenType.Broadcaster, access_token="new")]
        await manager.load()

        assert manager.token(TokenType.User) == ""
        assert manager.token(TokenType.Broadcaster) == "new"
        assert manager._refresh == {}
        assert manager._expires_at == {}

    async def test_a_failed_read_leaves_memory_as_it_was(
        self, manager: TwitchTokenManager, token_db: TokenDb
    ) -> None:
        token_db.rows = [OAuthToken(key=TokenType.App, access_token="kept")]
        await manager.load()
        token_db.error = ConnectionError("database is down")

        with pytest.raises(ConnectionError):
            await manager.load()

        assert manager.token(TokenType.App) == "kept"

    async def test_a_naive_expiry_is_read_as_utc(
        self, manager: TwitchTokenManager, token_db: TokenDb
    ) -> None:
        token_db.rows = [
            OAuthToken(
                key=TokenType.App,
                access_token="a",
                expires_at=datetime(2026, 7, 1, 12, 0),
            )
        ]

        await manager.load()

        assert manager._expires_at[TokenType.App].timezone_name == "UTC"


class TestNeedsRefresh:
    def expires(self, manager: TwitchTokenManager, when: pendulum.DateTime) -> None:
        manager._expires_at[TokenType.App] = when

    def test_an_unknown_expiry_never_asks_for_a_refresh_it_reacts_to_a_401_instead(
        self, manager: TwitchTokenManager, now: pendulum.DateTime
    ) -> None:
        assert manager.needs_refresh(TokenType.App) is False

    def test_a_token_with_plenty_of_time_left_is_fine(
        self, manager: TwitchTokenManager, now: pendulum.DateTime
    ) -> None:
        self.expires(manager, NOW.add(hours=1))

        assert manager.needs_refresh(TokenType.App) is False

    def test_a_token_already_expired_is_due(
        self, manager: TwitchTokenManager, now: pendulum.DateTime
    ) -> None:
        self.expires(manager, NOW.subtract(seconds=1))

        assert manager.needs_refresh(TokenType.App) is True

    def test_it_is_due_a_minute_early_so_a_request_in_flight_cannot_cross_the_edge(
        self, manager: TwitchTokenManager, now: pendulum.DateTime
    ) -> None:
        self.expires(manager, NOW.add(seconds=60))
        assert manager.needs_refresh(TokenType.App) is True

        self.expires(manager, NOW.add(seconds=61))
        assert manager.needs_refresh(TokenType.App) is False

    def test_each_identity_is_judged_on_its_own_expiry(
        self, manager: TwitchTokenManager, now: pendulum.DateTime
    ) -> None:
        manager._expires_at[TokenType.User] = NOW.subtract(minutes=5)
        manager._expires_at[TokenType.Broadcaster] = NOW.add(hours=2)

        assert manager.needs_refresh(TokenType.User) is True
        assert manager.needs_refresh(TokenType.Broadcaster) is False
        assert manager.needs_refresh(TokenType.App) is False


class TestStore:
    async def test_keeps_the_token_in_memory_and_stamps_an_expiry(
        self, manager: TwitchTokenManager, token_db: TokenDb, now: pendulum.DateTime
    ) -> None:
        await manager._store(TokenType.User, "access", "refresh", 3600, ["a", "b"])

        assert manager.token(TokenType.User) == "access"
        assert manager._refresh[TokenType.User] == "refresh"
        assert manager._expires_at[TokenType.User] == NOW.add(seconds=3600)

    async def test_writes_one_upsert_that_updates_everything_but_the_key(
        self, manager: TwitchTokenManager, token_db: TokenDb, now: pendulum.DateTime
    ) -> None:
        await manager._store(TokenType.User, "access", "refresh", 3600, ["a"])

        sql, params = upsert_of(token_db)
        assert "ON CONFLICT (key) DO UPDATE" in sql
        for column in ("access_token", "refresh_token", "expires_at", "scopes"):
            assert f"{column} = excluded.{column}" in sql
        assert "key = excluded.key" not in sql
        assert params["access_token"] == "access"
        assert params["scopes"] == ["a"]

    async def test_the_on_update_hook_does_not_fire_for_on_conflict_so_the_timestamp_is_set(
        self, manager: TwitchTokenManager, token_db: TokenDb, now: pendulum.DateTime
    ) -> None:
        await manager._store(TokenType.User, "a", "r", 1)

        sql, _ = upsert_of(token_db)
        assert "updated_at = now()" in sql

    async def test_no_scopes_are_stored_as_an_empty_list(
        self, manager: TwitchTokenManager, token_db: TokenDb, now: pendulum.DateTime
    ) -> None:
        await manager._store(TokenType.User, "a", "r", 1)

        assert upsert_of(token_db)[1]["scopes"] == []

    async def test_a_refresh_that_returns_no_new_refresh_token_keeps_the_old_one(
        self, manager: TwitchTokenManager, token_db: TokenDb, now: pendulum.DateTime
    ) -> None:
        manager._refresh[TokenType.App] = "old-refresh"

        await manager._store(TokenType.App, "new-access", None, 60)

        assert manager._refresh[TokenType.App] == "old-refresh"
        assert upsert_of(token_db)[1]["refresh_token"] == "old-refresh"

    async def test_an_identity_with_no_refresh_token_at_all_stores_none(
        self, manager: TwitchTokenManager, token_db: TokenDb, now: pendulum.DateTime
    ) -> None:
        await manager._store(TokenType.App, "access", None, 60)

        assert upsert_of(token_db)[1]["refresh_token"] is None
        assert TokenType.App not in manager._refresh

    async def test_no_expiry_clears_a_known_one_in_memory_and_stores_null(
        self, manager: TwitchTokenManager, token_db: TokenDb, now: pendulum.DateTime
    ) -> None:
        manager._expires_at[TokenType.User] = NOW.add(hours=1)

        await manager._store(TokenType.User, "a", "r", None)

        assert TokenType.User not in manager._expires_at
        assert upsert_of(token_db)[1]["expires_at"] is None
        assert manager.needs_refresh(TokenType.User) is False

    async def test_the_stored_expiry_is_now_plus_the_lifetime(
        self, manager: TwitchTokenManager, token_db: TokenDb, now: pendulum.DateTime
    ) -> None:
        await manager._store(TokenType.App, "a", None, 14400)

        assert upsert_of(token_db)[1]["expires_at"] == NOW + timedelta(seconds=14400)

    async def test_memory_is_updated_even_when_the_database_write_fails(
        self, manager: TwitchTokenManager, token_db: TokenDb, now: pendulum.DateTime
    ) -> None:
        """Documented by the order in _store: the new token is usable at once, and the
        failure surfaces to the caller, who reports it."""
        token_db.error = ConnectionError("database is down")

        with pytest.raises(ConnectionError):
            await manager._store(TokenType.App, "usable", None, 60)

        assert manager.token(TokenType.App) == "usable"


class TestSetTokens:
    def refresh_response(self, scope: list[str] | str) -> RefreshResponse:
        return RefreshResponse(
            access_token="a",
            expires_in=10,
            refresh_token="r",
            scope=scope,
            token_type="bearer",
        )

    @pytest.mark.parametrize(
        ("setter", "identity"),
        [
            ("set_user_access_token", TokenType.User),
            ("set_broadcaster_access_token", TokenType.Broadcaster),
        ],
    )
    async def test_each_setter_stores_under_its_own_identity(
        self,
        setter: str,
        identity: TokenType,
        manager: TwitchTokenManager,
        token_db: TokenDb,
        now: pendulum.DateTime,
    ) -> None:
        await getattr(manager, setter)(self.refresh_response(["x", "y"]))

        assert manager.token(identity) == "a"
        assert {t for t in TokenType if manager.token(t)} == {identity}
        assert upsert_of(token_db)[1]["scopes"] == ["x", "y"]

    async def test_a_single_scope_string_is_stored_as_a_list_of_one(
        self, manager: TwitchTokenManager, token_db: TokenDb, now: pendulum.DateTime
    ) -> None:
        await manager.set_user_access_token(self.refresh_response("chat:read"))

        assert upsert_of(token_db)[1]["scopes"] == ["chat:read"]
