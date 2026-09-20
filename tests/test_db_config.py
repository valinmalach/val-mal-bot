from collections.abc import Callable

import pytest

from valmal.core.settings import settings
from valmal.db import config as db_config
from valmal.db.config import _translate_query, get_database_url


@pytest.fixture
def url(monkeypatch: pytest.MonkeyPatch) -> Callable[[str], None]:
    def use(value: str) -> None:
        monkeypatch.setattr(settings, "database_url", value)

    return use


class TestDatabaseUrl:
    @pytest.mark.parametrize(
        "scheme",
        [
            "postgresql",
            "postgres",
            "postgresql+asyncpg",
            "postgresql+psycopg2",
            "postgresql+psycopg",
        ],
    )
    def test_every_postgres_scheme_is_pointed_at_asyncpg(
        self, scheme: str, url: Callable[[str], None]
    ) -> None:
        url(f"{scheme}://user:pw@host:5432/db")

        assert get_database_url() == "postgresql+asyncpg://user:pw@host:5432/db"

    def test_credentials_host_port_and_database_survive(
        self, url: Callable[[str], None]
    ) -> None:
        url("postgresql://us%40er:p%2Fw@db.internal:6543/my_db")

        assert get_database_url() == (
            "postgresql+asyncpg://us%40er:p%2Fw@db.internal:6543/my_db"
        )

    def test_a_url_with_no_query_gains_none(self, url: Callable[[str], None]) -> None:
        url("postgresql://u:p@h/d")

        assert "?" not in get_database_url()

    def test_a_scheme_that_is_not_postgres_is_returned_untouched(
        self, url: Callable[[str], None]
    ) -> None:
        """Nothing here knows what to do with it, and guessing would hide the mistake."""
        url("mysql://u:p@h/d?sslmode=require")

        assert get_database_url() == "mysql://u:p@h/d?sslmode=require"

    def test_railways_sslmode_becomes_asyncpgs_ssl(
        self, url: Callable[[str], None]
    ) -> None:
        url("postgresql://u:p@h/d?sslmode=require")

        assert get_database_url() == "postgresql+asyncpg://u:p@h/d?ssl=require"

    def test_a_fragment_is_kept(self, url: Callable[[str], None]) -> None:
        url("postgresql://u:p@h/d#frag")

        assert get_database_url().endswith("#frag")

    def test_the_driver_constant_is_asyncpg(self) -> None:
        assert db_config.ASYNC_DRIVER == "postgresql+asyncpg"


class TestTranslateQuery:
    def test_an_empty_query_is_left_empty(self) -> None:
        assert _translate_query("") == ""

    @pytest.mark.parametrize(
        ("mode", "ssl"),
        [
            ("disable", "disable"),
            ("allow", "prefer"),
            ("prefer", "prefer"),
            ("require", "require"),
            ("verify-ca", "verify-ca"),
            ("verify-full", "verify-full"),
        ],
    )
    def test_each_libpq_sslmode_has_an_asyncpg_ssl_equivalent(
        self, mode: str, ssl: str
    ) -> None:
        assert _translate_query(f"sslmode={mode}") == f"ssl={ssl}"

    def test_an_unknown_sslmode_is_passed_through_for_asyncpg_to_judge(self) -> None:
        assert _translate_query("sslmode=weird") == "ssl=weird"

    @pytest.mark.parametrize(
        "option",
        [
            "channel_binding=require",
            "target_session_attrs=read-write",
            "connect_timeout=10",
            "options=-c%20search_path%3Dx",
        ],
    )
    def test_options_asyncpg_rejects_are_dropped(self, option: str) -> None:
        """asyncpg.connect() raises TypeError on these, and SQLAlchemy passes them
        through uncoerced."""
        assert _translate_query(f"application_name=bot&{option}") == (
            "application_name=bot"
        )

    def test_an_explicit_ssl_wins_over_a_translated_sslmode(self) -> None:
        assert _translate_query("sslmode=require&ssl=verify-full") == "ssl=verify-full"

    def test_unrelated_parameters_are_kept_in_order(self) -> None:
        assert _translate_query("b=2&a=1&c=3") == "b=2&a=1&c=3"

    def test_a_blank_value_is_kept(self) -> None:
        assert _translate_query("application_name=") == "application_name="

    def test_the_first_sslmode_wins_when_repeated(self) -> None:
        assert _translate_query("sslmode=require&sslmode=disable") == "ssl=require"

    def test_a_blank_sslmode_adds_no_ssl(self) -> None:
        assert _translate_query("sslmode=&a=1") == "a=1"

    def test_values_are_reencoded(self) -> None:
        assert (
            _translate_query("application_name=my%20bot") == "application_name=my+bot"
        )
