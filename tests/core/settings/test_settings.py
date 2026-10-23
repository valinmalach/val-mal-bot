from pathlib import Path

import pytest

from tests.core.settings.support import REQUIRED
from valmal.core import settings


def test_loads_the_required_values_and_defaults_the_rest(
    env: pytest.MonkeyPatch,
) -> None:
    loaded = settings._load()

    assert loaded.discord_token == "discord-secret-value"
    assert loaded.database_url == "postgresql://user:hunter2@host:5432/db"
    assert loaded.app_url == "https://bot.example"
    assert loaded.port == 8000
    assert loaded.db_echo is False
    assert loaded.use_test_bot is False
    assert loaded.test_discord_token is None


@pytest.mark.parametrize("name", list(REQUIRED))
def test_a_missing_required_variable_is_named(
    name: str, env: pytest.MonkeyPatch
) -> None:
    env.delenv(name)

    with pytest.raises(RuntimeError, match=f"Bad environment configuration: {name}"):
        settings._load()


@pytest.mark.parametrize("name", list(REQUIRED))
def test_a_blank_required_variable_fails_now_not_at_the_request_that_needed_it(
    name: str, env: pytest.MonkeyPatch
) -> None:
    """A variable that exists but was never filled in arrives as ''."""
    env.setenv(name, "")

    with pytest.raises(RuntimeError, match=name):
        settings._load()


def test_every_bad_variable_is_named_in_one_message(env: pytest.MonkeyPatch) -> None:
    env.delenv("DISCORD_TOKEN")
    env.setenv("APP_URL", "")

    with pytest.raises(RuntimeError) as caught:
        settings._load()

    assert "DISCORD_TOKEN" in str(caught.value)
    assert "APP_URL" in str(caught.value)


def test_the_error_never_carries_the_raw_values(env: pytest.MonkeyPatch) -> None:
    """The values are credentials; that is what `from None` and the rebuilt
    message are for."""
    env.setenv("PORT", "not-a-port-hunter2")
    env.setenv("DISCORD_TOKEN", "")

    with pytest.raises(RuntimeError) as caught:
        settings._load()

    message = str(caught.value)
    assert "hunter2" not in message
    assert "not-a-port" not in message
    assert "discord-secret-value" not in message
    assert caught.value.__cause__ is None
    assert caught.value.__suppress_context__ is True


def test_a_non_numeric_port_is_named(env: pytest.MonkeyPatch) -> None:
    env.setenv("PORT", "eight thousand")

    with pytest.raises(RuntimeError, match="PORT"):
        settings._load()


@pytest.mark.parametrize(
    ("name", "attribute", "expected"),
    [
        ("PORT", "port", 8000),
        ("DB_ECHO", "db_echo", False),
        ("USE_TEST_BOT", "use_test_bot", False),
    ],
)
def test_a_blank_optional_variable_falls_back_to_its_default(
    name: str, attribute: str, expected: object, env: pytest.MonkeyPatch
) -> None:
    """Railway and a copied .env.example both supply '', which int and bool
    would otherwise reject outright."""
    env.setenv(name, "")

    assert getattr(settings._load(), attribute) == expected


@pytest.mark.parametrize(
    ("value", "expected"), [("1", True), ("true", True), ("0", False), ("false", False)]
)
def test_boolean_flags_read_the_usual_spellings(
    value: str, expected: bool, env: pytest.MonkeyPatch
) -> None:
    env.setenv("DB_ECHO", value)

    assert settings._load().db_echo is expected


def test_a_set_port_is_an_integer(env: pytest.MonkeyPatch) -> None:
    env.setenv("PORT", "9123")

    assert settings._load().port == 9123


def test_variables_that_are_not_declared_are_ignored(env: pytest.MonkeyPatch) -> None:
    """Railway injects a great many of its own."""
    env.setenv("RAILWAY_ENVIRONMENT", "production")
    env.setenv("SOMETHING_ELSE", "x")

    settings._load()


def test_a_dotenv_file_holding_variables_that_are_not_declared_still_loads(
    env: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """The file is read separately from the environment, so the test above does
    not cover it."""
    (tmp_path / ".env").write_text(
        f"RAILWAY_ENVIRONMENT=production{chr(10)}SOMETHING_ELSE=x{chr(10)}",
        encoding="utf-8",
    )

    assert settings._load().app_url == "https://bot.example"


def test_variable_names_are_case_insensitive(env: pytest.MonkeyPatch) -> None:
    env.delenv("APP_URL")
    env.setenv("app_url", "https://lower.example")

    assert settings._load().app_url == "https://lower.example"


def test_a_dotenv_file_supplies_what_the_environment_does_not(
    env: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    env.delenv("APP_URL")
    (tmp_path / ".env").write_text(
        "APP_URL=https://from-file.example\n", encoding="utf-8"
    )

    assert settings._load().app_url == "https://from-file.example"


def test_the_environment_wins_over_the_dotenv_file(
    env: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    (tmp_path / ".env").write_text(
        "APP_URL=https://from-file.example\n", encoding="utf-8"
    )

    assert settings._load().app_url == "https://bot.example"


class TestActiveDiscordToken:
    def test_the_real_token_by_default(self, env: pytest.MonkeyPatch) -> None:
        assert settings._load().active_discord_token == "discord-secret-value"

    def test_the_test_token_when_the_test_bot_is_on(
        self, env: pytest.MonkeyPatch
    ) -> None:
        env.setenv("USE_TEST_BOT", "1")
        env.setenv("TEST_DISCORD_TOKEN", "test-bot-token")

        assert settings._load().active_discord_token == "test-bot-token"

    @pytest.mark.parametrize("test_token", [None, ""])
    def test_the_test_bot_without_a_token_refuses_to_fall_back_to_the_real_one(
        self, test_token: str | None, env: pytest.MonkeyPatch
    ) -> None:
        """Quietly logging in as the production bot from a dev machine is worse
        than failing."""
        env.setenv("USE_TEST_BOT", "1")
        if test_token is not None:
            env.setenv("TEST_DISCORD_TOKEN", test_token)

        with pytest.raises(RuntimeError, match="TEST_DISCORD_TOKEN is empty"):
            _ = settings._load().active_discord_token

    def test_the_test_token_is_ignored_while_the_test_bot_is_off(
        self, env: pytest.MonkeyPatch
    ) -> None:
        env.setenv("TEST_DISCORD_TOKEN", "test-bot-token")

        assert settings._load().active_discord_token == "discord-secret-value"


def test_the_dotenv_file_skips_comments_and_lines_without_a_value(
    env: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    lines = [
        "# TEST_DISCORD_TOKEN=commented",
        "",
        "not a pair",
        "TEST_DISCORD_TOKEN = x=y ",
    ]
    (tmp_path / ".env").write_text(chr(10).join(lines), encoding="utf-8")

    assert settings._load().test_discord_token == "x=y"


@pytest.mark.parametrize(
    ("line", "expected"),
    [
        ('APP_URL="https://quoted.example"', "https://quoted.example"),
        ("APP_URL='https://single.example' # note", "https://single.example"),
        ("export APP_URL=https://exported.example", "https://exported.example"),
        ("APP_URL=https://commented.example # local", "https://commented.example"),
    ],
)
def test_the_dotenv_file_is_read_as_python_dotenv_read_it(
    line: str, expected: str, env: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """Docker Compose reads the same file, and strips the quotes too."""
    env.delenv("APP_URL")
    (tmp_path / ".env").write_text(line, encoding="utf-8")

    assert settings._load().app_url == expected


def test_a_hash_inside_an_unquoted_value_is_not_a_comment(
    env: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    (tmp_path / ".env").write_text("TEST_DISCORD_TOKEN=to#ken # note", encoding="utf-8")

    assert settings._load().test_discord_token == "to#ken"


def test_a_backslash_escapes_a_quote_inside_double_quotes(
    env: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    escaped = f'{chr(92)}"'
    (tmp_path / ".env").write_text(
        f'TEST_DISCORD_TOKEN="a{escaped}b" # c', encoding="utf-8"
    )

    assert settings._load().test_discord_token == 'a"b'


B = chr(92)


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        (f'"C:{B}path"', f"C:{B}path"),
        (f'"a{B}nb"', f"a{chr(10)}b"),
        (f'"a{B}{B}b"', f"a{B}b"),
        (f"'it{B}'s'", "it's"),
        (f"'a{B}nb'", f"a{B}nb"),
        (f"'a{B}{B}b'", f"a{B}b"),
    ],
)
def test_quoted_values_decode_the_escapes_python_dotenv_decoded(
    raw: str, expected: str
) -> None:
    """Only those: a backslash before anything else is part of the value."""
    assert settings._unquoted(raw) == expected


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        ("TRUE", True),
        ("yes", True),
        ("On", True),
        ("y", True),
        ("t", True),
        ("NO", False),
        ("off", False),
    ],
)
def test_booleans_take_the_words_pydantic_settings_took(
    value: str, expected: bool, env: pytest.MonkeyPatch
) -> None:
    env.setenv("USE_TEST_BOT", value)
    env.setenv("TEST_DISCORD_TOKEN", "test-bot-token")

    assert settings._load().use_test_bot is expected


def test_a_word_that_is_not_a_boolean_is_still_refused(env: pytest.MonkeyPatch) -> None:
    env.setenv("DB_ECHO", "maybe")

    with pytest.raises(RuntimeError, match="DB_ECHO"):
        settings._load()


class TestSupplied:
    def test_a_blank_optional_field_is_dropped(self) -> None:
        assert settings._supplied({"port": "", "db_echo": "1"}) == {"db_echo": "1"}

    def test_a_blank_required_field_is_kept_to_be_rejected(self) -> None:
        assert settings._supplied({"discord_token": ""}) == {"discord_token": ""}

    def test_an_undeclared_name_is_dropped(self) -> None:
        assert settings._supplied({"railway_environment": "production"}) == {}
