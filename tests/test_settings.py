from pathlib import Path
from typing import ClassVar

import pytest
from pydantic import ValidationError

import config
from config import Settings

# Names of every variable Settings reads, so a test can start from a blank slate
# rather than from whatever the developer's shell happens to export.
REQUIRED = {
    "DISCORD_TOKEN": "discord-secret-value",
    "TWITCH_CLIENT_ID": "client-id-value",
    "TWITCH_CLIENT_SECRET": "client-secret-value",
    "TWITCH_WEBHOOK_SECRET": "webhook-secret-value",
    "DATABASE_URL": "postgresql://user:hunter2@host:5432/db",
    "APP_URL": "https://bot.example",
}
OPTIONAL = ["TEST_DISCORD_TOKEN", "PORT", "DB_ECHO", "USE_TEST_BOT"]


@pytest.fixture
def env(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> pytest.MonkeyPatch:
    """A clean environment in a directory with no .env, so nothing leaks in.

    Settings reads `.env` from the working directory; the developer's real one
    holds real credentials.
    """
    monkeypatch.chdir(tmp_path)
    for name in [*REQUIRED, *OPTIONAL]:
        monkeypatch.delenv(name, raising=False)
    for name, value in REQUIRED.items():
        monkeypatch.setenv(name, value)
    return monkeypatch


def test_loads_the_required_values_and_defaults_the_rest(
    env: pytest.MonkeyPatch,
) -> None:
    loaded = config._load()

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
        config._load()


@pytest.mark.parametrize("name", list(REQUIRED))
def test_a_blank_required_variable_fails_now_not_at_the_request_that_needed_it(
    name: str, env: pytest.MonkeyPatch
) -> None:
    """A variable that exists but was never filled in arrives as ''."""
    env.setenv(name, "")

    with pytest.raises(RuntimeError, match=name):
        config._load()


def test_every_bad_variable_is_named_in_one_message(env: pytest.MonkeyPatch) -> None:
    env.delenv("DISCORD_TOKEN")
    env.setenv("APP_URL", "")

    with pytest.raises(RuntimeError) as caught:
        config._load()

    assert "DISCORD_TOKEN" in str(caught.value)
    assert "APP_URL" in str(caught.value)


def test_the_error_never_carries_the_raw_values(env: pytest.MonkeyPatch) -> None:
    """pydantic's own message quotes the input, which would put credentials in the
    logs; that is what `from None` and the rebuilt message are for."""
    env.setenv("PORT", "not-a-port-hunter2")
    env.setenv("DISCORD_TOKEN", "")

    with pytest.raises(RuntimeError) as caught:
        config._load()

    message = str(caught.value)
    assert "hunter2" not in message
    assert "not-a-port" not in message
    assert "discord-secret-value" not in message
    assert caught.value.__cause__ is None
    assert caught.value.__suppress_context__ is True


def test_a_non_numeric_port_is_named(env: pytest.MonkeyPatch) -> None:
    env.setenv("PORT", "eight thousand")

    with pytest.raises(RuntimeError, match="PORT"):
        config._load()


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

    assert getattr(config._load(), attribute) == expected


@pytest.mark.parametrize(
    ("value", "expected"), [("1", True), ("true", True), ("0", False), ("false", False)]
)
def test_boolean_flags_read_the_usual_spellings(
    value: str, expected: bool, env: pytest.MonkeyPatch
) -> None:
    env.setenv("DB_ECHO", value)

    assert config._load().db_echo is expected


def test_a_set_port_is_an_integer(env: pytest.MonkeyPatch) -> None:
    env.setenv("PORT", "9123")

    assert config._load().port == 9123


def test_variables_that_are_not_declared_are_ignored(env: pytest.MonkeyPatch) -> None:
    """Railway injects a great many of its own."""
    env.setenv("RAILWAY_ENVIRONMENT", "production")
    env.setenv("SOMETHING_ELSE", "x")

    config._load()


def test_a_dotenv_file_holding_variables_that_are_not_declared_still_loads(
    env: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """extra applies to a .env file, not to the process environment, so the test
    above cannot tell extra="ignore" from extra="forbid"."""
    (tmp_path / ".env").write_text(
        "RAILWAY_ENVIRONMENT=production" + chr(10) + "SOMETHING_ELSE=x" + chr(10),
        encoding="utf-8",
    )

    assert config._load().app_url == "https://bot.example"


def test_variable_names_are_case_insensitive(env: pytest.MonkeyPatch) -> None:
    env.delenv("APP_URL")
    env.setenv("app_url", "https://lower.example")

    assert config._load().app_url == "https://lower.example"


def test_a_dotenv_file_supplies_what_the_environment_does_not(
    env: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    env.delenv("APP_URL")
    (tmp_path / ".env").write_text(
        "APP_URL=https://from-file.example\n", encoding="utf-8"
    )

    assert config._load().app_url == "https://from-file.example"


def test_the_environment_wins_over_the_dotenv_file(
    env: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    (tmp_path / ".env").write_text(
        "APP_URL=https://from-file.example\n", encoding="utf-8"
    )

    assert config._load().app_url == "https://bot.example"


class TestActiveDiscordToken:
    def test_the_real_token_by_default(self, env: pytest.MonkeyPatch) -> None:
        assert config._load().active_discord_token == "discord-secret-value"

    def test_the_test_token_when_the_test_bot_is_on(
        self, env: pytest.MonkeyPatch
    ) -> None:
        env.setenv("USE_TEST_BOT", "1")
        env.setenv("TEST_DISCORD_TOKEN", "test-bot-token")

        assert config._load().active_discord_token == "test-bot-token"

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
            _ = config._load().active_discord_token

    def test_the_test_token_is_ignored_while_the_test_bot_is_off(
        self, env: pytest.MonkeyPatch
    ) -> None:
        env.setenv("TEST_DISCORD_TOKEN", "test-bot-token")

        assert config._load().active_discord_token == "discord-secret-value"


class TestBlankIsUnset:
    """The before-validator, through the only door pydantic offers: validation."""

    REQUIRED_FIELDS: ClassVar[dict[str, str]] = {
        name.lower(): value for name, value in REQUIRED.items()
    }

    def test_a_non_mapping_passes_through_for_pydantic_to_reject(self) -> None:
        with pytest.raises(ValidationError):
            Settings.model_validate("not a dict")

    def test_a_blank_optional_field_takes_its_default(self) -> None:
        loaded = Settings.model_validate(
            {**self.REQUIRED_FIELDS, "port": "", "PORT": ""}
        )

        assert loaded.port == 8000

    def test_a_blank_required_field_is_still_rejected(self) -> None:
        with pytest.raises(ValidationError):
            Settings.model_validate({**self.REQUIRED_FIELDS, "discord_token": ""})

    def test_a_value_that_is_not_blank_is_kept(self) -> None:
        assert Settings.model_validate({**self.REQUIRED_FIELDS, "port": "9"}).port == 9
