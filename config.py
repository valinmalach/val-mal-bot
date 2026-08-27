"""Environment-backed settings, validated once at import."""

from pydantic import ValidationError
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Credentials and per-instance configuration.

    Everything describing what the bot *does* belongs in Postgres, not here.
    """

    discord_token: str
    test_discord_token: str | None = None

    twitch_client_id: str
    twitch_client_secret: str
    twitch_webhook_secret: str

    database_url: str
    app_url: str

    # Moving to app_setting; still read from the environment until the app
    # reads its configuration from the database.
    twitch_bot_user_id: str
    twitch_broadcaster_id: str

    db_echo: bool = False

    # extra="ignore" so the many variables Railway injects into the environment
    # do not have to be declared here.
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )


def _load() -> Settings:
    try:
        # pyright cannot see that BaseSettings fills these from the environment.
        return Settings()  # pyright: ignore[reportCallIssue]
    except ValidationError as e:
        problems = ", ".join(
            f"{str(err['loc'][0]).upper()} ({err['msg'].lower()})" for err in e.errors()
        )
        # from None: pydantic's own message quotes the raw input, which would put
        # token prefixes in the logs.
        raise RuntimeError(f"Bad environment configuration: {problems}") from None


settings = _load()
