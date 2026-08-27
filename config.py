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
    # Railway injects PORT; 8000 matches what the tunnel targets locally.
    port: int = 8000

    db_echo: bool = False

    # Local development: run against the test bot and a local database rather
    # than the production ones.
    use_test_bot: bool = False

    # extra="ignore" so the many variables Railway injects into the environment
    # do not have to be declared here.
    @property
    def active_discord_token(self) -> str:
        """The token to log in with, honouring use_test_bot."""
        if not self.use_test_bot:
            return self.discord_token
        if not self.test_discord_token:
            raise RuntimeError("USE_TEST_BOT is set but TEST_DISCORD_TOKEN is empty")
        return self.test_discord_token

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
