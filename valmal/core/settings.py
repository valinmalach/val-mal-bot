"""Environment-backed settings, validated once at import."""

import os
from collections.abc import Iterator, Mapping
from pathlib import Path
from typing import Annotated

import msgspec

# A variable that exists but was never filled in arrives as "", which passes a
# plain str and only fails later, at the login or the request that needed it.
NonEmpty = Annotated[str, msgspec.Meta(min_length=1)]


class Settings(msgspec.Struct, kw_only=True):
    """Credentials and per-instance configuration.

    Everything describing what the bot *does* belongs in Postgres, not here.
    """

    discord_token: NonEmpty
    test_discord_token: str | None = None

    twitch_client_id: NonEmpty
    twitch_client_secret: NonEmpty
    twitch_webhook_secret: NonEmpty

    database_url: NonEmpty
    app_url: NonEmpty
    # Railway injects PORT; 8000 is just a sane local default.
    port: int = 8000

    db_echo: bool = False

    # Local development: run against the test bot and a local database rather
    # than the production ones.
    use_test_bot: bool = False

    @property
    def active_discord_token(self) -> str:
        """The token to log in with, honouring use_test_bot."""
        if not self.use_test_bot:
            return self.discord_token
        if not self.test_discord_token:
            raise RuntimeError("USE_TEST_BOT is set but TEST_DISCORD_TOKEN is empty")
        return self.test_discord_token


def _dotenv(path: Path) -> dict[str, str]:
    """`KEY=VALUE` lines from a local `.env`, if there is one; Railway has none."""
    if not path.is_file():
        return {}
    pairs: dict[str, str] = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        key, sep, value = line.partition("=")
        if sep and not key.lstrip().startswith("#"):
            pairs[key.strip()] = value.strip()
    return pairs


def _blank_is_unset(values: Mapping[str, str]) -> dict[str, str]:
    """Keep the declared names, minus an empty optional one, so it takes its default.

    Railway and a copied .env.example both supply "", which int and bool reject
    outright. The many variables Railway injects that are not declared are dropped.
    """
    optional = {f.name for f in msgspec.structs.fields(Settings) if not f.required}
    declared = {f.name for f in msgspec.structs.fields(Settings)}
    return {
        name: value
        for name, value in values.items()
        if name in declared and (value != "" or name not in optional)
    }


def _problems(supplied: Mapping[str, str]) -> Iterator[str]:
    """Every bad variable, where msgspec stops at the first."""
    for field in msgspec.structs.fields(Settings):
        if field.name not in supplied:
            if field.required:
                yield f"{field.name.upper()} (missing)"
            continue
        try:
            msgspec.convert(supplied[field.name], field.type, strict=False)
        except msgspec.ValidationError as e:
            yield f"{field.name.upper()} ({str(e).lower()})"


def _load() -> Settings:
    # Matched case-insensitively, and the real environment wins over the file.
    merged = {k.lower(): v for k, v in _dotenv(Path(".env")).items()} | {
        k.lower(): v for k, v in os.environ.items()
    }
    supplied = _blank_is_unset(merged)
    try:
        return msgspec.convert(supplied, Settings, strict=False)
    except msgspec.ValidationError:
        problems = ", ".join(_problems(supplied))
        # from None: the values are credentials, and nothing of them belongs in a log.
        raise RuntimeError(f"Bad environment configuration: {problems}") from None


settings = _load()
