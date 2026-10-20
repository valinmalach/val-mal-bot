"""Environment-backed settings, validated once at import."""

import os
import re
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

    use_test_bot: bool = False

    @property
    def active_discord_token(self) -> str:
        """The token to log in with, honouring use_test_bot."""
        if not self.use_test_bot:
            return self.discord_token
        if not self.test_discord_token:
            raise RuntimeError("USE_TEST_BOT is set but TEST_DISCORD_TOKEN is empty")
        return self.test_discord_token


_FIELDS = msgspec.structs.fields(Settings)
_DECLARED = {f.name for f in _FIELDS}
_OPTIONAL = {f.name for f in _FIELDS if not f.required}
_BOOLS = {f.name for f in _FIELDS if f.type is bool}

# A backslash escapes the next character inside double quotes, as in python-dotenv.
_DOUBLE_QUOTED = re.compile(r'"((?:[^"\\]|\\.)*)"')

# msgspec reads only true/false/1/0, and a .env or Railway value may be a word.
_BOOL_WORDS: dict[str, str] = {
    **dict.fromkeys(("yes", "y", "on", "t"), "true"),
    **dict.fromkeys(("no", "n", "off", "f"), "false"),
}


def _dotenv(path: Path) -> dict[str, str]:
    """`KEY=VALUE` lines from a local `.env`, if there is one; Railway has none.

    Read as python-dotenv and Docker Compose read it, as far as this file goes: an
    `export ` prefix, a quoted value, and a ` #` comment after an unquoted one.
    """
    if not path.is_file():
        return {}
    pairs: dict[str, str] = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        key, sep, value = line.strip().removeprefix("export ").partition("=")
        if not sep or key.startswith("#"):
            continue
        value = value.strip()
        if quoted := _DOUBLE_QUOTED.match(value):
            value = re.sub(r"\\(.)", r"\1", quoted[1])
        elif value[:1] == "'" and (end := value.find("'", 1)) > 0:
            value = value[1:end]
        else:
            value = value.partition(" #")[0].rstrip()
        pairs[key.strip()] = value
    return pairs


def _supplied(values: Mapping[str, str]) -> dict[str, str]:
    """The declared names, minus an empty optional one, so it takes its default.

    Railway and a copied .env.example both supply "", which int and bool reject
    outright. The many variables Railway injects that are not declared are dropped.
    """
    return {
        name: _BOOL_WORDS.get(value.lower(), value) if name in _BOOLS else value
        for name, value in values.items()
        if name in _DECLARED and (value != "" or name not in _OPTIONAL)
    }


def _problems(supplied: Mapping[str, str]) -> Iterator[str]:
    """Every bad variable, where msgspec stops at the first."""
    for field in _FIELDS:
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
    supplied = _supplied(merged)
    try:
        return msgspec.convert(supplied, Settings, strict=False)
    except msgspec.ValidationError:
        problems = ", ".join(_problems(supplied))
        # from None: the values are credentials, and nothing of them belongs in a log.
        raise RuntimeError(f"Bad environment configuration: {problems}") from None


settings = _load()
