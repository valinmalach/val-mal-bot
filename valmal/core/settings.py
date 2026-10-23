"""Environment-backed settings, validated once at import."""

import codecs
import os
import re
from collections.abc import Iterator, Mapping
from pathlib import Path
from typing import Annotated

import msgspec

from valmal.core import urls

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
    # Twitch's rule. Any other value fails at subscription create, which a migration
    # reaches only after its delete.
    twitch_webhook_secret: Annotated[str, msgspec.Meta(pattern=r"\A[ -~]{10,100}\Z")]

    # Both are checked by format in _problems, against valmal/core/urls.py: a
    # mangled DATABASE_URL would fail inside SQLAlchemy, and every EventSub callback
    # is built on APP_URL, so a bad one fails each recreate after its delete.
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

# python-dotenv's quoting: each quote style, and the escapes it decodes inside it.
_QUOTED = {
    '"': (re.compile(r'"((?:\\"|[^"])*)"'), re.compile(r"\\[\\'\"abfnrtv]")),
    "'": (re.compile(r"'((?:\\'|[^'])*)'"), re.compile(r"\\[\\']")),
}

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
        pairs[key.strip()] = _unquoted(value.strip())
    return pairs


def _unquoted(value: str) -> str:
    if value[:1] in _QUOTED:
        whole, escapes = _QUOTED[value[0]]
        if quoted := whole.match(value):
            return escapes.sub(
                lambda m: codecs.decode(m[0], "unicode_escape"), quoted[1]
            )
    return value.partition(" #")[0].rstrip()


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


_FORMATS = {"app_url": urls.is_app_url, "database_url": urls.is_database_url}


def _problems(supplied: Mapping[str, str]) -> Iterator[str]:
    """Every bad variable, where msgspec stops at the first."""
    for field in _FIELDS:
        if field.name not in supplied:
            if field.required:
                yield f"{field.name.upper()} (missing)"
            continue
        value = supplied[field.name]
        try:
            msgspec.convert(value, field.type, strict=False)
        except msgspec.ValidationError as e:
            yield f"{field.name.upper()} ({str(e).lower()})"
            continue
        if (check := _FORMATS.get(field.name)) and not check(value):
            yield f"{field.name.upper()} (not a URL this setting accepts)"


def _load() -> Settings:
    # Matched case-insensitively, and the real environment wins over the file.
    merged = {k.lower(): v for k, v in _dotenv(Path(".env")).items()} | {
        k.lower(): v for k, v in os.environ.items()
    }
    supplied = _supplied(merged)
    if problems := ", ".join(_problems(supplied)):
        # from None: the values are credentials, and nothing of them belongs in a log.
        raise RuntimeError(f"Bad environment configuration: {problems}") from None
    return msgspec.convert(supplied, Settings, strict=False)


settings = _load()
