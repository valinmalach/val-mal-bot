"""Values Twitch or asyncpg would refuse fail at import, before any delete.

Each is checked by its format alone, so a value mangled in the Railway editor stops
the bot at startup rather than reaching Twitch.
"""

import pytest

from valmal.core import settings


@pytest.mark.parametrize(
    "url",
    [
        "https://{{valin.up.railway.app",
        "http://bot.example",
        "https://bot.example:8443",
        "http://localhost:99999",
        "http://localhost:0",
        "https://bot.example/path",
        "bot.example",
        f"https://bot.example{chr(10)}",
    ],
)
def test_an_app_url_twitch_would_refuse_fails_now(
    url: str, env: pytest.MonkeyPatch
) -> None:
    """Every callback is built on it; a bad one fails each recreate after its delete."""
    env.setenv("APP_URL", url)

    with pytest.raises(RuntimeError, match="APP_URL"):
        settings._load()


@pytest.mark.parametrize(
    "url",
    [
        "https://valin.up.railway.app",
        "https://bot.example/",
        "https://bot.example:443",
        "http://localhost:8000",
        "http://localhost:65535",
        "http://127.0.0.1",
    ],
)
def test_an_app_url_twitch_accepts_or_a_local_one_loads(
    url: str, env: pytest.MonkeyPatch
) -> None:
    env.setenv("APP_URL", url)

    assert settings._load().app_url == url


@pytest.mark.parametrize(
    "url",
    [
        "{{postgresql://u:p@host:5432/db",
        "postgresql://u:p@host:5432/",
        "mysql://u:p@host/db",
        "postgresql://u:p@host:65536/db",
        "postgresql://u:p@host:0/db",
        f"postgresql://u:p@host/db{chr(10)}",
    ],
)
def test_a_database_url_that_cannot_be_parsed_fails_now(
    url: str, env: pytest.MonkeyPatch
) -> None:
    env.setenv("DATABASE_URL", url)

    with pytest.raises(RuntimeError, match="DATABASE_URL") as caught:
        settings._load()

    assert url.strip() not in str(caught.value)
    assert "u:p" not in str(caught.value)


@pytest.mark.parametrize(
    "url",
    [
        "postgresql://postgres:pw@postgres.railway.internal:5432/railway",
        "postgres://localhost/db",
        "postgresql://u:p@host/db?sslmode=require",
        "postgresql+asyncpg://u:p@host/db",
        "postgresql://u:p@[::1]:5432/db",
    ],
)
def test_a_database_url_asyncpg_can_use_loads(
    url: str, env: pytest.MonkeyPatch
) -> None:
    env.setenv("DATABASE_URL", url)

    assert settings._load().database_url == url


@pytest.mark.parametrize(
    "secret",
    ["x" * 9, "x" * 101, "x" * 9 + chr(233), "x" * 9 + chr(10), "x" * 10 + chr(10)],
)
def test_a_webhook_secret_twitch_would_refuse_fails_now(
    secret: str, env: pytest.MonkeyPatch
) -> None:
    """Twitch takes 10 to 100 ASCII characters, and finds out only at a create."""
    env.setenv("TWITCH_WEBHOOK_SECRET", secret)

    with pytest.raises(RuntimeError, match="TWITCH_WEBHOOK_SECRET") as caught:
        settings._load()

    assert secret not in str(caught.value)


@pytest.mark.parametrize("secret", ["x" * 10, "x" * 100, "a-Z_9 ~!" * 8])
def test_a_webhook_secret_twitch_accepts_loads(
    secret: str, env: pytest.MonkeyPatch
) -> None:
    env.setenv("TWITCH_WEBHOOK_SECRET", secret)

    assert settings._load().twitch_webhook_secret == secret
