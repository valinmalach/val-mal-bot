import pytest

from valmal.core import urls

NL = chr(10)


class TestAppUrl:
    @pytest.mark.parametrize(
        "url",
        [
            "https://valin.up.railway.app",
            "https://bot.example/",
            "https://bot.example:443",
            "http://localhost:8000",
            "http://127.0.0.1",
        ],
    )
    def test_accepts_what_twitch_takes_as_a_callback_root(self, url: str) -> None:
        assert urls.is_app_url(url)

    @pytest.mark.parametrize(
        "url",
        [
            "https://{{valin.up.railway.app",
            "http://bot.example",
            "https://bot.example:8443",
            "https://bot.example/path",
            "https://bot.example?x",
            "https://bot.example#x",
            "https://user@bot.example",
            "https://bad-.example",
            "https://bot",
            "https://bot.example:",
            "https://bot.example.",
            "https://bot.example/%zz",
            "http://localhost:99999",
            "http://localhost:0",
            "http://localhost:port",
            "bot.example",
            f"https://bot.example{NL}",
        ],
    )
    def test_refuses_anything_else(self, url: str) -> None:
        assert not urls.is_app_url(url)

    def test_an_uppercase_scheme_is_refused_because_callbacks_compare_as_text(
        self,
    ) -> None:
        assert not urls.is_app_url("HTTPS://bot.example")


class TestDatabaseUrl:
    @pytest.mark.parametrize(
        "url",
        [
            "postgresql://postgres:pw@postgres.railway.internal:5432/railway",
            "postgresql+asyncpg://u:p@host/db",
            "postgres://localhost/db",
            "POSTGRESQL://u:p@host/db",
            "postgresql://u:p@[::1]:5432/db",
            "postgresql://u:p@host/db?sslmode=require",
            "postgresql://u:p%40ss@host/db",
        ],
    )
    def test_accepts_what_db_config_can_rewrite(self, url: str) -> None:
        assert urls.is_database_url(url)

    @pytest.mark.parametrize(
        "url",
        [
            "{{postgresql://u:p@host:5432/db",
            "postgresql://u:p@{{host/db",
            "postgresql://u:p@bad-/db",
            "postgresql://u:p@host:65536/db",
            "postgresql://u:p@host/",
            "postgresql://u:p@host/a/b",
            "postgresql://u:p@host/db/",
            "postgresql://u:p@host/db#x",
            "postgresql://u:p@host:/db",
            "mysql://u:p@host/db",
            "postgresql://u:p%@host/db",
            "postgresql://u:p%4@host/db",
            f"postgresql://u:p@host/db{NL}",
        ],
    )
    def test_refuses_anything_else(self, url: str) -> None:
        assert not urls.is_database_url(url)


class TestHttpsUrl:
    @pytest.mark.parametrize(
        "url",
        [
            "https://static-cdn.jtvnw.net/x/profile_image-300x300.png",
            "https://static-cdn.jtvnw.net/previews-ttv/live_user_x-400x225.jpg",
        ],
    )
    def test_accepts_a_twitch_cdn_image(self, url: str) -> None:
        assert urls.is_https_url(url)

    @pytest.mark.parametrize(
        "url",
        [
            "",
            "http://cdn.example/x.png",
            "https://cdn.example:99999/x.png",
            "https://cdn.example:port/x.png",
            "https://cdn.example/{width}x{height}.png",
            "https://bad-.example/x.png",
            "https://cdn/x.png",
            "javascript:x",
            "https://user:pw@cdn.example/x.png",
            "https://cdn.example/x%g1.png",
        ],
    )
    def test_refuses_what_discord_would_not_fetch(self, url: str) -> None:
        assert not urls.is_https_url(url)
