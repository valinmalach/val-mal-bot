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
            "postgresql://u:p%20%7E@host/db",
            "postgresql://u:p@db.example./db",
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
            "postgresql://u:p@host/%00",
            "postgresql://u%0A:p@host/db",
            "postgresql://u:p@host/db%7f",
            "postgresql://u:p@host:/db",
            "mysql://u:p@host/db",
            "postgresql://u:p%@host/db",
            "postgresql://u:p%4@host/db",
            f"postgresql://u:p@host/db{NL}",
        ],
    )
    def test_refuses_anything_else(self, url: str) -> None:
        assert not urls.is_database_url(url)


class TestTwitchImageUrl:
    @pytest.mark.parametrize(
        "url",
        [
            "https://static-cdn.jtvnw.net/x/profile_image-300x300.png",
            "https://static-cdn.jtvnw.net/previews-ttv/live_user_x-400x225.jpg",
            "https://static-cdn.jtvnw.net/x.jpg?v=2",
        ],
    )
    def test_accepts_twitchs_image_cdn(self, url: str) -> None:
        assert urls.is_twitch_image_url(url)

    @pytest.mark.parametrize(
        "url",
        [
            "",
            "http://static-cdn.jtvnw.net/x.png",
            "https://static-cdn.jtvnw.net:8443/x.png",
            "https://static-cdn.jtvnw.net:port/x.png",
            "https://static-cdn.jtvnw.net/{width}x{height}.png",
            "https://static-cdn.jtvnw.net/x.png#y",
            "https://user:pw@static-cdn.jtvnw.net/x.png",
            "https://cdn.example/x.png",
            "https://127.0.0.1/x.png",
            "https://10.0.0.1/x.png",
            "https://localhost/x.png",
            "https://static-cdn.jtvnw.net.evil.example/x.png",
            "javascript:x",
        ],
    )
    def test_refuses_anything_off_twitchs_cdn(self, url: str) -> None:
        assert not urls.is_twitch_image_url(url)
