import discord
import pendulum
import pytest

from db.models import DiscordRole
from services.config import config
from services.twitch import live_alert_embeds as embeds
from tests.live_alert.support import NOW, channel, stream, user, video

pytestmark = pytest.mark.usefixtures("embed_config")

URL = "https://www.twitch.tv/valinmalach"


class TestTwitchUrl:
    def test_a_login_is_linked(self) -> None:
        assert embeds.twitch_url("valinmalach") == URL

    def test_an_empty_login_is_a_deliberate_generic_link_not_a_refusal(self) -> None:
        """_close passes "" when both lookups it prefers failed; refusing it would turn
        a stream the bot cannot fully describe into an alert that can never close."""
        assert embeds.twitch_url("") == "https://www.twitch.tv/"

    @pytest.mark.parametrize(
        "login",
        [
            "a)b",
            "a b",
            "bob/evil",
            "bob?x=1",
            "bob#frag",
            "<b>",
            "bob\n",
            "x" * 26,
            "é",
        ],
    )
    def test_anything_that_is_not_a_login_is_refused_because_it_lands_in_a_link(
        self, login: str
    ) -> None:
        """An unescaped ")" would close the markdown link early."""
        with pytest.raises(ValueError, match="Not a Twitch login"):
            embeds.twitch_url(login)


class TestVodUrl:
    @pytest.mark.parametrize("video_id", ["1", "555", "9" * 20, "007"])
    def test_a_numeric_id_is_linked(self, video_id: str) -> None:
        assert embeds.vod_url(video_id) == f"https://www.twitch.tv/videos/{video_id}"

    @pytest.mark.parametrize(
        "video_id", ["", "9" * 21, "12a", "-1", "1.5", " 1", "1\n", "١٢", "../x"]
    )
    def test_anything_else_is_refused_rather_than_trusting_helixs_url_field(
        self, video_id: str
    ) -> None:
        with pytest.raises(ValueError, match="Not a Twitch video id"):
            embeds.vod_url(video_id)


class TestMention:
    def test_only_the_stream_alerts_channel_pings_the_live_alerts_role(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(
            config,
            "_roles",
            {"live_alerts": DiscordRole(key="live_alerts", role_id=777, name="x")},
        )

        assert embeds.mention(5001) == "<@&777>"
        assert embeds.mention(5002) is None
        assert embeds.mention(0) is None


class TestWatchButton:
    def test_is_one_link_button_that_never_times_out(self) -> None:
        view = embeds.watch_button(URL)

        assert view.timeout is None
        (button,) = view.children
        assert isinstance(button, discord.ui.Button)
        assert (button.label, button.url, button.style) == (
            "Watch",
            URL,
            discord.ButtonStyle.link,
        )


class TestLinkable:
    @pytest.mark.parametrize(
        ("text", "escaped"),
        [
            ("plain", "plain"),
            ("a](https://evil.example)", r"a\]\(https://evil.example\)"),
            ("**bold**", r"\*\*bold\*\*"),
            ("_i_ ~s~ `c` |s|", r"\_i\_ \~s\~ \`c\` \|s\|"),
            ("back\\slash", "back\\\\slash"),
        ],
    )
    def test_escapes_everything_that_could_end_or_start_a_link(
        self, text: str, escaped: str
    ) -> None:
        assert embeds._linkable(text) == escaped

    def test_a_title_cannot_close_the_bots_own_link_and_open_another(self) -> None:
        escaped = embeds._linkable("x](https://evil)[y")

        assert "](" not in escaped.replace(r"\]\(", "")


class TestAnnouncement:
    def test_carries_the_title_link_game_and_viewers(self) -> None:
        embed = embeds.announcement_embed(
            stream(title="Ranked", game_name="Chess", viewer_count=42), user(), URL
        )

        assert embed.description == f"[**Ranked**]({URL})"
        assert embed.color is not None and embed.color.value == 0x9146FF
        assert [(f.name, f.value, f.inline) for f in embed.fields] == [
            ("Game", "Chess", True),
            ("Viewers", "42", True),
        ]

    def test_is_timestamped_at_the_streams_start(self) -> None:
        embed = embeds.announcement_embed(
            stream(started_at="2026-06-15T11:00:00Z"), user(), URL
        )

        assert embed.timestamp == pendulum.datetime(2026, 6, 15, 11)

    def test_the_author_is_the_title_template_with_the_display_name_and_the_avatar(
        self,
    ) -> None:
        embed = embeds.announcement_embed(
            stream(user_name="Valin"), user(profile_image_url="https://cdn/p.png"), URL
        )

        assert embed.author.name == "Valin is live!"
        assert embed.author.icon_url == "https://cdn/p.png"
        assert embed.author.url == URL

    def test_without_a_profile_there_is_no_avatar(self) -> None:
        embed = embeds.announcement_embed(stream(), None, URL)

        assert embed.author.icon_url is None

    def test_the_thumbnail_is_sized_and_cache_busted(self) -> None:
        embed = embeds.announcement_embed(
            stream(thumbnail_url="https://cdn/live_{width}x{height}.jpg"), None, URL
        )

        assert (
            embed.image.url == f"https://cdn/live_400x225.jpg?cb={int(NOW.timestamp())}"
        )

    def test_a_title_cannot_inject_a_link(self) -> None:
        embed = embeds.announcement_embed(
            stream(title="](https://evil.example)"), None, URL
        )

        assert embed.description == rf"[**\]\(https://evil.example\)**]({URL})"

    def test_a_game_name_is_escaped_in_its_field(self) -> None:
        embed = embeds.announcement_embed(stream(game_name="**x** [y](z)"), None, URL)

        value = embed.fields[0].value or ""
        assert "**x**" not in value
        assert value.startswith("\\*\\*x\\*\\*")

    def test_the_author_name_is_plain_text_and_left_alone(self) -> None:
        """An author line is plain text to Discord; escaping it would show the backslashes."""
        embed = embeds.announcement_embed(stream(user_name="*Star*"), None, URL)

        assert embed.author.name == "*Star* is live!"

    def test_there_is_no_footer_yet(self) -> None:
        assert embeds.announcement_embed(stream(), None, URL).footer.text is None


class TestLiveEmbed:
    def build(self, **overrides: object) -> discord.Embed:
        return embeds.live_embed(
            stream(**overrides), user(), URL, "2 hours", "<t:1:f>", NOW
        )

    def test_adds_the_start_time_field_and_the_age_footer(self) -> None:
        embed = self.build()

        assert [f.name for f in embed.fields] == ["Game", "Viewers", "Started"]
        assert embed.fields[2].value == "<t:1:f>"
        assert embed.footer.text == "Live for 2 hours"

    def test_is_stamped_now_not_at_the_start(self) -> None:
        assert self.build().timestamp == NOW

    def test_escapes_title_and_game_like_the_announcement(self) -> None:
        embed = self.build(title="a](b)", game_name="_g_")

        assert embed.description == rf"[**a\]\(b\)**]({URL})"
        assert embed.fields[0].value == r"\_g\_"


class TestOfflineEmbed:
    def build(self, **kwargs: object) -> discord.Embed:
        defaults: dict[str, object] = {
            "stream": None,
            "vod": None,
            "channel": None,
            "user_info": None,
            "url": URL,
            "age": "3 hours",
            "now": NOW,
        }
        return embeds.offline_embed(**(defaults | kwargs))  # pyright: ignore[reportArgumentType]

    def test_everything_missing_reads_unknown_rather_than_failing(self) -> None:
        embed = self.build()

        assert embed.description == "**Unknown**"
        assert embed.author.name == "Unknown was live"
        assert embed.fields[0].value == "Unknown"
        assert embed.footer.text == "Streamed for 3 hours"

    def test_the_title_prefers_the_stream_then_the_vod_then_the_channel(self) -> None:
        assert (
            self.build(
                stream=stream(title="S"),
                vod=video(title="V"),
                channel=channel(title="C"),
            ).description
            == "**S**"
        )
        assert (
            self.build(vod=video(title="V"), channel=channel(title="C")).description
            == "**V**"
        )
        assert self.build(channel=channel(title="C")).description == "**C**"

    def test_the_name_prefers_the_stream_then_the_profile(self) -> None:
        assert (
            self.build(
                stream=stream(user_name="FromStream"), user_info=user()
            ).author.name
            == "FromStream was live"
        )
        assert (
            self.build(user_info=user(display_name="FromProfile")).author.name
            == "FromProfile was live"
        )

    def test_the_game_prefers_the_stream_then_the_channel(self) -> None:
        assert (
            self.build(stream=stream(game_name="A"), channel=channel(game_name="B"))
            .fields[0]
            .value
            == "A"
        )
        assert self.build(channel=channel(game_name="B")).fields[0].value == "B"

    def test_a_vod_adds_a_field_built_from_its_validated_id(self) -> None:
        embed = self.build(vod=video(id="555", url="https://evil.example/not-used"))

        assert [f.name for f in embed.fields] == ["Game", "VOD"]
        assert embed.fields[1].value == "[Watch](https://www.twitch.tv/videos/555)"

    def test_a_vod_with_a_non_numeric_id_means_no_field_not_a_broken_embed(
        self,
    ) -> None:
        embed = self.build(vod=video(id="abc](evil)"))

        assert [f.name for f in embed.fields] == ["Game"]

    def test_a_title_is_escaped_in_bold(self) -> None:
        assert self.build(stream=stream(title="**x**")).description == r"**\*\*x\*\***"

    def test_the_avatar_comes_from_the_profile_when_there_is_one(self) -> None:
        assert (
            self.build(
                user_info=user(profile_image_url="https://cdn/p.png")
            ).author.icon_url
            == "https://cdn/p.png"
        )
        assert self.build().author.icon_url is None

    def test_is_stamped_now(self) -> None:
        assert self.build().timestamp == NOW
