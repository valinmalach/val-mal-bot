import discord
import pendulum
import pytest

from tests.audit.support import (
    AVATAR,
    COLORS,
    DEFAULT_AVATAR,
    guild,
    invite,
    person,
)
from valmal.bot import audit
from valmal.bot.audit import (
    DEFAULT_MISSING_CONTENT,
    EMPTY_CONTENT,
    UNKNOWN_USER,
    UNNAMED_EVENT,
)
from valmal.core.config import config

BACKSLASH = chr(92)


class TestCap:
    def test_text_within_the_limit_is_untouched(self) -> None:
        assert audit._cap("abc", 3) == "abc"
        assert audit._cap("", 3) == ""

    def test_one_over_is_cut_to_exactly_the_limit_with_an_ellipsis(self) -> None:
        assert audit._cap("abcd", 3) == "..."
        assert audit._cap("abcdefghij", 8) == "abcde..."

    @pytest.mark.parametrize("limit", [256, 1024, 4096])
    def test_the_result_never_exceeds_the_limit_discord_rejects(
        self, limit: int
    ) -> None:
        for extra in (1, 2, 3, 10, limit):
            assert len(audit._cap("x" * (limit + extra), limit)) == limit

    def test_a_cut_through_an_escape_drops_the_lone_backslash(self) -> None:
        """Left in, it would escape the first dot of the ellipsis."""
        text = "a" * 5 + BACKSLASH + "*" + "b" * 10

        assert audit._cap(text, 9) == "a" * 5 + "..."

    def test_a_backslash_that_is_itself_escaped_is_kept(self) -> None:
        text = "a" * 5 + BACKSLASH * 2 + "b" * 10

        assert audit._cap(text, 10) == "a" * 5 + BACKSLASH * 2 + "..."

    @pytest.mark.parametrize("run", range(1, 8))
    def test_the_ellipsis_is_never_preceded_by_an_odd_run_of_backslashes(
        self, run: int
    ) -> None:
        text = "a" * 4 + BACKSLASH * run + "z" * 20

        capped = audit._cap(text, 4 + run + 3)

        before = capped.removesuffix("...")
        trailing = len(before) - len(before.rstrip(BACKSLASH))
        assert trailing % 2 == 0 and capped.endswith("...")

    def test_the_cut_keeps_the_head_not_the_tail(self) -> None:
        assert audit._cap("head" + "-" * 50, 10).startswith("head")


class TestField:
    def test_empty_is_replaced_because_discord_rejects_an_empty_value(self) -> None:
        assert audit._field("") == EMPTY_CONTENT

    def test_the_limit_is_1024(self) -> None:
        assert audit._field("x" * 1024) == "x" * 1024
        assert len(audit._field("x" * 1025)) == 1024


class TestSaid:
    def test_escapes_markdown_a_person_wrote(self) -> None:
        assert audit._said("a_b*c") == f"a{BACKSLASH}_b{BACKSLASH}*c"

    def test_a_masked_link_loses_its_opening_bracket(self) -> None:
        """A label of the writer's choosing on a URL of theirs must not render."""
        said = audit._said("[click](http://evil.example)")

        assert said.startswith(f"{BACKSLASH}[")

    def test_empty_and_only_whitespace_are_handled_as_field_values(self) -> None:
        assert audit._said("") == EMPTY_CONTENT
        assert audit._said(" ") == " "

    def test_escaping_can_double_the_length_and_the_result_is_still_capped(
        self,
    ) -> None:
        said = audit._said("*" * 2000)

        assert len(said) <= 1024 and said.endswith("...")
        assert said.removesuffix("...").endswith(f"{BACKSLASH}*")

    def test_a_mention_is_left_alone_because_an_embed_never_pings(self) -> None:
        assert audit._said("<@123>") == "<@123>"


class TestNamed:
    def test_escapes_the_name_because_a_description_renders_markdown(self) -> None:
        assert audit._named(person(name="a_b*")) == f"a{BACKSLASH}_b{BACKSLASH}*"

    def test_modern_usernames_have_no_discriminator(self) -> None:
        assert audit._named(person(discriminator="0")) == "val_mal".replace(
            "_", f"{BACKSLASH}_"
        )

    def test_a_legacy_discriminator_is_appended(self) -> None:
        assert audit._named(person(name="val", discriminator="1234")) == "val#1234"


class TestQuoted:
    def test_no_text_is_the_bots_own_marker_unescaped(self) -> None:
        assert audit._quoted(None) == DEFAULT_MISSING_CONTENT
        assert "`" in DEFAULT_MISSING_CONTENT

    def test_empty_text_is_a_message_that_had_no_text_not_a_missing_one(self) -> None:
        assert audit._quoted("") == EMPTY_CONTENT

    def test_a_persons_text_is_escaped(self) -> None:
        assert audit._quoted("*hi*") == f"{BACKSLASH}*hi{BACKSLASH}*"


class TestAuthorLines:
    def test_a_person_is_named_with_their_avatar(self) -> None:
        embed = discord.Embed()

        audit._by(embed, person(name="val", discriminator="1234"))

        assert embed.author.name == "val#1234"
        assert embed.author.icon_url == AVATAR

    def test_a_person_with_no_avatar_gets_the_default_one(self) -> None:
        embed = discord.Embed()

        audit._by(embed, person(avatar=None))

        assert embed.author.icon_url == DEFAULT_AVATAR

    def test_the_name_is_not_escaped_because_an_author_line_is_plain_text(
        self,
    ) -> None:
        embed = discord.Embed()

        audit._by(embed, person(name="a_b*c"))

        assert embed.author.name == "a_b*c"

    def test_no_deleter_is_unknown_user_with_no_icon(self) -> None:
        embed = discord.Embed()

        audit._by(embed, None)

        assert embed.author.name == UNKNOWN_USER
        assert embed.author.icon_url is None

    def test_a_very_long_name_is_capped_at_256(self) -> None:
        embed = discord.Embed()

        audit._by(embed, person(name="n" * 400))

        assert len(embed.author.name or "") == 256

    def test_a_caption_names_the_event_with_its_icon(self) -> None:
        embed = discord.Embed()

        audit._captioned(embed, "Member Joined", "https://icon")

        assert embed.author.name == "Member Joined"
        assert embed.author.icon_url == "https://icon"

    def test_a_caption_from_a_missing_template_falls_back_rather_than_going_empty(
        self,
    ) -> None:
        embed = discord.Embed()

        audit._captioned(embed, "", None)

        assert embed.author.name == UNNAMED_EVENT

    def test_a_long_caption_is_capped_at_256(self) -> None:
        embed = discord.Embed()

        audit._captioned(embed, "c" * 300, None)

        assert len(embed.author.name or "") == 256


class TestEmbed:
    def test_takes_its_colour_from_the_configuration_by_key(self, log: object) -> None:
        embed = audit._embed("text", "embed_color_danger")

        assert embed.colour is not None
        assert embed.colour.value == COLORS["embed_color_danger"]

    def test_a_colour_with_no_row_falls_back_to_discord_blue(self, log: object) -> None:
        embed = audit._embed("text", "embed_color_missing")

        assert embed.colour is not None and embed.colour.value == 0x337FD5

    def test_is_stamped_with_now(self, log: object) -> None:
        before = pendulum.now().subtract(seconds=1)

        embed = audit._embed("text", "embed_color_info")

        assert embed.timestamp is not None and embed.timestamp > before

    def test_the_description_is_capped_at_4096(self, log: object) -> None:
        assert len(audit._embed("d" * 5000, "embed_color_info").description or "") == (
            4096
        )

    def test_a_template_renders_its_values_into_the_description(
        self, log: object
    ) -> None:
        embed = audit._templated(
            "audit_bulk_deleted", "embed_color_info", count=3, channel="#c"
        )

        assert embed.description == "bulk 3 #c"

    def test_a_template_row_that_is_missing_still_produces_an_embed(
        self, log: object, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr("valmal.core.safe_format.notify_soon", lambda *a, **k: None)
        monkeypatch.setattr(config, "_templates", {})

        embed = audit._templated("audit_bulk_deleted", "embed_color_info", count=3)

        assert embed.description == ""


class TestGuildOf:
    def test_a_guild_is_named_with_its_icon(self) -> None:
        assert audit._guild_of(invite(where=guild("Home", "https://g"))) == (
            "Home",
            "https://g",
        )

    def test_a_guild_with_no_icon_has_none(self) -> None:
        assert audit._guild_of(invite(where=guild("Home", None))) == ("Home", None)

    def test_an_invite_with_no_guild_is_an_unknown_one(self) -> None:
        assert audit._guild_of(invite(where=None)) == ("Unknown Guild", None)

    def test_a_guild_the_bot_holds_only_an_id_for_is_an_unknown_one(self) -> None:
        assert audit._guild_of(invite(where=discord.Object(id=5))) == (
            "Unknown Guild",
            None,
        )
