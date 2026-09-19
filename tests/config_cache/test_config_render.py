from typing import Any

import pytest

from db.models import (
    DiscordChannel,
    DiscordEmbed,
    DiscordEmbedField,
    MessageTemplate,
)
from services.config import ConfigCache, RenderedEmbed, RenderedField
from tests.config_cache.support import role

pytestmark = pytest.mark.anyio


# --- render ---------------------------------------------------------------------


class TestRender:
    @pytest.fixture
    async def cache(self, load: Any) -> ConfigCache:
        return await load(
            DiscordChannel(key="promo", channel_id=111),
            DiscordChannel(key="rules_2", channel_id=222),
            role("follower", 333),
        )

    async def test_channel_and_role_placeholders_become_mentions(
        self, cache: ConfigCache
    ) -> None:
        assert cache.render("see {channel:promo}", source="s") == "see <#111>"
        assert cache.render("hi {role:follower}", source="s") == "hi <@&333>"

    async def test_several_placeholders_in_one_text(self, cache: ConfigCache) -> None:
        text = "{channel:promo}{role:follower} and {channel:rules_2}"

        assert cache.render(text, source="s") == "<#111><@&333> and <#222>"

    async def test_text_without_placeholders_is_unchanged(
        self, cache: ConfigCache
    ) -> None:
        assert cache.render("nothing {here} at all", source="s") == (
            "nothing {here} at all"
        )

    async def test_a_slug_with_no_row_is_left_as_written_with_a_notice_naming_the_source(
        self, cache: ConfigCache, notices: list
    ) -> None:
        assert cache.render("x {channel:gone}", source="message_template:hello") == (
            "x {channel:gone}"
        )

        ((text, key),) = notices
        assert "message_template:hello" in text
        assert "{channel:gone}" in text
        assert key == "render-missing-channel:gone:message_template:hello"

    async def test_one_dangling_slug_in_two_rows_is_two_notices(
        self, cache: ConfigCache, notices: list
    ) -> None:
        cache.render("{role:gone}", source="row one")
        cache.render("{role:gone}", source="row two")

        assert [key for _, key in notices] == [
            "render-missing-role:gone:row one",
            "render-missing-role:gone:row two",
        ]

    async def test_a_doubled_brace_is_left_untouched(
        self, cache: ConfigCache, notices: list
    ) -> None:
        assert cache.render("{{channel:promo}}", source="s") == "{{channel:promo}}"
        assert cache.render("{{role:follower}}", source="s") == "{{role:follower}}"
        assert not notices

    async def test_only_lowercase_slugs_are_placeholders(
        self, cache: ConfigCache
    ) -> None:
        assert cache.render("{channel:PROMO}", source="s") == "{channel:PROMO}"
        assert cache.render("{channel:pro-mo}", source="s") == "{channel:pro-mo}"

    async def test_an_unknown_kind_is_not_a_placeholder(
        self, cache: ConfigCache
    ) -> None:
        assert cache.render("{user:promo}", source="s") == "{user:promo}"

    async def test_a_stored_value_that_looks_like_a_placeholder_is_not_expanded_twice(
        self, cache: ConfigCache
    ) -> None:
        assert cache.render("{channel:promo}", source="s") == "<#111>"


# --- template -------------------------------------------------------------------


class TestTemplate:
    async def test_a_missing_row_is_an_empty_string_and_a_notice_not_an_exception(
        self, load: Any, notices: list
    ) -> None:
        cache = await load()

        assert cache.template("nope") == ""

        ((text, key),) = notices
        assert "'nope'" in text
        assert key == "template-missing:nope"

    async def test_placeholders_resolve_before_fields_are_filled(
        self, load: Any
    ) -> None:
        """str.format reads {channel:promo} as a format spec and raises KeyError on
        the brace it does not own, so the order is what makes both work."""
        cache = await load(
            DiscordChannel(key="promo", channel_id=5),
            MessageTemplate(key="t", content="Hi {name}, go to {channel:promo}"),
        )

        assert cache.template("t", name="bob") == "Hi bob, go to <#5>"

    async def test_without_values_placeholders_still_resolve(self, load: Any) -> None:
        cache = await load(
            DiscordChannel(key="promo", channel_id=5),
            MessageTemplate(key="t", content="go to {channel:promo}"),
        )

        assert cache.template("t") == "go to <#5>"

    async def test_a_dangling_slug_survives_formatting_as_written(
        self, load: Any, notices: list
    ) -> None:
        cache = await load(MessageTemplate(key="t", content="{channel:gone} {name}"))

        assert cache.template("t", name="bob") == "{channel:gone} bob"
        assert len(notices) == 1

    async def test_the_notice_names_the_template_row_at_fault(
        self, load: Any, notices: list
    ) -> None:
        cache = await load(MessageTemplate(key="birthday", content="{role:gone}"))

        cache.template("birthday")

        assert "message_template:birthday" in notices[0][0]

    async def test_a_field_nobody_passed_is_left_as_written(self, load: Any) -> None:
        cache = await load(MessageTemplate(key="t", content="{a} {b}"))

        assert cache.template("t", a=1) == "1 {b}"

    async def test_a_malformed_template_degrades_to_its_own_text(
        self, load: Any, notices: list
    ) -> None:
        cache = await load(MessageTemplate(key="t", content="oops {name"))

        assert cache.template("t", name="bob") == "oops {name"
        assert len(notices) == 1

    async def test_a_value_may_carry_braces_or_mentions_without_being_reread(
        self, load: Any
    ) -> None:
        cache = await load(MessageTemplate(key="t", content="hello {name}"))

        assert cache.template("t", name="{channel:x} <@1>") == "hello {channel:x} <@1>"


# --- embed ----------------------------------------------------------------------


class TestEmbed:
    async def test_an_unknown_embed_is_none(self, load: Any) -> None:
        assert (await load()).embed("nope") is None

    async def test_a_stored_embed_comes_back_rendered_and_immutable(
        self, load: Any
    ) -> None:
        cache = await load(
            DiscordChannel(key="rules", channel_id=9),
            DiscordEmbed(
                key="rules_embed",
                title="Read {channel:rules}",
                description="Ping {role:mods}",
                color=0xABCDEF,
                channel_key="rules",
            ),
            role("mods", 77),
            DiscordEmbedField(
                embed_key="rules_embed",
                position=1,
                name="Where {channel:rules}",
                value="Who {role:mods}",
                inline=True,
            ),
        )

        embed = cache.embed("rules_embed")

        assert embed == RenderedEmbed(
            title="Read <#9>",
            description="Ping <@&77>",
            color=0xABCDEF,
            channel_key="rules",
            fields=(RenderedField(name="Where <#9>", value="Who <@&77>", inline=True),),
        )
        assert isinstance(embed.fields, tuple)  # pyright: ignore[reportOptionalMemberAccess]

    async def test_an_empty_title_or_description_is_none_not_an_empty_string(
        self, load: Any
    ) -> None:
        cache = await load(
            DiscordEmbed(key="a", title=None, description=None),
            DiscordEmbed(key="b", title="", description=""),
        )

        for key in ("a", "b"):
            embed = cache.embed(key)
            assert embed is not None
            assert embed.title is None
            assert embed.description is None

    async def test_a_stale_slug_in_a_field_is_named_by_embed_and_position(
        self, load: Any, notices: list
    ) -> None:
        cache = await load(
            DiscordEmbed(key="e"),
            DiscordEmbedField(embed_key="e", position=4, name="n", value="{role:gone}"),
        )

        cache.embed("e")

        assert "discord_embed_field:e:4" in notices[0][0]

    async def test_doubled_braces_are_left_as_written_unlike_a_template(
        self, load: Any
    ) -> None:
        cache = await load(DiscordEmbed(key="e", description="{{literal}}"))

        embed = cache.embed("e")

        assert embed is not None and embed.description == "{{literal}}"

    async def test_embeds_are_listed_in_stored_position_order(self, load: Any) -> None:
        cache = await load(
            DiscordEmbed(key="c", position=3),
            DiscordEmbed(key="a", position=1),
            DiscordEmbed(key="b", position=2),
        )

        assert cache.embed_keys() == ["a", "b", "c"]

    async def test_embeds_for_one_channel_are_read_off_the_rows_without_rendering(
        self, load: Any, notices: list
    ) -> None:
        cache = await load(
            DiscordEmbed(key="x", channel_key="rules", position=2, title="{role:gone}"),
            DiscordEmbed(key="y", channel_key="roles", position=1),
            DiscordEmbed(key="z", channel_key="rules", position=1),
        )

        assert cache.embed_keys_for_channel("rules") == ["z", "x"]
        assert cache.embed_keys_for_channel("nowhere") == []
        assert not notices, "a stale slug in one bound elsewhere is not this caller's"

    async def test_roles_for_an_embed_are_ordered_by_position(self, load: Any) -> None:
        cache = await load(
            role("b", 2, embed_key="panel", position=2),
            role("a", 1, embed_key="panel", position=1),
            role("z", 9, embed_key="other", position=1),
            role("loose", 8),
        )

        assert [r.key for r in cache.roles_for_embed("panel")] == ["a", "b"]
        assert cache.roles_for_embed("none") == []
