from types import SimpleNamespace
from unittest.mock import MagicMock

import discord
import pytest

from services.present import get_channel_mention, get_discriminator, get_pfp, quoted


def _user(
    avatar_url: str | None, default_url: str = "https://cdn/default.png"
) -> discord.User:
    avatar = SimpleNamespace(url=avatar_url) if avatar_url else None
    return SimpleNamespace(  # pyright: ignore[reportReturnType]
        avatar=avatar, default_avatar=SimpleNamespace(url=default_url)
    )


def test_pfp_is_the_avatar_when_there_is_one() -> None:
    assert get_pfp(_user("https://cdn/a.png")) == "https://cdn/a.png"


def test_pfp_falls_back_to_the_default_avatar() -> None:
    assert get_pfp(_user(None, "https://cdn/d.png")) == "https://cdn/d.png"


@pytest.mark.parametrize(
    ("discriminator", "expected"),
    [("0", ""), ("1234", "#1234"), ("0001", "#0001"), ("", "#")],
)
def test_discriminator_is_hidden_only_for_the_new_username_system(
    discriminator: str, expected: str
) -> None:
    member = SimpleNamespace(discriminator=discriminator)

    assert get_discriminator(member) == expected  # pyright: ignore[reportArgumentType]


def _channel(kind: type, **attrs: object) -> MagicMock:
    channel = MagicMock(spec=kind)
    for name, value in attrs.items():
        setattr(channel, name, value)
    return channel


def test_an_unknown_or_bare_id_channel_is_named_unknown() -> None:
    assert get_channel_mention(None) == "Unknown Channel"
    assert get_channel_mention(discord.Object(id=1)) == "Unknown Channel"


def test_a_group_dm_is_its_name() -> None:
    assert get_channel_mention(_channel(discord.GroupChannel, name="the gang")) == (
        "the gang"
    )


def test_a_dm_is_not_named_because_it_has_no_mention() -> None:
    assert get_channel_mention(_channel(discord.DMChannel)) == "a DM"


def test_a_group_dm_is_checked_before_the_generic_private_channel() -> None:
    """GroupChannel is a PrivateChannel too; it must not fall through to the
    generic answer and lose its name."""
    group = _channel(discord.GroupChannel, name="g")

    assert isinstance(group, discord.abc.PrivateChannel)
    assert get_channel_mention(group) == "g"


def test_any_other_private_channel_is_generic() -> None:
    class Other(discord.abc.PrivateChannel):
        pass

    assert get_channel_mention(MagicMock(spec=Other)) == "a private channel"


@pytest.mark.parametrize(
    "kind",
    [discord.TextChannel, discord.VoiceChannel, discord.Thread, discord.ForumChannel],
)
def test_a_guild_channel_or_thread_is_its_mention(kind: type) -> None:
    assert get_channel_mention(_channel(kind, mention="<#42>")) == "<#42>"


def test_a_partial_channel_is_its_mention() -> None:
    partial = _channel(discord.PartialMessageable, mention="<#7>")

    assert get_channel_mention(partial) == "<#7>"


class TestQuoted:
    def test_wraps_in_a_code_span(self) -> None:
        assert quoted("hello") == "`hello`"

    def test_backticks_are_removed_because_an_escaped_one_still_closes_the_span(
        self,
    ) -> None:
        assert quoted("a`b``c") == "`abc`"

    def test_only_backticks_leaves_an_empty_span(self) -> None:
        assert quoted("`" * 80) == "``"

    def test_truncates_before_stripping_so_the_result_is_never_longer_than_the_cap(
        self,
    ) -> None:
        assert quoted("x" * 500) == "`" + "x" * 50 + "`"

    def test_a_mention_is_left_inert_by_the_span_and_not_altered(self) -> None:
        assert quoted("<@123> @everyone") == "`<@123> @everyone`"

    def test_markdown_is_not_escaped_because_a_span_renders_none(self) -> None:
        assert quoted("**bold** ~~x~~ _y_ ||z||") == "`**bold** ~~x~~ _y_ ||z||`"

    @pytest.mark.parametrize("value", ["", " ", "\n", "🎮"])
    def test_awkward_values_still_produce_a_span(self, value: str) -> None:
        result = quoted(value)

        assert result.startswith("`") and result.endswith("`")
        assert result.count("`") == 2

    def test_the_result_never_contains_a_backtick_but_its_delimiters(self) -> None:
        for value in ["`", "``", "a`b", "`a`", "```code```", "x" * 49 + "`y"]:
            assert quoted(value).count("`") == 2, value
