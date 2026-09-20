from typing import Any

import pytest

import valmal.core.config as service_config
from tests.config_cache.support import DatabaseDown, role, setting
from valmal.core.config import ConfigCache, _coerce
from valmal.db.models import (
    DiscordAutoResponse,
    DiscordChannel,
    DiscordEmbed,
    DiscordEmbedField,
    MessageTemplate,
    SettingValueType,
    TwitchCommand,
    TwitchCommandComponent,
    TwitchCommandResponse,
)

pytestmark = pytest.mark.anyio


# --- load -----------------------------------------------------------------------


class TestLoad:
    async def test_a_new_cache_is_not_loaded_until_load_runs(self, load: Any) -> None:
        assert ConfigCache().loaded is False

        cache = await load()

        assert cache.loaded is True

    async def test_channels_and_roles_are_read_by_key(self, load: Any) -> None:
        cache = await load(
            DiscordChannel(key="audit_logs", channel_id=11),
            DiscordChannel(key="welcome", channel_id=22),
            role("follower", 33, custom_id="btn_follower"),
        )

        assert cache.channel("audit_logs") == 11
        assert cache.channel("welcome") == 22
        assert cache.role("follower") == 33

    async def test_a_role_is_found_by_its_button_custom_id_only_when_it_has_one(
        self, load: Any
    ) -> None:
        cache = await load(
            role("with", 1, custom_id="btn_with"),
            role("without", 2),
            role("blank", 3, custom_id=""),
        )

        assert cache.role_for_custom_id("btn_with").key == "with"  # pyright: ignore[reportOptionalMemberAccess]
        assert cache.role_for_custom_id("") is None
        assert cache.role_for_custom_id("btn_unknown") is None

    async def test_settings_are_coerced_to_their_declared_type(self, load: Any) -> None:
        cache = await load(
            setting("guild_id", "123", SettingValueType.INTEGER),
            setting("command_prefix", "!", SettingValueType.STRING),
            setting("feature", "true", SettingValueType.BOOLEAN),
            setting("list", '["a", "b"]', SettingValueType.JSON),
            setting("empty", None, SettingValueType.INTEGER),
        )

        assert cache.setting("guild_id") == 123
        assert cache.setting("command_prefix") == "!"
        assert cache.setting("feature") is True
        assert cache.setting("list") == ["a", "b"]
        assert cache.setting("empty") is None

    async def test_templates_are_read_by_key(self, load: Any) -> None:
        cache = await load(MessageTemplate(key="hello", content="Hello {name}"))

        assert cache.template("hello", name="bob") == "Hello bob"

    async def test_embed_fields_are_grouped_by_embed_and_ordered_by_position(
        self, load: Any
    ) -> None:
        cache = await load(
            DiscordEmbed(key="rules", title="Rules"),
            DiscordEmbed(key="other", title="Other"),
            DiscordEmbedField(embed_key="rules", position=2, name="two", value="2"),
            DiscordEmbedField(embed_key="other", position=1, name="o", value="o"),
            DiscordEmbedField(embed_key="rules", position=1, name="one", value="1"),
        )

        rules = cache.embed("rules")
        assert rules is not None
        assert [f.name for f in rules.fields] == ["one", "two"]
        other = cache.embed("other")
        assert other is not None
        assert [f.name for f in other.fields] == ["o"]

    async def test_only_enabled_auto_responses_and_commands_are_kept(
        self, load: Any
    ) -> None:
        cache = await load(
            DiscordAutoResponse(trigger="on", response="yes", enabled=True),
            DiscordAutoResponse(trigger="off", response="no", enabled=False),
            TwitchCommand(name="live", enabled=True),
            TwitchCommand(name="dead", enabled=False),
        )

        assert cache.auto_response("on") == "yes"
        assert cache.auto_response("off") is None
        assert cache.command("live") is not None
        assert cache.command("dead") is None

    async def test_command_responses_and_components_are_ordered_by_position(
        self, load: Any
    ) -> None:
        cache = await load(
            TwitchCommandResponse(command_name="c", position=2, message="second"),
            TwitchCommandResponse(command_name="c", position=1, message="first"),
            TwitchCommandResponse(command_name="d", position=1, message="other"),
            TwitchCommandComponent(parent_name="p", child_name="b", position=2),
            TwitchCommandComponent(parent_name="p", child_name="a", position=1),
        )

        assert cache.command_responses("c") == ["first", "second"]
        assert cache.command_responses("d") == ["other"]
        assert cache.command_components("p") == ["a", "b"]

    async def test_loading_again_replaces_what_was_held_rather_than_merging(
        self, load: Any
    ) -> None:
        cache = await load(
            DiscordChannel(key="old", channel_id=1),
            DiscordEmbed(key="old_embed"),
            DiscordEmbedField(embed_key="old_embed", name="f", value="v"),
            TwitchCommandResponse(command_name="old_cmd", message="m"),
        )

        await load(DiscordChannel(key="new", channel_id=2), cache=cache)

        assert cache.channel("new") == 2
        with pytest.raises(KeyError):
            cache.channel("old")
        assert cache.embed("old_embed") is None
        assert cache.command_responses("old_cmd") == []

    async def test_a_read_that_fails_leaves_the_cache_as_it_was(
        self, load: Any, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        cache = await load(DiscordChannel(key="kept", channel_id=1))

        monkeypatch.setattr(service_config, "session_scope", DatabaseDown)

        with pytest.raises(ConnectionError):
            await cache.load()

        assert cache.channel("kept") == 1
        assert cache.loaded is True

    async def test_a_failed_first_load_leaves_it_unloaded(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(service_config, "session_scope", DatabaseDown)
        cache = ConfigCache()

        with pytest.raises(ConnectionError):
            await cache.load()

        assert cache.loaded is False

    async def test_every_configuration_table_is_read_exactly_once(
        self, load: Any
    ) -> None:
        cache = await load()

        entities = [
            s.column_descriptions[0]["entity"] for s in cache.session.statements
        ]
        assert sorted(e.__name__ for e in entities) == sorted(
            [
                "DiscordChannel",
                "DiscordRole",
                "AppSetting",
                "MessageTemplate",
                "DiscordEmbed",
                "DiscordEmbedField",
                "DiscordAutoResponse",
                "TwitchCommand",
                "TwitchCommandResponse",
                "TwitchCommandComponent",
            ]
        )


# --- _coerce --------------------------------------------------------------------


class TestCoerce:
    @pytest.mark.parametrize(
        ("value", "kind", "expected"),
        [
            ("42", SettingValueType.INTEGER, 42),
            ("-7", SettingValueType.INTEGER, -7),
            ("plain", SettingValueType.STRING, "plain"),
            ("1", SettingValueType.BOOLEAN, True),
            ("true", SettingValueType.BOOLEAN, True),
            ("TRUE", SettingValueType.BOOLEAN, True),
            (" Yes ", SettingValueType.BOOLEAN, True),
            ("0", SettingValueType.BOOLEAN, False),
            ("false", SettingValueType.BOOLEAN, False),
            ("no", SettingValueType.BOOLEAN, False),
            ("", SettingValueType.BOOLEAN, False),
            ("anything else", SettingValueType.BOOLEAN, False),
            ('{"a": 1}', SettingValueType.JSON, {"a": 1}),
            ("[1, 2]", SettingValueType.JSON, [1, 2]),
            ("null", SettingValueType.JSON, None),
        ],
    )
    def test_by_declared_type(
        self, value: str, kind: SettingValueType, expected: object
    ) -> None:
        assert _coerce(setting("k", value, kind)) == expected

    @pytest.mark.parametrize("kind", list(SettingValueType))
    def test_a_null_value_is_none_whatever_the_type(
        self, kind: SettingValueType
    ) -> None:
        assert _coerce(setting("k", None, kind)) is None

    def test_a_malformed_integer_or_json_raises_rather_than_guessing(self) -> None:
        with pytest.raises(ValueError):
            _coerce(setting("k", "twelve", SettingValueType.INTEGER))
        with pytest.raises(ValueError):
            _coerce(setting("k", "{not json", SettingValueType.JSON))


# --- accessors ------------------------------------------------------------------


class TestAccessors:
    async def test_a_missing_channel_or_role_names_the_missing_key(self) -> None:
        cache = ConfigCache()

        with pytest.raises(KeyError, match="No discord_channel row keyed 'nope'"):
            cache.channel("nope")
        with pytest.raises(KeyError, match="No discord_role row keyed 'nope'"):
            cache.role("nope")

    async def test_the_key_error_does_not_chain_the_internal_dict_lookup(self) -> None:
        with pytest.raises(KeyError) as caught:
            ConfigCache().channel("nope")

        assert caught.value.__suppress_context__ is True

    async def test_setting_returns_the_default_for_an_absent_key(
        self, load: Any
    ) -> None:
        cache = await load(setting("present", "1", SettingValueType.INTEGER))

        assert cache.setting("present") == 1
        assert cache.setting("absent") is None
        assert cache.setting("absent", "fallback") == "fallback"

    async def test_a_present_setting_that_is_null_is_none_not_the_default(
        self, load: Any
    ) -> None:
        cache = await load(setting("blank", None, SettingValueType.STRING))

        assert cache.setting("blank", "fallback") is None

    async def test_color_is_an_integer(self, load: Any) -> None:
        cache = await load(
            setting("embed_color_info", "3374037", SettingValueType.INTEGER)
        )

        assert cache.color("embed_color_info") == 3374037

    async def test_color_falls_back_for_a_missing_or_null_setting(
        self, load: Any
    ) -> None:
        cache = await load(setting("null_color", None, SettingValueType.INTEGER))

        assert cache.color("missing") == 0x337FD5
        assert cache.color("null_color") == 0x337FD5
        assert cache.color("missing", default=7) == 7

    async def test_a_string_color_is_read_as_a_decimal_integer(self, load: Any) -> None:
        cache = await load(setting("c", "255", SettingValueType.STRING))

        assert cache.color("c") == 255

    async def test_command_lookups_default_to_empty(self) -> None:
        cache = ConfigCache()

        assert cache.command("none") is None
        assert cache.command_responses("none") == []
        assert cache.command_components("none") == []
