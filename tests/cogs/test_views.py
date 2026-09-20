from types import SimpleNamespace
from typing import Any

import discord
import pytest

import views
from services.config import RenderedEmbed, RenderedField, config
from views import RolePickerView, build_embed, persistent_views, role_panels

pytestmark = pytest.mark.anyio

GAME = chr(0x1F3AE)
MUSIC = chr(0x1F3B5)


def role(custom_id: str | None, emoji: str | None, key: str = "r") -> Any:
    return SimpleNamespace(key=key, custom_id=custom_id, emoji=emoji, role_id=1)


def button_of(view: RolePickerView, index: int) -> Any:
    return view.children[index]


def rendered(**overrides: Any) -> RenderedEmbed:
    return RenderedEmbed(
        **{
            "title": "Pick a role",
            "description": "Press a button",
            "color": 0x123456,
            "channel_key": "roles",
            "fields": (),
        }
        | overrides
    )


@pytest.fixture
def stored(monkeypatch: pytest.MonkeyPatch) -> dict[str, Any]:
    """The rows the views read, keyed the way config hands them out."""
    rows: dict[str, Any] = {
        "roles": {"pronouns": [role("p_he", GAME), role("p_she", MUSIC)]},
        "embeds": {"pronouns": rendered()},
        "channels": {"roles": 500},
        "order": {"roles": ["pronouns"]},
    }
    monkeypatch.setattr(
        config, "roles_for_embed", lambda key: rows["roles"].get(key, [])
    )
    monkeypatch.setattr(config, "embed", lambda key: rows["embeds"].get(key))
    monkeypatch.setattr(config, "embed_keys", lambda: list(rows["embeds"]))
    monkeypatch.setattr(
        config, "embed_keys_for_channel", lambda key: rows["order"].get(key, [])
    )

    def channel(key: str) -> int:
        try:
            return rows["channels"][key]
        except KeyError:
            raise KeyError(f"No discord_channel row keyed {key!r}") from None

    monkeypatch.setattr(config, "channel", channel)
    return rows


class TestRolePickerView:
    async def test_has_one_button_per_role_with_that_roles_emoji_and_id(
        self, stored: dict[str, Any]
    ) -> None:
        view = RolePickerView("pronouns")

        buttons = [b for b in view.children if isinstance(b, discord.ui.Button)]
        assert [(str(b.emoji), b.custom_id) for b in buttons] == [
            (GAME, "p_he"),
            (MUSIC, "p_she"),
        ]

    async def test_never_times_out_because_it_has_to_outlive_a_restart(
        self, stored: dict[str, Any]
    ) -> None:
        assert RolePickerView("pronouns").timeout is None

    async def test_is_persistent_so_discord_accepts_it_for_add_view(
        self, stored: dict[str, Any]
    ) -> None:
        assert RolePickerView("pronouns").is_persistent() is True

    @pytest.mark.parametrize(
        ("custom_id", "emoji"), [(None, GAME), ("x", None), ("", GAME), ("x", "")]
    )
    async def test_a_role_with_no_emoji_or_no_custom_id_gets_no_button(
        self, custom_id: str | None, emoji: str | None, stored: dict[str, Any]
    ) -> None:
        stored["roles"]["pronouns"] = [role(custom_id, emoji)]

        assert RolePickerView("pronouns").children == []

    async def test_an_embed_with_no_roles_is_an_empty_view(
        self, stored: dict[str, Any]
    ) -> None:
        assert RolePickerView("nothing").children == []

    async def test_each_button_toggles_its_own_role(
        self, stored: dict[str, Any], monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """One callback closed over the loop variable would toggle the last role always."""
        seen: list[str | None] = []

        async def pressed(interaction: object, button: discord.ui.Button) -> None:
            seen.append(button.custom_id)

        monkeypatch.setattr("services.roles.roles_button_pressed", pressed)
        view = RolePickerView("pronouns")

        for index in range(len(view.children)):
            await button_of(view, index).callback(object())

        assert seen == ["p_he", "p_she"]

    async def test_the_callback_passes_the_interaction_through(
        self, stored: dict[str, Any], monkeypatch: pytest.MonkeyPatch
    ) -> None:
        got: list[object] = []

        async def pressed(interaction: object, button: object) -> None:
            got.append(interaction)

        monkeypatch.setattr("services.roles.roles_button_pressed", pressed)
        marker = object()

        await button_of(RolePickerView("pronouns"), 0).callback(marker)

        assert got == [marker]

    async def test_a_press_that_fails_reaches_the_view_error_handler_not_silence(
        self, stored: dict[str, Any], monkeypatch: pytest.MonkeyPatch
    ) -> None:
        async def pressed(interaction: object, button: object) -> None:
            raise RuntimeError("boom")

        monkeypatch.setattr("services.roles.roles_button_pressed", pressed)

        with pytest.raises(RuntimeError):
            await button_of(RolePickerView("pronouns"), 0).callback(object())


class TestBuildEmbed:
    async def test_carries_the_stored_text_and_colour(
        self, stored: dict[str, Any]
    ) -> None:
        embed = build_embed("pronouns")

        assert (embed.title, embed.description) == ("Pick a role", "Press a button")
        assert embed.colour is not None and embed.colour.value == 0x123456

    async def test_an_embed_with_no_colour_gets_the_default(
        self, stored: dict[str, Any]
    ) -> None:
        stored["embeds"]["pronouns"] = rendered(color=None)

        embed = build_embed("pronouns")

        assert embed.colour is not None and embed.colour.value == views.DEFAULT_COLOUR

    async def test_a_colour_of_zero_is_black_not_the_default(
        self, stored: dict[str, Any]
    ) -> None:
        """Only None means unset; a falsy colour is still a colour."""
        stored["embeds"]["pronouns"] = rendered(color=0)

        embed = build_embed("pronouns")

        assert embed.colour is not None and embed.colour.value == 0

    async def test_fields_keep_their_order_and_inline_flag(
        self, stored: dict[str, Any]
    ) -> None:
        stored["embeds"]["pronouns"] = rendered(
            fields=(
                RenderedField(name="One", value="1", inline=True),
                RenderedField(name="Two", value="2", inline=False),
            )
        )

        embed = build_embed("pronouns")

        assert [(f.name, f.value, f.inline) for f in embed.fields] == [
            ("One", "1", True),
            ("Two", "2", False),
        ]

    async def test_an_embed_with_no_text_is_still_an_embed(
        self, stored: dict[str, Any]
    ) -> None:
        stored["embeds"]["pronouns"] = rendered(title=None, description=None)

        embed = build_embed("pronouns")

        assert embed.title is None and embed.description is None

    async def test_a_key_with_no_row_says_which_one(
        self, stored: dict[str, Any]
    ) -> None:
        with pytest.raises(KeyError, match="No discord_embed row keyed 'ghost'"):
            build_embed("ghost")


class TestRolePanels:
    async def test_pairs_each_embed_with_its_view_and_the_channel_it_is_for(
        self, stored: dict[str, Any]
    ) -> None:
        ((embed, view, channel_id),) = role_panels("roles")

        assert embed.title == "Pick a role"
        assert isinstance(view, RolePickerView) and len(view.children) == 2
        assert channel_id == 500

    async def test_keeps_the_stored_order(self, stored: dict[str, Any]) -> None:
        stored["embeds"] |= {"a": rendered(title="A"), "b": rendered(title="B")}
        stored["order"]["roles"] = ["b", "pronouns", "a"]

        titles = [embed.title for embed, _, _ in role_panels("roles")]

        assert titles == ["B", "Pick a role", "A"]

    async def test_a_channel_with_no_embeds_is_an_empty_list_even_with_no_channel_row(
        self, stored: dict[str, Any]
    ) -> None:
        """Nothing configured is not a failed command."""
        assert role_panels("nowhere") == []

    async def test_embeds_for_a_channel_that_has_no_row_fail_loudly(
        self, stored: dict[str, Any]
    ) -> None:
        stored["order"]["lost"] = ["pronouns"]

        with pytest.raises(KeyError, match="discord_channel"):
            role_panels("lost")


class TestPersistentViews:
    async def test_one_view_per_stored_embed(self, stored: dict[str, Any]) -> None:
        stored["embeds"] |= {"other": rendered()}

        made = persistent_views()

        assert len(made) == 2
        assert all(isinstance(v, RolePickerView) for v in made)

    async def test_none_stored_is_none_registered(self, stored: dict[str, Any]) -> None:
        stored["embeds"].clear()

        assert persistent_views() == []
