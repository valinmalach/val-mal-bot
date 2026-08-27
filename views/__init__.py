"""Role-picker embeds and their buttons, built from the database.

Nothing here is importable state: an embed or view is constructed on demand, so
editing a row changes what the next /rules or /roles posts without a redeploy.
"""

from discord import Colour, Embed, Interaction
from discord.ui import Button, View

from services.config import config

__all__ = ["RolePickerView", "build_embed", "persistent_views", "role_panels"]

DEFAULT_COLOUR = Colour.dark_blue().value


class RolePickerView(View):
    """One toggle button per role listed on the given embed."""

    def __init__(self, embed_key: str) -> None:
        super().__init__(timeout=None)
        for role in config.roles_for_embed(embed_key):
            if not role.emoji or not role.custom_id:
                continue
            button: Button[View] = Button(emoji=role.emoji, custom_id=role.custom_id)
            button.callback = _toggle(button)
            self.add_item(button)


def _toggle(button: Button[View]):
    async def callback(interaction: Interaction) -> None:
        from services import roles_button_pressed

        await roles_button_pressed(interaction, button)

    return callback


def build_embed(key: str) -> Embed:
    """Render one stored embed, resolving channel and role placeholders."""
    stored = config.embed(key)
    if stored is None:
        raise KeyError(f"No discord_embed row keyed {key!r}")

    embed = Embed(
        title=stored.title,
        description=config.render(stored.description) if stored.description else None,
        color=stored.color if stored.color is not None else DEFAULT_COLOUR,
    )
    for field in config.embed_fields(key):
        embed.add_field(
            name=field.name,
            value=config.render(field.value),
            inline=field.inline,
        )
    return embed


def role_panels(channel_key: str) -> list[tuple[Embed, RolePickerView, int]]:
    """Every embed destined for one channel, in stored order."""
    panels = []
    for key in config.embed_keys():
        stored = config.embed(key)
        if stored is None or stored.channel_key != channel_key:
            continue
        panels.append(
            (build_embed(key), RolePickerView(key), config.channel(channel_key))
        )
    return panels


def persistent_views() -> list[RolePickerView]:
    """A view per embed, so buttons keep working after a restart."""
    return [RolePickerView(key) for key in config.embed_keys()]
