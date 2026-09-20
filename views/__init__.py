"""Role-picker embeds and their buttons, built from the database.

Nothing here is importable state: an embed or view is constructed on demand, so
editing a row changes what the next /rules or /roles posts without a redeploy.
"""

from discord import Colour, Embed, Interaction
from discord.ui import Button, View

from valmal.core.config import config

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


# Unannotated on purpose: Callable[[Interaction], ...] drops the parameter's name,
# which Button.callback's signature requires.
def _toggle(button: Button[View]):  # noqa: ANN202
    async def callback(interaction: Interaction) -> None:
        from services.roles import roles_button_pressed

        await roles_button_pressed(interaction, button)

    return callback


def build_embed(key: str) -> Embed:
    """One stored embed as a Discord object; config has already resolved its text."""
    stored = config.embed(key)
    if stored is None:
        raise KeyError(f"No discord_embed row keyed {key!r}")

    embed = Embed(
        title=stored.title,
        description=stored.description,
        color=stored.color if stored.color is not None else DEFAULT_COLOUR,
    )
    for field in stored.fields:
        embed.add_field(name=field.name, value=field.value, inline=field.inline)
    return embed


def role_panels(channel_key: str) -> list[tuple[Embed, RolePickerView, int]]:
    """Every embed destined for one channel, in stored order."""
    # config.channel stays inside: it raises for a key with no row, and hoisting
    # it would turn "nothing is configured for this panel" from an empty list
    # into a failed command.
    return [
        (build_embed(key), RolePickerView(key), config.channel(channel_key))
        for key in config.embed_keys_for_channel(channel_key)
    ]


def persistent_views() -> list[RolePickerView]:
    """A view per embed, so buttons keep working after a restart."""
    return [RolePickerView(key) for key in config.embed_keys()]
