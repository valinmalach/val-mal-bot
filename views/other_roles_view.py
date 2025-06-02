import discord
from discord import Interaction
from discord.ui import Button, View

from services import roles_button_pressed


class OtherRolesView(View):
    def __init__(self) -> None:
        super().__init__(timeout=None)

    @discord.ui.button(emoji="📽️")
    async def streamer_role(self, interaction: Interaction, button: Button) -> None:
        await roles_button_pressed(interaction, button)

    @discord.ui.button(emoji="🎮")
    async def gamer_role(self, interaction: Interaction, button: Button) -> None:
        await roles_button_pressed(interaction, button)

    @discord.ui.button(emoji="🎨")
    async def artist_role(self, interaction: Interaction, button: Button) -> None:
        await roles_button_pressed(interaction, button)


OTHER_ROLES_EMBED = (
    discord.Embed(
        title="Other Roles",
        color=discord.Color.dark_blue(),
    )
    .add_field(
        name="📽️Streamer",
        value="",
        inline=False,
    )
    .add_field(
        name="🎮Gamer",
        value="",
        inline=False,
    )
    .add_field(
        name="🎨Artist",
        value="",
        inline=False,
    )
)
