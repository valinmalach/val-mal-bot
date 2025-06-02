import discord
from discord import Interaction
from discord.ui import Button, View

from services import roles_button_pressed


class DMsOpenView(View):
    def __init__(self) -> None:
        super().__init__(timeout=None)

    @discord.ui.button(emoji="🟩")
    async def dms_open(self, interaction: Interaction, button: Button) -> None:
        await roles_button_pressed(interaction, button)

    @discord.ui.button(emoji="🟨")
    async def ask_to_dm(self, interaction: Interaction, button: Button) -> None:
        await roles_button_pressed(interaction, button)

    @discord.ui.button(emoji="🟥")
    async def dms_closed(self, interaction: Interaction, button: Button) -> None:
        await roles_button_pressed(interaction, button)


DMS_OPEN_EMBED = (
    discord.Embed(
        title="DMs Open?",
        color=discord.Color.dark_blue(),
    )
    .add_field(
        name="🟩DMs Open",
        value="",
        inline=False,
    )
    .add_field(
        name="🟨Ask to DM",
        value="",
        inline=False,
    )
    .add_field(
        name="🟥DMs Closed",
        value="",
        inline=False,
    )
)
