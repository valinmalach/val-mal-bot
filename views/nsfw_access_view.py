import discord
from discord import Interaction
from discord.ui import Button, View

from services import roles_button_pressed


class NSFWAccessView(View):
    def __init__(self) -> None:
        super().__init__(timeout=None)

    @discord.ui.button(emoji="🔞")
    async def nsfw_access(self, interaction: Interaction, button: Button) -> None:
        await roles_button_pressed(interaction, button)


NSFW_ACCESS_EMBED = discord.Embed(
    title="🔞NSFW Access",
    color=discord.Color.dark_blue(),
)
