import discord
from discord import Interaction
from discord.ui import Button, View

from constants import (
    ASK_TO_DM_ROLE,
    DM_REQUESTS_CHANNEL,
    DMS_CLOSED_ROLE,
    DMS_OPEN_ROLE,
)
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
        title="# DMs Open?",
        color=discord.Color.dark_blue(),
    )
    .add_field(
        name="",
        value=f"<@&{DMS_OPEN_ROLE}>",
        inline=False,
    )
    .add_field(
        name="",
        value=f"<@&{ASK_TO_DM_ROLE}> (Ask in <#{DM_REQUESTS_CHANNEL}>)",
        inline=False,
    )
    .add_field(
        name="",
        value=f"<@&{DMS_CLOSED_ROLE}>",
        inline=False,
    )
)
