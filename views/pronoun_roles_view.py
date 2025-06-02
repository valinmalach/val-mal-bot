import discord
from discord import Interaction
from discord.ui import Button, View

from constants import HE_HIM_ROLE, OTHER_ASK_ROLE, SHE_HER_ROLE, THEY_THEM_ROLE
from services import roles_button_pressed


class PronounRolesView(View):
    def __init__(self) -> None:
        super().__init__(timeout=None)

    @discord.ui.button(emoji="🙋‍♂️")
    async def he_him(self, interaction: Interaction, button: Button) -> None:
        await roles_button_pressed(interaction, button)

    @discord.ui.button(emoji="🙋‍♀️")
    async def she_her(self, interaction: Interaction, button: Button) -> None:
        await roles_button_pressed(interaction, button)

    @discord.ui.button(emoji="🙋")
    async def they_them(self, interaction: Interaction, button: Button) -> None:
        await roles_button_pressed(interaction, button)

    @discord.ui.button(emoji="❓")
    async def other_ask(self, interaction: Interaction, button: Button) -> None:
        await roles_button_pressed(interaction, button)


PRONOUN_ROLES_EMBED = (
    discord.Embed(
        title="Pronouns",
        color=discord.Color.dark_blue(),
    )
    .add_field(
        name="",
        value=f"<@&{HE_HIM_ROLE}>",
        inline=False,
    )
    .add_field(
        name="",
        value=f"<@&{SHE_HER_ROLE}>",
        inline=False,
    )
    .add_field(
        name="",
        value=f"<@&{THEY_THEM_ROLE}>",
        inline=False,
    )
    .add_field(
        name="",
        value=f"<@&{OTHER_ASK_ROLE}>",
        inline=False,
    )
)
