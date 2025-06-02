import discord
from discord import Interaction
from discord.ui import Button, View

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
        name="🙋‍♂️He/Him",
        value="",
        inline=False,
    )
    .add_field(
        name="🙋‍♀️She/Her",
        value="",
        inline=False,
    )
    .add_field(
        name="🙋They/Them",
        value="",
        inline=False,
    )
    .add_field(
        name="❓Other/Ask",
        value="",
        inline=False,
    )
)
