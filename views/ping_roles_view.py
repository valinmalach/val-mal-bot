import discord
from discord import Interaction
from discord.ui import Button, View

from services import roles_button_pressed


class PingRolesView(View):
    def __init__(self) -> None:
        super().__init__(timeout=None)

    @discord.ui.button(emoji="📢")
    async def announcements_role(
        self, interaction: Interaction, button: Button
    ) -> None:
        await roles_button_pressed(interaction, button)

    @discord.ui.button(emoji="🔴")
    async def live_alert_role(self, interaction: Interaction, button: Button) -> None:
        await roles_button_pressed(interaction, button)

    @discord.ui.button(emoji="❗")
    async def general_ping_role(self, interaction: Interaction, button: Button) -> None:
        await roles_button_pressed(interaction, button)

    @discord.ui.button(emoji="🦋")
    async def bluesky_role(self, interaction: Interaction, button: Button) -> None:
        await roles_button_pressed(interaction, button)

    @discord.ui.button(emoji="🎁")
    async def free_stuff_role(self, interaction: Interaction, button: Button) -> None:
        await roles_button_pressed(interaction, button)


PING_ROLES_EMBED = (
    discord.Embed(
        title="Ping Roles",
        color=discord.Color.dark_blue(),
    )
    .add_field(
        name="📢Announcements",
        value="",
        inline=False,
    )
    .add_field(
        name="🔴Live Alerts",
        value="",
        inline=False,
    )
    .add_field(
        name="❗Ping Role",
        value="",
        inline=False,
    )
    .add_field(
        name="🦋Bluesky",
        value="",
        inline=False,
    )
    .add_field(
        name="🎁Free Stuff",
        value="",
        inline=False,
    )
)
