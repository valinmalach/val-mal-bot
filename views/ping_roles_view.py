import discord
from discord import Interaction
from discord.ui import Button, View

from constants import (
    ANNOUNCEMENTS_ROLE,
    BLUESKY_ROLE,
    FREE_STUFF_ROLE,
    LIVE_ALERTS_ROLE,
    PING_ROLE,
)
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
        name=f"<@&{ANNOUNCEMENTS_ROLE}>",
        value="",
        inline=False,
    )
    .add_field(
        name=f"<@&{LIVE_ALERTS_ROLE}>",
        value="",
        inline=False,
    )
    .add_field(
        name=f"<@&{PING_ROLE}>",
        value="",
        inline=False,
    )
    .add_field(
        name=f"<@&{BLUESKY_ROLE}>",
        value="",
        inline=False,
    )
    .add_field(
        name=f"<@&{FREE_STUFF_ROLE}>",
        value="",
        inline=False,
    )
)
