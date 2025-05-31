import asyncio

import sentry_sdk
from discord import (
    CategoryChannel,
    DMChannel,
    ForumChannel,
    GroupChannel,
    Interaction,
    app_commands,
)
from discord.ext.commands import Bot, Cog


class Admin(Cog):
    def __init__(self, bot: Bot) -> None:
        self.bot = bot

    @app_commands.command(description="Restarts the bot")
    @app_commands.commands.default_permissions(administrator=True)
    async def restart(self, interaction: Interaction) -> None:
        await interaction.response.send_message("Restarting...")
        await asyncio.create_subprocess_exec(
            "powershell.exe", "-File", "C:\\val-mal-bot\\restart_bot.ps1"
        )

    @app_commands.command(description="Deletes all messages in the channel")
    @app_commands.commands.default_permissions(administrator=True)
    @sentry_sdk.trace()
    async def nuke(self, interaction: Interaction) -> None:
        if interaction.channel is None or isinstance(
            interaction.channel,
            (ForumChannel, CategoryChannel, DMChannel, GroupChannel),
        ):
            return
        await interaction.response.send_message("Nuking channel...")
        await interaction.channel.purge(limit=100000)


async def setup(bot: Bot) -> None:
    await bot.add_cog(Admin(bot))
