import asyncio

import sentry_sdk
from discord import Interaction, app_commands
from discord.ext.commands import Bot, Cog


class Admin(Cog):
    def __init__(self, bot: Bot):
        self.bot = bot

    @sentry_sdk.trace
    @sentry_sdk.monitor
    @app_commands.command(description="Restarts the bot")
    @app_commands.commands.default_permissions(administrator=True)
    async def restart(self, interaction: Interaction):
        await interaction.response.send_message("Restarting...")
        await asyncio.create_subprocess_exec(
            "powershell.exe", "-File", "C:\\val-mal-bot\\restart_bot.ps1"
        )

    @sentry_sdk.trace
    @sentry_sdk.monitor
    @app_commands.command(description="Deletes all messages in the channel")
    @app_commands.commands.default_permissions(administrator=True)
    async def nuke(self, interaction: Interaction):
        await interaction.response.send_message("Nuking channel...")
        await interaction.channel.purge(limit=100000)


async def setup(bot: Bot):
    await bot.add_cog(Admin(bot))
