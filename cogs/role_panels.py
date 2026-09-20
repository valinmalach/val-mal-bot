from discord import Interaction, app_commands
from discord.ext.commands import Bot, Cog

from services.send import send_embed
from valmal.core.config import config
from views import role_panels


class RolePanels(Cog):
    def __init__(self, bot: Bot) -> None:
        self.bot = bot

    @app_commands.command(description="Sends the rules embed to the rules channel")
    @app_commands.commands.default_permissions(administrator=True)
    @app_commands.checks.has_permissions(administrator=True)
    async def rules(self, interaction: Interaction) -> None:
        for embed, view, channel_id in role_panels("rules"):
            await send_embed(embed, channel_id, view)
        await interaction.response.send_message(config.template("admin_rules_sent"))

    @app_commands.command(description="Sends the roles embeds to the roles channel")
    @app_commands.commands.default_permissions(administrator=True)
    @app_commands.checks.has_permissions(administrator=True)
    async def roles(self, interaction: Interaction) -> None:
        for embed, view, channel_id in role_panels("roles"):
            await send_embed(embed, channel_id, view)
        await interaction.response.send_message(config.template("admin_roles_sent"))


async def setup(bot: Bot) -> None:
    await bot.add_cog(RolePanels(bot))
