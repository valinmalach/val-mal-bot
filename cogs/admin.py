import asyncio
import logging
from typing import List

import discord
from discord import (
    CategoryChannel,
    DMChannel,
    ForumChannel,
    GroupChannel,
    Interaction,
    app_commands,
)
from discord.ext.commands import Bot, Cog

from constants import ROLES_CHANNEL, RULES_CHANNEL
from services import (
    get_subscriptions,
    get_users,
    send_embed,
    subscribe_to_user,
)
from views import (
    DMS_OPEN_EMBED,
    NSFW_ACCESS_EMBED,
    OTHER_ROLES_EMBED,
    PING_ROLES_EMBED,
    PRONOUN_ROLES_EMBED,
    RULES_EMBED,
    DMsOpenView,
    NSFWAccessView,
    OtherRolesView,
    PingRolesView,
    PronounRolesView,
    RulesView,
)

logger = logging.getLogger(__name__)


class Admin(Cog):
    def __init__(self, bot: Bot) -> None:
        self.bot = bot

    @app_commands.command(description="Restarts the bot")
    @app_commands.commands.default_permissions(administrator=True)
    async def restart(self, interaction: Interaction) -> None:
        from services.helper.parquet_cache import parquet_cache

        await interaction.response.send_message("Restarting...")
        await parquet_cache.stop()
        await asyncio.create_subprocess_exec(
            "powershell.exe", "-File", "C:\\val-mal-bot\\restart_bot.ps1"
        )

    @app_commands.command(description="Deletes all messages in the channel")
    @app_commands.commands.default_permissions(administrator=True)
    async def nuke(self, interaction: Interaction) -> None:
        if interaction.channel is None or isinstance(
            interaction.channel,
            (ForumChannel, CategoryChannel, DMChannel, GroupChannel),
        ):
            logger.warning(
                f"Nuke aborted: invalid channel type {type(interaction.channel)}"
            )
            return
        await interaction.response.send_message("Nuking channel...")
        await interaction.channel.purge(limit=None)

    @app_commands.command(description="Sends the rules embed to the rules channel")
    @app_commands.commands.default_permissions(administrator=True)
    async def rules(self, interaction: Interaction) -> None:
        embed = RULES_EMBED
        view = RulesView()

        await send_embed(
            embed,
            RULES_CHANNEL,
            view,
        )
        await interaction.response.send_message("Rules embed send to rules channel!")

    @app_commands.command(description="Sends the roles embeds to the roles channel")
    @app_commands.commands.default_permissions(administrator=True)
    async def roles(self, interaction: Interaction) -> None:
        embeds = [
            PING_ROLES_EMBED,
            NSFW_ACCESS_EMBED,
            PRONOUN_ROLES_EMBED,
            OTHER_ROLES_EMBED,
            DMS_OPEN_EMBED,
        ]
        views = [
            PingRolesView(),
            NSFWAccessView(),
            PronounRolesView(),
            OtherRolesView(),
            DMsOpenView(),
        ]

        for embed, view in zip(embeds, views):
            await send_embed(embed, ROLES_CHANNEL, view)
        await interaction.response.send_message("Roles embeds send to roles channel!")

    @app_commands.command(description="Gets all active subscriptions' users")
    @app_commands.commands.default_permissions(administrator=True)
    async def subscriptions(self, interaction: Interaction) -> None:
        subscriptions = await get_subscriptions()
        if not subscriptions:
            embed = discord.Embed(
                title="No Subscriptions",
                description="There are no active subscriptions.",
                color=discord.Color.red(),
            )
            await interaction.response.send_message(embed=embed)
            return

        grouped_subscriptions: dict[str, List[str]] = {}
        for subscription in subscriptions:
            sub_type = subscription.type
            if sub_type not in grouped_subscriptions:
                grouped_subscriptions[sub_type] = []
            if subscription.condition.broadcaster_user_id:
                grouped_subscriptions[sub_type].append(
                    subscription.condition.broadcaster_user_id
                )
        embed = discord.Embed(
            title="Active Subscriptions",
            description="Here are the active subscriptions grouped by type.",
            color=discord.Color.blue(),
        )
        for sub_type, user_ids in grouped_subscriptions.items():
            if not user_ids:
                continue
            users = await get_users(user_ids)
            if not users:
                continue
            user_names = [user.display_name for user in users if user]
            if not user_names:
                continue
            user_names.sort()
            embed.add_field(
                name=sub_type,
                value=f"* {'\n* '.join(user_names)}",
                inline=False,
            )
        await interaction.response.send_message(embed=embed)

    @app_commands.command(
        description="Subscribe to online and offline events for a user"
    )
    @app_commands.commands.default_permissions(administrator=True)
    @app_commands.describe(
        username="The username of the user to subscribe to",
    )
    async def subscribe(self, interaction: Interaction, username: str) -> None:
        success = await subscribe_to_user(username)
        await interaction.response.send_message(
            content=f"Subscribed to {username}"
            if success
            else f"Failed to subscribe to {username}"
        )


async def setup(bot: Bot) -> None:
    await bot.add_cog(Admin(bot))
