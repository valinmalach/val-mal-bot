import asyncio
import logging
from typing import List

import discord
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

from constants import ROLES_CHANNEL, RULES_CHANNEL
from services import get_subscriptions, get_users, send_embed, subscribe_to_user
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
        logger.info(
            "Restarting bot as requested by user %s in channel %s",
            interaction.user,
            interaction.channel,
        )
        await interaction.response.send_message("Restarting...")
        logger.info("Sent restart confirmation response to %s", interaction.user)
        await asyncio.create_subprocess_exec(
            "powershell.exe", "-File", "C:\\val-mal-bot\\restart_bot.ps1"
        )
        logger.info("Restart script executed")

    @app_commands.command(description="Restarts the bot without pip")
    @app_commands.commands.default_permissions(administrator=True)
    async def raw_restart(self, interaction: Interaction) -> None:
        logger.info(
            "Restarting bot without pip as requested by user %s", interaction.user
        )
        await interaction.response.send_message("Restarting without pip...")
        logger.info("Sent raw restart confirmation to %s", interaction.user)
        await asyncio.create_subprocess_exec(
            "powershell.exe", "-File", "C:\\val-mal-bot\\restart_bot_without_pip.ps1"
        )
        logger.info("Raw restart script executed")

    @app_commands.command(description="Deletes all messages in the channel")
    @app_commands.commands.default_permissions(administrator=True)
    @sentry_sdk.trace()
    async def nuke(self, interaction: Interaction) -> None:
        logger.info(
            "Purging all messages in channel %s as requested by user %s",
            interaction.channel,
            interaction.user,
        )
        if interaction.channel is None or isinstance(
            interaction.channel,
            (ForumChannel, CategoryChannel, DMChannel, GroupChannel),
        ):
            logger.warning(
                "Nuke aborted: invalid channel type %s", type(interaction.channel)
            )
            return
        await interaction.response.send_message("Nuking channel...")
        logger.info("Starting channel purge: %s", interaction.channel)
        await interaction.channel.purge(limit=None)
        logger.info("Channel purge completed: %s", interaction.channel)

    @app_commands.command(description="Sends the rules embed to the rules channel")
    @app_commands.commands.default_permissions(administrator=True)
    async def rules(self, interaction: Interaction) -> None:
        logger.info(
            "Sending rules embed to rules channel as requested by user %s",
            interaction.user,
        )
        embed = RULES_EMBED
        view = RulesView()

        await send_embed(
            embed,
            RULES_CHANNEL,
            view,
        )
        logger.info("Rules embed sent to channel %s", RULES_CHANNEL)
        await interaction.response.send_message("Rules embed send to rules channel!")
        logger.info("Sent confirmation message for rules command")

    @app_commands.command(description="Sends the roles embeds to the roles channel")
    @app_commands.commands.default_permissions(administrator=True)
    async def roles(self, interaction: Interaction) -> None:
        logger.info(
            "Sending roles embeds to roles channel as requested by user %s",
            interaction.user,
        )
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
            logger.info(
                "Sending roles embed %s to channel %s",
                embed.title if hasattr(embed, "title") else embed.description,
                ROLES_CHANNEL,
            )
            await send_embed(embed, ROLES_CHANNEL, view)
        logger.info("All role embeds sent to channel %s", ROLES_CHANNEL)
        await interaction.response.send_message("Roles embeds send to roles channel!")
        logger.info("Sent confirmation message for roles command")

    @app_commands.command(description="Gets all active subscriptions' users")
    @app_commands.commands.default_permissions(administrator=True)
    async def subscriptions(self, interaction: Interaction) -> None:
        logger.info("Fetching active subscriptions for user %s", interaction.user)
        subscriptions = await get_subscriptions()
        logger.info(
            "Fetched %d subscriptions", len(subscriptions) if subscriptions else 0
        )
        if not subscriptions:
            embed = discord.Embed(
                title="No Subscriptions",
                description="There are no active subscriptions.",
                color=discord.Color.red(),
            )
            await interaction.response.send_message(embed=embed)
            logger.info("Sent no subscriptions embed")
            return

        # Group subscriptions by type
        grouped_subscriptions: dict[str, List[str]] = {}
        for subscription in subscriptions:
            sub_type = subscription.type
            logger.info(
                "Grouping subscription of type %s for broadcaster_id %s",
                sub_type,
                subscription.condition.broadcaster_user_id,
            )
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
            logger.info(
                "Building subscription field for type=%s with %d user_ids",
                sub_type,
                len(user_ids),
            )
            if not user_ids:
                continue
            users = await get_users(user_ids)
            logger.info(
                "Fetched %d users for subscription type=%s",
                len(users) if users else 0,
                sub_type,
            )
            if not users:
                continue
            user_names = [user.display_name for user in users if user]
            logger.info("Compiled user display names: %s", user_names)
            if not user_names:
                continue
            user_names.sort()
            embed.add_field(
                name=sub_type,
                value=f"* {'\n* '.join(user_names)}",
                inline=False,
            )
        await interaction.response.send_message(embed=embed)
        logger.info("Sent subscriptions embed with %d fields", len(embed.fields))

    @app_commands.command(
        description="Subscribe to online and offline events for a user"
    )
    @app_commands.commands.default_permissions(administrator=True)
    @app_commands.describe(
        username="The username of the user to subscribe to",
    )
    async def subscribe(self, interaction: Interaction, username: str) -> None:
        logger.info("Subscribing to user %s", username)
        success = await subscribe_to_user(username)
        logger.info(
            f"Subscribed to user {username}"
            if success
            else f"Failed to subscribe to {username}"
        )
        await interaction.response.send_message(
            content=f"Subscribed to {username}"
            if success
            else f"Failed to subscribe to {username}"
        )


async def setup(bot: Bot) -> None:
    await bot.add_cog(Admin(bot))
