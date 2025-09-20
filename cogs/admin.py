import asyncio
import logging
from typing import List

import discord
import polars as pl
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
from services import (
    delete_row_from_parquet,
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
        logger.info(
            f"Restarting bot as requested by user {interaction.user} in channel {interaction.channel}"
        )
        await interaction.response.send_message("Restarting...")
        logger.info(f"Sent restart confirmation response to {interaction.user}")
        await asyncio.create_subprocess_exec(
            "powershell.exe", "-File", "C:\\val-mal-bot\\restart_bot.ps1"
        )
        logger.info("Restart script executed")

    @app_commands.command(description="Restarts the bot without uv sync")
    @app_commands.commands.default_permissions(administrator=True)
    async def raw_restart(self, interaction: Interaction) -> None:
        logger.info(
            f"Restarting bot without uv sync as requested by user {interaction.user}"
        )
        await interaction.response.send_message("Restarting without uv sync...")
        logger.info(f"Sent raw restart confirmation to {interaction.user}")
        await asyncio.create_subprocess_exec(
            "powershell.exe",
            "-File",
            "C:\\val-mal-bot\\restart_bot_without_uv_sync.ps1",
        )
        logger.info("Raw restart script executed")

    @app_commands.command(description="Deletes all messages in the channel")
    @app_commands.commands.default_permissions(administrator=True)
    @sentry_sdk.trace()
    async def nuke(self, interaction: Interaction) -> None:
        logger.info(
            f"Purging all messages in channel {interaction.channel} as requested by user {interaction.user}"
        )
        if interaction.channel is None or isinstance(
            interaction.channel,
            (ForumChannel, CategoryChannel, DMChannel, GroupChannel),
        ):
            logger.warning(
                f"Nuke aborted: invalid channel type {type(interaction.channel)}"
            )
            return
        await interaction.response.send_message("Nuking channel...")
        logger.info(f"Starting channel purge: {interaction.channel}")
        await interaction.channel.purge(limit=None)
        logger.info(f"Channel purge completed: {interaction.channel}")

    @app_commands.command(description="Sends the rules embed to the rules channel")
    @app_commands.commands.default_permissions(administrator=True)
    async def rules(self, interaction: Interaction) -> None:
        logger.info(
            f"Sending rules embed to rules channel as requested by user {interaction.user}"
        )
        embed = RULES_EMBED
        view = RulesView()

        await send_embed(
            embed,
            RULES_CHANNEL,
            view,
        )
        logger.info(f"Rules embed sent to channel {RULES_CHANNEL}")
        await interaction.response.send_message("Rules embed send to rules channel!")
        logger.info("Sent confirmation message for rules command")

    @app_commands.command(description="Sends the roles embeds to the roles channel")
    @app_commands.commands.default_permissions(administrator=True)
    async def roles(self, interaction: Interaction) -> None:
        logger.info(
            f"Sending roles embeds to roles channel as requested by user {interaction.user}"
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
                f"Sending roles embed {embed.title if hasattr(embed, 'title') else embed.description} to channel {ROLES_CHANNEL}"
            )
            await send_embed(embed, ROLES_CHANNEL, view)
        logger.info(f"All role embeds sent to channel {ROLES_CHANNEL}")
        await interaction.response.send_message("Roles embeds send to roles channel!")
        logger.info("Sent confirmation message for roles command")

    @app_commands.command(description="Gets all active subscriptions' users")
    @app_commands.commands.default_permissions(administrator=True)
    async def subscriptions(self, interaction: Interaction) -> None:
        logger.info(f"Fetching active subscriptions for user {interaction.user}")
        subscriptions = await get_subscriptions()
        logger.info(
            f"Fetched {len(subscriptions) if subscriptions else 0} subscriptions"
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
                f"Grouping subscription of type {sub_type} for broadcaster_id {subscription.condition.broadcaster_user_id}"
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
                f"Building subscription field for type={sub_type} with {len(user_ids)} user_ids"
            )
            if not user_ids:
                continue
            users = await get_users(user_ids)
            logger.info(
                f"Fetched {len(users) if users else 0} users for subscription type={sub_type}"
            )
            if not users:
                continue
            user_names = [user.display_name for user in users if user]
            logger.info(f"Compiled user display names: {user_names}")
            if not user_names:
                continue
            user_names.sort()
            embed.add_field(
                name=sub_type,
                value=f"* {'\n* '.join(user_names)}",
                inline=False,
            )
        await interaction.response.send_message(embed=embed)
        logger.info(f"Sent subscriptions embed with {len(embed.fields)} fields")

    @app_commands.command(
        description="Subscribe to online and offline events for a user"
    )
    @app_commands.commands.default_permissions(administrator=True)
    @app_commands.describe(
        username="The username of the user to subscribe to",
    )
    async def subscribe(self, interaction: Interaction, username: str) -> None:
        logger.info(f"Subscribing to user {username}")
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

    @app_commands.command(
        description="Delete all messages sent by the bot in messages.parquet"
    )
    @app_commands.commands.default_permissions(administrator=True)
    async def delete_messages(self, interaction: Interaction) -> None:
        logger.info("Deleting all messages sent by the bot")
        try:
            df = pl.read_parquet("data/messages.parquet")
            if self.bot.user is None:
                await interaction.response.send_message(
                    "Bot user is not available. Cannot delete messages."
                )
                return
            rows_to_delete = df.filter(pl.col("author_id") == self.bot.user.id)
            for row in rows_to_delete.iter_rows(named=True):
                message_id = row["id"]
                delete_row_from_parquet("data/messages.parquet", message_id)
                logger.info(f"Deleted message with ID {message_id}")
            logger.info("Deleted all messages sent by the bot")
            await interaction.response.send_message(
                content="Deleted all messages sent by the bot"
            )
        except Exception as e:
            logger.error("Failed to delete messages sent by the bot", exc_info=e)
            await interaction.response.send_message(
                f"Failed to delete messages sent by the bot: {e}"
            )


async def setup(bot: Bot) -> None:
    await bot.add_cog(Admin(bot))
