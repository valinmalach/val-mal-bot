import asyncio
from typing import List

import discord
import sentry_sdk
from discord import (
    CategoryChannel,
    DMChannel,
    ForumChannel,
    GroupChannel,
    Interaction,
    Message,
    app_commands,
)
from discord.ext.commands import Bot, Cog

from constants import ROLES_CHANNEL, RULES_CHANNEL
from services import get_subscriptions, get_users, send_embed
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

    @app_commands.command(description="Restarts the bot without pip")
    @app_commands.commands.default_permissions(administrator=True)
    async def raw_restart(self, interaction: Interaction) -> None:
        await interaction.response.send_message("Restarting without pip...")
        await asyncio.create_subprocess_exec(
            "powershell.exe", "-File", "C:\\val-mal-bot\\restart_bot_without_pip.ps1"
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
        iterator = interaction.channel.history(limit=100000)
        ret: List[Message] = []
        count = 0

        async for message in iterator:
            if count == 100:
                to_delete = ret[-100:]
                await interaction.channel.delete_messages(to_delete)
                count = 0
                await asyncio.sleep(1)

            count += 1
            ret.append(message)

        # Some messages remaining to poll
        if count >= 2:
            # more than 2 messages -> bulk delete
            to_delete = ret[-count:]
            await interaction.channel.delete_messages(to_delete)
        elif count == 1:
            # delete a single message
            await ret[-1].delete()

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

        # Group subscriptions by type
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


async def setup(bot: Bot) -> None:
    await bot.add_cog(Admin(bot))
