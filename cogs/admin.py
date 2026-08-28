import logging

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

from constants import TokenType
from services import (
    get_subscriptions,
    get_users,
    send_embed,
    subscribe_to_user,
    unsubscribe_to_user,
)
from services.config import config
from services.twitch.oauth import create_authorization_start_url
from views import role_panels

logger = logging.getLogger(__name__)

_PURGE_MESSAGE_LIMIT_MAX = 500


class Admin(Cog):
    def __init__(self, bot: Bot) -> None:
        self.bot = bot

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
        await interaction.response.send_message(config.template("admin_nuking"))
        await interaction.channel.purge(limit=None)

    @app_commands.command(
        description="Deletes the most recent messages in this channel or thread"
    )
    @app_commands.commands.default_permissions(administrator=True)
    @app_commands.describe(
        count="How many messages to delete (max 500; bulk deletes use batches of up to 100; messages older than 14 days are skipped)",
    )
    async def purge(
        self,
        interaction: Interaction,
        count: app_commands.Range[int, 1, _PURGE_MESSAGE_LIMIT_MAX],
    ) -> None:
        ch = interaction.channel
        if ch is None or isinstance(
            ch,
            (ForumChannel, CategoryChannel, DMChannel, GroupChannel),
        ):
            logger.warning("Purge aborted: invalid channel type %s", type(ch))
            await interaction.response.send_message(
                config.template("admin_wrong_channel"),
                ephemeral=True,
            )
            return
        if not hasattr(ch, "purge"):
            await interaction.response.send_message(
                config.template("admin_no_bulk_delete"),
                ephemeral=True,
            )
            return
        await interaction.response.defer(ephemeral=True)
        try:
            deleted = await ch.purge(limit=count)
        except discord.Forbidden:
            await interaction.followup.send(
                config.template("admin_purge_forbidden"),
                ephemeral=True,
            )
            return
        except discord.HTTPException as e:
            logger.exception("Purge failed")
            await interaction.followup.send(
                config.template("admin_purge_failed", error=e),
                ephemeral=True,
            )
            return
        await interaction.followup.send(
            config.template("admin_purge_done", count=len(deleted)),
            ephemeral=True,
        )

    @app_commands.command(description="Sends the rules embed to the rules channel")
    @app_commands.commands.default_permissions(administrator=True)
    async def rules(self, interaction: Interaction) -> None:
        for embed, view, channel_id in role_panels("rules"):
            await send_embed(embed, channel_id, view)
        await interaction.response.send_message(config.template("admin_rules_sent"))

    @app_commands.command(description="Sends the roles embeds to the roles channel")
    @app_commands.commands.default_permissions(administrator=True)
    async def roles(self, interaction: Interaction) -> None:
        for embed, view, channel_id in role_panels("roles"):
            await send_embed(embed, channel_id, view)
        await interaction.response.send_message(config.template("admin_roles_sent"))

    @app_commands.command(
        name="twitch-auth",
        description="Generate owner-only links for the two Twitch user grants",
    )
    @app_commands.commands.default_permissions(administrator=True)
    async def twitch_auth(self, interaction: Interaction) -> None:
        if interaction.user.id != config.setting("owner_id"):
            await interaction.response.send_message(
                "Only the configured bot owner can replace Twitch OAuth grants.",
                ephemeral=True,
            )
            return

        bot_user_id = config.setting("twitch_bot_user_id")
        broadcaster_id = config.setting("twitch_broadcaster_id")
        broadcaster_username = config.setting("broadcaster_username")

        view = discord.ui.View(timeout=600)
        view.add_item(
            discord.ui.Button(
                label="1. Authorize bot account",
                style=discord.ButtonStyle.link,
                url=create_authorization_start_url(TokenType.User),
            )
        )
        view.add_item(
            discord.ui.Button(
                label="2. Authorize broadcaster",
                style=discord.ButtonStyle.link,
                url=create_authorization_start_url(TokenType.Broadcaster),
            )
        )
        await interaction.response.send_message(
            "Create the two missing `oauth_token` grants:\n"
            f"1. **User:** log in as the bot/moderator account with Twitch ID "
            f"`{bot_user_id}`.\n"
            f"2. **Broadcaster:** log in as `{broadcaster_username}` with Twitch ID "
            f"`{broadcaster_id}`.\n\n"
            "Twitch will be forced to show the authorization step. Each link expires "
            "after 10 minutes and can be completed once. The callback rejects a token "
            "from the wrong account or with missing configured scopes.",
            view=view,
            ephemeral=True,
        )

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

        grouped_subscriptions: dict[str, list[str]] = {}
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

    @app_commands.command(
        description="Unsubscribe from online and offline events for a user"
    )
    @app_commands.commands.default_permissions(administrator=True)
    @app_commands.describe(
        username="The username of the user to unsubscribe from",
    )
    async def unsubscribe(self, interaction: Interaction, username: str) -> None:
        success = await unsubscribe_to_user(username)
        await interaction.response.send_message(
            content=f"Unsubscribed from {username}"
            if success
            else f"Failed to unsubscribe from {username}"
        )


async def setup(bot: Bot) -> None:
    await bot.add_cog(Admin(bot))
