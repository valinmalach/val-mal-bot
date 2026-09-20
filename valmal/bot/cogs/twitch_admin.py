import discord
from discord import Interaction, app_commands
from discord.ext.commands import Bot, Cog
from discord.utils import escape_markdown

from valmal.bot.present import quoted
from valmal.core.config import config
from valmal.core.errors import report
from valmal.db.models.enums import TokenType
from valmal.twitch.client.api import (
    get_subscriptions,
    get_users,
    subscribe_to_user,
    unsubscribe_to_user,
)
from valmal.twitch.client.helix import HelixError
from valmal.twitch.eventsub.commands import is_twitch_login
from valmal.twitch.eventsub.migrate import migrate
from valmal.twitch.eventsub.migrate_plan import summary
from valmal.twitch.eventsub.router import WEBHOOK_PATHS
from valmal.twitch.oauth.grants import create_authorization_start_url


def _login(value: str) -> str | None:
    """The Twitch login this names, or None. Leading @ is how people type one."""
    candidate = value.strip().removeprefix("@")
    return candidate if is_twitch_login(candidate) else None


# Discord refuses a whole message when one field's value goes past this, so a long
# list is split across fields instead of being cut off.
_FIELD_VALUE_LIMIT = 1024


def _bulleted(names: list[str]) -> list[str]:
    """The names as bullet lines, escaped and packed into as few field values as fit.

    A display name is chosen by the person it belongs to, and an underscore in
    one is enough to italicise the rest of the list, so each is escaped here.
    """
    values: list[str] = []
    current = ""
    for name in names:
        line = f"* {escape_markdown(name)}"[:_FIELD_VALUE_LIMIT]
        if current and len(current) + 1 + len(line) > _FIELD_VALUE_LIMIT:
            values.append(current)
            current = line
        else:
            current = f"{current}\n{line}" if current else line
    if current:
        values.append(current)
    return values


async def _refuse_login(interaction: Interaction, value: str) -> None:
    # Stripped of mentions here, escaped and truncated by `quoted`: this is the
    # one string in this file that carries what somebody typed, and it is only
    # ever reached by a value that failed _login - so unlike a real login it
    # can hold anything at all.
    await interaction.response.send_message(
        f"{quoted(value)} is not a Twitch username:"
        " 1-25 characters, letters, digits and underscore.",
        ephemeral=True,
        allowed_mentions=discord.AllowedMentions.none(),
    )


class TwitchAdmin(Cog):
    def __init__(self, bot: Bot) -> None:
        self.bot = bot

    @app_commands.command(
        name="twitch-auth",
        description="Generate owner-only links for the two Twitch user grants",
    )
    @app_commands.commands.default_permissions(administrator=True)
    # No has_permissions here: checks are ANDed, and the owner check below
    # already enforces a runtime identity nothing can reconfigure away. Adding
    # "administrator" on top would block the owner in any guild where they
    # are not also an admin, for no gain.
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

    @app_commands.command(description="Gets all subscriptions' users")
    @app_commands.commands.default_permissions(administrator=True)
    @app_commands.checks.has_permissions(administrator=True)
    async def subscriptions(self, interaction: Interaction) -> None:
        try:
            subscriptions = await get_subscriptions()
        except HelixError as e:
            await report(e, "Failed to list Twitch subscriptions")
            await interaction.response.send_message(
                "Could not reach Twitch to list subscriptions."
            )
            return

        if not subscriptions:
            embed = discord.Embed(
                title="No Subscriptions",
                description="There are no subscriptions.",
                color=discord.Color.red(),
            )
            await interaction.response.send_message(embed=embed)
            return

        grouped_subscriptions: dict[str, list[str]] = {}
        for subscription in subscriptions:
            # A subscription Twitch has disabled delivers nothing, and looks
            # identical here to a working one unless its status is on the label.
            sub_type = (
                subscription.type
                if subscription.status == "enabled"
                else f"{subscription.type} ({subscription.status})"
            )
            if sub_type not in grouped_subscriptions:
                grouped_subscriptions[sub_type] = []
            if subscription.condition.broadcaster_user_id:
                grouped_subscriptions[sub_type].append(
                    subscription.condition.broadcaster_user_id
                )
        embed = discord.Embed(
            title="Subscriptions",
            description=(
                "Here are the subscriptions grouped by type. Anything not "
                "enabled is labelled with its status and will not deliver."
            ),
            color=discord.Color.blue(),
        )
        for sub_type, user_ids in grouped_subscriptions.items():
            if not user_ids:
                continue
            try:
                users = await get_users(user_ids)
            except HelixError as e:
                await report(e, f"Failed to fetch users for {sub_type}")
                continue
            if not users:
                continue
            user_names = [user.display_name for user in users if user]
            if not user_names:
                continue
            user_names.sort()
            for number, value in enumerate(_bulleted(user_names)):
                embed.add_field(
                    name=sub_type if number == 0 else f"{sub_type} (continued)",
                    value=value,
                    inline=False,
                )
        await interaction.response.send_message(embed=embed)

    @app_commands.command(
        description="Subscribe to online and offline events for a user"
    )
    @app_commands.commands.default_permissions(administrator=True)
    @app_commands.checks.has_permissions(administrator=True)
    @app_commands.describe(
        username="The username of the user to subscribe to",
    )
    async def subscribe(self, interaction: Interaction, username: str) -> None:
        login = _login(username)
        if login is None:
            await _refuse_login(interaction, username)
            return

        try:
            found = await subscribe_to_user(login)
        except HelixError as e:
            await report(e, f"Failed to subscribe {login}")
            await interaction.response.send_message(
                content=f"Could not reach Twitch to subscribe {escape_markdown(login)}"
            )
            return

        await interaction.response.send_message(
            content=f"Subscribed to {escape_markdown(login)}"
            if found
            else f"No Twitch user called {escape_markdown(login)}"
        )

    @app_commands.command(
        name="migrate-subscriptions",
        description="Repoint every EventSub subscription at this deployment's URL",
    )
    @app_commands.commands.default_permissions(administrator=True)
    # No has_permissions: see the comment on twitch_auth above, same reasoning.
    @app_commands.describe(
        confirm="Actually do it. Without this it reports what it would do and stops.",
    )
    async def migrate_subscriptions(
        self, interaction: Interaction, confirm: bool = False
    ) -> None:
        if interaction.user.id != config.setting("owner_id"):
            await interaction.response.send_message(
                "Only the configured bot owner can migrate Twitch subscriptions.",
                ephemeral=True,
            )
            return

        # Helix is asked once per subscription and then twice more per repoint,
        # which is well past Discord's three seconds.
        await interaction.response.defer(ephemeral=True)
        try:
            outcome = await migrate(WEBHOOK_PATHS, confirm=confirm)
        except HelixError as e:
            await report(e, "Failed to migrate Twitch subscriptions")
            await interaction.followup.send(
                "Could not reach Twitch to list the subscriptions, so nothing was"
                " touched."
            )
            return

        await interaction.followup.send(summary(outcome, confirm=confirm))

    @app_commands.command(
        description="Unsubscribe from online and offline events for a user"
    )
    @app_commands.commands.default_permissions(administrator=True)
    @app_commands.checks.has_permissions(administrator=True)
    @app_commands.describe(
        username="The username of the user to unsubscribe from",
    )
    async def unsubscribe(self, interaction: Interaction, username: str) -> None:
        login = _login(username)
        if login is None:
            await _refuse_login(interaction, username)
            return

        try:
            found = await unsubscribe_to_user(login)
        except HelixError as e:
            await report(e, f"Failed to unsubscribe {login}")
            await interaction.response.send_message(
                content=f"Could not reach Twitch to unsubscribe {escape_markdown(login)}"
            )
            return

        await interaction.response.send_message(
            content=f"Unsubscribed from {escape_markdown(login)}"
            if found
            else f"No Twitch user called {escape_markdown(login)}"
        )


async def setup(bot: Bot) -> None:
    await bot.add_cog(TwitchAdmin(bot))
