import logging

import discord
from discord import Interaction, app_commands
from discord.ext.commands import Bot, Cog

from valmal.bot.send import UNSENDABLE_CHANNEL_TYPES
from valmal.core.config import config

logger = logging.getLogger(__name__)

_PURGE_MESSAGE_LIMIT_MAX = 500


async def _purge(interaction: Interaction, limit: int | None) -> None:
    """Shared body for `nuke` (limit=None) and `purge` (limit=count)."""
    ch = interaction.channel
    if ch is None or isinstance(ch, UNSENDABLE_CHANNEL_TYPES):
        # Distinct per caller, as it was before the two shared this body: an
        # operator grepping logs for one command's abort should still find it.
        logger.warning(
            "%s aborted: invalid channel type %s",
            "Nuke" if limit is None else "Purge",
            type(ch),
        )
        await interaction.response.send_message(
            config.template("admin_wrong_channel"), ephemeral=True
        )
        return
    if not hasattr(ch, "purge"):
        await interaction.response.send_message(
            config.template("admin_no_bulk_delete"), ephemeral=True
        )
        return

    if limit is None:
        await interaction.response.send_message(config.template("admin_nuking"))
        await ch.purge(limit=None)
        return

    await interaction.response.defer(ephemeral=True)
    try:
        deleted = await ch.purge(limit=limit)
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


class Moderation(Cog):
    def __init__(self, bot: Bot) -> None:
        self.bot = bot

    @app_commands.command(description="Deletes all messages in the channel")
    @app_commands.commands.default_permissions(administrator=True)
    @app_commands.checks.has_permissions(administrator=True)
    async def nuke(self, interaction: Interaction) -> None:
        await _purge(interaction, None)

    @app_commands.command(
        description="Deletes the most recent messages in this channel or thread"
    )
    @app_commands.commands.default_permissions(administrator=True)
    @app_commands.checks.has_permissions(administrator=True)
    @app_commands.describe(
        count="How many messages to delete (max 500; bulk deletes use batches of up to 100; messages older than 14 days are skipped)",
    )
    async def purge(
        self,
        interaction: Interaction,
        count: app_commands.Range[int, 1, _PURGE_MESSAGE_LIMIT_MAX],
    ) -> None:
        await _purge(interaction, count)


async def setup(bot: Bot) -> None:
    await bot.add_cog(Moderation(bot))
