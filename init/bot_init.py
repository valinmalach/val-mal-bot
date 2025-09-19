import asyncio
import logging

import discord
import pandas as pd
import sentry_sdk
from discord import CategoryChannel, ForumChannel
from discord.abc import PrivateChannel
from discord.ext.commands import Bot

from constants import BOT_ADMIN_CHANNEL, GUILD_ID

MY_GUILD = discord.Object(id=GUILD_ID)
logger = logging.getLogger(__name__)


async def restart_live_alert_tasks() -> None:
    from services import update_alert

    logger.info("Attempting to restart live alert tasks.")

    logger.info("Getting live alert records.")
    df = pd.read_parquet("data/live_alerts.parquet")
    logger.info(f"Successfully fetched live alert records. Found {len(df)} records.")

    logger.info(f"Processing {len(df)} records.")
    for _, alert in df.iterrows():
        broadcaster_id = str(alert["id"])
        channel_id = int(alert["channel_id"])
        message_id = int(alert["message_id"])
        stream_id = str(alert["stream_id"])
        stream_started_at = str(alert["stream_started_at"])
        logger.info(
            f"Processing alert: broadcaster_id={broadcaster_id}, channel_id={channel_id}, message_id={message_id}, stream_id={stream_id}, stream_started_at={stream_started_at}"
        )
        asyncio.create_task(
            update_alert(
                broadcaster_id=broadcaster_id,
                channel_id=channel_id,
                message_id=message_id,
                stream_id=stream_id,
                stream_started_at=stream_started_at,
            )
        )
        logger.info(
            f"Created task to update alert for broadcaster_id={broadcaster_id}."
        )

    logger.info("Finished restarting live alert tasks.")


class MyBot(Bot):
    def __init__(self, *, command_prefix: str, intents: discord.Intents) -> None:
        super().__init__(command_prefix=command_prefix, intents=intents)
        self.case_insensitive = True

    async def setup_hook(self) -> None:
        self.tree.copy_global_to(guild=MY_GUILD)
        await self.tree.sync(guild=MY_GUILD)

        from views import (
            DMsOpenView,
            NSFWAccessView,
            OtherRolesView,
            PingRolesView,
            PronounRolesView,
            RulesView,
        )

        # register all persistent Views so buttons still work after a restart
        self.add_view(RulesView())
        self.add_view(PingRolesView())
        self.add_view(NSFWAccessView())
        self.add_view(PronounRolesView())
        self.add_view(OtherRolesView())
        self.add_view(DMsOpenView())


bot = MyBot(command_prefix="$", intents=discord.Intents.all())


@bot.event
@sentry_sdk.trace()
async def on_ready() -> None:
    logger.info("Creating task to restart live alert tasks from Xata.")
    asyncio.create_task(restart_live_alert_tasks())

    channel = bot.get_channel(BOT_ADMIN_CHANNEL)
    if channel is None or isinstance(
        channel, (ForumChannel, CategoryChannel, PrivateChannel)
    ):
        return
    await channel.send("Started successfully!")
