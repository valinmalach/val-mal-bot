import asyncio
import logging

import discord
import polars as pl
import sentry_sdk
from discord import CategoryChannel, ForumChannel
from discord.abc import PrivateChannel
from discord.ext.commands import Bot

from constants import BOT_ADMIN_CHANNEL, GUILD_ID

MY_GUILD = discord.Object(id=GUILD_ID)
logger = logging.getLogger(__name__)


async def restart_live_alert_tasks() -> None:
    from services import update_alert

    df = pl.read_parquet("data/live_alerts.parquet")

    for alert in df.iter_rows(named=True):
        broadcaster_id = alert["id"]
        channel_id = alert["channel_id"]
        message_id = alert["message_id"]
        stream_id = alert["stream_id"]
        stream_started_at = alert["stream_started_at"]
        asyncio.create_task(
            update_alert(
                broadcaster_id=broadcaster_id,
                channel_id=channel_id,
                message_id=message_id,
                stream_id=stream_id,
                stream_started_at=stream_started_at,
            )
        )
        await asyncio.sleep(1)


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
    asyncio.create_task(restart_live_alert_tasks())

    channel = bot.get_channel(BOT_ADMIN_CHANNEL)
    if channel is None or isinstance(
        channel, (ForumChannel, CategoryChannel, PrivateChannel)
    ):
        return
    await channel.send("Started successfully!")
