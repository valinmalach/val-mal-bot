import asyncio
import logging
import os

import discord
from discord import CategoryChannel, ForumChannel
from discord.abc import PrivateChannel
from discord.ext.commands import Bot
from dotenv import load_dotenv

from constants import BOT_ADMIN_CHANNEL, GUILD_ID, LIVE_ALERTS
from services.helper.parquet_cache import parquet_cache

load_dotenv()

logger = logging.getLogger(__name__)

MY_GUILD = discord.Object(id=GUILD_ID)

TWITCH_BROADCASTER_ID = os.getenv("TWITCH_BROADCASTER_ID")


async def restart_live_alert_tasks() -> None:
    from services import read_parquet_cached, update_alert

    df = await read_parquet_cached(LIVE_ALERTS)

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


async def activate_if_live() -> None:
    from services import get_stream_info
    from services.twitch.shoutout_queue import shoutout_queue

    if not TWITCH_BROADCASTER_ID:
        return

    stream_info = await get_stream_info(int(TWITCH_BROADCASTER_ID))
    if stream_info and stream_info.type == "live":
        asyncio.create_task(shoutout_queue.activate())


class MyBot(Bot):
    def __init__(self, *, command_prefix: str, intents: discord.Intents) -> None:
        super().__init__(command_prefix=command_prefix, intents=intents)
        self.case_insensitive = True

    async def setup_hook(self) -> None:
        parquet_cache.start()

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

    async def close(self) -> None:
        await parquet_cache.stop()
        await super().close()


bot = MyBot(command_prefix="$", intents=discord.Intents.all())


@bot.event
async def on_ready() -> None:
    asyncio.create_task(restart_live_alert_tasks())
    asyncio.create_task(activate_if_live())

    channel = bot.get_channel(BOT_ADMIN_CHANNEL)
    if channel is None or isinstance(
        channel, (ForumChannel, CategoryChannel, PrivateChannel)
    ):
        return
    await channel.send("Started successfully!")
