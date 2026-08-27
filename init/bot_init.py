import asyncio
import logging

import discord
from discord import CategoryChannel, ForumChannel
from discord.abc import PrivateChannel
from discord.ext.commands import Bot

logger = logging.getLogger(__name__)


async def restart_live_alert_tasks() -> None:
    from db import repository
    from services import start_alert_updater

    for alert in await repository.list_live_alerts():
        start_alert_updater(
            broadcaster_id=alert.broadcaster_id,
            channel_id=alert.channel_id,
            message_id=alert.message_id,
            stream_id=alert.stream_id,
            stream_started_at=alert.stream_started_at.isoformat(),
        )
        await asyncio.sleep(1)


async def activate_if_live() -> None:
    from services import get_stream_info
    from services.config import config
    from services.twitch.shoutout_queue import shoutout_queue

    stream_info = await get_stream_info(int(config.setting("twitch_broadcaster_id")))
    if stream_info and stream_info.type == "live":
        _ = asyncio.create_task(shoutout_queue.activate())


async def run_background_tasks():
    await asyncio.gather(
        restart_live_alert_tasks(), activate_if_live(), return_exceptions=True
    )


class MyBot(Bot):
    def __init__(self, *, command_prefix: str, intents: discord.Intents) -> None:
        super().__init__(command_prefix=command_prefix, intents=intents)
        self.case_insensitive = True

    async def setup_hook(self) -> None:
        from services.config import config
        from services.twitch.token_manager import token_manager

        await config.load()
        await token_manager.load()

        self.command_prefix = config.setting("command_prefix", "$")
        guild = discord.Object(id=config.setting("guild_id"))

        self.tree.copy_global_to(guild=guild)
        await self.tree.sync(guild=guild)

        from views import persistent_views

        # register all persistent Views so buttons still work after a restart
        for view in persistent_views():
            self.add_view(view)

    async def close(self) -> None:
        from db import dispose_engine

        await super().close()
        await dispose_engine()


bot = MyBot(command_prefix="$", intents=discord.Intents.all())


@bot.event
async def on_ready() -> None:
    from services.config import config

    _ = asyncio.create_task(run_background_tasks())

    channel = bot.get_channel(config.channel("bot_admin"))
    if channel is None or isinstance(
        channel, (ForumChannel, CategoryChannel, PrivateChannel)
    ):
        return
    await channel.send(config.template("discord_startup"))
