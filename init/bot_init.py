import asyncio
import logging

import discord
from discord.ext.commands import Bot

from background import fire_and_forget
from errors import notify

logger = logging.getLogger(__name__)

_startup_announced = False


async def restart_live_alert_tasks() -> None:
    from services.twitch import live_alert

    await live_alert.restore_all()


async def activate_if_live() -> None:
    from errors import report
    from services.config import config
    from services.twitch.api import get_stream
    from services.twitch.helix import HelixError
    from services.twitch.shoutout_queue import shoutout_queue

    try:
        stream = await get_stream(int(config.setting("twitch_broadcaster_id")))
    except HelixError as e:
        # gather(return_exceptions=True) upstream would swallow this silently.
        await report(e, "Could not check whether the broadcaster is live at startup")
        return

    if stream and stream.type == "live":
        fire_and_forget(shoutout_queue.activate(), name="shoutout-queue")


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
    global _startup_announced
    from services.config import config

    fire_and_forget(run_background_tasks(), name="startup-tasks")

    if _startup_announced:
        # on_ready fires again every time the gateway session cannot be resumed,
        # which is a reconnect, not a startup.
        logger.info("Reconnected to Discord")
        return

    # Only a delivered announcement counts as announced: one that could not be
    # sent is tried again on the next reconnect.
    _startup_announced = await notify(config.template("discord_startup"))
