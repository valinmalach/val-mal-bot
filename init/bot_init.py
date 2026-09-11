import asyncio
import logging

import discord
from discord.ext.commands import Bot

from background import fire_and_forget
from errors import notify, report

logger = logging.getLogger(__name__)

_startup_announced = False


async def run_background_tasks() -> None:
    """Bring the per-process helpers back up, saying which one failed.

    The arms are gathered so one cannot delay the other, and their results are
    read: `return_exceptions=True` on its own is silence, because the gather
    completes normally and `background._finished` sees nothing to report. An
    unreachable database at boot would then leave every stored alert without an
    updater for the life of the process with nothing said. `main()` reads its
    cog-loading results the same way.

    Each arm is named for what was lost rather than for the function that lost
    it, because that is what the admin channel needs to act on.
    """
    # Deferred: both reach services.send, which imports this package for `bot`.
    from services.twitch import live_alert, stream_session

    arms = (
        ("restore the live alert updaters", live_alert.restore_all()),
        ("resume the stream session", stream_session.resume()),
    )
    results = await asyncio.gather(*(arm for _, arm in arms), return_exceptions=True)
    for (lost, _), result in zip(arms, results, strict=True):
        if isinstance(result, Exception):
            await report(result, f"Startup could not {lost}")


class MyBot(Bot):
    def __init__(self, *, command_prefix: str, intents: discord.Intents) -> None:
        super().__init__(command_prefix=command_prefix, intents=intents)
        self.case_insensitive = True

    async def setup_hook(self) -> None:
        from services.config import config
        from services.twitch.shoutout_queue import shoutout_queue
        from services.twitch.token_manager import token_manager

        await config.load()
        await token_manager.load()

        # Here rather than in on_ready, which fires again on every gateway
        # reconnect: one drainer is wanted for the life of the process, and it
        # idles until a session puts something in the queue.
        fire_and_forget(shoutout_queue.drain(), name="shoutout-queue")

        self.command_prefix = config.setting("command_prefix", "$")
        guild = discord.Object(id=config.setting("guild_id"))

        self.tree.copy_global_to(guild=guild)
        await self.tree.sync(guild=guild)

        from views import persistent_views

        # register all persistent Views so buttons still work after a restart
        for view in persistent_views():
            self.add_view(view)

    async def close(self) -> None:
        from db.session import dispose_engine

        await super().close()
        await dispose_engine()


bot = MyBot(command_prefix="$", intents=discord.Intents.all())


@bot.tree.error
async def on_app_command_error(
    interaction: discord.Interaction, error: discord.app_commands.AppCommandError
) -> None:
    """The floor under every slash command.

    Each command guards its own body, so this only fires when one forgets - which
    is exactly the case where nobody would otherwise hear about it, and the
    person who ran it would be left on a spinner.
    """
    from errors import report
    from services.config import config

    command = interaction.command.qualified_name if interaction.command else "unknown"
    # Reported first: the original failure is the thing that must be recorded,
    # whatever happens when this tries to answer.
    await report(error, f"Unhandled error in /{command}")

    try:
        # Composing the answer reads configuration, which is its own way to fail.
        text = config.template("command_failed")
        if interaction.response.is_done():
            await interaction.followup.send(text, ephemeral=True)
        else:
            await interaction.response.send_message(text, ephemeral=True)
    except Exception as unanswerable:  # noqa: BLE001
        await report(unanswerable, f"Could not tell anyone that /{command} failed")


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
