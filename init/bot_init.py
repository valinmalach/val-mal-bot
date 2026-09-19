import asyncio
import logging
import sys

import discord
from discord.ext.commands import Bot

from background import fire_and_forget
from errors import notify, report

logger = logging.getLogger(__name__)

# Before this there were four different answers to "run once per process"
# scattered across bot_init.py and cogs/tasks.py. What's left is one guard per
# question on_ready actually has to answer: has the Helix/DB-heavy startup
# work run yet (_started, decided once), and has the startup announcement
# been delivered yet (_announced, retried on reconnect until it has).
_started = False
_announced = False


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


async def _answer(
    interaction: discord.Interaction, template_key: str, command: str, verb: str
) -> None:
    from errors import report
    from services.config import config

    try:
        # Composing the answer reads configuration, which is its own way to fail.
        text = config.template(template_key)
        if interaction.response.is_done():
            await interaction.followup.send(text, ephemeral=True)
        else:
            await interaction.response.send_message(text, ephemeral=True)
    except Exception as unanswerable:  # noqa: BLE001
        await report(unanswerable, f"Could not tell anyone that /{command} {verb}")


@bot.tree.error
async def on_app_command_error(
    interaction: discord.Interaction, error: discord.app_commands.AppCommandError
) -> None:
    """The floor under every slash command.

    Two different things land here. A command that forgets to guard its own
    body raises past it - the case where nobody would otherwise hear about it,
    and the person who ran it would be left on a spinner. A `MissingPermissions`
    check failure also lands here, deliberately: `has_permissions` raises it by
    design, so that branch answers the person directly instead of reporting it
    as a bug.
    """
    from errors import report

    command = interaction.command.qualified_name if interaction.command else "unknown"

    if isinstance(error, discord.app_commands.MissingPermissions):
        # A refusal `has_permissions` raises by design - a server admin has
        # reconfigured who may run this command - not a bug, so it answers the
        # person rather than reporting to the admin channel.
        await _answer(
            interaction, "command_no_permission", command, "lacked permission"
        )
        return

    # Reported first: the original failure is the thing that must be recorded,
    # whatever happens when this tries to answer.
    await report(error, f"Unhandled error in /{command}")
    await _answer(interaction, "command_failed", command, "failed")


@bot.event
async def on_error(event_method: str, /, *args: object, **kwargs: object) -> None:
    """The floor under every gateway listener, cog listeners included.

    discord.py wraps each dispatched listener's call in its own try/except and
    calls this from inside it on failure, handing over which `on_*` method it
    was - the one thing that varied across the 13 near-identical
    ``except Exception: await report(...)`` blocks this replaces in
    `cogs/events.py`. `sys.exc_info()` still resolves the exception here,
    since this runs from inside that except block's dynamic scope.
    """
    exc = sys.exc_info()[1]
    if isinstance(exc, Exception):
        await report(exc, f"Fatal error with {event_method} event")


@bot.event
async def on_ready() -> None:
    """The only per-connection entry point left, answering two different
    questions rather than blurring them into one.

    on_ready fires again every time the gateway session cannot be resumed -
    a reconnect, not a restart. Whether `run_background_tasks` has run is
    decided once, by `_started`: the guard is what stops its Helix/DB-heavy
    work from running again on every reconnect, rather than relying on
    `live_alert._start` and `stream_session._start` to no-op it away. Whether
    the startup announcement has been *delivered* is a separate question with
    its own state, `_announced` - a send that fails is worth retrying on the
    next reconnect, so it is not folded into the same guard.
    """
    global _started, _announced
    from services.config import config

    if not _started:
        _started = True
        fire_and_forget(run_background_tasks(), name="startup-tasks")

    if _announced:
        logger.info("Reconnected to Discord")
        return

    _announced = await notify(config.template("discord_startup"))
