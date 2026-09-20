import asyncio
import logging
import sys

import discord
from discord.ext.commands import Bot

from valmal.core.background import fire_and_forget
from valmal.core.errors import notify, report

logger = logging.getLogger(__name__)

# One flag per question on_ready has to answer: has the Helix/DB-heavy startup
# work *succeeded* (_started, cleared by run_background_tasks on failure so a
# reconnect retries), and has the startup announcement been delivered
# (_announced, likewise retried on reconnect until it has).
_started = False
_announced = False


async def run_background_tasks() -> None:
    """Bring the per-process helpers back up, saying which one failed.

    The arms are gathered so one cannot delay the other, and their results are
    read: `return_exceptions=True` alone is silence, because the gather completes
    normally and `background._finished` sees nothing to report. `main()` reads
    its cog-loading results the same way.

    Each arm is named for what was lost, not for the function that lost it.

    A failed arm resets `_started` so the next reconnect retries. `_started` is
    set before this runs so an overlapping reconnect cannot fire a second
    attempt, but a boot-time Twitch or database outage must not strand a live
    alert or the stream session for the rest of the process.
    """
    # Deferred: both reach valmal.bot.send, which imports this package for `bot`.
    from valmal.twitch.stream import live_alert, stream_session

    arms = (
        ("restore the live alert updaters", live_alert.restore_all()),
        ("resume the stream session", stream_session.resume()),
    )
    results = await asyncio.gather(*(arm for _, arm in arms), return_exceptions=True)
    failed = False
    for (lost, _), result in zip(arms, results, strict=True):
        if isinstance(result, Exception):
            failed = True
            await report(result, f"Startup could not {lost}")

    if failed:
        global _started
        _started = False


class MyBot(Bot):
    def __init__(self, *, command_prefix: str, intents: discord.Intents) -> None:
        super().__init__(command_prefix=command_prefix, intents=intents)
        self.case_insensitive = True

    async def setup_hook(self) -> None:
        from valmal.bot.cogs.tasks import Tasks
        from valmal.core.config import config
        from valmal.twitch.oauth.token_manager import token_manager
        from valmal.twitch.stream.shoutout_queue import shoutout_queue

        await config.load()
        await token_manager.load()

        # Here rather than in on_ready, which fires again on every reconnect: one
        # drainer is wanted for the life of the process, idle until something is queued.
        fire_and_forget(shoutout_queue.drain(), name="shoutout-queue")

        # Also here, not in Tasks.cog_load(): that runs before login(), when
        # Client._ready does not exist, so each loop's before_loop would raise on
        # bot.wait_until_ready() and the loop would die silently. setup_hook runs
        # inside login(), after _ready exists, and only once.
        tasks_cog = self.get_cog("Tasks")
        if isinstance(tasks_cog, Tasks):
            tasks_cog.check_birthdays.start()
            tasks_cog.recheck_subscriptions.start()

        self.command_prefix = config.setting("command_prefix", "$")
        guild = discord.Object(id=config.setting("guild_id"))

        self.tree.copy_global_to(guild=guild)
        await self.tree.sync(guild=guild)

        from valmal.bot.views import persistent_views

        # register all persistent Views so buttons still work after a restart
        for view in persistent_views():
            self.add_view(view)

    async def close(self) -> None:
        from valmal.db.session import dispose_engine

        await super().close()
        await dispose_engine()


bot = MyBot(command_prefix="$", intents=discord.Intents.all())

# CheckFailure subclasses that are not somebody being refused: the bot lacking
# a permission is a fault to report, and a cooldown is not a permission at all.
_NOT_A_REFUSAL = (
    discord.app_commands.BotMissingPermissions,
    discord.app_commands.CommandOnCooldown,
)


async def _answer(
    interaction: discord.Interaction, template_key: str, command: str, verb: str
) -> None:
    from valmal.core.config import config
    from valmal.core.errors import report

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
    and the person who ran it would be left on a spinner. A refused check also
    lands here, deliberately: `has_permissions` raises `MissingPermissions` and
    `has_configured_role` a plain `CheckFailure`, both by design, so that branch
    answers the person directly instead of reporting it as a bug.
    """
    from valmal.core.errors import report

    command = interaction.command.qualified_name if interaction.command else "unknown"

    if isinstance(error, discord.app_commands.CheckFailure) and not isinstance(
        error, _NOT_A_REFUSAL
    ):
        # A check doing its job (an admin reconfigured who may run this, or
        # somebody lacks the follower role) is not a bug: it answers the person
        # instead of reporting.
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

    discord.py calls this from inside the listener's except block on failure,
    handing over which `on_*` event it was, so `sys.exc_info()` still resolves
    the exception. A function on the `bot` instance via `@bot.event`, matching
    `on_ready`, not a method on `MyBot`. View/button callbacks have their own
    `on_error` and are not covered.
    """
    exc = sys.exc_info()[1]
    if isinstance(exc, Exception):
        await report(exc, f"Fatal error with {event_method} event")


@bot.event
async def on_ready() -> None:
    """The only per-connection entry point left; two flags answer two questions.

    on_ready fires again whenever the gateway session cannot be resumed. `_started`
    says whether `run_background_tasks` has *succeeded*, so a reconnect does not
    repeat its Helix/DB-heavy work; that function clears it on failure so a later
    reconnect retries. `_announced` says whether the startup notice has been
    *delivered*; a failed send is worth retrying on the next reconnect too, so it
    is not folded into the same guard.
    """
    global _started, _announced
    from valmal.core.config import config

    if not _started:
        _started = True
        fire_and_forget(run_background_tasks(), name="startup-tasks")

    if _announced:
        logger.info("Reconnected to Discord")
        return

    _announced = await notify(config.template("discord_startup"))
