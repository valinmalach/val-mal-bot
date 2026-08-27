import io
import logging
import traceback
from datetime import datetime
from typing import ClassVar

import discord
import pendulum
from discord.ext import tasks
from discord.ext.commands import Bot, Cog

from constants import ErrorDetails
from db import DiscordUser, repository
from services import next_birthday, send_message
from services.config import config

logger = logging.getLogger(__name__)

# A tick that ran late should still announce. A birthday staler than this is
# only moved on, so a bot that was down for days does not greet the wrong day.
_ANNOUNCE_GRACE_SECONDS = 24 * 60 * 60


class Tasks(Cog):
    def __init__(self, bot: Bot) -> None:
        self.bot = bot

    @Cog.listener()
    async def on_ready(self) -> None:
        if not self.check_birthdays.is_running():
            self.check_birthdays.start()

    _quarter_hours: ClassVar[list[pendulum.Time]] = [
        pendulum.Time(hour, minute) for hour in range(24) for minute in (0, 15, 30, 45)
    ]

    async def log_error(self, message: str, traceback_str: str) -> None:
        traceback_buffer = io.BytesIO(traceback_str.encode("utf-8"))
        traceback_file = discord.File(traceback_buffer, filename="traceback.txt")
        await send_message(message, config.channel("bot_admin"), file=traceback_file)

    async def _handle_fatal_error(self, e: Exception, context: str) -> None:
        """Handle fatal errors with consistent logging and notification."""
        error_details: ErrorDetails = {
            "type": type(e).__name__,
            "message": str(e),
            "args": e.args,
            "traceback": traceback.format_exc(),
        }
        error_msg = f"{context} - Type: {error_details['type']}, Message: {error_details['message']}, Args: {error_details['args']}"
        logger.error(f"{error_msg}\nTraceback:\n{error_details['traceback']}")
        await self.log_error(error_msg, error_details["traceback"])

    @tasks.loop(time=_quarter_hours)
    async def check_birthdays(self) -> None:
        try:
            now = pendulum.now("UTC").replace(second=0, microsecond=0)
            due = await repository.users_due_birthday(now)
            await self._process_birthday_records(due)
        except Exception as e:  # noqa: BLE001
            error_details: ErrorDetails = {
                "type": type(e).__name__,
                "message": str(e),
                "args": e.args,
                "traceback": traceback.format_exc(),
            }
            error_msg = f"Fatal error during birthday check task - Type: {error_details['type']}, Message: {error_details['message']}, Args: {error_details['args']}"
            logger.error(f"{error_msg}\nTraceback:\n{error_details['traceback']}")
            await self.log_error(error_msg, error_details["traceback"])

    async def _process_birthday_records(self, due: list[DiscordUser]) -> None:
        now = pendulum.now("UTC")
        for record in due:
            try:
                await self._process_birthday(record, now)
            except Exception as e:  # noqa: BLE001
                await self._handle_fatal_error(
                    e,
                    f"Failed to process birthday for user {record.username} (ID: {record.id})",
                )

    async def _process_birthday(
        self, record: DiscordUser, now: pendulum.DateTime
    ) -> None:
        if record.birthday is None:
            return

        if self._within_announce_grace(record.birthday, now):
            await self._announce_birthday(record)
        else:
            logger.warning(
                f"Birthday for user {record.username} (ID: {record.id}) was due at {record.birthday} and is too stale to announce; rescheduling"
            )

        # Rescheduled either way: a record left in the past comes due on every
        # run from here on, and never announces on the right day again.
        leap = bool(record.is_birthday_leap)
        next_at = next_birthday(record.birthday, leap, now)
        await repository.upsert_user(record.id, record.username, next_at, leap)

    @staticmethod
    def _within_announce_grace(birthday: datetime, now: pendulum.DateTime) -> bool:
        late = (now - pendulum.instance(birthday)).total_seconds()
        return late <= _ANNOUNCE_GRACE_SECONDS

    async def _announce_birthday(self, record: DiscordUser) -> None:
        user = self.bot.get_user(record.id)
        if user is None:
            logger.warning(f"Discord user ID {record.id} not found in guild cache")
            await send_message(
                f"_announce_birthday: User with ID {record.id} not found.",
                config.channel("bot_admin"),
            )
            return
        await send_message(
            config.template("discord_birthday", mention=user.mention),
            config.channel("shoutouts"),
        )


async def setup(bot: Bot) -> None:
    await bot.add_cog(Tasks(bot))
