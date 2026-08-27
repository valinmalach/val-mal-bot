import io
import logging
import traceback
from typing import ClassVar

import discord
import pendulum
from discord.ext import tasks
from discord.ext.commands import Bot, Cog

from constants import ErrorDetails
from db import DiscordUser, repository
from services import get_next_leap, send_message
from services.config import config

logger = logging.getLogger(__name__)


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
            birthday_users = await repository.users_with_birthday(now)
            await self._process_birthday_records(birthday_users)
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

    async def _process_birthday_records(self, birthdays_now: list[DiscordUser]) -> None:
        now = pendulum.now("UTC")
        for record in birthdays_now:
            user_id = record.id
            user = self.bot.get_user(user_id)
            if user is None:
                logger.warning(f"Discord user ID {user_id} not found in guild cache")
                await send_message(
                    f"_process_birthday_records: User with ID {user_id} not found.",
                    config.channel("bot_admin"),
                )
                continue
            await send_message(
                f"Happy Birthday {user.mention}!",
                config.channel("shoutouts"),
            )
            if record.birthday is None:
                continue
            leap = bool(record.is_birthday_leap)
            # get_next_leap on the following year, otherwise a 29 February
            # birthday landing in a leap year reschedules onto itself.
            next_year = get_next_leap(now.year + 1) if leap else now.year + 1
            next_birthday = pendulum.instance(record.birthday).replace(year=next_year)
            try:
                await repository.upsert_user(
                    user_id, record.username, next_birthday, leap
                )
            except Exception as e:  # noqa: BLE001
                error_details: ErrorDetails = {
                    "type": type(e).__name__,
                    "message": str(e),
                    "args": e.args,
                    "traceback": traceback.format_exc(),
                }
                error_msg = f"Failed to update birthday for user {record.username} (ID: {user_id}) - Type: {error_details['type']}, Message: {error_details['message']}, Args: {error_details['args']}"
                logger.error(f"{error_msg}\nTraceback:\n{error_details['traceback']}")
                await self.log_error(error_msg, error_details["traceback"])


async def setup(bot: Bot) -> None:
    await bot.add_cog(Tasks(bot))
