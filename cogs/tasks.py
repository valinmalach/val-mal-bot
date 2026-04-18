import io
import logging
import os
import shutil
import traceback

import discord
import pendulum
import polars as pl
from discord.ext import tasks
from discord.ext.commands import Bot, Cog
from dotenv import load_dotenv
from polars import DataFrame

from constants import (
    BOT_ADMIN_CHANNEL,
    SHOUTOUTS_CHANNEL,
    USERS,
    ErrorDetails,
    UserRecord,
)
from services import (
    get_next_leap,
    read_parquet_cached,
    send_message,
    update_birthday,
)

load_dotenv()

logger = logging.getLogger(__name__)


class Tasks(Cog):
    def __init__(self, bot: Bot) -> None:
        self.bot = bot

    @Cog.listener()
    async def on_ready(self) -> None:
        if not self.check_birthdays.is_running():
            self.check_birthdays.start()
        if not self.backup_data.is_running():
            self.backup_data.start()

    _quarter_hours = [
        pendulum.Time(hour, minute) for hour in range(24) for minute in (0, 15, 30, 45)
    ]

    async def log_error(self, message: str, traceback_str: str) -> None:
        traceback_buffer = io.BytesIO(traceback_str.encode("utf-8"))
        traceback_file = discord.File(traceback_buffer, filename="traceback.txt")
        await send_message(message, BOT_ADMIN_CHANNEL, file=traceback_file)

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

    @tasks.loop(time=pendulum.Time(0, 0, 0, 0))
    async def backup_data(self) -> None:
        try:
            date_string = pendulum.now().format("YYYY-MM-DD")
            backup_path = f"C:/backups/data_{date_string}/"

            os.makedirs(backup_path, exist_ok=True)

            for item in os.listdir("data/"):
                source_path = os.path.join("data/", item)
                dest_path = os.path.join(backup_path, item)
                try:
                    if os.path.isdir(source_path):
                        shutil.copytree(source_path, dest_path, dirs_exist_ok=True)
                    else:
                        shutil.copy2(source_path, dest_path)
                except Exception as e:
                    is_dir = os.path.isdir(source_path)
                    error_details: ErrorDetails = {
                        "type": type(e).__name__,
                        "message": str(e),
                        "args": e.args,
                        "traceback": traceback.format_exc(),
                    }
                    error_msg = f"Error backing up {'directory' if is_dir else 'file'} {item} - Type: {error_details['type']}, Message: {error_details['message']}, Args: {error_details['args']}"
                    logger.error(
                        f"{error_msg}\nTraceback:\n{error_details['traceback']}"
                    )
                    await self.log_error(error_msg, error_details["traceback"])
        except Exception as e:
            error_details: ErrorDetails = {
                "type": type(e).__name__,
                "message": str(e),
                "args": e.args,
                "traceback": traceback.format_exc(),
            }
            error_msg = f"Fatal error during backup data task - Type: {error_details['type']}, Message: {error_details['message']}, Args: {error_details['args']}"
            logger.error(f"{error_msg}\nTraceback:\n{error_details['traceback']}")
            await self.log_error(error_msg, error_details["traceback"])

    @tasks.loop(time=_quarter_hours)
    async def check_birthdays(self) -> None:
        try:
            now = (
                pendulum.now("UTC")
                .replace(second=0, microsecond=0)
                .strftime("%Y-%m-%dT%H:%M:%S.000Z")
            )

            df = await read_parquet_cached(USERS)
            birthday_users = df.filter(pl.col("birthday") == now)
            await self._process_birthday_records(birthday_users)
        except Exception as e:
            error_details: ErrorDetails = {
                "type": type(e).__name__,
                "message": str(e),
                "args": e.args,
                "traceback": traceback.format_exc(),
            }
            error_msg = f"Fatal error during birthday check task - Type: {error_details['type']}, Message: {error_details['message']}, Args: {error_details['args']}"
            logger.error(f"{error_msg}\nTraceback:\n{error_details['traceback']}")
            await self.log_error(error_msg, error_details["traceback"])

    async def _process_birthday_records(self, birthdays_now: DataFrame) -> None:
        now = pendulum.now()
        for record in birthdays_now.iter_rows(named=True):
            user_id = record["id"]
            user = self.bot.get_user(int(user_id))
            if user is None:
                logger.warning(f"Discord user ID {user_id} not found in guild cache")
                await send_message(
                    f"_process_birthday_records: User with ID {user_id} not found.",
                    BOT_ADMIN_CHANNEL,
                )
                continue
            await send_message(
                f"Happy Birthday {user.mention}!",
                SHOUTOUTS_CHANNEL,
            )
            if record["isBirthdayLeap"]:
                leap = True
                next_birthday = f"{get_next_leap(now.year)}{record['birthday'][4:]}"
            else:
                leap = False
                next_birthday = f"{now.year + 1}{record['birthday'][4:]}"
            updated_record: UserRecord = {
                "id": user_id,
                "username": record["username"],
                "birthday": next_birthday,
                "isBirthdayLeap": leap,
            }
            try:
                update_birthday(updated_record)
            except Exception as e:
                error_details: ErrorDetails = {
                    "type": type(e).__name__,
                    "message": str(e),
                    "args": e.args,
                    "traceback": traceback.format_exc(),
                }
                error_msg = f"Failed to update birthday for user {record['username']} (ID: {user_id}) - Type: {error_details['type']}, Message: {error_details['message']}, Args: {error_details['args']}"
                logger.error(f"{error_msg}\nTraceback:\n{error_details['traceback']}")
                await self.log_error(error_msg, error_details["traceback"])


async def setup(bot: Bot) -> None:
    await bot.add_cog(Tasks(bot))
