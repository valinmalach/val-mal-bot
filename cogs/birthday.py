import io
import logging
import traceback
from typing import Literal

import discord
import pendulum
import polars as pl
from discord import Interaction, app_commands
from discord.app_commands import Choice, Range
from discord.ext.commands import Bot, GroupCog
from pendulum import DateTime

from constants import (
    BOT_ADMIN_CHANNEL,
    FOLLOWER_ROLE,
    MAX_DAYS,
    OWNER_ID,
    ErrorDetails,
    Months,
    UserRecord,
)
from services import get_next_leap, read_parquet_cached, send_message, update_birthday

logger = logging.getLogger(__name__)


class Birthday(GroupCog):
    def __init__(self, bot: Bot) -> None:
        self.bot = bot

    @app_commands.command(name="set", description="Set your birthday")
    @app_commands.checks.has_role(FOLLOWER_ROLE)
    @app_commands.describe(
        month="The month of your birthday",
        day="The day of your birthday",
        timezone="Your timezone (Optional. If left blank, will default to GMT+0)",
    )
    async def set_birthday(
        self,
        interaction: Interaction,
        month: Months,
        day: Range[int, 1, 31],
        timezone: str = "UTC",
    ) -> None:
        try:
            if timezone not in pendulum.timezones():
                await interaction.response.send_message(
                    f"Sorry. I've never heard of the timezone {timezone}. "
                    + "Have you tried using the autocomplete options provided? "
                    + "Because those are the only timezones I know of."
                )
                logger.warning(f"Invalid timezone provided: {timezone}")
                return

            if day > MAX_DAYS[month]:
                await interaction.response.send_message(
                    f"{month.name} doesn't have that many days..."
                )
                logger.warning(f"Invalid day {day} for month {month.name}")
                return

            now = pendulum.now(timezone).replace(second=0, microsecond=0)
            year = now.year

            if month == Months.February and day == 29:
                try:
                    birthday_this_year = DateTime(
                        year=year,
                        month=month.value,
                        day=day,
                        tzinfo=pendulum.timezone(timezone),
                    )
                except ValueError:
                    birthday_this_year = None

                if birthday_this_year is None or birthday_this_year <= now:
                    year = get_next_leap(year)
            else:
                birthday_this_year = DateTime(
                    year=year,
                    month=month.value,
                    day=day,
                    tzinfo=pendulum.timezone(timezone),
                )
                if birthday_this_year <= now:
                    year += 1

            record: UserRecord = {
                "id": interaction.user.id,
                "username": interaction.user.name,
                "birthday": (
                    DateTime.strptime(
                        f"{year}-{month.value:02d}-{day:02d} 00:00:00",
                        "%Y-%m-%d %H:%M:%S",
                    )
                    .replace(tzinfo=pendulum.timezone(timezone))
                    .astimezone(pendulum.timezone("UTC"))
                    .strftime("%Y-%m-%dT%H:%M:%S.000Z")
                ),
                "isBirthdayLeap": month == Months.February and day == 29,
            }
            success, error = await update_birthday(record)
            if not success and error:
                error_details: ErrorDetails = {
                    "type": type(error).__name__,
                    "message": str(error),
                    "args": error.args,
                    "traceback": traceback.format_exc(),
                }
                error_msg = f"Exception in set_birthday for user={interaction.user.id} - Type: {error_details['type']}, Message: {error_details['message']}, Args: {error_details['args']}"
                logger.error(f"{error_msg}\nTraceback:\n{error_details['traceback']}")
                await self._birthday_operation_failed(
                    interaction, error_msg, "set", error_details["traceback"]
                )
                return

            if month == Months.February and day == 29:
                await interaction.response.send_message(
                    "That's an unfortunate birthday 😦\n\n"
                    + "Ah well, looks like I'll only wish you every 4 years!"
                )
            else:
                await interaction.response.send_message(
                    "I've remembered your birthday! "
                    + "I'll wish you at midnight of your selected timezone!"
                )
        except Exception as e:
            error_details: ErrorDetails = {
                "type": type(e).__name__,
                "message": str(e),
                "args": e.args,
                "traceback": traceback.format_exc(),
            }
            error_msg = f"Exception in set_birthday for user={interaction.user.id} - Type: {error_details['type']}, Message: {error_details['message']}, Args: {error_details['args']}"
            logger.error(f"{error_msg}\nTraceback:\n{error_details['traceback']}")
            await self._birthday_operation_failed(
                interaction, error_msg, "set", error_details["traceback"]
            )

    @set_birthday.autocomplete("timezone")
    async def timezone_autocomplete(
        self, _: Interaction, current_input: str
    ) -> list[Choice[str]]:
        choices = [
            Choice(name=tz, value=tz)
            for tz in pendulum.timezones()
            if current_input.lower() in tz.lower()
        ]
        return choices[:25]

    @app_commands.command(
        name="remove", description="Removes your birthday, if it exists"
    )
    @app_commands.checks.has_role(FOLLOWER_ROLE)
    async def remove_birthday(
        self,
        interaction: Interaction,
    ) -> None:
        try:
            df = await read_parquet_cached("data/users.parquet")
            existing_user_row = df.filter(pl.col("id") == interaction.user.id)
            if existing_user_row.height == 0:
                existing_user = None
            else:
                existing_user = existing_user_row.row(0, named=True)

            if existing_user is None:
                await send_message(
                    f"User {interaction.user.name} ({interaction.user.id}) attempted to remove a birthday but had no record.",
                    BOT_ADMIN_CHANNEL,
                )
                await interaction.response.send_message(
                    "An error occurred while trying to remove your birthday."
                )
                return

            record: UserRecord = {
                "id": interaction.user.id,
                "username": interaction.user.name,
                "birthday": None,
                "isBirthdayLeap": None,
            }
            success, error = await update_birthday(record)
            if not success and error:
                error_details: ErrorDetails = {
                    "type": type(error).__name__,
                    "message": str(error),
                    "args": error.args,
                    "traceback": traceback.format_exc(),
                }
                error_msg = f"Failed to remove birthday for user={interaction.user.id} - Type: {error_details['type']}, Message: {error_details['message']}, Args: {error_details['args']}"
                logger.error(f"{error_msg}\nTraceback:\n{error_details['traceback']}")
                await self._birthday_operation_failed(
                    interaction, error_msg, "forget", error_details["traceback"]
                )
                return

            try:
                if existing_user.get("birthday"):
                    await interaction.response.send_message(
                        "I've removed your birthday! I won't wish you anymore!"
                    )
                    return

                await interaction.response.send_message(
                    "You had no birthday to remove. "
                    + "Maybe try setting one first before asking me to remove it?"
                )
            except Exception as e:
                error_details: ErrorDetails = {
                    "type": type(e).__name__,
                    "message": str(e),
                    "args": e.args,
                    "traceback": traceback.format_exc(),
                }
                error_msg = f"Error while sending response for birthday removal: {error_details['message']}"
                logger.error(f"{error_msg}\nTraceback:\n{error_details['traceback']}")
                await self._birthday_operation_failed(
                    interaction, error_msg, "forget", error_details["traceback"]
                )
        except Exception as e:
            error_details: ErrorDetails = {
                "type": type(e).__name__,
                "message": str(e),
                "args": e.args,
                "traceback": traceback.format_exc(),
            }
            error_msg = f"Exception in remove_birthday for user={interaction.user.id}: {error_details['message']}"
            logger.error(f"{error_msg}\nTraceback:\n{error_details['traceback']}")
            await self._birthday_operation_failed(
                interaction, error_msg, "forget", error_details["traceback"]
            )

    async def _birthday_operation_failed(
        self,
        interaction: Interaction,
        error_msg: str,
        set_forget: Literal["set", "forget"],
        traceback_str: str,
    ) -> None:
        mention = (
            interaction.guild.owner.mention
            if interaction.guild and interaction.guild.owner
            else f"<@{OWNER_ID}>"
        )
        await interaction.response.send_message(
            f"Oops, it seems like I couldn't {set_forget} your birthday...\n\n"
            + f"# {mention} FIX MEEEE!!!"
        )
        traceback_buffer = io.BytesIO(traceback_str.encode("utf-8"))
        traceback_file = discord.File(traceback_buffer, filename="traceback.txt")
        await send_message(
            f"Failed to {set_forget} birthday for {interaction.user.name}: {error_msg}",
            BOT_ADMIN_CHANNEL,
            file=traceback_file,
        )


async def setup(bot: Bot) -> None:
    await bot.add_cog(Birthday(bot))
