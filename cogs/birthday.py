import io
import logging
import traceback
from typing import Literal

import discord
import pendulum
import polars as pl
from discord import Interaction, Member, User, app_commands
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
            # Validate inputs
            validation_error = await self._validate_birthday_inputs(
                interaction, month, day, timezone
            )
            if validation_error:
                return

            # Calculate next birthday year
            year = self._calculate_next_birthday_year(month, day, timezone)

            # Create user record
            record = self._create_birthday_record(
                interaction.user, month, day, year, timezone
            )

            # Update birthday in database
            await self._update_birthday_database(interaction, record)

            # Send success response
            await self._send_birthday_set_response(interaction, month, day)

        except Exception as e:
            await self._handle_set_birthday_exception(interaction, e)

    async def _validate_birthday_inputs(
        self, interaction: Interaction, month: Months, day: int, timezone: str
    ) -> bool:
        """Validate timezone and day inputs. Returns True if there was an error."""
        if timezone not in pendulum.timezones():
            await interaction.response.send_message(
                f"Sorry. I've never heard of the timezone {timezone}. "
                + "Have you tried using the autocomplete options provided? "
                + "Because those are the only timezones I know of."
            )
            logger.warning(f"Invalid timezone provided: {timezone}")
            return True

        if day > MAX_DAYS[month]:
            await interaction.response.send_message(
                f"{month.name} doesn't have that many days..."
            )
            logger.warning(f"Invalid day {day} for month {month.name}")
            return True

        return False

    def _calculate_next_birthday_year(
        self, month: Months, day: int, timezone: str
    ) -> int:
        """Calculate the year for the next occurrence of the birthday."""
        now = pendulum.now(timezone).replace(second=0, microsecond=0)
        year = now.year

        if month == Months.February and day == 29:
            return self._calculate_leap_year(year, now, timezone)

        birthday_this_year = DateTime(
            year=year,
            month=month.value,
            day=day,
            tzinfo=pendulum.timezone(timezone),
        )
        return year + 1 if birthday_this_year <= now else year

    def _calculate_leap_year(self, year: int, now: DateTime, timezone: str) -> int:
        """Calculate the next leap year for February 29th birthdays."""
        try:
            birthday_this_year = DateTime(
                year=year,
                month=2,
                day=29,
                tzinfo=pendulum.timezone(timezone),
            )
        except ValueError:
            birthday_this_year = None

        if birthday_this_year is None or birthday_this_year <= now:
            return get_next_leap(year)
        return year

    def _create_birthday_record(
        self, user: User | Member, month: Months, day: int, year: int, timezone: str
    ) -> UserRecord:
        """Create a UserRecord for the birthday."""
        birthday_datetime = (
            DateTime.strptime(
                f"{year}-{month.value:02d}-{day:02d} 00:00:00",
                "%Y-%m-%d %H:%M:%S",
            )
            .replace(tzinfo=pendulum.timezone(timezone))
            .astimezone(pendulum.timezone("UTC"))
            .strftime("%Y-%m-%dT%H:%M:%S.000Z")
        )

        return {
            "id": user.id,
            "username": user.name,
            "birthday": birthday_datetime,
            "isBirthdayLeap": month == Months.February and day == 29,
        }

    async def _update_birthday_database(
        self, interaction: Interaction, record: UserRecord
    ) -> None:
        """Update the birthday in the database with error handling."""
        try:
            await update_birthday(record)
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
            raise  # Re-raise to be caught by the main exception handler

    async def _send_birthday_set_response(
        self, interaction: Interaction, month: Months, day: int
    ) -> None:
        """Send appropriate success message based on birthday type."""
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

    async def _handle_set_birthday_exception(
        self, interaction: Interaction, e: Exception
    ) -> None:
        """Handle exceptions that occur during birthday setting."""
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
            try:
                await update_birthday(record)
            except Exception as e:
                error_details: ErrorDetails = {
                    "type": type(e).__name__,
                    "message": str(e),
                    "args": e.args,
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
