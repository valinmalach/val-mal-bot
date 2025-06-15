import logging
from datetime import datetime
from zoneinfo import ZoneInfo

import pytz
import sentry_sdk
from discord import Interaction, app_commands
from discord.app_commands import Choice, Range
from discord.ext.commands import Bot, GroupCog

from constants import BOT_ADMIN_CHANNEL, FOLLOWER_ROLE, MAX_DAYS, OWNER_ID, Months
from init import xata_client
from services import get_next_leap, send_message, update_birthday

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
    @sentry_sdk.trace()
    async def set_birthday(
        self,
        interaction: Interaction,
        month: Months,
        day: Range[int, 1, 31],
        timezone: str = "UTC",
    ) -> None:
        logger.info(
            "Validating and setting birthday for user %s with month=%s, day=%s, timezone=%s",
            interaction.user.id,
            month,
            day,
            timezone,
        )
        try:
            logger.info("Validating provided timezone: %s", timezone)
            if timezone not in pytz.all_timezones:
                await interaction.response.send_message(
                    f"Sorry. I've never heard of the timezone {timezone}. "
                    + "Have you tried using the autocomplete options provided? "
                    + "Because those are the only timezones I know of."
                )
                logger.warning("Invalid timezone provided: %s", timezone)
                return

            logger.info("Validating provided day %s for month %s", day, month.name)
            if day > MAX_DAYS[month]:
                await interaction.response.send_message(
                    f"{month.name} doesn't have that many days..."
                )
                logger.warning("Invalid day %s for month %s", day, month)
                return

            logger.info("Creating timezone object for timezone: %s", timezone)
            tz = ZoneInfo(timezone)
            now = datetime.now(tz).replace(second=0, microsecond=0)
            logger.info("Current datetime in timezone %s: %s", timezone, now)
            year = now.year

            logger.info(
                "Checking if date %s-%s-%s requires leap year handling",
                year,
                month.value,
                day,
            )
            if month == Months.February and day == 29:
                logger.info("Applying leap year logic for Feb 29 birthday")
                try:
                    birthday_this_year = datetime(year, month.value, day, tzinfo=tz)
                except ValueError:
                    birthday_this_year = None

                if birthday_this_year is None or birthday_this_year <= now:
                    logger.info(
                        "Feb 29 invalid for current year; locating next leap year"
                    )
                    year = get_next_leap(year)
            else:
                logger.info("Calculating next occurrence for non-leap date")
                birthday_this_year = datetime(year, month.value, day, tzinfo=tz)
                if birthday_this_year <= now:
                    year += 1

            logger.info(
                "Constructed birthday record for database insertion: year=%s, month=%s, day=%s",
                year,
                month,
                day,
            )
            logger.info("Building birthday record payload for database")
            record = {
                "username": interaction.user.name,
                "birthday": (
                    datetime.strptime(
                        f"{year}-{month.value:02d}-{day:02d} 00:00:00",
                        "%Y-%m-%d %H:%M:%S",
                    )
                    .replace(tzinfo=ZoneInfo(timezone))
                    .astimezone(ZoneInfo("UTC"))
                    .strftime("%Y-%m-%dT%H:%M:%S.000Z")
                ),
                "isBirthdayLeap": month == Months.February and day == 29,
            }
            logger.info(
                "Updating birthday in database for user ID %s", interaction.user.id
            )
            success = update_birthday(str(interaction.user.id), record)
            logger.info("Database update returned success status: %s", success[0])
            if not success[0]:
                await self._set_birthday_failed(interaction, success[1])
                return

            if month == Months.February and day == 29:
                logger.info(
                    "Sending response for leap year birthday user=%s",
                    interaction.user.id,
                )
                await interaction.response.send_message(
                    "That's an unfortunate birthday 😦\n\n"
                    + "Ah well, looks like I'll only wish you every 4 years!"
                )
            else:
                logger.info(
                    "Birthday set successfully for user=%s", interaction.user.id
                )
                await interaction.response.send_message(
                    "I've remembered your birthday! "
                    + "I'll wish you at midnight of your selected timezone!"
                )
        except Exception as e:
            logger.error(
                "Exception in set_birthday for user=%s: %s", interaction.user.id, e
            )
            sentry_sdk.capture_exception(e)
            await self._set_birthday_failed(interaction, e)

    @set_birthday.autocomplete("timezone")
    @sentry_sdk.trace()
    async def timezone_autocomplete(
        self, _: Interaction, current_input: str
    ) -> list[Choice[str]]:
        choices = [
            Choice(name=tz, value=tz)
            for tz in pytz.all_timezones
            if current_input.lower() in tz.lower()
        ]
        return choices[:25]

    @app_commands.command(
        name="remove", description="Removes your birthday, if it exists"
    )
    @app_commands.checks.has_role(FOLLOWER_ROLE)
    @sentry_sdk.trace()
    async def remove_birthday(
        self,
        interaction: Interaction,
    ) -> None:
        logger.info("Removing birthday record for user %s", interaction.user.id)
        try:
            try:
                logger.info(
                    "Fetching user record from database for user ID %s",
                    interaction.user.id,
                )
                existing_user = xata_client.records().get(
                    "users", str(interaction.user.id)
                )
                if not existing_user.is_success():
                    logger.error(
                        "Failed to fetch user record for user %s: %s",
                        interaction.user.id,
                        existing_user.error_message,
                    )
                    await self._forget_birthday_failed(
                        interaction, existing_user.error_message
                    )
                    return
            except Exception as e:
                logger.error(
                    "Error fetching user record for user %s: %s",
                    interaction.user.id,
                    e,
                )
                sentry_sdk.capture_exception(e)
                await self._forget_birthday_failed(interaction, e)
                return

            logger.info("Constructing payload to remove birthday record for database")
            record = {
                "username": interaction.user.name,
                "birthday": None,
                "isBirthdayLeap": None,
            }
            success = update_birthday(str(interaction.user.id), record)
            if not success[0]:
                logger.error(
                    "Failed to remove birthday for user %s: %s",
                    interaction.user.id,
                    success[1],
                )
                await self._forget_birthday_failed(interaction, success[1])
                return

            logger.info("Birthday removal successful for user %s", interaction.user.id)
            try:
                if existing_user.is_success() and existing_user.get("birthday"):
                    await interaction.response.send_message(
                        "I've removed your birthday! I won't wish you anymore!"
                    )
                    return

                await interaction.response.send_message(
                    "You had no birthday to remove. "
                    + "Maybe try setting one first before asking me to remove it?"
                )
            except Exception as e:
                logger.error("Error while sending response for birthday removal: %s", e)
                sentry_sdk.capture_exception(e)
                await self._forget_birthday_failed(interaction, e)
        except Exception as e:
            logger.error(
                "Exception in remove_birthday for user=%s: %s", interaction.user.id, e
            )
            sentry_sdk.capture_exception(e)
            await self._forget_birthday_failed(interaction, e)

    @sentry_sdk.trace()
    async def _set_birthday_failed(
        self, interaction: Interaction, e: Exception | str | None
    ) -> None:
        logger.info("Notifying user %s of birthday set failure", interaction.user.id)
        mention = (
            interaction.guild.owner.mention
            if interaction.guild and interaction.guild.owner
            else f"<@{OWNER_ID}>"
        )
        await interaction.response.send_message(
            "Sorry, it seems like I couldn't set your birthday...\n\n"
            + f"# {mention} FIX MEEEE!!!"
        )
        await send_message(
            f"Failed to set birthday for {interaction.user.name}: {e}",
            BOT_ADMIN_CHANNEL,
        )

    @sentry_sdk.trace()
    async def _forget_birthday_failed(
        self, interaction: Interaction, e: Exception | str | None
    ) -> None:
        logger.info(
            "Notifying user %s of birthday removal failure", interaction.user.id
        )
        mention = (
            interaction.guild.owner.mention
            if interaction.guild and interaction.guild.owner
            else f"<@{OWNER_ID}>"
        )
        await interaction.response.send_message(
            "Oops, it seems like I couldn't forget your birthday...\n\n"
            + f"# {mention} FIX MEEEE!!!"
        )
        await send_message(
            f"Failed to remove birthday for {interaction.user.name}: {e}",
            BOT_ADMIN_CHANNEL,
        )


async def setup(bot: Bot) -> None:
    await bot.add_cog(Birthday(bot))
