import os
from datetime import datetime
from enum import Enum
from zoneinfo import ZoneInfo

import pytz
from discord import Interaction, User, app_commands
from discord.app_commands import Choice, Range
from discord.ext.commands import Bot, GroupCog
from dotenv import load_dotenv
from xata import XataClient

from helper import get_next_leap_year

load_dotenv()

XATA_API_KEY = os.getenv("XATA_API_KEY")
DATABASE_URL = os.getenv("DATABASE_URL")

xata_client = XataClient(api_key=XATA_API_KEY, db_url=DATABASE_URL)


class Months(Enum):
    January = 1
    February = 2
    March = 3
    April = 4
    May = 5
    June = 6
    July = 7
    August = 8
    September = 9
    October = 10
    November = 11
    December = 12


MAX_DAYS = {
    Months.January: 31,
    Months.February: 29,
    Months.March: 31,
    Months.April: 30,
    Months.May: 31,
    Months.June: 30,
    Months.July: 31,
    Months.August: 31,
    Months.September: 30,
    Months.October: 31,
    Months.November: 30,
    Months.December: 31,
}


class Birthday(GroupCog):
    def __init__(self, bot: Bot):
        self.bot = bot

    @app_commands.command(name="set", description="Set your birthday")
    @app_commands.checks.has_role(1291769015190032435)
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
    ):
        if timezone not in pytz.all_timezones:
            await interaction.response.send_message(
                f"Sorry. I've never heard of the timezone {timezone}. "
                + "Have you tried using the autocomplete options provided? "
                + "Because those are the only timezones I know of."
            )
            return

        if day > MAX_DAYS[month]:
            await interaction.response.send_message(
                f"{month.name} doesn't have that many days..."
            )
            return

        tz = ZoneInfo(timezone)
        now = datetime.now(tz)
        year = now.year

        if month == Months.February and day == 29:
            try:
                birthday_this_year = datetime(year, month.value, day, tzinfo=tz)
            except ValueError:
                birthday_this_year = None

            if birthday_this_year is None or birthday_this_year.date() <= now.date():
                year = get_next_leap_year(year)
        else:
            birthday_this_year = datetime(year, month.value, day, tzinfo=tz)
            if birthday_this_year.date() <= now.date():
                year += 1

        record = {
            "username": interaction.user.name,
            "birthday": (
                datetime.strptime(
                    f"{year}-{month.value:02d}-{day:02d} 00:00:00", "%Y-%m-%d %H:%M:%S"
                )
                .replace(tzinfo=ZoneInfo(timezone))
                .astimezone(ZoneInfo("UTC"))
                .strftime("%Y-%m-%dT%H:%M:%S.000Z")
            ),
        }
        success = self._update_birthday(interaction.user, record)
        if not success:
            await interaction.response.send_message(
                "Sorry, it seems like I couldn't set your birthday...\n\n"
                + "# <@389318636201967628> FIX MEEEE!!!"
            )
            return

        if month == Months.February and day == 29:
            await interaction.response.send_message(
                "That's an unfortunate birthday :(\n\n"
                + "Don't worry! If it's not a leap year, I'll wish you on both the "
                + "28th of February and the 1st of March!"
            )
        else:
            await interaction.response.send_message(
                "I've remembered your birthday! "
                + "I'll wish you at midnight of your selected timezone!"
            )

    @set_birthday.autocomplete("timezone")
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
    @app_commands.checks.has_role(1291769015190032435)
    async def remove_birthday(
        self,
        interaction: Interaction,
    ):
        existing_user = xata_client.records().get("users", str(interaction.user.id))

        record = {
            "username": interaction.user.name,
            "birthday": None,
        }
        success = self._update_birthday(interaction.user, record)
        if not success:
            await interaction.response.send_message(
                "Oops, it seems like I couldn't forget your birthday...\n\n"
                + "# <@389318636201967628> FIX MEEEE!!!"
            )
            return

        if existing_user.is_success() and existing_user.get("birthday"):
            await interaction.response.send_message(
                "I've removed your birthday! I won't wish you anymore!"
            )
            return

        await interaction.response.send_message(
            "You had no birthday to remove. "
            + "Maybe try setting one first before asking me to remove it?"
        )

    @staticmethod
    def _update_birthday(user: User, record: dict[str, str]) -> bool:
        existing_user = xata_client.records().get("users", str(user.id))
        if existing_user.is_success():
            resp = xata_client.records().update("users", str(user.id), record)
        else:
            resp = xata_client.records().insert_with_id("users", str(user.id), record)
        return resp.is_success()


async def setup(bot: Bot):
    await bot.add_cog(Birthday(bot))
