import os
from datetime import datetime
from enum import Enum
from zoneinfo import ZoneInfo

import discord
import pytz
from discord import app_commands
from discord.ext import commands
from dotenv import load_dotenv
from xata import XataClient

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


class Commands(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot

    @app_commands.command(name="birthday", description="Set your birthday")
    @app_commands.describe(
        month="The month of your birthday",
        day="The day of your birthday",
        timezone="Your timezone (Optional. If left blank, will default to GMT+0)",
    )
    async def set_birthday(
        self,
        interaction: discord.Interaction,
        month: Months,
        day: app_commands.Range[int, 1, 31],
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

        record = {
            "username": interaction.user.name,
            "birthday": (
                datetime.strptime(
                    f"1970-{month.value:02d}-{day:02d} 00:00:00", "%Y-%m-%d %H:%M:%S"
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
            return

        await interaction.response.send_message(
            "I've remembered your birthday! I'll wish you at midnight of your selected timezone!"
        )

    @set_birthday.autocomplete("timezone")
    async def timezone_autocomplete(
        self, _: discord.Interaction, current_input: str
    ) -> list[app_commands.Choice[str]]:
        choices = [
            app_commands.Choice(name=tz, value=tz)
            for tz in pytz.all_timezones
            if current_input.lower() in tz.lower()
        ]
        return choices[:25]

    def _update_birthday(self, user: discord.User, record: dict[str, str]) -> bool:
        existing_user = xata_client.records().get("users", str(user.id))
        if existing_user.is_success():
            resp = xata_client.records().update("users", user.id, record)
        else:
            resp = xata_client.records().insert_with_id("users", str(user.id), record)
        return resp.is_success()


async def setup(bot: commands.Bot):
    await bot.add_cog(Commands(bot))
