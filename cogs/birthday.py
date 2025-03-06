import os
from datetime import datetime
from zoneinfo import ZoneInfo

import pytz
from discord import Interaction, app_commands
from discord.app_commands import Choice, Range
from discord.ext.commands import Bot, GroupCog
from dotenv import load_dotenv
from xata import XataClient

from constants import FOLLOWER_ROLE, MAX_DAYS, Months
from helper import get_next_leap_year, update_birthday

load_dotenv()

XATA_API_KEY = os.getenv("XATA_API_KEY")
DATABASE_URL = os.getenv("DATABASE_URL")

xata_client = XataClient(api_key=XATA_API_KEY, db_url=DATABASE_URL)


class Birthday(GroupCog):
    def __init__(self, bot: Bot):
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
        now = datetime.now(tz).replace(second=0, microsecond=0)
        year = now.year

        if month == Months.February and day == 29:
            try:
                birthday_this_year = datetime(year, month.value, day, tzinfo=tz)
            except ValueError:
                birthday_this_year = None

            if birthday_this_year is None or birthday_this_year <= now:
                year = get_next_leap_year(year)
        else:
            birthday_this_year = datetime(year, month.value, day, tzinfo=tz)
            if birthday_this_year <= now:
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
            "isBirthdayLeap": month == Months.February and day == 29,
        }
        success = update_birthday(xata_client, str(interaction.user.id), record)
        if not success:
            await interaction.response.send_message(
                "Sorry, it seems like I couldn't set your birthday...\n\n"
                + f"# {interaction.guild.owner.mention} FIX MEEEE!!!"
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
    @app_commands.checks.has_role(FOLLOWER_ROLE)
    async def remove_birthday(
        self,
        interaction: Interaction,
    ):
        existing_user = xata_client.records().get("users", str(interaction.user.id))

        record = {
            "username": interaction.user.name,
            "birthday": None,
            "isBirthdayLeap": False,
        }
        success = update_birthday(xata_client, str(interaction.user.id), record)
        if not success:
            await interaction.response.send_message(
                "Oops, it seems like I couldn't forget your birthday...\n\n"
                + f"# {interaction.guild.owner.mention} FIX MEEEE!!!"
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


async def setup(bot: Bot):
    await bot.add_cog(Birthday(bot))
