import logging
from typing import Literal

import pendulum
from discord import Interaction, app_commands
from discord.app_commands import Choice, Range
from discord.ext.commands import Bot, GroupCog

from constants import MAX_DAYS, Months
from db import repository
from errors import notify, report
from services import next_birthday_on
from services.config import config, has_configured_role

logger = logging.getLogger(__name__)


class Birthday(GroupCog):
    def __init__(self, bot: Bot) -> None:
        self.bot = bot

    @app_commands.command(name="set", description="Set your birthday")
    @has_configured_role("follower")
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
            validation_error = await self._validate_birthday_inputs(
                interaction, month, day, timezone
            )
            if validation_error:
                return

            is_leap = month == Months.February and day == 29
            await repository.upsert_user(
                interaction.user.id,
                interaction.user.name,
                next_birthday_on(month, day, timezone, pendulum.now("UTC")),
                is_leap,
            )

            await interaction.response.send_message(
                config.template("birthday_set_leap" if is_leap else "birthday_set")
            )

        except Exception as e:  # noqa: BLE001
            await self._birthday_operation_failed(interaction, e, "set")

    async def _validate_birthday_inputs(
        self, interaction: Interaction, month: Months, day: int, timezone: str
    ) -> bool:
        """Validate timezone and day inputs. Returns True if there was an error."""
        if timezone not in pendulum.timezones():
            await interaction.response.send_message(
                config.template("birthday_bad_timezone", timezone=timezone)
            )
            # !r, because the value is whatever the user typed: a bare newline
            # in it would otherwise forge a log line.
            logger.warning(f"Invalid timezone provided: {timezone!r}")
            return True

        if day > MAX_DAYS[month]:
            await interaction.response.send_message(
                config.template("birthday_bad_day", month=month.name)
            )
            logger.warning(f"Invalid day {day} for month {month.name}")
            return True

        return False

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
    @has_configured_role("follower")
    async def remove_birthday(
        self,
        interaction: Interaction,
    ) -> None:
        try:
            existing_user = await repository.get_user(interaction.user.id)
            if existing_user is None:
                await notify(
                    f"User {interaction.user.name} ({interaction.user.id}) attempted to remove a birthday but had no record."
                )
                await interaction.response.send_message(
                    config.template("birthday_remove_failed")
                )
                return

            had_birthday = existing_user.birthday is not None
            # The explicit Nones are the removal: they clear both columns.
            await repository.upsert_user(
                interaction.user.id, interaction.user.name, None, None
            )

            if had_birthday:
                await interaction.response.send_message(
                    config.template("birthday_removed")
                )
                return

            await interaction.response.send_message(
                config.template("birthday_none_to_remove")
            )
        except Exception as e:  # noqa: BLE001
            await self._birthday_operation_failed(interaction, e, "forget")

    async def _birthday_operation_failed(
        self,
        interaction: Interaction,
        e: Exception,
        set_forget: Literal["set", "forget"],
    ) -> None:
        mention = (
            interaction.guild.owner.mention
            if interaction.guild and interaction.guild.owner
            else f"<@{config.setting('owner_id')}>"
        )
        await interaction.response.send_message(
            config.template(
                "birthday_operation_failed", action=set_forget, mention=mention
            )
        )
        await report(
            e,
            f"Failed to {set_forget} birthday for {interaction.user.name} "
            f"(ID: {interaction.user.id})",
        )


async def setup(bot: Bot) -> None:
    await bot.add_cog(Birthday(bot))
