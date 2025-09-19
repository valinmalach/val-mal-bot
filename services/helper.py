import hashlib
import hmac
import logging
from datetime import datetime, timezone
from functools import cache
from typing import Optional

import discord
import pandas as pd
import sentry_sdk
from dateutil import relativedelta
from dateutil.parser import isoparse
from discord import (
    CategoryChannel,
    DMChannel,
    Embed,
    ForumChannel,
    GroupChannel,
    Interaction,
    Member,
    Object,
    PartialEmoji,
    PartialInviteChannel,
    PartialMessageable,
    Role,
    StageChannel,
    TextChannel,
    Thread,
    User,
    VoiceChannel,
)
from discord.abc import GuildChannel, PrivateChannel
from discord.ui import Button, View
from pandas import DataFrame

from constants import EMOJI_ROLE_MAP
from init import bot

logger = logging.getLogger(__name__)


@sentry_sdk.trace()
def upsert_row_to_parquet(
    row_data: dict, filepath: str, id_column: str = "id"
) -> tuple[bool, Exception | None]:
    """
    Upsert a single row into a parquet file based on a unique identifier column.

    Args:
        row_data: Dictionary containing the row data to upsert
        filepath: Path to the parquet file
        id_column: Name of the unique identifier column (default: "id")
    """
    try:
        # Convert row data to DataFrame
        new_row_df = pd.DataFrame([row_data])

        # Load existing data
        existing_df = pd.read_parquet(filepath)

        # Check if ID already exists
        id_value = row_data[id_column]
        if id_value in existing_df[id_column].values:
            # Update existing row
            existing_df.loc[existing_df[id_column] == id_value, new_row_df.columns] = (
                new_row_df.values[0]
            )
            combined_df = existing_df
        else:
            # Insert new row
            combined_df = pd.concat([existing_df, new_row_df], ignore_index=True)

        # Save back to parquet
        combined_df.to_parquet(filepath, index=False)
        return True, None
    except Exception as e:
        logger.error(f"Error upserting row to parquet: {e}")
        sentry_sdk.capture_exception(e)
        return False, e


@sentry_sdk.trace()
def delete_row_from_parquet(
    id_value: str, filepath: str, id_column: str = "id"
) -> tuple[bool, Exception | None]:
    """
    Delete a single row from a parquet file based on a unique identifier column.

    Args:
        id_value: Value of the unique identifier for the row to delete
        filepath: Path to the parquet file
        id_column: Name of the unique identifier column (default: "id")
    """
    try:
        # Load existing data
        existing_df = pd.read_parquet(filepath)

        # Check if ID exists
        if id_value in existing_df[id_column].values:
            # Delete the row
            updated_df: DataFrame = existing_df[existing_df[id_column] != id_value]

            # Save back to parquet
            updated_df.to_parquet(filepath, index=False)
        return True, None
    except Exception as e:
        logger.error(f"Error deleting row from parquet: {e}")
        sentry_sdk.capture_exception(e)
        return False, e


@sentry_sdk.trace()
async def send_message(content: str, channel_id: int) -> Optional[int]:
    channel = bot.get_channel(channel_id)
    if channel is None or isinstance(
        channel, (ForumChannel, CategoryChannel, PrivateChannel)
    ):
        return
    return (await channel.send(content)).id


@sentry_sdk.trace()
async def send_embed(
    embed: Embed,
    channel_id: int,
    view: Optional[View] = None,
    content: Optional[str] = None,
) -> Optional[int]:
    channel = bot.get_channel(channel_id)
    if channel is None or isinstance(
        channel, (ForumChannel, CategoryChannel, PrivateChannel)
    ):
        return
    if view:
        return (await channel.send(content=content, embed=embed, view=view)).id
    return (await channel.send(content=content, embed=embed)).id


@sentry_sdk.trace()
async def edit_embed(
    message_id: int,
    embed: Embed,
    channel_id: int,
    view: Optional[View] = None,
    content: Optional[str] = None,
) -> None:
    channel = bot.get_channel(channel_id)
    if channel is None or isinstance(
        channel, (ForumChannel, CategoryChannel, PrivateChannel)
    ):
        return
    message = await channel.fetch_message(message_id)
    if view:
        await message.edit(content=content, embed=embed, view=view)
    else:
        await message.edit(content=content, embed=embed, view=None)


@sentry_sdk.trace()
def get_pfp(member: User | Member) -> str:
    return member.avatar.url if member.avatar else member.default_avatar.url


@sentry_sdk.trace()
def get_discriminator(member: User | Member) -> str:
    return "" if member.discriminator == "0" else f"#{member.discriminator}"


@sentry_sdk.trace()
def update_birthday(record: dict[str, str]) -> tuple[bool, Exception | None]:
    return upsert_row_to_parquet(record, "data/users.parquet")


@sentry_sdk.trace()
def get_channel_mention(
    channel: (
        VoiceChannel
        | StageChannel
        | ForumChannel
        | TextChannel
        | CategoryChannel
        | PartialInviteChannel
        | DMChannel
        | PartialMessageable
        | GroupChannel
        | Thread
        | PrivateChannel
        | GuildChannel
        | Object
        | None
    ),
) -> str:
    if channel is None or isinstance(channel, Object):
        return "Unknown Channel"
    if isinstance(channel, GroupChannel):
        return f"{channel.name}"
    if isinstance(channel, DMChannel):
        return "a DM"
    if isinstance(channel, PrivateChannel):
        return "a private channel"
    return f"{channel.mention}"


@sentry_sdk.trace()
def get_age(date_time: datetime, limit_units: int = -1) -> str:
    now = datetime.now(timezone.utc)
    if date_time <= now:
        age = relativedelta.relativedelta(now, date_time)
    else:
        age = relativedelta.relativedelta(date_time, now)
    years, months, days, hours, minutes, seconds, microseconds = (
        age.years,
        age.months,
        age.days,
        age.hours,
        age.minutes,
        age.seconds,
        age.microseconds,
    )
    seconds += microseconds / 1000000
    parts = []
    if years > 0 or months > 0:
        if years:
            parts.append(format_unit(years, "year"))
        if months:
            parts.append(format_unit(months, "month"))
        if days:
            parts.append(format_unit(days, "day"))
    else:
        if weeks := days // 7:
            parts.append(format_unit(weeks, "week"))
        if days := days % 7:
            parts.append(format_unit(days, "day"))
        if hours:
            parts.append(format_unit(hours, "hour"))
        if minutes:
            parts.append(format_unit(minutes, "minute"))
        if seconds or not parts:
            seconds = round(seconds)
            parts.append(format_unit(seconds, "second"))
    parts = parts[:limit_units]
    return ", ".join(parts)


@sentry_sdk.trace()
@cache
def is_leap(year: int) -> bool:
    return (year % 400 == 0) or (year % 100 != 0) and (year % 4 == 0)


@sentry_sdk.trace()
@cache
def get_next_leap(year: int) -> int:
    while not is_leap(year):
        year += 1
    return year


@sentry_sdk.trace()
@cache
def get_ordinal_suffix(n: int) -> str:
    if 10 <= n % 100 <= 13:
        return f"{n}th"
    last_digit = n % 10
    if last_digit == 1:
        return f"{n}st"
    elif last_digit == 2:
        return f"{n}nd"
    elif last_digit == 3:
        return f"{n}rd"
    return f"{n}th"


@sentry_sdk.trace()
@cache
def format_unit(value: int, unit: str) -> str:
    return f"{value} {f'{unit}s' if value != 1 else unit}"


@sentry_sdk.trace()
@cache
def get_hmac_message(
    twitch_message_id: str, twitch_message_timestamp: str, body: str
) -> str:
    return twitch_message_id + twitch_message_timestamp + body


@sentry_sdk.trace()
@cache
def get_hmac(secret: str, message: str) -> str:
    return hmac.new(
        secret.encode("utf-8"), message.encode("utf-8"), hashlib.sha256
    ).hexdigest()


@sentry_sdk.trace()
@cache
def verify_message(hmac_str: str, verify_signature: str) -> bool:
    return hmac.compare_digest(hmac_str, verify_signature)


@sentry_sdk.trace()
@cache
def parse_rfc3339(date_str: str) -> datetime:
    """
    Parse an RFC3339 / ISO-8601 timestamp (e.g. '2025-05-31T12:34:56Z')
    and return a timezone-aware datetime.
    """
    return isoparse(date_str)


@sentry_sdk.trace()
def get_member_role(
    guild_id: int, user_id: int, emoji: PartialEmoji
) -> tuple[Member | None, Role | None]:
    guild = bot.get_guild(guild_id)
    if not guild:
        return None, None

    member = guild.get_member(user_id)
    if not member:
        return None, None

    role_name = EMOJI_ROLE_MAP.get(emoji.name)
    if not role_name:
        return None, None

    role = discord.utils.get(guild.roles, name=role_name)
    return (member, role) if role else (None, None)


@sentry_sdk.trace()
async def toggle_role(
    guild_id: int, user_id: int, emoji: PartialEmoji
) -> Optional[tuple[bool, Role]]:
    member, role = get_member_role(guild_id, user_id, emoji)
    if not member or not role:
        return

    if member.get_role(role.id) is None:
        await member.add_roles(role)
        return True, role
    else:
        await member.remove_roles(role)
        return False, role


@sentry_sdk.trace()
async def roles_button_pressed(interaction: Interaction, button: Button) -> None:
    guild_id = interaction.guild_id
    member_id = interaction.user.id
    emoji = button.emoji
    if not guild_id or not emoji:
        await interaction.response.send_message(
            "An error has occurred. Contact an admin.",
            ephemeral=True,
        )
        return
    res = await toggle_role(guild_id, member_id, emoji)
    if res is None:
        await interaction.response.send_message(
            "An error has occured. Contact an admin.",
            ephemeral=True,
        )
        return
    success, role = res
    if not success:
        await interaction.response.send_message(
            f"Your {role.mention} role has been removed.",
            ephemeral=True,
        )
        return
    await interaction.response.send_message(
        f"You have received the {role.mention} role.",
        ephemeral=True,
    )
