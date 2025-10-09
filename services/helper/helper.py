import asyncio
import hashlib
import hmac
import logging
from functools import cache
from typing import Optional

import discord
import pendulum
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
from pendulum import DateTime
from polars import DataFrame

from constants import EMOJI_ROLE_MAP
from init import bot
from services.helper.parquet_cache import parquet_cache

logger = logging.getLogger(__name__)


async def upsert_row_to_parquet_async(
    row_data: dict, filepath: str, id_column: str = "id"
) -> tuple[bool, Exception | None]:
    return await parquet_cache.upsert_row(row_data, filepath, id_column)


def delete_row_from_parquet(
    id_value: str | int, filepath: str, id_column: str = "id"
) -> tuple[bool, Exception | None]:
    loop = asyncio.get_event_loop()
    return loop.run_until_complete(
        parquet_cache.delete_row(id_value, filepath, id_column)
    )


async def read_parquet_cached(filepath: str) -> DataFrame:
    return await parquet_cache.read_df(filepath)


async def send_message(content: str, channel_id: int) -> Optional[int]:
    channel = bot.get_channel(channel_id)
    if channel is None or isinstance(
        channel, (ForumChannel, CategoryChannel, PrivateChannel)
    ):
        return
    return (await channel.send(content)).id


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


def get_pfp(member: User | Member) -> str:
    return member.avatar.url if member.avatar else member.default_avatar.url


def get_discriminator(member: User | Member) -> str:
    return "" if member.discriminator == "0" else f"#{member.discriminator}"


async def update_birthday(record: dict[str, str]) -> tuple[bool, Exception | None]:
    return await upsert_row_to_parquet_async(record, "data/users.parquet")


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


def get_age(date_time: DateTime, limit_units: int = -1) -> str:
    now = pendulum.now("UTC")
    age = now - date_time if date_time <= now else date_time - now
    years, months, days, hours, minutes, seconds = (
        age.years,
        age.months,
        age.remaining_days,
        age.hours,
        age.minutes,
        age.remaining_seconds,
    )
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


@cache
def is_leap(year: int) -> bool:
    return (year % 400 == 0) or (year % 100 != 0) and (year % 4 == 0)


@cache
def get_next_leap(year: int) -> int:
    while not is_leap(year):
        year += 1
    return year


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


@cache
def format_unit(value: int, unit: str) -> str:
    return f"{value} {f'{unit}s' if value != 1 else unit}"


@cache
def get_hmac_message(
    twitch_message_id: str, twitch_message_timestamp: str, body: str
) -> str:
    return twitch_message_id + twitch_message_timestamp + body


@cache
def get_hmac(secret: str, message: str) -> str:
    return hmac.new(
        secret.encode("utf-8"), message.encode("utf-8"), hashlib.sha256
    ).hexdigest()


@cache
def verify_message(hmac_str: str, verify_signature: str) -> bool:
    return hmac.compare_digest(hmac_str, verify_signature)


@cache
def parse_rfc3339(date_str: str) -> DateTime:
    """
    Parse an RFC3339 / ISO-8601 timestamp (e.g. '2025-05-31T12:34:56Z')
    and return a timezone-aware DateTime.
    """
    parsed = pendulum.parse(date_str)
    if not isinstance(parsed, DateTime):
        raise ValueError(f"Expected DateTime string, got: {date_str}")
    return parsed


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
