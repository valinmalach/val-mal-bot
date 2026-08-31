import hashlib
import hmac
import logging
import re
from datetime import datetime
from functools import cache

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

from constants import Months
from init import bot
from services.config import config

logger = logging.getLogger(__name__)


async def _sendable(channel_id: int, quiet: bool):
    """The channel, or None once it has been said that there isn't one.

    Most callers here are audit logging, which discards what it gets back: a
    channel the bot cannot resolve would otherwise stop the log dead with
    nothing anywhere to say it had.
    """
    channel = bot.get_channel(channel_id)
    if channel is not None and not isinstance(
        channel, (ForumChannel, CategoryChannel, PrivateChannel)
    ):
        return channel

    if not quiet:
        # Deferred: errors imports this module.
        from errors import notify

        await notify(
            f"Channel {channel_id} could not be resolved, so nothing sent to it"
            f" is arriving.",
            key=f"channel-unresolved:{channel_id}",
        )
    return None


async def send_message(
    content: str,
    channel_id: int,
    file: discord.File | None = None,
    quiet: bool = False,
) -> int | None:
    # quiet is for errors.py alone: announcing an unreachable admin channel
    # through the admin channel does not terminate.
    channel = await _sendable(channel_id, quiet)
    if channel is None:
        return None
    if file:
        return (await channel.send(content, file=file)).id
    return (await channel.send(content)).id


async def send_embed(
    embed: Embed,
    channel_id: int,
    view: View | None = None,
    content: str | None = None,
) -> int | None:
    channel = await _sendable(channel_id, quiet=False)
    if channel is None:
        return None
    if view:
        return (await channel.send(content=content, embed=embed, view=view)).id
    return (await channel.send(content=content, embed=embed)).id


async def edit_embed(
    message_id: int,
    embed: Embed,
    channel_id: int,
    view: View | None = None,
    content: str | None = None,
) -> bool:
    """False when the channel cannot be resolved, so callers can tell a no-op from an edit."""
    channel = bot.get_channel(channel_id)
    if channel is None or isinstance(
        channel, (ForumChannel, CategoryChannel, PrivateChannel)
    ):
        logger.warning(
            f"Channel {channel_id} unavailable; skipped editing message {message_id}"
        )
        return False
    message = await channel.fetch_message(message_id)
    if view:
        await message.edit(content=content, embed=embed, view=view)
    else:
        await message.edit(content=content, embed=embed, view=None)
    return True


def get_pfp(member: User | Member) -> str:
    return member.avatar.url if member.avatar else member.default_avatar.url


def get_discriminator(member: User | Member) -> str:
    return "" if member.discriminator == "0" else f"#{member.discriminator}"


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

    parts = _get_age_parts(years, months, days, hours, minutes, seconds)
    parts = parts[:limit_units] if limit_units > 0 else parts
    return ", ".join(parts)


def _get_age_parts(
    years: int, months: int, days: int, hours: int, minutes: int, seconds: int
) -> list[str]:
    """Extract age parts based on the time units."""
    if years > 0 or months > 0:
        return _get_large_time_units(years, months, days)
    return _get_small_time_units(days, hours, minutes, seconds)


def _get_large_time_units(years: int, months: int, days: int) -> list[str]:
    """Get age parts for years, months, and days."""
    parts = []
    if years:
        parts.append(format_unit(years, "year"))
    if months:
        parts.append(format_unit(months, "month"))
    if days:
        parts.append(format_unit(days, "day"))
    return parts


def _get_small_time_units(
    days: int, hours: int, minutes: int, seconds: int
) -> list[str]:
    """Get age parts for smaller time units (weeks, days, hours, minutes, seconds)."""
    parts = []

    weeks, remaining_days = divmod(days, 7)
    if weeks:
        parts.append(format_unit(weeks, "week"))
    if remaining_days:
        parts.append(format_unit(remaining_days, "day"))
    if hours:
        parts.append(format_unit(hours, "hour"))
    if minutes:
        parts.append(format_unit(minutes, "minute"))
    if seconds or not parts:
        parts.append(format_unit(seconds, "second"))

    return parts


@cache
def _is_leap_year(year: int) -> bool:
    return (year % 400 == 0) or (year % 100 != 0) and (year % 4 == 0)


@cache
def _next_leap_year(year: int) -> int:
    while not _is_leap_year(year):
        year += 1
    return year


def is_leap_day(month: Months, day: int) -> bool:
    """Whether a birthday falls on 29 February, and so exists only some years.

    Derived here and nowhere else: the answer picks the year to store the
    birthday in and is also stored beside it, and those two must not disagree.
    """
    return month == Months.February and day == 29


def next_birthday(birthday: datetime, is_leap: bool, after: DateTime) -> DateTime:
    """The next occurrence of a stored birthday, strictly after ``after``.

    A stored birthday is a UTC instant for one particular year, so advancing it
    is a year bump; 29 February has to land on a leap year to exist at all.
    """
    moment = pendulum.instance(birthday)
    if moment > after:
        return moment

    # The flag is the authority, but a 29 February instant cannot be replaced
    # into a common year whatever the flag says.
    if is_leap or (moment.month == 2 and moment.day == 29):
        candidate = moment.replace(year=_next_leap_year(after.year))
        if candidate > after:
            return candidate
        return moment.replace(year=_next_leap_year(after.year + 1))

    candidate = moment.replace(year=after.year)
    return candidate if candidate > after else moment.replace(year=after.year + 1)


def next_birthday_on(
    month: Months, day: int, timezone: str, after: DateTime
) -> DateTime:
    """The start of the next ``month``/``day`` in ``timezone``, after ``after``.

    Local midnight, or the first instant of that day in the zones that spring
    forward across it and have no midnight on that date.
    """
    zone = pendulum.timezone(timezone)
    leap = is_leap_day(month, day)
    # Their year, not UTC's: east of UTC the two disagree for part of every day,
    # and starting from UTC's leaves both candidates in the past.
    local_year = after.in_tz(zone).year
    year = _next_leap_year(local_year) if leap else local_year
    midnight = DateTime(year, month.value, day, tzinfo=zone).in_tz("UTC")
    if midnight > after:
        return midnight

    year = _next_leap_year(year + 1) if leap else year + 1
    return DateTime(year, month.value, day, tzinfo=zone).in_tz("UTC")


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


def get_hmac_message(
    twitch_message_id: str, twitch_message_timestamp: str, body: str
) -> str:
    """Not cached: every argument comes from an unverified request."""
    return twitch_message_id + twitch_message_timestamp + body


def get_hmac(secret: str, message: str) -> str:
    return hmac.new(
        secret.encode("utf-8"), message.encode("utf-8"), hashlib.sha256
    ).hexdigest()


def verify_message(hmac_str: str, verify_signature: str) -> bool:
    return hmac.compare_digest(hmac_str, verify_signature)


# Exactly what gets through: pendulum takes a space for the T and would accept
# it, and rejects a lowercase t or z, so neither belongs in the shape.
_RFC3339 = re.compile(
    r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d{1,9})?(Z|[+-]\d{2}:\d{2})"
)


def parse_rfc3339(date_str: str) -> DateTime:
    """An RFC3339 timestamp (e.g. '2025-05-31T12:34:56Z') as an aware DateTime.

    Shape-checked first: pendulum.parse on its own also accepts durations,
    intervals, and zone-less times that it silently calls UTC.
    """
    # One error type for the caller to catch. The shape check alone still lets
    # through 2025-13-45T99:99:99Z, which pendulum throws its own type at.
    rejected = ValueError(f"Not an RFC3339 timestamp: {date_str[:40]!r}")
    if not _RFC3339.fullmatch(date_str):
        raise rejected
    try:
        parsed = pendulum.parse(date_str)
    except Exception as unparseable:
        raise rejected from unparseable
    if not isinstance(parsed, DateTime) or parsed.tzinfo is None:
        raise rejected
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

    role_name = config.role_name_for_emoji(emoji.name)
    if not role_name:
        return None, None

    role = discord.utils.get(guild.roles, name=role_name)
    return (member, role) if role else (None, None)


async def toggle_role(
    guild_id: int, user_id: int, emoji: PartialEmoji
) -> tuple[bool, Role] | None:
    member, role = get_member_role(guild_id, user_id, emoji)
    if not member or not role:
        return None

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
            config.template("discord_role_error"),
            ephemeral=True,
        )
        return
    res = await toggle_role(guild_id, member_id, emoji)
    if res is None:
        await interaction.response.send_message(
            config.template("discord_role_error"),
            ephemeral=True,
        )
        return
    success, role = res
    if not success:
        await interaction.response.send_message(
            config.template("discord_role_removed", role=role.mention),
            ephemeral=True,
        )
        return
    await interaction.response.send_message(
        config.template("discord_role_added", role=role.mention),
        ephemeral=True,
    )
