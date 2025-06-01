import hashlib
import hmac
from datetime import datetime, timezone
from functools import cache
from typing import Optional

import sentry_sdk
from dateutil import relativedelta
from dateutil.parser import isoparse
from discord import (
    CategoryChannel,
    DMChannel,
    Embed,
    ForumChannel,
    GroupChannel,
    Member,
    Object,
    PartialInviteChannel,
    PartialMessageable,
    StageChannel,
    TextChannel,
    Thread,
    User,
    VoiceChannel,
)
from discord.abc import GuildChannel, PrivateChannel
from discord.ui import View

from init import bot, xata_client


@sentry_sdk.trace()
async def send_message(message: str, channel_id: int) -> Optional[int]:
    channel = bot.get_channel(channel_id)
    if channel is None or isinstance(
        channel, (ForumChannel, CategoryChannel, PrivateChannel)
    ):
        return
    return (await channel.send(message)).id


@sentry_sdk.trace()
async def send_embed(
    embed: Embed, channel_id: int, view: Optional[View] = None
) -> Optional[int]:
    channel = bot.get_channel(channel_id)
    if channel is None or isinstance(
        channel, (ForumChannel, CategoryChannel, PrivateChannel)
    ):
        return
    if view:
        return (await channel.send(embed=embed, view=view)).id
    return (await channel.send(embed=embed)).id


@sentry_sdk.trace()
async def edit_embed(
    message_id: int,
    embed: Embed,
    channel_id: int,
    view: Optional[View] = None,
) -> None:
    channel = bot.get_channel(channel_id)
    if channel is None or isinstance(
        channel, (ForumChannel, CategoryChannel, PrivateChannel)
    ):
        return
    message = await channel.fetch_message(message_id)
    if view:
        await message.edit(embed=embed, view=view)
    await message.edit(embed=embed)


@sentry_sdk.trace()
def get_pfp(member: User | Member) -> str:
    return member.avatar.url if member.avatar else member.default_avatar.url


@sentry_sdk.trace()
def get_discriminator(member: User | Member) -> str:
    return "" if member.discriminator == "0" else f"#{member.discriminator}"


@sentry_sdk.trace()
def update_birthday(
    user_id: str, record: dict[str, str]
) -> tuple[bool, Exception | None]:
    try:
        resp = xata_client.records().upsert("users", user_id, record)
        return resp.is_success(), None
    except Exception as e:
        sentry_sdk.capture_exception(e)
        return False, e


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
def get_age(date_time: datetime) -> str:
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
            parts.append(format_unit(seconds, "second"))
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
