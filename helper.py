import datetime
from functools import cache

from dateutil import relativedelta
from discord import Embed, Member
from discord.ext.commands import Bot
from xata import XataClient


async def send_message(message: str, bot: Bot, channel_id: int):
    await bot.get_channel(channel_id).send(message)


async def send_embed(embed: Embed, bot: Bot, channel_id: int):
    await bot.get_channel(channel_id).send(embed=embed)


def get_pfp(member: Member) -> str:
    return member.avatar.url if member.avatar else member.default_avatar.url


def get_discriminator(member: Member) -> str:
    return "" if member.discriminator == "0" else f"#{member.discriminator}"


def update_birthday(
    xata_client: XataClient, user_id: str, record: dict[str, str]
) -> tuple[bool, Exception | None]:
    try:
        resp = xata_client.records().upsert("users", user_id, record)
        return resp.is_success(), None
    except Exception as e:
        return False, e


def get_age(date_time: datetime) -> str:
    now = datetime.datetime.now(datetime.timezone.utc)
    age = relativedelta.relativedelta(now, date_time)
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
            parts.append(format_unit(hours, "hr"))
        if minutes:
            parts.append(format_unit(minutes, "min"))
        if seconds or not parts:
            parts.append(format_unit(seconds, "sec"))
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
    return f"{value} {f"{unit}s" if value != 1 else unit}"
