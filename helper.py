from functools import lru_cache

from discord.ext.commands import Bot
from xata import XataClient


async def send_discord_message(message: str, bot: Bot, channel_id: int):
    await bot.get_channel(channel_id).send(message)


@lru_cache
def is_leap_year(year: int) -> bool:
    return (year % 400 == 0) or (year % 100 != 0) and (year % 4 == 0)


@lru_cache
def get_next_leap_year(year: int) -> int:
    while not is_leap_year(year):
        year += 1
    return year


def update_birthday(
    xata_client: XataClient, user_id: str, record: dict[str, str]
) -> tuple[bool, Exception | None]:
    try:
        existing_user = xata_client.records().get("users", user_id)
        if existing_user.is_success():
            resp = xata_client.records().update("users", user_id, record)
        else:
            resp = xata_client.records().insert_with_id("users", user_id, record)
        return resp.is_success(), None
    except Exception as e:
        return False, e
