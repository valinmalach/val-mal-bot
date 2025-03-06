from discord import User
from discord.ext.commands import Bot
from xata import XataClient


async def send_discord_message(message: str, bot: Bot, channel_id: int):
    await bot.get_channel(channel_id).send(message)


def is_leap_year(year: int) -> bool:
    return (year % 400 == 0) or (year % 100 != 0) and (year % 4 == 0)


def get_next_leap_year(year: int) -> int:
    while not is_leap_year(year):
        year += 1
    return year


def update_birthday(
    xata_client: XataClient, user: User, record: dict[str, str]
) -> bool:
    existing_user = xata_client.records().get("users", str(user.id))
    if existing_user.is_success():
        resp = xata_client.records().update("users", str(user.id), record)
    else:
        resp = xata_client.records().insert_with_id("users", str(user.id), record)
    return resp.is_success()
