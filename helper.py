from discord.ext.commands import Bot


async def send_discord_message(message: str, bot: Bot, channel_id: int):
    await bot.get_channel(channel_id).send(message)


def is_leap_year(year: int) -> bool:
    return (year % 400 == 0) or (year % 100 != 0) and (year % 4 == 0)

def get_next_leap_year(year: int) -> int:
    while not is_leap_year(year):
        year += 1
    return year