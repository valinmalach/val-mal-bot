from discord.ext.commands import Bot


async def send_discord_message(message: str, bot: Bot, channel_id: int):
    await bot.get_channel(channel_id).send(message)
