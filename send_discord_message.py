from discord.ext import commands


async def send_discord_message(message: str, bot: commands.Bot, channel_id: int):
    await bot.get_channel(channel_id).send(message)
