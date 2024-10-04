async def send_discord_message(message, bot, channel_id):
    channel = bot.get_channel(channel_id)
    await channel.send(message)
