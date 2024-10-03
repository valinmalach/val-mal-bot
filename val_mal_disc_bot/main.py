import asyncio
import os

import discord
from discord.ext import commands
from dotenv import load_dotenv
from quart import Quart, request

load_dotenv()

DISCORD_TOKEN = os.getenv("DISCORD_TOKEN")
TWITCH_CLIENT_ID = os.getenv("TWITCH_CLIENT_ID")
TWITCH_CLIENT_SECRET = os.getenv("TWITCH_CLIENT_SECRET")
TWITCH_USER_ID = os.getenv("TWITCH_USER_ID")
DISCORD_STREAM_ALERTS_CHANNEL_ID = int(os.getenv("DISCORD_STREAM_ALERTS_CHANNEL_ID"))
WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET")
APP_URL = os.getenv("APP_URL")

intents = discord.Intents.all()

bot = commands.Bot(
    command_prefix="!",
    intents=intents,
    case_insensitive=True,
    reload=True,
    max_messages=1000000000,
)
bot.remove_command("help")

app = Quart(__name__)


@app.before_serving
async def before_serving():
    loop = asyncio.get_event_loop()
    await bot.login(DISCORD_TOKEN)
    loop.create_task(bot.connect())


@bot.event
async def on_ready():
    print(f"{bot.user} has connected to Discord!")


@bot.event
async def on_message(message):
    if message.author == bot.user:
        return

    if message.content == "ping":
        await message.channel.send("pong")

    if message.content == "plap":
        await message.channel.send("clank")


@app.route("/webhook/twitch", methods=["POST"])
async def twitch_webhook():
    headers = request.headers
    body = await request.get_json()

    if (
        "Twitch-Eventsub-Message-Type" in headers
        and headers["Twitch-Eventsub-Message-Type"] == "webhook_callback_verification"
    ):
        return body["challenge"]

    # Handle the "stream.online" event
    if body.get("subscription", {}).get("type") == "stream.online":
        print("Stream is live, sending message to Discord.")
        # Send a message to Discord when the stream goes live
        await send_discord_message(
            f"I am live on Twitch! Come join at https://www.twitch.tv/{TWITCH_USER_ID}",
            DISCORD_STREAM_ALERTS_CHANNEL_ID,
        )

    return {"status": "ok"}


async def send_discord_message(message, channel):
    channel = bot.get_channel(channel)
    await channel.send(message)


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000)
