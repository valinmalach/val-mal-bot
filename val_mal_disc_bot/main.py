import asyncio
import os

import discord
import uvicorn
from dotenv import load_dotenv
from fastapi import FastAPI, Request

load_dotenv()

DISCORD_TOKEN = os.getenv("DISCORD_TOKEN")
TWITCH_CLIENT_ID = os.getenv("TWITCH_CLIENT_ID")
TWITCH_CLIENT_SECRET = os.getenv("TWITCH_CLIENT_SECRET")
TWITCH_USER_ID = os.getenv("TWITCH_USER_ID")
DISCORD_STREAM_ALERTS_CHANNEL_ID = int(os.getenv("DISCORD_STREAM_ALERTS_CHANNEL_ID"))
WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET")
APP_URL = os.getenv("APP_URL")

intents = discord.Intents.default()
intents.message_content = True
client = discord.Client(intents=intents)

app = FastAPI()


@client.event
async def on_ready():
    print(f"{client.user} has connected to Discord!")


@client.event
async def on_message(message):
    if message.author == client.user:
        return

    if message.content == "ping":
        await message.channel.send("pong")

    if message.content == "plap":
        await message.channel.send("clank")


@app.post("/webhook/twitch")
async def twitch_webhook(request: Request):
    await send_discord_message("Test alert", DISCORD_STREAM_ALERTS_CHANNEL_ID)
    headers = request.headers
    body = await request.json()

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


async def send_discord_message(message, channel_id):
    if channel := client.get_channel(channel_id):
        await channel.send(message)


async def main():
    # Start FastAPI in the background
    asyncio.create_task(await uvicorn.run(app, host="0.0.0.0", port=8000))

    # Run the Discord bot
    await client.start(DISCORD_TOKEN)


if __name__ == "__main__":
    asyncio.run(main())
