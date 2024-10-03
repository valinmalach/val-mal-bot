import asyncio
import os

import discord
import requests
import uvicorn
from discord.ext import tasks
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


def subscribe_to_twitch_webhook():
    access_token = get_twitch_oauth_token()
    headers = {
        "Client-ID": TWITCH_CLIENT_ID,
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json",
    }

    callback_url = f"{APP_URL}/webhook/twitch"

    data = {
        "hub.callback": callback_url,
        "hub.mode": "subscribe",
        "hub.topic": f"https://api.twitch.tv/helix/streams?user_id={TWITCH_USER_ID}",
        "hub.lease_seconds": 864000,
        "hub.secret": WEBHOOK_SECRET,
    }

    response = requests.post(
        "https://api.twitch.tv/helix/webhooks/hub",
        json=data,
        headers=headers,
        timeout=60,
    )
    response.raise_for_status()
    print("Subscribed to Twitch webhook:", response.status_code, response.json())


def get_twitch_oauth_token():
    url = "https://id.twitch.tv/oauth2/token"
    params = {
        "client_id": TWITCH_CLIENT_ID,
        "client_secret": TWITCH_CLIENT_SECRET,
        "grant_type": "client_credentials",
    }
    response = requests.post(url, params=params, timeout=60)
    return response.json()["access_token"]


# Renew webhook subscription every 9 days (a little less to be safe)
@tasks.loop(hours=216)
async def renew_twitch_webhook():
    subscribe_to_twitch_webhook()


async def main():
    # Subscribe to the Twitch webhook once when the bot starts
    subscribe_to_twitch_webhook()

    # Run the FastAPI app in a background thread
    loop = asyncio.get_event_loop()
    loop.create_task(uvicorn.run(app, host="0.0.0.0", port=8000))

    # Run the Discord bot
    await client.start(DISCORD_TOKEN)


if __name__ == "__main__":
    asyncio.run(main())
