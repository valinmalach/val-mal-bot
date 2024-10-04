import asyncio
import os
import subprocess
import sys

import discord
from discord.ext import commands
from discord.ext.commands.errors import (
    ExtensionAlreadyLoaded,
    ExtensionFailed,
    ExtensionNotFound,
    NoEntryPointError,
)
from dotenv import load_dotenv
from quart import Quart, request

from send_discord_message import send_discord_message

load_dotenv()

DISCORD_TOKEN = os.getenv("DISCORD_TOKEN")

intents = discord.Intents.all()

bot = commands.Bot(
    command_prefix="$",
    intents=intents,
    case_insensitive=True,
    reload=True,
    max_messages=1000000000,
)


def __init__(self, bot_instance):
    self.bot = bot_instance
    self.member = discord.Member
    self.guild = discord.guild


@bot.event
async def on_ready(self):
    await send_discord_message(
        "Started successfully!", self.bot, 1291023411765837919  # bot-spam channel
    )


app = Quart(__name__)


async def main():
    bot.remove_command("help")
    for ext in [
        "cogs.events",
    ]:
        try:
            bot.load_extension(ext)
        except (
            ExtensionNotFound,
            ExtensionAlreadyLoaded,
            NoEntryPointError,
            ExtensionFailed,
        ) as e:
            print(f"Something went wrong when loading extension {ext}: {e}")
    loop = asyncio.get_event_loop()
    await bot.login(DISCORD_TOKEN)
    loop.create_task(bot.connect())


@app.before_serving
async def before_serving():
    await main()


@bot.command()
async def restart(ctx):
    if ctx.author.id == 389318636201967628:  # Owner's user id
        await ctx.send("Updating...")

        try:
            print("Updating from git...")
            subprocess.run(
                ["git", "pull"],
                capture_output=True,
                text=True,
                cwd="/home/valinmalach/val-mal-bot",
                check=True,
            )
            print("Installing requirements...")
            subprocess.run(
                ["pip3", "install", "-r", "requirements.txt", "-U"],
                capture_output=True,
                text=True,
                cwd="/home/valinmalach/val-mal-bot",
                check=True,
            )
        except subprocess.CalledProcessError as error:
            await ctx.send(f"Update failed: {error}")
            return

        await ctx.send("Restarting to apply updates...")
        os.execv(sys.executable, [sys.executable] + sys.argv)
    else:
        await ctx.send(
            "I don't know who you are, and I don't know what you want. "
            + "If you stop now, that'll be the end of it. I will not look for you, "
            + "I will not pursue you. But if you don't, I will look for you, "
            + "I will find you, and I will ban you."
        )


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
            "I am live on Twitch! Come join at https://www.twitch.tv/valinmalach",
            bot,
            1285276760044474461,  # stream-alerts channel
        )

    return {"status": "ok"}


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000)
