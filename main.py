import asyncio
import os
import subprocess

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
TWITCH_WEBHOOK_SECRET = os.getenv("TWITCH_WEBHOOK_SECRET")

intents = discord.Intents.all()

bot = commands.Bot(
    command_prefix="$",
    intents=intents,
    case_insensitive=True,
    reload=True,
    max_messages=1000000000,
)


@bot.event
async def on_ready():
    await send_discord_message(
        "Started successfully!", bot, 1291023411765837919  # bot-spam channel
    )



@bot.command()
async def restart(ctx: commands.Context):
    if ctx.author.id == 389318636201967628:  # Owner's user id
        try:
            await ctx.send("Restarting...")
            subprocess.run(
                ["powershell.exe", "-File", "C:\\val-mal-bot\\restart_bot.ps1"],
                check=True,
            )
        except subprocess.CalledProcessError as error:
            await ctx.send(f"Update failed: {error}")
            return
    else:
        await ctx.send(
            "I don't know who you are, and I don't know what you want. "
            + "If you stop now, that'll be the end of it. I will not look for you, "
            + "I will not pursue you. But if you don't, I will look for you, "
            + "I will find you, and I will ban you."
        )


@bot.command()
async def nuke(ctx: commands.Context):
    if ctx.author.id == 389318636201967628:
        await ctx.channel.purge()


async def main():
    bot.remove_command("help")
    for ext in [
        "cogs.events",
    ]:
        try:
            await bot.load_extension(ext)
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


app = Quart(__name__)


@app.before_serving
async def before_serving():
    await main()


@app.route("/webhook/twitch", methods=["POST"])
async def twitch_webhook():
    headers = request.headers
    body = await request.get_json()

    print(headers)
    print(body)

    if (
        "Twitch-Eventsub-Message-Type" in headers
        and headers["Twitch-Eventsub-Message-Type"] == "webhook_callback_verification"
    ):
        return body["challenge"]

    if body.get("subscription", {}).get("type") == "stream.online":
        await send_discord_message(
            "<@&1292348044888768605> Valin has gone live!\n"
            + "Come join at https://www.twitch.tv/valinmalach",
            bot,
            1285276760044474461,  # stream-alerts channel
        )

    return {"status": "ok"}


@app.route("/health", methods=["GET"])
async def health():
    print("Health check")
    return {"status": "ok"}


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000)
