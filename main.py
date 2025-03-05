import truststore

truststore.inject_into_ssl()

import asyncio
import hashlib
import hmac
import os

import discord
from discord import app_commands
from discord.ext import commands
from discord.ext.commands.errors import (
    ExtensionAlreadyLoaded,
    ExtensionFailed,
    ExtensionNotFound,
    NoEntryPointError,
)
from dotenv import load_dotenv
from quart import Quart, Response, abort, request
from werkzeug.datastructures import Headers
from xata import XataClient

from send_discord_message import send_discord_message

load_dotenv()

XATA_API_KEY = os.getenv("XATA_API_KEY")
DATABASE_URL = os.getenv("DATABASE_URL")

xata_client = XataClient(api_key=XATA_API_KEY, db_url=DATABASE_URL)

DISCORD_TOKEN = os.getenv("DISCORD_TOKEN")
MY_GUILD = discord.Object(id=813237030385090580)

TWITCH_WEBHOOK_SECRET = os.getenv("TWITCH_WEBHOOK_SECRET")

TWITCH_MESSAGE_ID = "Twitch-Eventsub-Message-Id"
TWITCH_MESSAGE_TYPE = "Twitch-Eventsub-Message-Type"
TWITCH_MESSAGE_TIMESTAMP = "Twitch-Eventsub-Message-Timestamp"
TWITCH_MESSAGE_SIGNATURE = "Twitch-Eventsub-Message-Signature"
HMAC_PREFIX = "sha256="


async def is_owner(context: commands.Context) -> bool:
    if context.author.id == 389318636201967628:  # Owner's user id
        return True

    await context.send(
        "I don't know who you are, and I don't know what you want.\n"
        + "If you stop now, that'll be the end of it.\n"
        + "I will not look for you, I will not pursue you.\n"
        + "But if you don't...\n"
        + "I will look for you, I **will** find you\n"
        + "## and I will ban you."
    )
    return False


class MyBot(commands.Bot):
    def __init__(self, *, command_prefix: str, intents: discord.Intents):
        super().__init__(command_prefix=command_prefix, intents=intents)
        self.case_insensitive = True

    async def setup_hook(self):
        self.tree.copy_global_to(guild=MY_GUILD)
        await self.tree.sync(guild=MY_GUILD)


intents = discord.Intents.all()
bot = MyBot(command_prefix="$", intents=intents)


@bot.event
async def on_ready():
    await send_discord_message(
        "Started successfully!", bot, 1346408909442781237  # bot-admin channel
    )


@bot.tree.command(description="Restarts the bot")
@app_commands.checks.has_role(1285278282966896701)
async def restart(ctx: commands.Context):
    if not is_owner(ctx):
        return

    await ctx.send("Restarting...")
    await asyncio.create_subprocess_exec(
        "powershell.exe", "-File", "C:\\val-mal-bot\\restart_bot.ps1"
    )


@bot.tree.command(description="Deletes all messages in the channel")
@app_commands.checks.has_role(1285278282966896701)
async def nuke(ctx: commands.Context):
    if is_owner(ctx):
        await ctx.channel.purge(limit=1000000000)


async def main():
    bot.remove_command("help")
    for ext in [
        "cogs.events",
        "cogs.tasks",
        "cogs.commands",
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


def get_hmac_message(headers: Headers, body: str) -> str:
    return headers[TWITCH_MESSAGE_ID] + headers[TWITCH_MESSAGE_TIMESTAMP] + body


def get_hmac(secret: str, message: str) -> str:
    return hmac.new(
        secret.encode("utf-8"), message.encode("utf-8"), hashlib.sha256
    ).hexdigest()


def verify_message(hmac_str: str, verifySignature: str) -> bool:
    return hmac.compare_digest(hmac_str, verifySignature)


app = Quart(__name__)


@app.before_serving
async def before_serving():
    await main()


@app.route("/webhook/twitch", methods=["POST"])
async def twitch_webhook():
    headers = request.headers
    body_str = await request.get_data(as_text=True)
    body = await request.get_json()

    if headers.get(TWITCH_MESSAGE_TYPE) == "webhook_callback_verification":
        return body["challenge"]

    message = get_hmac_message(headers, body_str)
    secret_hmac = HMAC_PREFIX + get_hmac(TWITCH_WEBHOOK_SECRET, message)

    if verify_message(secret_hmac, headers[TWITCH_MESSAGE_SIGNATURE]):
        if body.get("subscription", {}).get("type", "") == "stream.online":
            await send_discord_message(
                "<@&1292348044888768605> Valin has gone live!\n"
                + "Come join at https://www.twitch.tv/valinmalach",
                bot,
                1285276760044474461,  # stream-alerts channel
            )

        return Response(status=200)

    await send_discord_message(
        "403: Forbidden request on /webhook/twitch. Signature does not match.",
        bot,
        1346408909442781237,  # bot-admin channel
    )
    print("403: Forbidden. Signature does not match.")
    abort(403)
    return Response(status=403)


@app.route("/health", methods=["GET"])
async def health():
    return Response("Healthy", 200)


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000)
