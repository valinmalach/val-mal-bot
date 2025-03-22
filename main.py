import truststore

truststore.inject_into_ssl()

import asyncio
import hashlib
import hmac
import os

import discord
import quart
from discord.ext.commands import Bot
from discord.ext.commands.errors import (
    ExtensionAlreadyLoaded,
    ExtensionFailed,
    ExtensionNotFound,
    NoEntryPointError,
)
from dotenv import load_dotenv
from quart import Quart, request
from werkzeug.datastructures import Headers
from xata import XataClient

from constants import (
    BOT_ADMIN_CHANNEL,
    GUILD_ID,
    LIVE_ALERTS_ROLE,
    STREAM_ALERTS_CHANNEL,
)
from helper import send_message

load_dotenv()

XATA_API_KEY = os.getenv("XATA_API_KEY")
DATABASE_URL = os.getenv("DATABASE_URL")

xata_client = XataClient(api_key=XATA_API_KEY, db_url=DATABASE_URL)

DISCORD_TOKEN = os.getenv("DISCORD_TOKEN")
MY_GUILD = discord.Object(id=GUILD_ID)
COGS = ["cogs.admin", "cogs.birthday", "cogs.events", "cogs.tasks"]

TWITCH_WEBHOOK_SECRET = os.getenv("TWITCH_WEBHOOK_SECRET")

TWITCH_MESSAGE_ID = "Twitch-Eventsub-Message-Id"
TWITCH_MESSAGE_TYPE = "Twitch-Eventsub-Message-Type"
TWITCH_MESSAGE_TIMESTAMP = "Twitch-Eventsub-Message-Timestamp"
TWITCH_MESSAGE_SIGNATURE = "Twitch-Eventsub-Message-Signature"
HMAC_PREFIX = "sha256="


class MyBot(Bot):
    def __init__(self, *, command_prefix: str, intents: discord.Intents):
        super().__init__(command_prefix=command_prefix, intents=intents)
        self.case_insensitive = True

    async def setup_hook(self):
        self.tree.copy_global_to(guild=MY_GUILD)
        await self.tree.sync(guild=MY_GUILD)


bot = MyBot(command_prefix="$", intents=discord.Intents.all())


@bot.event
async def on_ready():
    await send_message("Started successfully!", bot, BOT_ADMIN_CHANNEL)


@bot.tree.command(description="Reload all extensions")
@discord.app_commands.commands.default_permissions(administrator=True)
async def reload(interaction: discord.Interaction):
    try:
        await interaction.response.send_message("Reloading extensions...")
        process = await asyncio.create_subprocess_exec(
            "powershell.exe", "-File", "C:\\val-mal-bot\\git_pull.ps1"
        )
        await process.wait()
        for ext in COGS:
            try:
                bot.reload_extension(ext)
            except Exception as err:
                await interaction.response.send_message(
                    f"Something went wrong when loading extension {ext}: {err}"
                )
        await interaction.response.send_message("Reloaded!")
    except Exception as e:
        send_message(f"Error reloading extensions: {e}", bot, BOT_ADMIN_CHANNEL)


async def main():
    try:
        bot.remove_command("help")
        for ext in COGS:
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
    except Exception as e:
        print(f"Error connecting the bot: {e}")


def get_hmac_message(headers: Headers, body: str) -> str:
    return headers[TWITCH_MESSAGE_ID] + headers[TWITCH_MESSAGE_TIMESTAMP] + body


def get_hmac(secret: str, message: str) -> str:
    return hmac.new(
        secret.encode("utf-8"), message.encode("utf-8"), hashlib.sha256
    ).hexdigest()


def verify_message(hmac_str: str, verify_signature: str) -> bool:
    return hmac.compare_digest(hmac_str, verify_signature)


app = Quart(__name__)


@app.before_serving
async def before_serving():
    await main()


@app.route("/webhook/twitch", methods=["POST"])
async def twitch_webhook():
    try:
        headers = request.headers
        body_str = await request.get_data(as_text=True)
        body: dict[str, str | dict[str, str]] = await request.get_json()

        if headers.get(TWITCH_MESSAGE_TYPE) == "webhook_callback_verification":
            return body["challenge"]

        message = get_hmac_message(headers, body_str)
        secret_hmac = HMAC_PREFIX + get_hmac(TWITCH_WEBHOOK_SECRET, message)

        if verify_message(secret_hmac, headers[TWITCH_MESSAGE_SIGNATURE]):
            if body.get("subscription", {}).get("type", "") == "stream.online":
                await send_message(
                    f"<@&{LIVE_ALERTS_ROLE}> Valin has gone live!\n"
                    + "Come join at https://www.twitch.tv/valinmalach",
                    bot,
                    STREAM_ALERTS_CHANNEL,
                )

            return ""

        await send_message(
            "403: Forbidden request on /webhook/twitch. Signature does not match.",
            bot,
            BOT_ADMIN_CHANNEL,
        )
        print("403: Forbidden. Signature does not match.")
        quart.abort(403)
    except Exception as e:
        await send_message(
            f"500: Internal server error on /webhook/twitch: {e}",
            bot,
            BOT_ADMIN_CHANNEL,
        )
        print(f"500: Internal server error: {e}")
        quart.abort(500)


@app.route("/health", methods=["GET"])
async def health():
    return "Healthy"


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000)
