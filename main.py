import truststore

truststore.inject_into_ssl()

import asyncio
import hashlib
import hmac
import os
import subprocess

import discord
from atproto import Client
from discord.ext import commands, tasks
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

BLUESKY_LOGIN = os.getenv("BLUESKY_LOGIN")
BLUESKY_APP_PASSWORD = os.getenv("BLUESKY_APP_PASSWORD")

at_client = Client()
at_client.login(BLUESKY_LOGIN, BLUESKY_APP_PASSWORD)

XATA_API_KEY = os.getenv("XATA_API_KEY")
DATABASE_URL = os.getenv("DATABASE_URL")

xata_client = XataClient(api_key=XATA_API_KEY, db_url=DATABASE_URL)

DISCORD_TOKEN = os.getenv("DISCORD_TOKEN")
TWITCH_WEBHOOK_SECRET = os.getenv("TWITCH_WEBHOOK_SECRET")

TWITCH_MESSAGE_ID = "Twitch-Eventsub-Message-Id"
TWITCH_MESSAGE_TYPE = "Twitch-Eventsub-Message-Type"
TWITCH_MESSAGE_TIMESTAMP = "Twitch-Eventsub-Message-Timestamp"
TWITCH_MESSAGE_SIGNATURE = "Twitch-Eventsub-Message-Signature"
HMAC_PREFIX = "sha256="

intents = discord.Intents.all()

bot = commands.Bot(
    command_prefix="$",
    intents=intents,
    case_insensitive=True,
    reload=True,
    max_messages=1000000000,
)


@tasks.loop(minutes=1)
async def check_posts():
    last_sync_date_time = xata_client.data().query(
        "bluesky", {"columns": ["date"], "sort": {"date": "desc"}, "page": {"size": 1}}
    )["records"][0]["date"]

    # Get all posts, filter by author handle and last sync, and sort by indexed_at
    posts = sorted(
        [
            feed.post
            for feed in at_client.get_author_feed(actor=BLUESKY_LOGIN).feed
            if feed.post.author.handle == BLUESKY_LOGIN
            and feed.post.indexed_at > last_sync_date_time
        ],
        key=lambda post: post.indexed_at,
    )

    # Build a list with each post's id, date, and URL
    posts = [
        {
            "id": post.uri.split("/")[-1],
            "date": post.indexed_at,
            "url": f"https://fxbsky.app/profile/valinmalach.bsky.social/post/{post.uri.split('/')[-1]}",
        }
        for post in posts
    ]

    for post in posts:
        post_id = post.pop("id")
        resp = xata_client.records().insert_with_id("bluesky", post_id, post)
        if resp.is_success():
            await send_discord_message(
                f"<@&1345584502805626973>\n\n{post["url"]}",
                bot,
                1345582916050354369,  # bluesky announcement channel
            )
        else:
            print(f"Failed to insert post {post_id}.")


@bot.event
async def on_ready():
    check_posts.start()
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
    if ctx.author.id == 389318636201967628:  # Owner's user id
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

    if (
        TWITCH_MESSAGE_TYPE in headers
        and headers[TWITCH_MESSAGE_TYPE] == "webhook_callback_verification"
    ):
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
    else:
        print("403: Forbidden. Signature does not match.")
        abort(403)


@app.route("/health", methods=["GET"])
async def health():
    return Response("Healthy", 200)


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000)
