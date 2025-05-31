import truststore

truststore.inject_into_ssl()

import asyncio
import os
from typing import Any, Optional

import discord
import quart
import requests
import sentry_sdk
from discord.ext.commands import Bot
from discord.ext.commands.errors import (
    ExtensionAlreadyLoaded,
    ExtensionFailed,
    ExtensionNotFound,
    ExtensionNotLoaded,
    NoEntryPointError,
)
from discord.ui import View
from dotenv import load_dotenv
from quart import Quart, ResponseReturnValue, request
from sentry_sdk.integrations.quart import QuartIntegration

from constants import (
    BOT_ADMIN_CHANNEL,
    COGS,
    GUILD_ID,
    LIVE_ALERTS_ROLE,
    STREAM_ALERTS_CHANNEL,
)
from helper import (
    get_hmac,
    get_hmac_message,
    parse_rfc3339,
    send_embed,
    send_message,
    verify_message,
)
from models.auth_response import AuthResponse
from models.stream_info import StreamInfo, StreamInfoResponse
from models.stream_online_event_sub import StreamOnlineEventSub
from models.user import UserInfo, UserInfoResponse

load_dotenv()

sentry_sdk.init(
    dsn="https://8a7232f8683fae9b47c91b194053ed11@o4508900413865984.ingest.us.sentry.io/4508900418584576",
    integrations=[QuartIntegration()],
    # Add data like request headers and IP for users,
    # see https://docs.sentry.io/platforms/python/data-management/data-collected/ for more info
    send_default_pii=True,
    # Set traces_sample_rate to 1.0 to capture 100%
    # of transactions for tracing.
    traces_sample_rate=1.0,
    # Set profile_session_sample_rate to 1.0 to profile 100%
    # of profile sessions.
    profile_session_sample_rate=1.0,
    # Set profile_lifecycle to "trace" to automatically
    # run the profiler on when there is an active transaction
    profile_lifecycle="trace",
)

sentry_sdk.profiler.start_profiler()  # type: ignore


DISCORD_TOKEN = os.getenv("DISCORD_TOKEN")
MY_GUILD = discord.Object(id=GUILD_ID)


TWITCH_WEBHOOK_SECRET = os.getenv("TWITCH_WEBHOOK_SECRET")

TWITCH_MESSAGE_ID = "Twitch-Eventsub-Message-Id"
TWITCH_MESSAGE_TYPE = "Twitch-Eventsub-Message-Type"
TWITCH_MESSAGE_TIMESTAMP = "Twitch-Eventsub-Message-Timestamp"
TWITCH_MESSAGE_SIGNATURE = "Twitch-Eventsub-Message-Signature"
HMAC_PREFIX = "sha256="

TWITCH_CLIENT_ID = os.getenv("TWITCH_CLIENT_ID")
TWITCH_CLIENT_SECRET = os.getenv("TWITCH_CLIENT_SECRET")
access_token = ""


class MyBot(Bot):
    def __init__(self, *, command_prefix: str, intents: discord.Intents) -> None:
        super().__init__(command_prefix=command_prefix, intents=intents)
        self.case_insensitive = True

    async def setup_hook(self) -> None:
        self.tree.copy_global_to(guild=MY_GUILD)
        await self.tree.sync(guild=MY_GUILD)


bot = MyBot(command_prefix="$", intents=discord.Intents.all())


@bot.event
@sentry_sdk.trace()
async def on_ready() -> None:
    await send_message("Started successfully!", bot, BOT_ADMIN_CHANNEL)


@bot.tree.command(description="Reload all extensions")
@discord.app_commands.commands.default_permissions(administrator=True)
@sentry_sdk.trace()
async def reload(interaction: discord.Interaction) -> None:
    try:
        await interaction.response.send_message("Reloading extensions...")
        process = await asyncio.create_subprocess_exec(
            "powershell.exe", "-File", "C:\\val-mal-bot\\git_pull.ps1"
        )
        await process.wait()
        for ext in COGS:
            try:
                await bot.reload_extension(ext)
            except ExtensionNotLoaded as e:
                await bot.load_extension(ext)
            except Exception as e:
                sentry_sdk.capture_exception(e)
                await send_message(
                    f"Something went wrong when loading extension {ext}: {e}",
                    bot,
                    BOT_ADMIN_CHANNEL,
                )
        await send_message("Reloaded!", bot, BOT_ADMIN_CHANNEL)
    except Exception as e:
        sentry_sdk.capture_exception(e)
        await send_message(f"Error reloading extensions: {e}", bot, BOT_ADMIN_CHANNEL)


@sentry_sdk.trace()
async def main() -> None:
    try:
        if not DISCORD_TOKEN:
            raise ValueError("DISCORD_TOKEN is not set in the environment variables.")
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
                sentry_sdk.capture_exception(e)
                print(f"Something went wrong when loading extension {ext}: {e}")

        loop = asyncio.get_event_loop()
        await bot.login(DISCORD_TOKEN)
        loop.create_task(bot.connect())
    except Exception as e:
        sentry_sdk.capture_exception(e)
        print(f"Error connecting the bot: {e}")


@sentry_sdk.trace()
async def refresh_access_token() -> bool:
    global access_token
    url = f"https://id.twitch.tv/oauth2/token?client_id={TWITCH_CLIENT_ID}&client_secret={TWITCH_CLIENT_SECRET}&grant_type=client_credentials"

    response = requests.post(url)
    if response.status_code != 200:
        await send_message(
            f"Failed to refresh access token: {response.status_code} {response.text}",
            bot,
            BOT_ADMIN_CHANNEL,
        )
        return False
    auth_response = AuthResponse.model_validate(response.json())
    if auth_response.token_type == "bearer":
        access_token = auth_response.access_token
        return True
    else:
        await send_message(
            f"Unexpected token type: {auth_response.token_type}", bot, BOT_ADMIN_CHANNEL
        )
        return False


@sentry_sdk.trace()
async def get_user(id: str) -> Optional[UserInfo]:
    global access_token
    if not access_token:
        refresh_success = await refresh_access_token()
        if not refresh_success:
            return None

    url = f"https://api.twitch.tv/helix/users?id={id}"
    headers = {
        "Client-ID": TWITCH_CLIENT_ID,
        "Authorization": f"Bearer {access_token}",
    }
    response = requests.get(url, headers=headers)
    if response.status_code == 401:
        if await refresh_access_token():
            headers["Authorization"] = f"Bearer {access_token}"
            response = requests.get(url, headers=headers)
        else:
            return None
    if response.status_code != 200:
        await send_message(
            f"Failed to fetch user info: {response.status_code} {response.text}",
            bot,
            BOT_ADMIN_CHANNEL,
        )
        return None
    stream_info_response = UserInfoResponse.model_validate(response.json())
    return stream_info_response.data[0] if stream_info_response.data else None


@sentry_sdk.trace()
async def get_stream_info(broadcaster_id: str) -> Optional[StreamInfo]:
    global access_token
    if not access_token:
        refresh_success = await refresh_access_token()
        if not refresh_success:
            return None

    url = f"https://api.twitch.tv/helix/streams?user_id={broadcaster_id}"
    headers = {
        "Client-ID": TWITCH_CLIENT_ID,
        "Authorization": f"Bearer {access_token}",
    }
    response = requests.get(url, headers=headers)
    if response.status_code == 401:
        if await refresh_access_token():
            headers["Authorization"] = f"Bearer {access_token}"
            response = requests.get(url, headers=headers)
        else:
            return None
    if response.status_code != 200:
        await send_message(
            f"Failed to fetch stream info: {response.status_code} {response.text}",
            bot,
            BOT_ADMIN_CHANNEL,
        )
        return None
    stream_info_response = StreamInfoResponse.model_validate(response.json())
    return stream_info_response.data[0] if stream_info_response.data else None


app = Quart(__name__)


@app.before_serving
async def before_serving():
    await main()


@app.route("/webhook/twitch", methods=["POST"])
async def twitch_webhook() -> ResponseReturnValue:
    try:
        headers = request.headers
        body: dict[str, Any] = await request.get_json()

        if headers.get(TWITCH_MESSAGE_TYPE) == "webhook_callback_verification":
            return body.get("challenge", "") or ""

        twitch_message_id = headers.get(TWITCH_MESSAGE_ID, "")
        twitch_message_timestamp = headers.get(TWITCH_MESSAGE_TIMESTAMP, "")
        body_str = await request.get_data(as_text=True)
        message = get_hmac_message(
            twitch_message_id, twitch_message_timestamp, body_str
        )
        secret_hmac = HMAC_PREFIX + get_hmac(TWITCH_WEBHOOK_SECRET, message)

        twitch_message_signature = headers.get(TWITCH_MESSAGE_SIGNATURE, "")
        if verify_message(secret_hmac, twitch_message_signature):
            event_sub = StreamOnlineEventSub.model_validate(body)
            if event_sub.subscription.type == "stream.online":
                stream_info = await get_stream_info(event_sub.event.broadcaster_user_id)
                user_info = await get_user(event_sub.event.broadcaster_user_id)
                if not stream_info:
                    url = f"https://www.twitch.tv/{event_sub.event.broadcaster_user_login}"
                    await send_message(
                        f"<@&{LIVE_ALERTS_ROLE}> Valin has gone live!\n"
                        + f"Come join at {url}",
                        bot,
                        STREAM_ALERTS_CHANNEL,
                    )
                    await send_message(
                        "Failed to fetch stream info for the online event.",
                        bot,
                        BOT_ADMIN_CHANNEL,
                    )
                    return ""

                url = f"https://www.twitch.tv/{stream_info.user_login}"
                embed = (
                    discord.Embed(
                        description=f"[**{stream_info.title}**]({url})",
                        color=0x9046FF,
                        timestamp=parse_rfc3339(stream_info.started_at),
                    )
                    .set_author(
                        name=f"{stream_info.user_name} is now live!",
                        icon_url=user_info.profile_image_url if user_info else None,
                        url=url,
                    )
                    .add_field(
                        name="**Game**",
                        value=f"{stream_info.game_name}",
                        inline=True,
                    )
                    .add_field(
                        name="**Viewers**",
                        value=f"{stream_info.viewer_count}",
                        inline=True,
                    )
                    .set_image(
                        url=stream_info.thumbnail_url.replace(
                            "{width}x{height}", "400x225"
                        )
                    )
                )
                view = View(timeout=None)
                view.add_item(
                    discord.ui.Button(
                        label="Watch Stream", style=discord.ButtonStyle.link, url=url
                    )
                )
                await send_embed(embed, bot, BOT_ADMIN_CHANNEL, view)

            return ""

        await send_message(
            "403: Forbidden request on /webhook/twitch. Signature does not match.",
            bot,
            BOT_ADMIN_CHANNEL,
        )
        print("403: Forbidden. Signature does not match.")
        quart.abort(403)
    except Exception as e:
        sentry_sdk.capture_exception(e)
        await send_message(
            f"500: Internal server error on /webhook/twitch: {e}",
            bot,
            BOT_ADMIN_CHANNEL,
        )
        print(f"500: Internal server error: {e}")
        quart.abort(500)


@app.route("/health", methods=["GET"])
async def health() -> str:
    return "Healthy"


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000)
