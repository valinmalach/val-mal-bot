import truststore

truststore.inject_into_ssl()

import asyncio
import os
from datetime import datetime
from typing import Any

import discord
import quart
import sentry_sdk
from discord.ext.commands.errors import (
    ExtensionAlreadyLoaded,
    ExtensionFailed,
    ExtensionNotFound,
    NoEntryPointError,
)
from discord.ui import View
from dotenv import load_dotenv
from quart import Quart, ResponseReturnValue, request
from sentry_sdk.integrations.quart import QuartIntegration

from constants import (
    BOT_ADMIN_CHANNEL,
    COGS,
    HMAC_PREFIX,
    LIVE_ALERTS_ROLE,
    STREAM_ALERTS_CHANNEL,
    TWITCH_MESSAGE_ID,
    TWITCH_MESSAGE_SIGNATURE,
    TWITCH_MESSAGE_TIMESTAMP,
    TWITCH_MESSAGE_TYPE,
)
from init.bot_init import bot
from init.xata_init import xata_client
from models.stream_offline_event_sub import StreamOfflineEventSub
from models.stream_online_event_sub import StreamOnlineEventSub
from services.helper import (
    edit_embed,
    get_age,
    get_hmac,
    get_hmac_message,
    parse_rfc3339,
    send_embed,
    send_message,
    verify_message,
)
from services.twitch_service import get_channel, get_stream_info, get_user, update_alert

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
TWITCH_WEBHOOK_SECRET = os.getenv("TWITCH_WEBHOOK_SECRET")


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
        if not verify_message(secret_hmac, twitch_message_signature):
            await send_message(
                "403: Forbidden request on /webhook/twitch. Signature does not match.",
                BOT_ADMIN_CHANNEL,
            )
            print("403: Forbidden. Signature does not match.")
            quart.abort(403)

        event_sub = StreamOnlineEventSub.model_validate(body)
        if event_sub.subscription.type != "stream.online":
            await send_message(
                "400: Bad request on /webhook/twitch. Invalid subscription type.",
                BOT_ADMIN_CHANNEL,
            )
            print("400: Bad request. Invalid subscription type.")
            quart.abort(400)

        broadcaster_id = event_sub.event.broadcaster_user_id
        stream_info = await get_stream_info(broadcaster_id)
        user_info = await get_user(broadcaster_id)
        if not stream_info:
            url = f"https://www.twitch.tv/{event_sub.event.broadcaster_user_login}"
            await send_message(
                f"<@&{LIVE_ALERTS_ROLE}> Valin has gone live!\n"
                + f"Come join at {url}",
                STREAM_ALERTS_CHANNEL,
            )
            await send_message(
                "Failed to fetch stream info for the online event.",
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
                url=stream_info.thumbnail_url.replace("{width}x{height}", "400x225")
            )
        )
        view = View(timeout=None)
        view.add_item(
            discord.ui.Button(
                label="Watch Stream", style=discord.ButtonStyle.link, url=url
            )
        )
        message_id = await send_embed(embed, STREAM_ALERTS_CHANNEL, view)
        if message_id is None:
            await send_message(
                f"Failed to send live alert message\nbroadcaster_id: {broadcaster_id}\nchannel_id: {STREAM_ALERTS_CHANNEL}",
                BOT_ADMIN_CHANNEL,
            )
            return ""
        alert = {
            "channel_id": STREAM_ALERTS_CHANNEL,
            "message_id": message_id,
            "stream_id": stream_info.id,
            "stream_started_at": stream_info.started_at,
        }
        resp = xata_client.records().upsert("live_alerts", broadcaster_id, alert)
        if resp.is_success():
            asyncio.create_task(
                update_alert(broadcaster_id, STREAM_ALERTS_CHANNEL, message_id)
            )
        else:
            await send_message(
                f"Failed to insert live alert message into database\nbroadcaster_id: {broadcaster_id}\nchannel_id: {STREAM_ALERTS_CHANNEL}\n message_id: {message_id}\n\n{resp.error_message}",
                BOT_ADMIN_CHANNEL,
            )

        return ""
    except Exception as e:
        sentry_sdk.capture_exception(e)
        await send_message(
            f"500: Internal server error on /webhook/twitch: {e}",
            BOT_ADMIN_CHANNEL,
        )
        print(f"500: Internal server error: {e}")
        quart.abort(500)


@app.route("/webhook/twitch/offline", methods=["POST"])
async def twitch_webhook_offline() -> ResponseReturnValue:
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
        if not verify_message(secret_hmac, twitch_message_signature):
            await send_message(
                "403: Forbidden request on /webhook/twitch/offline. Signature does not match.",
                BOT_ADMIN_CHANNEL,
            )
            print("403: Forbidden. Signature does not match.")
            quart.abort(403)

        event_sub = StreamOfflineEventSub.model_validate(body)
        if event_sub.subscription.type != "stream.offline":
            await send_message(
                "400: Bad request on /webhook/twitch/offline. Invalid subscription type.",
                BOT_ADMIN_CHANNEL,
            )
            print("400: Bad request. Invalid subscription type.")
            quart.abort(400)

        broadcaster_id = event_sub.event.broadcaster_user_id
        user_info = await get_user(broadcaster_id)
        channel_info = await get_channel(broadcaster_id)

        alert = xata_client.records().get("live_alerts", broadcaster_id)
        if not alert.is_success():
            await send_message(
                f"Failed to fetch live alert for {broadcaster_id}: {alert.error_message}",
                BOT_ADMIN_CHANNEL,
            )
            return ""

        channel_id = alert.get("channel_id", 0)
        message_id = alert.get("message_id", 0)
        stream_id = alert.get("stream_id", "")
        stream_started_at = alert.get("stream_started_at", "")

        url = f"https://www.twitch.tv/{event_sub.event.broadcaster_user_login}"
        embed = (
            discord.Embed(
                description=f"**{channel_info.title if channel_info else ''}**",
                color=0x9046FF,
                timestamp=datetime.now(),
            )
            .set_author(
                name=f"{event_sub.event.broadcaster_user_name} was live",
                icon_url=user_info.profile_image_url if user_info else None,
                url=url,
            )
            .add_field(
                name="**Game**",
                value=f"{channel_info.game_name if channel_info else ''}",
                inline=True,
            )
        )
        if stream_id:
            embed = embed.add_field(
                name="**VOD**",
                value=f"https://www.twitch.tv/videos/{stream_id}",
                inline=True,
            )
        if stream_started_at:
            started_at = parse_rfc3339(stream_started_at)
            age = get_age(started_at)
            embed = embed.set_footer(
                text=f"Online for {age} | Offline at",
            )
        await edit_embed(message_id, embed, channel_id)

        resp = xata_client.records().delete("live_alerts", broadcaster_id)
        if not resp.is_success():
            await send_message(
                f"Failed to delete live alert for {broadcaster_id}: {resp.error_message}",
                BOT_ADMIN_CHANNEL,
            )

        return ""
    except Exception as e:
        sentry_sdk.capture_exception(e)
        await send_message(
            f"500: Internal server error on /webhook/twitch/offline: {e}",
            BOT_ADMIN_CHANNEL,
        )
        print(f"500: Internal server error: {e}")
        quart.abort(500)


@app.route("/health", methods=["GET"])
async def health() -> str:
    return "Healthy"


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000)
