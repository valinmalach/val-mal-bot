import asyncio
import os
from datetime import datetime
from typing import Any

import discord
import quart
import sentry_sdk
from discord.ui import View
from dotenv import load_dotenv
from quart import Blueprint, ResponseReturnValue, request

from constants import (
    BOT_ADMIN_CHANNEL,
    HMAC_PREFIX,
    LIVE_ALERTS_ROLE,
    PROMO_CHANNEL,
    STREAM_ALERTS_CHANNEL,
    TWITCH_MESSAGE_ID,
    TWITCH_MESSAGE_SIGNATURE,
    TWITCH_MESSAGE_TIMESTAMP,
    TWITCH_MESSAGE_TYPE,
)
from init import xata_client
from models import StreamOfflineEventSub, StreamOnlineEventSub
from services import (
    edit_embed,
    get_age,
    get_channel,
    get_hmac,
    get_hmac_message,
    get_stream_info,
    get_stream_vod,
    get_user,
    parse_rfc3339,
    send_embed,
    send_message,
    update_alert,
    verify_message,
)

load_dotenv()

TWITCH_WEBHOOK_SECRET = os.getenv("TWITCH_WEBHOOK_SECRET")

twitch_bp = Blueprint("twitch", __name__)


@twitch_bp.route("/webhook/twitch", methods=["POST"])
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
        while not stream_info:
            await send_message(
                "Failed to fetch stream info for the online event. Retrying...",
                BOT_ADMIN_CHANNEL,
            )
            await asyncio.sleep(5)
            stream_info = await get_stream_info(broadcaster_id)

        channel = (
            STREAM_ALERTS_CHANNEL
            if stream_info.user_login == "valinmalach"
            else PROMO_CHANNEL
        )
        content = (
            f"<@&{LIVE_ALERTS_ROLE}>" if channel == STREAM_ALERTS_CHANNEL else None
        )

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
        message_id = await send_embed(embed, channel, view, content=content)
        if message_id is None:
            await send_message(
                f"Failed to send live alert message\nbroadcaster_id: {broadcaster_id}\nchannel_id: {channel}",
                BOT_ADMIN_CHANNEL,
            )
            return ""
        alert = {
            "channel_id": channel,
            "message_id": message_id,
            "stream_id": stream_info.id,
            "stream_started_at": stream_info.started_at,
        }
        resp = xata_client.records().upsert("live_alerts", broadcaster_id, alert)
        if resp.is_success():
            asyncio.create_task(update_alert(broadcaster_id, channel, message_id))
        else:
            await send_message(
                f"Failed to insert live alert message into database\nbroadcaster_id: {broadcaster_id}\nchannel_id: {channel}\n message_id: {message_id}\n\n{resp.error_message}",
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


@twitch_bp.route("/webhook/twitch/offline", methods=["POST"])
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

        vod_info = None
        for _ in range(5):
            try:
                vod_info = await get_stream_vod(broadcaster_id, stream_id)
                if vod_info:
                    break
            except Exception as e:
                sentry_sdk.capture_exception(e)
                await send_message(
                    f"Failed to fetch VOD info for {broadcaster_id}: {e}",
                    BOT_ADMIN_CHANNEL,
                )
            await asyncio.sleep(5)

        content = (
            f"<@&{LIVE_ALERTS_ROLE}>" if channel_id == STREAM_ALERTS_CHANNEL else None
        )

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
        if stream_id and vod_info:
            vod_url = vod_info.url
            embed = embed.add_field(
                name="**VOD**",
                value=f"[**Click to view**]({vod_url})",
                inline=True,
            )
        if stream_started_at:
            started_at = parse_rfc3339(stream_started_at)
            age = get_age(started_at, limit_units=2)
            embed = embed.set_footer(
                text=f"Online for {age} | Offline at",
            )
        await edit_embed(message_id, embed, channel_id, content=content)

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
