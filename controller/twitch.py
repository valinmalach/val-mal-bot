import asyncio
import logging
import os
from typing import Any

import discord
import pendulum
import polars as pl
import sentry_sdk
from discord.ui import View
from dotenv import load_dotenv
from fastapi import APIRouter, HTTPException, Request, Response

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
from models import StreamOfflineEventSub, StreamOnlineEventSub
from services import (
    delete_row_from_parquet,
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
    upsert_row_to_parquet,
    verify_message,
)
from services.twitch_shoutout_queue import shoutout_queue

load_dotenv()

TWITCH_WEBHOOK_SECRET = os.getenv("TWITCH_WEBHOOK_SECRET")

twitch_router = APIRouter()

logger = logging.getLogger(__name__)


@sentry_sdk.trace()
async def _twitch_webhook_task(broadcaster_id: int) -> None:
    try:
        stream_info = await get_stream_info(broadcaster_id)
        user_info = await get_user(broadcaster_id)
        while not stream_info:
            await asyncio.sleep(1)
            stream_info = await get_stream_info(broadcaster_id)

        channel = PROMO_CHANNEL
        if stream_info.user_login == "valinmalach":
            channel = STREAM_ALERTS_CHANNEL
            # Send stream start message
            asyncio.create_task(shoutout_queue.activate())

        content = (
            f"<@&{LIVE_ALERTS_ROLE}>" if channel == STREAM_ALERTS_CHANNEL else None
        )

        url = f"https://www.twitch.tv/{stream_info.user_login}"
        raw_thumb_url = stream_info.thumbnail_url.replace("{width}x{height}", "400x225")
        cache_busted_thumb_url = f"{raw_thumb_url}?cb={int(pendulum.now().timestamp())}"

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
            .set_image(url=cache_busted_thumb_url)
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
            logger.error(f"Failed to send embed for broadcaster {broadcaster_id}")
            return

        alert = {
            "id": broadcaster_id,
            "channel_id": channel,
            "message_id": message_id,
            "stream_id": int(stream_info.id),
            "stream_started_at": stream_info.started_at,
        }
        success, error = upsert_row_to_parquet(alert, "data/live_alerts.parquet")
        if success:
            asyncio.create_task(
                update_alert(
                    broadcaster_id,
                    channel,
                    message_id,
                    int(stream_info.id),
                    stream_info.started_at,
                )
            )
        else:
            logger.error(
                f"Failed to insert live alert message into parquet: {error}",
            )
            await send_message(
                f"Failed to insert live alert message into parquet\nbroadcaster_id: {broadcaster_id}\nchannel_id: {channel}\n message_id: {message_id}\n\n{error}",
                BOT_ADMIN_CHANNEL,
            )
    except Exception as e:
        logger.error(
            f"Error in _twitch_webhook_task for broadcaster_id={broadcaster_id}: {e}"
        )
        sentry_sdk.capture_exception(e)
        await send_message(
            f"Error in _twitch_webhook_task for {broadcaster_id}: {e}",
            BOT_ADMIN_CHANNEL,
        )


@sentry_sdk.trace()
async def _twitch_webhook_offline_task(event_sub: StreamOfflineEventSub) -> None:
    broadcaster_id = event_sub.event.broadcaster_user_id
    try:
        if event_sub.event.broadcaster_user_login == "valinmalach":
            await shoutout_queue.deactivate()

        user_info = await get_user(int(broadcaster_id))
        channel_info = await get_channel(int(broadcaster_id))

        df = pl.read_parquet("data/live_alerts.parquet")
        alert_row = df.filter(pl.col("id") == int(broadcaster_id))
        if alert_row.height == 0:
            logger.warning(
                f"Failed to fetch live alert for broadcaster_id={broadcaster_id}: No record found; Skipping"
            )
            return

        alert = alert_row.row(0, named=True)
        channel_id = alert.get("channel_id", 0)
        message_id = alert.get("message_id", 0)
        stream_id = alert.get("stream_id", "")
        stream_started_at = alert.get("stream_started_at", "")

        vod_info = None
        try:
            vod_info = await get_stream_vod(int(broadcaster_id), stream_id)
        except Exception as e:
            sentry_sdk.capture_exception(e)
            logger.error(
                f"Failed to fetch VOD info for broadcaster_id={broadcaster_id}: {e}"
            )
            await send_message(
                f"Failed to fetch VOD info for {broadcaster_id}: {e}",
                BOT_ADMIN_CHANNEL,
            )

        if not vod_info:
            logger.warning(
                f"No VOD info found for broadcaster_id={broadcaster_id}, {stream_id}"
            )

        content = (
            f"<@&{LIVE_ALERTS_ROLE}>" if channel_id == STREAM_ALERTS_CHANNEL else None
        )

        url = f"https://www.twitch.tv/{event_sub.event.broadcaster_user_login}"
        embed = (
            discord.Embed(
                description=f"**{channel_info.title if channel_info else ''}**",
                color=0x9046FF,
                timestamp=pendulum.now(),
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
        try:
            await edit_embed(message_id, embed, channel_id, content=content)
        except discord.NotFound:
            logger.warning(
                f"Message not found when editing offline embed for message_id={message_id}; continuing"
            )
        except Exception as e:
            logger.warning(
                f"Error while editing embed; Continuing without aborting: {e}"
            )

        success, error = delete_row_from_parquet(
            int(broadcaster_id), "data/live_alerts.parquet"
        )
        if not success:
            logger.error(
                f"Failed to delete live alert for broadcaster_id={broadcaster_id}: {error}"
            )
            await send_message(
                f"Failed to delete live alert for {broadcaster_id}: {error}",
                BOT_ADMIN_CHANNEL,
            )
    except Exception as e:
        logger.error(
            f"Error in _twitch_webhook_offline_task for broadcaster_id={broadcaster_id}: {e}"
        )
        sentry_sdk.capture_exception(e)
        await send_message(
            f"Error in _twitch_webhook_offline_task for {broadcaster_id}: {e}",
            BOT_ADMIN_CHANNEL,
        )


@twitch_router.post("/webhook/twitch")
async def twitch_webhook(request: Request) -> Response:
    try:
        headers = request.headers
        body: dict[str, Any] = await request.json()

        if headers.get(TWITCH_MESSAGE_TYPE) == "webhook_callback_verification":
            challenge = body.get("challenge", "")
            return Response(challenge or "", status_code=200)

        if headers.get(TWITCH_MESSAGE_TYPE, "").lower() == "revocation":
            subscription: dict[str, Any] = body.get("subscription", {})
            condition = subscription.get("condition", {})
            await send_message(
                f"Revoked {subscription.get('type', 'unknown')} notifications for condition: {condition} because {subscription.get('status', 'No reason provided')}",
                BOT_ADMIN_CHANNEL,
            )
            return Response(status_code=204)

        twitch_message_id = headers.get(TWITCH_MESSAGE_ID, "")
        twitch_message_timestamp = headers.get(TWITCH_MESSAGE_TIMESTAMP, "")
        body_str = (await request.body()).decode()
        message = get_hmac_message(
            twitch_message_id, twitch_message_timestamp, body_str
        )
        secret_hmac = HMAC_PREFIX + get_hmac(TWITCH_WEBHOOK_SECRET, message)

        twitch_message_signature = headers.get(TWITCH_MESSAGE_SIGNATURE, "")
        if not verify_message(secret_hmac, twitch_message_signature):
            logger.warning(
                f"403: Forbidden. Signature does not match: computed={secret_hmac}, received={twitch_message_signature}"
            )
            await send_message(
                "403: Forbidden request on /webhook/twitch. Signature does not match.",
                BOT_ADMIN_CHANNEL,
            )
            raise HTTPException(status_code=403)

        event_sub = StreamOnlineEventSub.model_validate(body)
        if event_sub.subscription.type != "stream.online":
            logger.warning(
                f"400: Bad request. Invalid subscription type: {event_sub.subscription.type}"
            )
            await send_message(
                "400: Bad request on /webhook/twitch. Invalid subscription type.",
                BOT_ADMIN_CHANNEL,
            )
            raise HTTPException(status_code=400)

        asyncio.create_task(
            _twitch_webhook_task(int(event_sub.event.broadcaster_user_id))
        )

        return Response(status_code=202)
    except Exception as e:
        logger.error(f"500: Internal server error on /webhook/twitch: {e}")
        sentry_sdk.capture_exception(e)
        await send_message(
            f"500: Internal server error on /webhook/twitch: {e}",
            BOT_ADMIN_CHANNEL,
        )
        raise HTTPException(status_code=500) from e


@twitch_router.post("/webhook/twitch/offline")
async def twitch_webhook_offline(request: Request) -> Response:
    try:
        headers = request.headers
        body: dict[str, Any] = await request.json()

        if (
            headers.get(TWITCH_MESSAGE_TYPE, "").lower()
            == "webhook_callback_verification"
        ):
            challenge = body.get("challenge", "")
            return Response(challenge or "", status_code=200)

        if headers.get(TWITCH_MESSAGE_TYPE, "").lower() == "revocation":
            subscription: dict[str, Any] = body.get("subscription", {})
            condition = subscription.get("condition", {})
            await send_message(
                f"Revoked {subscription.get('type', 'unknown')} notifications for condition: {condition} because {subscription.get('status', 'No reason provided')}",
                BOT_ADMIN_CHANNEL,
            )
            return Response(status_code=204)

        twitch_message_id = headers.get(TWITCH_MESSAGE_ID, "")
        twitch_message_timestamp = headers.get(TWITCH_MESSAGE_TIMESTAMP, "")
        body_str = (await request.body()).decode()
        message = get_hmac_message(
            twitch_message_id, twitch_message_timestamp, body_str
        )
        secret_hmac = HMAC_PREFIX + get_hmac(TWITCH_WEBHOOK_SECRET, message)

        twitch_message_signature = headers.get(TWITCH_MESSAGE_SIGNATURE, "")
        if not verify_message(secret_hmac, twitch_message_signature):
            logger.warning(
                f"403: Forbidden. Signature does not match: computed={secret_hmac}, received={twitch_message_signature}"
            )
            await send_message(
                "403: Forbidden request on /webhook/twitch/offline. Signature does not match.",
                BOT_ADMIN_CHANNEL,
            )
            raise HTTPException(status_code=403)

        event_sub = StreamOfflineEventSub.model_validate(body)
        if event_sub.subscription.type != "stream.offline":
            logger.warning(
                f"400: Bad request. Invalid subscription type: {event_sub.subscription.type}"
            )
            await send_message(
                "400: Bad request on /webhook/twitch/offline. Invalid subscription type.",
                BOT_ADMIN_CHANNEL,
            )
            raise HTTPException(status_code=400)

        asyncio.create_task(_twitch_webhook_offline_task(event_sub))

        return Response(status_code=202)
    except Exception as e:
        logger.error(f"500: Internal server error on /webhook/twitch/offline: {e}")
        sentry_sdk.capture_exception(e)
        await send_message(
            f"500: Internal server error on /webhook/twitch/offline: {e}",
            BOT_ADMIN_CHANNEL,
        )
        raise HTTPException(status_code=500) from e
