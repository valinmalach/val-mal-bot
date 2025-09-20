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

load_dotenv()

TWITCH_WEBHOOK_SECRET = os.getenv("TWITCH_WEBHOOK_SECRET")

twitch_router = APIRouter()

logger = logging.getLogger(__name__)


@sentry_sdk.trace()
async def _twitch_webhook_task(broadcaster_id: int) -> None:
    try:
        logger.info(f"Processing online event for broadcaster_id={broadcaster_id}")
        stream_info = await get_stream_info(broadcaster_id)
        user_info = await get_user(broadcaster_id)
        logger.info(f"Fetched stream_info={stream_info} and user_info={user_info}")
        while not stream_info:
            logger.info(f"Retrying get_stream_info for {broadcaster_id}")
            await asyncio.sleep(1)
            stream_info = await get_stream_info(broadcaster_id)

        channel = (
            STREAM_ALERTS_CHANNEL
            if stream_info.user_login == "valinmalach"
            else PROMO_CHANNEL
        )
        logger.info(f"Selected channel {channel}")
        content = (
            f"<@&{LIVE_ALERTS_ROLE}>" if channel == STREAM_ALERTS_CHANNEL else None
        )

        url = f"https://www.twitch.tv/{stream_info.user_login}"
        logger.info(f"Constructing embed for url={url}")
        # Cache-bust thumbnail URL to force Discord to refresh the image
        raw_thumb_url = stream_info.thumbnail_url.replace("{width}x{height}", "400x225")
        cache_busted_thumb_url = f"{raw_thumb_url}?cb={int(pendulum.now().timestamp())}"
        logger.info(f"Using cache-busted thumbnail URL: {cache_busted_thumb_url}")

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
        logger.info(f"Embed sent, message_id={message_id}")
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
            logger.info(
                f"Inserted live alert message into parquet: broadcaster_id={broadcaster_id}, channel_id={channel}, message_id={message_id}",
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
        logger.info(f"Processing offline event for {broadcaster_id}")
        user_info = await get_user(int(broadcaster_id))
        channel_info = await get_channel(int(broadcaster_id))
        logger.info(f"Fetched user_info={user_info} and channel_info={channel_info}")

        df = pl.read_parquet("data/live_alerts.parquet")
        alert_row = df.filter(pl.col("id") == int(broadcaster_id))
        if alert_row.height == 0:
            logger.error(
                f"Failed to fetch live alert for broadcaster_id={broadcaster_id}: No record found"
            )
            await send_message(
                f"Failed to fetch live alert for {broadcaster_id}: No record found",
                BOT_ADMIN_CHANNEL,
            )
            return

        alert = alert_row.row(0, named=True)
        logger.info(f"Fetched live alert for broadcaster_id={broadcaster_id}: {alert}")
        channel_id = alert.get("channel_id", 0)
        message_id = alert.get("message_id", 0)
        stream_id = alert.get("stream_id", "")
        stream_started_at = alert.get("stream_started_at", "")
        logger.info(
            f"Extracted alert fields: channel_id={channel_id}, message_id={message_id}, stream_id={stream_id}, stream_started_at={stream_started_at}"
        )

        vod_info = None
        logger.info(
            f"Beginning VOD lookup for broadcaster_id={broadcaster_id}, stream_id={stream_id}"
        )
        try:
            vod_info = await get_stream_vod(int(broadcaster_id), stream_id)
            if vod_info:
                logger.info(f"VOD info found: {vod_info}")
            else:
                logger.warning(
                    f"No VOD info found for broadcaster_id={broadcaster_id}, stream_id={stream_id}"
                )
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
            logger.warning(f"No VOD info found for broadcaster_id={broadcaster_id}")

        content = (
            f"<@&{LIVE_ALERTS_ROLE}>" if channel_id == STREAM_ALERTS_CHANNEL else None
        )
        logger.info(f"Prepared content mention: {content}")

        url = f"https://www.twitch.tv/{event_sub.event.broadcaster_user_login}"
        logger.info(f"Constructing embed for offline event with url={url}")
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
        logger.info("Embed object constructed for offline notification")
        if stream_id and vod_info:
            vod_url = vod_info.url
            embed = embed.add_field(
                name="**VOD**",
                value=f"[**Click to view**]({vod_url})",
                inline=True,
            )
            logger.info(f"Added VOD field to embed: {vod_url}")
        if stream_started_at:
            started_at = parse_rfc3339(stream_started_at)
            age = get_age(started_at, limit_units=2)
            embed = embed.set_footer(
                text=f"Online for {age} | Offline at",
            )
            logger.info(f"Set embed footer with age: {age}")
        # retry on Discord Server Error
        while True:
            logger.info(
                f"Attempting to edit embed message for offline event (message_id={message_id})"
            )
            try:
                await edit_embed(message_id, embed, channel_id, content=content)
                logger.info(
                    f"Successfully edited embed (offline) for message_id={message_id}"
                )
                break
            except discord.DiscordServerError:
                logger.warning(
                    "Discord server error while editing embed; retrying after sleep"
                )
                await asyncio.sleep(1)

        logger.info(
            f"Proceeding to delete live_alert record for broadcaster_id={broadcaster_id}"
        )
        success, error = delete_row_from_parquet(
            broadcaster_id, "data/live_alerts.parquet"
        )
        if not success:
            logger.error(
                f"Failed to delete live alert for broadcaster_id={broadcaster_id}: {error}"
            )
            await send_message(
                f"Failed to delete live alert for {broadcaster_id}: {error}",
                BOT_ADMIN_CHANNEL,
            )
        else:
            logger.info(
                f"Deleted live_alert record successfully for broadcaster_id={broadcaster_id}"
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
    logger.info("Webhook received: twitch_webhook start")
    try:
        headers = request.headers
        logger.info(f"Headers parsed: {dict(headers)}")
        body: dict[str, Any] = await request.json()
        logger.info(f"Body JSON parsed: {body}")

        if headers.get(TWITCH_MESSAGE_TYPE) == "webhook_callback_verification":
            challenge = body.get("challenge", "")
            logger.info(
                f"Responding to callback verification with challenge={challenge}"
            )
            return Response(challenge or "", status_code=200)

        if headers.get(TWITCH_MESSAGE_TYPE, "").lower() == "revocation":
            subscription: dict[str, Any] = body.get("subscription", {})
            logger.info(f"{subscription.get('type', 'unknown')} notifications revoked!")
            logger.info(f"reason: {subscription.get('status', 'No reason provided')}")
            condition = subscription.get("condition", {})
            logger.info(f"condition: {condition}")
            await send_message(
                f"Revoked {subscription.get('type', 'unknown')} notifications for condition: {condition} because {subscription.get('status', 'No reason provided')}",
                BOT_ADMIN_CHANNEL,
            )
            return Response(status_code=204)

        twitch_message_id = headers.get(TWITCH_MESSAGE_ID, "")
        twitch_message_timestamp = headers.get(TWITCH_MESSAGE_TIMESTAMP, "")
        body_str = (await request.body()).decode()
        logger.info("Request raw body retrieved")
        message = get_hmac_message(
            twitch_message_id, twitch_message_timestamp, body_str
        )
        logger.info("HMAC message constructed")
        secret_hmac = HMAC_PREFIX + get_hmac(TWITCH_WEBHOOK_SECRET, message)
        logger.info(f"Computed secret_hmac={secret_hmac}")

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
        logger.info("Signature verified")

        event_sub = StreamOnlineEventSub.model_validate(body)
        logger.info(f"Event subscription parsed: type={event_sub.subscription.type}")
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
        raise HTTPException(status_code=500)


@twitch_router.post("/webhook/twitch/offline")
async def twitch_webhook_offline(request: Request) -> Response:
    logger.info("Webhook received: twitch_webhook_offline start")
    try:
        headers = request.headers
        logger.info(f"Headers parsed: {dict(headers)}")
        body: dict[str, Any] = await request.json()
        logger.info(f"Body JSON parsed: {body}")

        if (
            headers.get(TWITCH_MESSAGE_TYPE, "").lower()
            == "webhook_callback_verification"
        ):
            challenge = body.get("challenge", "")
            logger.info(
                f"Responding to callback verification offline with challenge={challenge}"
            )
            return Response(challenge or "", status_code=200)

        if headers.get(TWITCH_MESSAGE_TYPE, "").lower() == "revocation":
            subscription: dict[str, Any] = body.get("subscription", {})
            logger.info(f"{subscription.get('type', 'unknown')} notifications revoked!")
            logger.info(f"reason: {subscription.get('status', 'No reason provided')}")
            condition = subscription.get("condition", {})
            logger.info(f"condition: {condition}")
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
        logger.info("Signature verified")

        event_sub = StreamOfflineEventSub.model_validate(body)
        logger.info(f"Event subscription parsed: type={event_sub.subscription.type}")
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
        raise HTTPException(status_code=500)
