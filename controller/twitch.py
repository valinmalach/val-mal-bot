import asyncio
import logging
import os
from datetime import datetime
from typing import Any

import discord
import quart
import sentry_sdk
from discord.ui import View
from dotenv import load_dotenv
from quart import Blueprint, Response, ResponseReturnValue, request

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

logger = logging.getLogger(__name__)


@sentry_sdk.trace()
async def _twitch_webhook_task(broadcaster_id: str) -> None:
    try:
        logger.info("Processing online event for broadcaster_id=%s", broadcaster_id)
        stream_info = await get_stream_info(broadcaster_id)
        user_info = await get_user(broadcaster_id)
        logger.info("Fetched stream_info=%s and user_info=%s", stream_info, user_info)
        while not stream_info:
            logger.info("Retrying get_stream_info for %s", broadcaster_id)
            await asyncio.sleep(1)
            stream_info = await get_stream_info(broadcaster_id)

        channel = (
            STREAM_ALERTS_CHANNEL
            if stream_info.user_login == "valinmalach"
            else PROMO_CHANNEL
        )
        logger.info("Selected channel %s", channel)
        content = (
            f"<@&{LIVE_ALERTS_ROLE}>" if channel == STREAM_ALERTS_CHANNEL else None
        )

        url = f"https://www.twitch.tv/{stream_info.user_login}"
        logger.info("Constructing embed for url=%s", url)
        # Cache-bust thumbnail URL to force Discord to refresh the image
        raw_thumb_url = stream_info.thumbnail_url.replace("{width}x{height}", "400x225")
        cache_busted_thumb_url = f"{raw_thumb_url}?cb={int(datetime.now().timestamp())}"
        logger.info("Using cache-busted thumbnail URL: %s", cache_busted_thumb_url)

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
        logger.info("Embed sent, message_id=%s", message_id)
        if message_id is None:
            await send_message(
                f"Failed to send live alert message\nbroadcaster_id: {broadcaster_id}\nchannel_id: {channel}",
                BOT_ADMIN_CHANNEL,
            )
            logger.error("Failed to send embed for broadcaster %s", broadcaster_id)
            return

        alert = {
            "channel_id": channel,
            "message_id": message_id,
            "stream_id": stream_info.id,
            "stream_started_at": stream_info.started_at,
        }
        resp = xata_client.records().upsert("live_alerts", broadcaster_id, alert)
        if resp.is_success():
            asyncio.create_task(
                update_alert(
                    broadcaster_id,
                    channel,
                    message_id,
                    stream_info.id,
                    stream_info.started_at,
                )
            )
            logger.info(
                "Inserted live alert message into database: broadcaster_id=%s, channel_id=%s, message_id=%s",
                broadcaster_id,
                channel,
                message_id,
            )
        else:
            logger.error(
                "Failed to insert live alert message into database: %s",
                resp.error_message,
            )
            await send_message(
                f"Failed to insert live alert message into database\nbroadcaster_id: {broadcaster_id}\nchannel_id: {channel}\n message_id: {message_id}\n\n{resp.error_message}",
                BOT_ADMIN_CHANNEL,
            )
    except Exception as e:
        logger.error(
            "Error in _twitch_webhook_task for broadcaster_id=%s: %s",
            broadcaster_id,
            e,
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
        logger.info("Processing offline event for %s", broadcaster_id)
        user_info = await get_user(broadcaster_id)
        channel_info = await get_channel(broadcaster_id)
        logger.info("Fetched user_info=%s and channel_info=%s", user_info, channel_info)

        alert = xata_client.records().get("live_alerts", broadcaster_id)
        if not alert.is_success():
            logger.error(
                "Failed to fetch live alert for broadcaster_id=%s: %s",
                broadcaster_id,
                alert.error_message,
            )
            await send_message(
                f"Failed to fetch live alert for {broadcaster_id}: {alert.error_message}",
                BOT_ADMIN_CHANNEL,
            )
            return

        logger.info(
            "Fetched live alert for broadcaster_id=%s: %s", broadcaster_id, alert
        )
        channel_id = alert.get("channel_id", 0)
        message_id = alert.get("message_id", 0)
        stream_id = alert.get("stream_id", "")
        stream_started_at = alert.get("stream_started_at", "")
        logger.info(
            "Extracted alert fields: channel_id=%s, message_id=%s, stream_id=%s, stream_started_at=%s",
            channel_id,
            message_id,
            stream_id,
            stream_started_at,
        )

        vod_info = None
        logger.info(
            "Beginning VOD lookup for broadcaster_id=%s, stream_id=%s",
            broadcaster_id,
            stream_id,
        )
        try:
            vod_info = await get_stream_vod(broadcaster_id, stream_id)
            if vod_info:
                logger.info("VOD info found: %s", vod_info)
            else:
                logger.warning(
                    "No VOD info found for broadcaster_id=%s, stream_id=%s",
                    broadcaster_id,
                    stream_id,
                )
        except Exception as e:
            sentry_sdk.capture_exception(e)
            logger.error(
                "Failed to fetch VOD info for broadcaster_id=%s: %s",
                broadcaster_id,
                e,
            )
            await send_message(
                f"Failed to fetch VOD info for {broadcaster_id}: {e}",
                BOT_ADMIN_CHANNEL,
            )

        if not vod_info:
            logger.warning(
                "No VOD info found for broadcaster_id=%s",
                broadcaster_id,
            )

        content = (
            f"<@&{LIVE_ALERTS_ROLE}>" if channel_id == STREAM_ALERTS_CHANNEL else None
        )
        logger.info("Prepared content mention: %s", content)

        url = f"https://www.twitch.tv/{event_sub.event.broadcaster_user_login}"
        logger.info("Constructing embed for offline event with url=%s", url)
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
        logger.info("Embed object constructed for offline notification")
        if stream_id and vod_info:
            vod_url = vod_info.url
            embed = embed.add_field(
                name="**VOD**",
                value=f"[**Click to view**]({vod_url})",
                inline=True,
            )
            logger.info("Added VOD field to embed: %s", vod_url)
        if stream_started_at:
            started_at = parse_rfc3339(stream_started_at)
            age = get_age(started_at, limit_units=2)
            embed = embed.set_footer(
                text=f"Online for {age} | Offline at",
            )
            logger.info("Set embed footer with age: %s", age)
        # retry on Discord Server Error
        while True:
            logger.info(
                "Attempting to edit embed message for offline event (message_id=%s)",
                message_id,
            )
            try:
                await edit_embed(message_id, embed, channel_id, content=content)
                logger.info(
                    "Successfully edited embed (offline) for message_id=%s", message_id
                )
                break
            except discord.DiscordServerError:
                logger.warning(
                    "Discord server error while editing embed; retrying after sleep"
                )
                await asyncio.sleep(1)

        logger.info(
            "Proceeding to delete live_alert record for broadcaster_id=%s",
            broadcaster_id,
        )
        resp = xata_client.records().delete("live_alerts", broadcaster_id)
        if not resp.is_success():
            logger.error(
                "Failed to delete live alert for broadcaster_id=%s: %s",
                broadcaster_id,
                resp.error_message,
            )
            await send_message(
                f"Failed to delete live alert for {broadcaster_id}: {resp.error_message}",
                BOT_ADMIN_CHANNEL,
            )
        else:
            logger.info(
                "Deleted live_alert record successfully for broadcaster_id=%s",
                broadcaster_id,
            )
    except Exception as e:
        logger.error(
            "Error in _twitch_webhook_offline_task for broadcaster_id=%s: %s",
            broadcaster_id,
            e,
        )
        sentry_sdk.capture_exception(e)
        await send_message(
            f"Error in _twitch_webhook_offline_task for {broadcaster_id}: {e}",
            BOT_ADMIN_CHANNEL,
        )


@twitch_bp.route("/webhook/twitch", methods=["POST"])
async def twitch_webhook() -> ResponseReturnValue:
    logger.info("Webhook received: twitch_webhook start")
    try:
        headers = request.headers
        logger.info("Headers parsed: %s", dict(headers))
        body: dict[str, Any] = await request.get_json()
        logger.info("Body JSON parsed: %s", body)

        if headers.get(TWITCH_MESSAGE_TYPE) == "webhook_callback_verification":
            challenge = body.get("challenge", "")
            logger.info(
                "Responding to callback verification with challenge=%s", challenge
            )
            return Response(challenge or "", status=200)

        if headers.get(TWITCH_MESSAGE_TYPE, "").lower() == "revocation":
            subscription: dict[str, Any] = body.get("subscription", {})
            logger.info(
                "%s notifications revoked!", subscription.get("type", "unknown")
            )
            logger.info("reason: %s", subscription.get("status", "No reason provided"))
            condition = subscription.get("condition", {})
            logger.info("condition: %s", condition)
            await send_message(
                f"Revoked {subscription.get('type', 'unknown')} notifications for condition: {condition} because {subscription.get('status', 'No reason provided')}",
                BOT_ADMIN_CHANNEL,
            )
            return Response(status=204)

        twitch_message_id = headers.get(TWITCH_MESSAGE_ID, "")
        twitch_message_timestamp = headers.get(TWITCH_MESSAGE_TIMESTAMP, "")
        body_str = await request.get_data(as_text=True)
        logger.info("Request raw body retrieved")
        message = get_hmac_message(
            twitch_message_id, twitch_message_timestamp, body_str
        )
        logger.info("HMAC message constructed")
        secret_hmac = HMAC_PREFIX + get_hmac(TWITCH_WEBHOOK_SECRET, message)
        logger.info("Computed secret_hmac=%s", secret_hmac)

        twitch_message_signature = headers.get(TWITCH_MESSAGE_SIGNATURE, "")
        if not verify_message(secret_hmac, twitch_message_signature):
            logger.warning(
                "403: Forbidden. Signature does not match: computed=%s, received=%s",
                secret_hmac,
                twitch_message_signature,
            )
            await send_message(
                "403: Forbidden request on /webhook/twitch. Signature does not match.",
                BOT_ADMIN_CHANNEL,
            )
            quart.abort(403)
        logger.info("Signature verified")

        event_sub = StreamOnlineEventSub.model_validate(body)
        logger.info("Event subscription parsed: type=%s", event_sub.subscription.type)
        if event_sub.subscription.type != "stream.online":
            logger.warning(
                "400: Bad request. Invalid subscription type: %s",
                event_sub.subscription.type,
            )
            await send_message(
                "400: Bad request on /webhook/twitch. Invalid subscription type.",
                BOT_ADMIN_CHANNEL,
            )
            quart.abort(400)

        asyncio.create_task(_twitch_webhook_task(event_sub.event.broadcaster_user_id))

        return Response(status=202)
    except Exception as e:
        logger.error("500: Internal server error on /webhook/twitch: %s", e)
        sentry_sdk.capture_exception(e)
        await send_message(
            f"500: Internal server error on /webhook/twitch: {e}",
            BOT_ADMIN_CHANNEL,
        )
        quart.abort(500)


@twitch_bp.route("/webhook/twitch/offline", methods=["POST"])
async def twitch_webhook_offline() -> ResponseReturnValue:
    logger.info("Webhook received: twitch_webhook_offline start")
    try:
        headers = request.headers
        logger.info("Headers parsed: %s", dict(headers))
        body: dict[str, Any] = await request.get_json()
        logger.info("Body JSON parsed: %s", body)

        if (
            headers.get(TWITCH_MESSAGE_TYPE, "").lower()
            == "webhook_callback_verification"
        ):
            challenge = body.get("challenge", "")
            logger.info(
                "Responding to callback verification offline with challenge=%s",
                challenge,
            )
            return Response(challenge or "", status=200)

        if headers.get(TWITCH_MESSAGE_TYPE, "").lower() == "revocation":
            subscription: dict[str, Any] = body.get("subscription", {})
            logger.info(
                "%s notifications revoked!", subscription.get("type", "unknown")
            )
            logger.info("reason: %s", subscription.get("status", "No reason provided"))
            condition = subscription.get("condition", {})
            logger.info("condition: %s", condition)
            await send_message(
                f"Revoked {subscription.get('type', 'unknown')} notifications for condition: {condition} because {subscription.get('status', 'No reason provided')}",
                BOT_ADMIN_CHANNEL,
            )
            return Response(status=204)

        twitch_message_id = headers.get(TWITCH_MESSAGE_ID, "")
        twitch_message_timestamp = headers.get(TWITCH_MESSAGE_TIMESTAMP, "")
        body_str = await request.get_data(as_text=True)
        message = get_hmac_message(
            twitch_message_id, twitch_message_timestamp, body_str
        )
        secret_hmac = HMAC_PREFIX + get_hmac(TWITCH_WEBHOOK_SECRET, message)

        twitch_message_signature = headers.get(TWITCH_MESSAGE_SIGNATURE, "")
        if not verify_message(secret_hmac, twitch_message_signature):
            logger.warning(
                "403: Forbidden. Signature does not match: computed=%s, received=%s",
                secret_hmac,
                twitch_message_signature,
            )
            await send_message(
                "403: Forbidden request on /webhook/twitch/offline. Signature does not match.",
                BOT_ADMIN_CHANNEL,
            )
            quart.abort(403)
        logger.info("Signature verified")

        event_sub = StreamOfflineEventSub.model_validate(body)
        logger.info("Event subscription parsed: type=%s", event_sub.subscription.type)
        if event_sub.subscription.type != "stream.offline":
            logger.warning(
                "400: Bad request. Invalid subscription type: %s",
                event_sub.subscription.type,
            )
            await send_message(
                "400: Bad request on /webhook/twitch/offline. Invalid subscription type.",
                BOT_ADMIN_CHANNEL,
            )
            quart.abort(400)

        asyncio.create_task(_twitch_webhook_offline_task(event_sub))

        return Response(status=202)
    except Exception as e:
        logger.error("500: Internal server error on /webhook/twitch/offline: %s", e)
        sentry_sdk.capture_exception(e)
        await send_message(
            f"500: Internal server error on /webhook/twitch/offline: {e}",
            BOT_ADMIN_CHANNEL,
        )
        quart.abort(500)
