import asyncio
import logging
import os
from typing import Any, Awaitable, Callable

import discord
import httpx
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
from models import (
    ChannelChatMessageEventSub,
    ChannelFollowEventSub,
    RefreshResponse,
    StreamOfflineEventSub,
    StreamOnlineEventSub,
)
from services import (
    delete_row_from_parquet,
    discord_command,
    edit_embed,
    everything,
    get_age,
    get_channel,
    get_hmac,
    get_hmac_message,
    get_stream_info,
    get_stream_vod,
    get_user,
    hug,
    kofi,
    lurk,
    megathon,
    parse_rfc3339,
    raid,
    send_embed,
    send_message,
    shoutout,
    socials,
    throne,
    twitch_send_message,
    unlurk,
    update_alert,
    upsert_row_to_parquet,
    verify_message,
)
from services.twitch.shoutout_queue import shoutout_queue
from services.twitch.token_manager import token_manager

load_dotenv()

TWITCH_CLIENT_ID = os.getenv("TWITCH_CLIENT_ID")
TWITCH_CLIENT_SECRET = os.getenv("TWITCH_CLIENT_SECRET")
TWITCH_WEBHOOK_SECRET = os.getenv("TWITCH_WEBHOOK_SECRET")
TWITCH_BOT_USER_ID = os.getenv("TWITCH_BOT_USER_ID")

twitch_router = APIRouter()

logger = logging.getLogger(__name__)

# Ad start message
# A 3 minute ad break is starting! Thank you for sticking with us through this break! valinmArrive Ads help support my content. Consider subscribing to remove ads and support the stream!

# Ad end message
# The ad break is finishing now! valinmArrive

# Raid start
# Have a great rest of your day! valinmHeart Don't forget to stay hydrated and take care of yourself! valinmHeart

# Post-raid message
# We just raided ${raidtargetname}. In case you got left behind, you can find them here: https://www.twitch.tv/${raidtargetlogin}


@sentry_sdk.trace()
async def validate_call(request: Request, endpoint: str) -> Response | None:
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
    message = get_hmac_message(twitch_message_id, twitch_message_timestamp, body_str)
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


@sentry_sdk.trace()
async def _stream_online_task(broadcaster_id: int) -> None:
    try:
        stream_info = await get_stream_info(broadcaster_id)
        user_info = await get_user(broadcaster_id)
        while not stream_info:
            await asyncio.sleep(1)
            stream_info = await get_stream_info(broadcaster_id)

        channel = PROMO_CHANNEL
        if stream_info.user_login == "valinmalach":
            channel = STREAM_ALERTS_CHANNEL
            asyncio.create_task(shoutout_queue.activate())
            await twitch_send_message(
                str(broadcaster_id),
                "NilavHcalam is here valinmArrive",
            )
            await twitch_send_message(
                str(broadcaster_id),
                f"{stream_info.user_name} is now live! Streaming {stream_info.game_name}: {stream_info.title}",
            )

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
            f"Error in _stream_online_task for broadcaster_id={broadcaster_id}: {e}"
        )
        sentry_sdk.capture_exception(e)
        await send_message(
            f"Error in _stream_online_task for {broadcaster_id}: {e}",
            BOT_ADMIN_CHANNEL,
        )


@sentry_sdk.trace()
async def _stream_offline_task(event_sub: StreamOfflineEventSub) -> None:
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
            f"Error in _stream_offline_task for broadcaster_id={broadcaster_id}: {e}"
        )
        sentry_sdk.capture_exception(e)
        await send_message(
            f"Error in _stream_offline_task for {broadcaster_id}: {e}",
            BOT_ADMIN_CHANNEL,
        )


@sentry_sdk.trace()
async def _channel_chat_message_task(event_sub: ChannelChatMessageEventSub) -> None:
    user_command_dict: dict[
        str, Callable[[ChannelChatMessageEventSub, str], Awaitable[None]]
    ] = {
        "lurk": lurk,
        "discord": discord_command,
        "kofi": kofi,
        "megathon": megathon,
        "raid": raid,
        "socials": socials,
        "throne": throne,
        "unlurk": unlurk,
        "hug": hug,
        "so": shoutout,
        "everything": everything,
    }
    try:
        has_bot_badge = any(
            badge.set_id == "bot-badge" for badge in event_sub.event.badges or []
        )
        if not event_sub.event.message.text.startswith("!") or has_bot_badge:
            return
        text_without_prefix = event_sub.event.message.text[1:]
        command_parts = text_without_prefix.split(" ", 1)
        command = command_parts[0].lower()
        args = command_parts[1] if len(command_parts) > 1 else ""

        if (
            event_sub.event.source_broadcaster_user_id is not None
            and event_sub.event.source_broadcaster_user_id
            != event_sub.event.broadcaster_user_id
        ):
            return

        async def default_command(
            event_sub: ChannelChatMessageEventSub, args: str
        ) -> None:
            pass

        await user_command_dict.get(command, default_command)(event_sub, args)
    except Exception as e:
        logger.error(f"Error processing Twitch chat webhook task: {e}")
        sentry_sdk.capture_exception(e)
        await send_message(
            f"Error processing Twitch chat webhook task: {e}", BOT_ADMIN_CHANNEL
        )


@sentry_sdk.trace()
async def _channel_follow_task(event_sub: ChannelFollowEventSub) -> None:
    try:
        await twitch_send_message(
            event_sub.event.broadcaster_user_id,
            f"Thank you for following, {event_sub.event.user_name}! valinmHeart Your support means a lot to me! I hope you enjoy your stay! valinmHeart",
        )
    except Exception as e:
        logger.error(f"Error processing Twitch follow webhook task: {e}")
        sentry_sdk.capture_exception(e)
        await send_message(
            f"Error processing Twitch follow webhook task: {e}", BOT_ADMIN_CHANNEL
        )


@twitch_router.get("/twitch/oauth/callback")
async def twitch_oauth_callback(code: str, state: str) -> Response:
    try:
        if state != TWITCH_WEBHOOK_SECRET:
            logger.warning(f"400: Bad request. Invalid state: {state}")
            await send_message(
                "400: Bad request on /twitch/oauth/callback. Invalid state.",
                BOT_ADMIN_CHANNEL,
            )
            raise HTTPException(status_code=400)

        params = {
            "client_id": TWITCH_CLIENT_ID,
            "client_secret": TWITCH_CLIENT_SECRET,
            "code": code,
            "grant_type": "authorization_code",
            "redirect_uri": "https://val-mal-bot.com/twitch/oauth/callback",
        }
        response = httpx.post("https://id.twitch.tv/oauth2/token", params=params)

        if response.status_code < 200 or response.status_code >= 300:
            logger.error(
                f"Token exchange failed with status={response.status_code}, response={response.text}"
            )
            await send_message(
                f"Failed to exchange token: {response.status_code} {response.text}",
                BOT_ADMIN_CHANNEL,
            )
            raise HTTPException(status_code=500)

        auth_response = RefreshResponse.model_validate(response.json())
        if auth_response.token_type != "bearer":
            logger.error(
                f"Token exchange failed: unexpected token type {auth_response.token_type}"
            )
            await send_message(
                f"Failed to exchange token: unexpected token type {auth_response.token_type}",
                BOT_ADMIN_CHANNEL,
            )
            raise HTTPException(status_code=500)

        await token_manager.set_user_access_token(auth_response)
        return Response(
            "Authorization successful! You can close this tab.", status_code=200
        )
    except HTTPException as e:
        raise e
    except Exception as e:
        logger.error(f"500: Internal server error on /twitch/oauth/callback: {e}")
        sentry_sdk.capture_exception(e)
        await send_message(
            f"500: Internal server error on /twitch/oauth/callback: {e}",
            BOT_ADMIN_CHANNEL,
        )
        raise HTTPException(status_code=500) from e


@twitch_router.post("/webhook/twitch")
async def stream_online_webhook(request: Request) -> Response:
    try:
        validation = await validate_call(request, "/webhook/twitch")
        if validation:
            return validation

        body: dict[str, Any] = await request.json()
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
            _stream_online_task(int(event_sub.event.broadcaster_user_id))
        )

        return Response(status_code=202)
    except HTTPException as e:
        raise e
    except Exception as e:
        logger.error(f"500: Internal server error on /webhook/twitch: {e}")
        sentry_sdk.capture_exception(e)
        await send_message(
            f"500: Internal server error on /webhook/twitch: {e}",
            BOT_ADMIN_CHANNEL,
        )
        raise HTTPException(status_code=500) from e


@twitch_router.post("/webhook/twitch/offline")
async def stream_offline_webhook(request: Request) -> Response:
    try:
        validation = await validate_call(request, "/webhook/twitch/offline")
        if validation:
            return validation

        body: dict[str, Any] = await request.json()
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

        asyncio.create_task(_stream_offline_task(event_sub))

        return Response(status_code=202)
    except HTTPException as e:
        raise e
    except Exception as e:
        logger.error(f"500: Internal server error on /webhook/twitch/offline: {e}")
        sentry_sdk.capture_exception(e)
        await send_message(
            f"500: Internal server error on /webhook/twitch/offline: {e}",
            BOT_ADMIN_CHANNEL,
        )
        raise HTTPException(status_code=500) from e


@twitch_router.post("/webhook/twitch/chat")
async def channel_chat_message_webhook(request: Request) -> Response:
    try:
        validation = await validate_call(request, "/webhook/twitch/chat")
        if validation:
            return validation

        body: dict[str, Any] = await request.json()
        event_sub = ChannelChatMessageEventSub.model_validate(body)
        if event_sub.subscription.type != "channel.chat.message":
            logger.warning(
                f"400: Bad request. Invalid subscription type: {event_sub.subscription.type}"
            )
            await send_message(
                "400: Bad request on /webhook/twitch/chat. Invalid subscription type.",
                BOT_ADMIN_CHANNEL,
            )
            raise HTTPException(status_code=400)

        asyncio.create_task(_channel_chat_message_task(event_sub))

        return Response(status_code=202)
    except HTTPException as e:
        raise e
    except Exception as e:
        logger.error(f"500: Internal server error on /webhook/twitch/chat: {e}")
        sentry_sdk.capture_exception(e)
        await send_message(
            f"500: Internal server error on /webhook/twitch/chat: {e}",
            BOT_ADMIN_CHANNEL,
        )
        raise HTTPException(status_code=500) from e


@twitch_router.post("/webhook/twitch/follow")
async def channel_follow_webhook(request: Request) -> Response:
    try:
        validation = await validate_call(request, "/webhook/twitch/follow")
        if validation:
            return validation

        body: dict[str, Any] = await request.json()
        event_sub = ChannelFollowEventSub.model_validate(body)
        if event_sub.subscription.type != "channel.follow":
            logger.warning(
                f"400: Bad request. Invalid subscription type: {event_sub.subscription.type}"
            )
            await send_message(
                "400: Bad request on /webhook/twitch/follow. Invalid subscription type.",
                BOT_ADMIN_CHANNEL,
            )
            raise HTTPException(status_code=400)

        asyncio.create_task(_channel_follow_task(event_sub))

        return Response(status_code=202)
    except HTTPException as e:
        raise e
    except Exception as e:
        logger.error(f"500: Internal server error on /webhook/twitch/follow: {e}")
        sentry_sdk.capture_exception(e)
        await send_message(
            f"500: Internal server error on /webhook/twitch/follow: {e}",
            BOT_ADMIN_CHANNEL,
        )
        raise HTTPException(status_code=500) from e
