import asyncio
import io
import logging
import os
import traceback
from typing import Any, Awaitable, Callable

import discord
import pendulum
import polars as pl
from discord.ui import View
from dotenv import load_dotenv
from fastapi import APIRouter, HTTPException, Request, Response

from constants import (
    BOT_ADMIN_CHANNEL,
    HMAC_PREFIX,
    LIVE_ALERTS,
    LIVE_ALERTS_ROLE,
    PROMO_CHANNEL,
    STREAM_ALERTS_CHANNEL,
    TWITCH_MESSAGE_ID,
    TWITCH_MESSAGE_SIGNATURE,
    TWITCH_MESSAGE_TIMESTAMP,
    TWITCH_MESSAGE_TYPE,
    ErrorDetails,
    LiveAlert,
)
from models import (
    Channel,
    ChannelAdBreakBeginEventSub,
    ChannelChatMessageEventSub,
    ChannelFollowEventSub,
    ChannelModerateEventSub,
    ChannelRaidEventSub,
    RefreshResponse,
    StreamOfflineEventSub,
    StreamOnlineEventSub,
    User,
    Video,
)
from services import (
    delete_row_from_parquet,
    discord_command,
    edit_embed,
    everything,
    get_ad_schedule,
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
    read_parquet_cached,
    send_embed,
    send_message,
    shoutout,
    socials,
    throne,
    twitch_send_message,
    unlurk,
    update_alert,
    upsert_row_to_parquet_async,
    verify_message,
)
from services.helper.http_client import http_client_manager
from services.twitch.shoutout_queue import shoutout_queue
from services.twitch.token_manager import token_manager

load_dotenv()

APP_URL = os.getenv("APP_URL")
TWITCH_BROADCASTER_ID = os.getenv("TWITCH_BROADCASTER_ID")
TWITCH_CLIENT_ID = os.getenv("TWITCH_CLIENT_ID")
TWITCH_CLIENT_SECRET = os.getenv("TWITCH_CLIENT_SECRET")
TWITCH_WEBHOOK_SECRET = os.getenv("TWITCH_WEBHOOK_SECRET")

logger = logging.getLogger(__name__)

twitch_router = APIRouter()


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
        logger.warning("403: Forbidden. Signature does not match.")
        await send_message(
            f"403: Forbidden request on {endpoint}. Signature does not match.",
            BOT_ADMIN_CHANNEL,
        )
        raise HTTPException(status_code=403)


async def log_error(message: str, traceback_str: str) -> None:
    traceback_buffer = io.BytesIO(traceback_str.encode("utf-8"))
    traceback_file = discord.File(traceback_buffer, filename="traceback.txt")
    await send_message(message, BOT_ADMIN_CHANNEL, file=traceback_file)


async def _stream_online_task(event_sub: StreamOnlineEventSub) -> None:
    broadcaster_id = int(event_sub.event.broadcaster_user_id)
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

        alert: LiveAlert = {
            "id": broadcaster_id,
            "channel_id": channel,
            "message_id": message_id,
            "stream_id": int(stream_info.id),
            "stream_started_at": stream_info.started_at,
        }

        try:
            await upsert_row_to_parquet_async(alert, LIVE_ALERTS)
            asyncio.create_task(
                update_alert(
                    broadcaster_id,
                    channel,
                    message_id,
                    int(stream_info.id),
                    stream_info.started_at,
                )
            )
        except Exception as e:
            error_details: ErrorDetails = {
                "type": type(e).__name__,
                "message": str(e),
                "args": e.args,
                "traceback": traceback.format_exc(),
            }
            error_msg = f"Failed to insert live alert message into parquet for broadcaster {broadcaster_id} - Type: {error_details['type']}, Message: {error_details['message']}, Args: {error_details['args']}"
            logger.error(f"{error_msg}\nTraceback:\n{error_details['traceback']}")
            await log_error(error_msg, error_details["traceback"])
    except Exception as e:
        error_details: ErrorDetails = {
            "type": type(e).__name__,
            "message": str(e),
            "args": e.args,
            "traceback": traceback.format_exc(),
        }
        error_msg = f"Error in _stream_online_task for {broadcaster_id} - Type: {error_details['type']}, Message: {error_details['message']}, Args: {error_details['args']}"
        logger.error(f"{error_msg}\nTraceback:\n{error_details['traceback']}")
        await log_error(error_msg, error_details["traceback"])


async def _cancel_ad_break_task_if_needed(broadcaster_user_login: str) -> None:
    """Cancel ad break notification task for the broadcaster if it exists."""
    if broadcaster_user_login == "valinmalach":
        await shoutout_queue.deactivate()
        existing_task = _ad_break_notification_tasks.get(broadcaster_user_login)
        if existing_task and not existing_task.done():
            existing_task.cancel()


async def _fetch_stream_data(
    broadcaster_id: int,
) -> tuple[User | None, Channel | None, dict[str, Any] | None]:
    """Fetch user info, channel info, and VOD info for the stream."""
    user_info = await get_user(broadcaster_id)
    channel_info = await get_channel(broadcaster_id)

    df = await read_parquet_cached(LIVE_ALERTS)
    alert_row = df.filter(pl.col("id") == broadcaster_id)
    if alert_row.height == 0:
        logger.warning(
            f"Failed to fetch live alert for broadcaster_id={broadcaster_id}: No record found; Skipping"
        )
        return None, None, None

    alert = alert_row.row(0, named=True)
    return user_info, channel_info, alert


async def _get_vod_info(broadcaster_id: int, stream_id: str) -> Video | None:
    """Safely fetch VOD information with error handling."""
    if not stream_id:
        return None

    try:
        return await get_stream_vod(broadcaster_id, int(stream_id))
    except Exception as e:
        error_details: ErrorDetails = {
            "type": type(e).__name__,
            "message": str(e),
            "args": e.args,
            "traceback": traceback.format_exc(),
        }
        error_msg = f"Failed to fetch VOD info for broadcaster {broadcaster_id} - Type: {error_details['type']}, Message: {error_details['message']}, Args: {error_details['args']}"
        logger.error(f"{error_msg}\nTraceback:\n{error_details['traceback']}")
        await log_error(error_msg, error_details["traceback"])
        return None


def _create_offline_embed(
    event_sub: StreamOfflineEventSub,
    user_info: User | None,
    channel_info: Channel | None,
    vod_info: Video | None,
    stream_started_at: str | None,
) -> discord.Embed:
    """Create the offline embed with all necessary information."""
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

    if vod_info:
        vod_url = vod_info.url
        embed = embed.add_field(
            name="**VOD**",
            value=f"[**Click to view**]({vod_url})",
            inline=True,
        )

    if stream_started_at:
        started_at = parse_rfc3339(stream_started_at)
        age = get_age(started_at, limit_units=2)
        embed = embed.set_footer(text=f"Online for {age} | Offline at")

    return embed


async def _update_offline_message(
    message_id: int, embed: discord.Embed, channel_id: int, content: str | None
) -> None:
    """Update the live alert message to show offline status."""
    try:
        await edit_embed(message_id, embed, channel_id, content=content)
    except discord.NotFound:
        logger.warning(
            f"Message not found when editing offline embed for message_id={message_id}; continuing"
        )
    except Exception as e:
        error_details: ErrorDetails = {
            "type": type(e).__name__,
            "message": str(e),
            "args": e.args,
            "traceback": traceback.format_exc(),
        }
        error_msg = f"Error editing offline embed - Type: {error_details['type']}, Message: {error_details['message']}, Args: {error_details['args']}"
        logger.error(f"{error_msg}\nTraceback:\n{error_details['traceback']}")
        await log_error(error_msg, error_details["traceback"])


async def _cleanup_live_alert(broadcaster_id: int) -> None:
    """Remove the live alert from storage."""
    try:
        await delete_row_from_parquet(broadcaster_id, LIVE_ALERTS)
    except Exception as e:
        error_details: ErrorDetails = {
            "type": type(e).__name__,
            "message": str(e),
            "args": e.args,
            "traceback": traceback.format_exc(),
        }
        error_msg = f"Failed to delete live alert for broadcaster {broadcaster_id} - Type: {error_details['type']}, Message: {error_details['message']}, Args: {error_details['args']}"
        logger.error(error_msg)
        await log_error(error_msg, error_details["traceback"])


async def _stream_offline_task(event_sub: StreamOfflineEventSub) -> None:
    broadcaster_id = int(event_sub.event.broadcaster_user_id)
    try:
        await _cancel_ad_break_task_if_needed(event_sub.event.broadcaster_user_login)

        user_info, channel_info, alert = await _fetch_stream_data(broadcaster_id)
        if alert is None:
            return

        channel_id = alert.get("channel_id", 0)
        message_id = alert.get("message_id", 0)
        stream_id = alert.get("stream_id", "")
        stream_started_at = alert.get("stream_started_at", "")

        vod_info = await _get_vod_info(broadcaster_id, stream_id)

        content = (
            f"<@&{LIVE_ALERTS_ROLE}>" if channel_id == STREAM_ALERTS_CHANNEL else None
        )

        embed = _create_offline_embed(
            event_sub, user_info, channel_info, vod_info, stream_started_at
        )

        await _update_offline_message(message_id, embed, channel_id, content)
        await _cleanup_live_alert(broadcaster_id)

    except Exception as e:
        error_details: ErrorDetails = {
            "type": type(e).__name__,
            "message": str(e),
            "args": e.args,
            "traceback": traceback.format_exc(),
        }
        error_msg = f"Error in _stream_offline_task for {broadcaster_id} - Type: {error_details['type']}, Message: {error_details['message']}, Args: {error_details['args']}"
        logger.error(f"{error_msg}\nTraceback:\n{error_details['traceback']}")
        await log_error(error_msg, error_details["traceback"])


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
        if not event_sub.event.message.text.startswith("!"):
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
            """Default no-op command handler."""
            pass

        await user_command_dict.get(command, default_command)(event_sub, args)
    except Exception as e:
        error_details: ErrorDetails = {
            "type": type(e).__name__,
            "message": str(e),
            "args": e.args,
            "traceback": traceback.format_exc(),
        }
        error_msg = f"Error processing Twitch chat webhook task - Type: {error_details['type']}, Message: {error_details['message']}, Args: {error_details['args']}"
        logger.error(f"{error_msg}\nTraceback:\n{error_details['traceback']}")
        await log_error(error_msg, error_details["traceback"])


async def _channel_follow_task(event_sub: ChannelFollowEventSub) -> None:
    try:
        await twitch_send_message(
            event_sub.event.broadcaster_user_id,
            f"Thank you for following, {event_sub.event.user_name}! valinmHeart Your support means a lot to me! I hope you enjoy your stay! valinmHeart",
        )
    except Exception as e:
        error_details: ErrorDetails = {
            "type": type(e).__name__,
            "message": str(e),
            "args": e.args,
            "traceback": traceback.format_exc(),
        }
        error_msg = f"Error processing Twitch follow webhook task - Type: {error_details['type']}, Message: {error_details['message']}, Args: {error_details['args']}"
        logger.error(f"{error_msg}\nTraceback:\n{error_details['traceback']}")
        await log_error(error_msg, error_details["traceback"])


_ad_break_notification_tasks: dict[str, asyncio.Task] = {}


async def _schedule_next_ad_break_notification(broadcaster_id: str) -> None:
    try:
        ad_schedule = await get_ad_schedule(int(broadcaster_id))
        if not ad_schedule:
            return

        next_ad_time = pendulum.from_timestamp(ad_schedule.next_ad_at)
        notify_time = next_ad_time.subtract(minutes=5)
        now = pendulum.now(tz=pendulum.UTC)
        wait_seconds = (notify_time - now).total_seconds()
        if wait_seconds > 0:
            await asyncio.sleep(wait_seconds)
            await twitch_send_message(
                broadcaster_id,
                "The next ad break will start in 5 minutes! Feel free to take a quick break while the ads run! valinmHeart",
            )
    except asyncio.CancelledError:
        logger.info(
            "Cancelled ad break notification task for broadcaster_id=%s",
            broadcaster_id,
        )
        raise
    except Exception as e:
        error_details: ErrorDetails = {
            "type": type(e).__name__,
            "message": str(e),
            "args": e.args,
            "traceback": traceback.format_exc(),
        }
        error_msg = f"Error scheduling next ad break notification - Type: {error_details['type']}, Message: {error_details['message']}, Args: {error_details['args']}"
        logger.error(f"{error_msg}\nTraceback:\n{error_details['traceback']}")
        await log_error(error_msg, error_details["traceback"])


async def _channel_ad_break_begin_task(event_sub: ChannelAdBreakBeginEventSub) -> None:
    try:
        ad_duration = event_sub.event.duration_seconds
        await twitch_send_message(
            event_sub.event.broadcaster_user_id,
            f"A {ad_duration // 60} minute ad break is starting! Thank you for sticking with us through this break! valinmArrive Ads help support my content. Consider subscribing to remove ads and support the stream!",
        )
        await asyncio.sleep(ad_duration)
        await twitch_send_message(
            event_sub.event.broadcaster_user_id,
            "The ad break is finishing now! valinmArrive",
        )
        existing_task = _ad_break_notification_tasks.get(
            event_sub.event.broadcaster_user_id
        )
        if existing_task and not existing_task.done():
            existing_task.cancel()
        task = asyncio.create_task(
            _schedule_next_ad_break_notification(event_sub.event.broadcaster_user_id)
        )
        _ad_break_notification_tasks[event_sub.event.broadcaster_user_id] = task
        task.add_done_callback(
            lambda t,
            broadcaster_id=event_sub.event.broadcaster_user_id: _ad_break_notification_tasks.pop(
                broadcaster_id, None
            )
        )
    except Exception as e:
        error_details: ErrorDetails = {
            "type": type(e).__name__,
            "message": str(e),
            "args": e.args,
            "traceback": traceback.format_exc(),
        }
        error_msg = f"Error processing Twitch ad break webhook task - Type: {error_details['type']}, Message: {error_details['message']}, Args: {error_details['args']}"
        logger.error(f"{error_msg}\nTraceback:\n{error_details['traceback']}")
        await log_error(error_msg, error_details["traceback"])


async def _oauth_callback_common(
    code: str, state: str, endpoint: str
) -> RefreshResponse:
    if state != TWITCH_WEBHOOK_SECRET:
        logger.warning(f"400: Bad request. Invalid state: {state}")
        await send_message(
            f"400: Bad request on {endpoint}. Invalid state.",
            BOT_ADMIN_CHANNEL,
        )
        raise HTTPException(status_code=400)

    params = {
        "client_id": TWITCH_CLIENT_ID,
        "client_secret": TWITCH_CLIENT_SECRET,
        "code": code,
        "grant_type": "authorization_code",
        "redirect_uri": f"{APP_URL}{endpoint}",
    }
    response = await http_client_manager.request(
        "POST", "https://id.twitch.tv/oauth2/token", params=params
    )

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
    return auth_response


@twitch_router.get("/twitch/oauth/callback")
async def twitch_oauth_callback(code: str, state: str) -> Response:
    try:
        auth_response = await _oauth_callback_common(
            code, state, "/twitch/oauth/callback"
        )
        await token_manager.set_user_access_token(auth_response)
        return Response(
            "Authorization successful! You can close this tab.", status_code=200
        )
    except HTTPException as e:
        raise e
    except Exception as e:
        error_details: ErrorDetails = {
            "type": type(e).__name__,
            "message": str(e),
            "args": e.args,
            "traceback": traceback.format_exc(),
        }
        error_msg = f"500: Internal server error on /twitch/oauth/callback - Type: {error_details['type']}, Message: {error_details['message']}, Args: {error_details['args']}"
        logger.error(f"{error_msg}\nTraceback:\n{error_details['traceback']}")
        await log_error(error_msg, error_details["traceback"])
        raise HTTPException(status_code=500) from e


@twitch_router.get("/twitch/oauth/callback/broadcaster")
async def twitch_oauth_callback_broadcaster(code: str, state: str) -> Response:
    try:
        auth_response = await _oauth_callback_common(
            code, state, "/twitch/oauth/callback/broadcaster"
        )
        await token_manager.set_broadcaster_access_token(auth_response)
        return Response(
            "Authorization successful! You can close this tab.", status_code=200
        )
    except HTTPException as e:
        raise e
    except Exception as e:
        error_details: ErrorDetails = {
            "type": type(e).__name__,
            "message": str(e),
            "args": e.args,
            "traceback": traceback.format_exc(),
        }
        error_msg = f"500: Internal server error on /twitch/oauth/callback/broadcaster - Type: {error_details['type']}, Message: {error_details['message']}, Args: {error_details['args']}"
        logger.error(f"{error_msg}\nTraceback:\n{error_details['traceback']}")
        await log_error(error_msg, error_details["traceback"])
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

        asyncio.create_task(_stream_online_task(event_sub))

        return Response(status_code=202)
    except HTTPException as e:
        raise e
    except Exception as e:
        error_details: ErrorDetails = {
            "type": type(e).__name__,
            "message": str(e),
            "args": e.args,
            "traceback": traceback.format_exc(),
        }
        error_msg = f"500: Internal server error on /webhook/twitch - Type: {error_details['type']}, Message: {error_details['message']}, Args: {error_details['args']}"
        logger.error(f"{error_msg}\nTraceback:\n{error_details['traceback']}")
        await log_error(error_msg, error_details["traceback"])
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
        error_details: ErrorDetails = {
            "type": type(e).__name__,
            "message": str(e),
            "args": e.args,
            "traceback": traceback.format_exc(),
        }
        error_msg = f"500: Internal server error on /webhook/twitch/offline - Type: {error_details['type']}, Message: {error_details['message']}, Args: {error_details['args']}"
        logger.error(f"{error_msg}\nTraceback:\n{error_details['traceback']}")
        await log_error(error_msg, error_details["traceback"])
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
        error_details: ErrorDetails = {
            "type": type(e).__name__,
            "message": str(e),
            "args": e.args,
            "traceback": traceback.format_exc(),
        }
        error_msg = f"500: Internal server error on /webhook/twitch/chat - Type: {error_details['type']}, Message: {error_details['message']}, Args: {error_details['args']}"
        logger.error(f"{error_msg}\nTraceback:\n{error_details['traceback']}")
        await log_error(error_msg, error_details["traceback"])
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
        error_details: ErrorDetails = {
            "type": type(e).__name__,
            "message": str(e),
            "args": e.args,
            "traceback": traceback.format_exc(),
        }
        error_msg = f"500: Internal server error on /webhook/twitch/follow - Type: {error_details['type']}, Message: {error_details['message']}, Args: {error_details['args']}"
        logger.error(f"{error_msg}\nTraceback:\n{error_details['traceback']}")
        await log_error(error_msg, error_details["traceback"])
        raise HTTPException(status_code=500) from e


@twitch_router.post("/webhook/twitch/adbreak")
async def channel_ad_break_begin_webhook(request: Request) -> Response:
    try:
        validation = await validate_call(request, "/webhook/twitch/adbreak")
        if validation:
            return validation

        body: dict[str, Any] = await request.json()
        event_sub = ChannelAdBreakBeginEventSub.model_validate(body)
        if event_sub.subscription.type != "channel.ad_break.begin":
            logger.warning(
                f"400: Bad request. Invalid subscription type: {event_sub.subscription.type}"
            )
            await send_message(
                "400: Bad request on /webhook/twitch/adbreak. Invalid subscription type.",
                BOT_ADMIN_CHANNEL,
            )
            raise HTTPException(status_code=400)

        asyncio.create_task(_channel_ad_break_begin_task(event_sub))

        return Response(status_code=202)
    except HTTPException as e:
        raise e
    except Exception as e:
        error_details: ErrorDetails = {
            "type": type(e).__name__,
            "message": str(e),
            "args": e.args,
            "traceback": traceback.format_exc(),
        }
        error_msg = f"500: Internal server error on /webhook/twitch/adbreak - Type: {error_details['type']}, Message: {error_details['message']}, Args: {error_details['args']}"
        logger.error(f"{error_msg}\nTraceback:\n{error_details['traceback']}")
        await log_error(error_msg, error_details["traceback"])
        raise HTTPException(status_code=500) from e


async def _channel_raid_task(event_sub: ChannelRaidEventSub) -> None:
    try:
        if event_sub.event.from_broadcaster_user_id == TWITCH_BROADCASTER_ID:
            await twitch_send_message(
                event_sub.event.from_broadcaster_user_id,
                f"We just raided {event_sub.event.to_broadcaster_user_name}. In case you got left behind, you can find them here: https://www.twitch.tv/{event_sub.event.to_broadcaster_user_login} valinmHeart",
            )
        elif event_sub.event.to_broadcaster_user_id == TWITCH_BROADCASTER_ID:
            await twitch_send_message(
                event_sub.event.to_broadcaster_user_id,
                f"!so {event_sub.event.from_broadcaster_user_login}",
            )
    except Exception as e:
        error_details: ErrorDetails = {
            "type": type(e).__name__,
            "message": str(e),
            "args": e.args,
            "traceback": traceback.format_exc(),
        }
        error_msg = f"Error processing Twitch raid webhook task - Type: {error_details['type']}, Message: {error_details['message']}, Args: {error_details['args']}"
        logger.error(f"{error_msg}\nTraceback:\n{error_details['traceback']}")
        await log_error(error_msg, error_details["traceback"])


@twitch_router.post("/webhook/twitch/raid")
async def channel_raid_webhook(request: Request) -> Response:
    try:
        validation = await validate_call(request, "/webhook/twitch/raid")
        if validation:
            return validation

        body: dict[str, Any] = await request.json()
        event_sub = ChannelRaidEventSub.model_validate(body)
        if event_sub.subscription.type != "channel.raid":
            logger.warning(
                f"400: Bad request. Invalid subscription type: {event_sub.subscription.type}"
            )
            await send_message(
                "400: Bad request on /webhook/twitch/raid. Invalid subscription type.",
                BOT_ADMIN_CHANNEL,
            )
            raise HTTPException(status_code=400)

        asyncio.create_task(_channel_raid_task(event_sub))

        return Response(status_code=202)
    except HTTPException as e:
        raise e
    except Exception as e:
        error_details: ErrorDetails = {
            "type": type(e).__name__,
            "message": str(e),
            "args": e.args,
            "traceback": traceback.format_exc(),
        }
        error_msg = f"500: Internal server error on /webhook/twitch/raid - Type: {error_details['type']}, Message: {error_details['message']}, Args: {error_details['args']}"
        logger.error(f"{error_msg}\nTraceback:\n{error_details['traceback']}")
        await log_error(error_msg, error_details["traceback"])
        raise HTTPException(status_code=500) from e


async def _channel_moderate_task(event_sub: ChannelModerateEventSub) -> None:
    try:
        if (
            event_sub.event.action != "raid"
            or event_sub.event.broadcaster_user_id != TWITCH_BROADCASTER_ID
        ):
            return
        await twitch_send_message(
            event_sub.event.broadcaster_user_id,
            "Have a great rest of your day! valinmHeart Don't forget to stay hydrated and take care of yourself! valinmHeart",
        )
    except Exception as e:
        error_details: ErrorDetails = {
            "type": type(e).__name__,
            "message": str(e),
            "args": e.args,
            "traceback": traceback.format_exc(),
        }
        error_msg = f"Error processing Twitch moderate webhook task - Type: {error_details['type']}, Message: {error_details['message']}, Args: {error_details['args']}"
        logger.error(f"{error_msg}\nTraceback:\n{error_details['traceback']}")
        await log_error(error_msg, error_details["traceback"])


@twitch_router.post("/webhook/twitch/moderate")
async def channel_moderate_webhook(request: Request) -> Response:
    try:
        validation = await validate_call(request, "/webhook/twitch/moderate")
        if validation:
            return validation

        body: dict[str, Any] = await request.json()
        event_sub = ChannelModerateEventSub.model_validate(body)
        if event_sub.subscription.type != "channel.moderate":
            logger.warning(
                f"400: Bad request. Invalid subscription type: {event_sub.subscription.type}"
            )
            await send_message(
                "400: Bad request on /webhook/twitch/moderate. Invalid subscription type.",
                BOT_ADMIN_CHANNEL,
            )
            raise HTTPException(status_code=400)

        asyncio.create_task(_channel_moderate_task(event_sub))

        return Response(status_code=202)
    except HTTPException as e:
        raise e
    except Exception as e:
        error_details: ErrorDetails = {
            "type": type(e).__name__,
            "message": str(e),
            "args": e.args,
            "traceback": traceback.format_exc(),
        }
        error_msg = f"500: Internal server error on /webhook/twitch/moderate - Type: {error_details['type']}, Message: {error_details['message']}, Args: {error_details['args']}"
        logger.error(f"{error_msg}\nTraceback:\n{error_details['traceback']}")
        await log_error(error_msg, error_details["traceback"])
        raise HTTPException(status_code=500) from e
