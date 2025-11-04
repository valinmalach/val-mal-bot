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
    BROADCASTER_USERNAME,
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
    upsert_row_to_parquet,
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


def get_error_details(e: Exception) -> ErrorDetails:
    return {
        "type": type(e).__name__,
        "message": str(e),
        "args": e.args,
        "traceback": traceback.format_exc(),
    }


async def handle_error(e: Exception, context: str) -> None:
    error_details = get_error_details(e)
    error_msg = f"{context} - Type: {error_details['type']}, Message: {error_details['message']}, Args: {error_details['args']}"
    logger.error(f"{error_msg}\nTraceback:\n{error_details['traceback']}")
    await log_error(error_msg, error_details["traceback"])


async def process_webhook(
    request: Request, endpoint: str, event_model, expected_type: str, task_func
) -> Response:
    try:
        validation = await validate_call(request, endpoint)
        if validation:
            return validation

        body: dict[str, Any] = await request.json()
        event_sub = event_model.model_validate(body)
        if event_sub.subscription.type != expected_type:
            logger.warning(
                f"400: Bad request. Invalid subscription type: {event_sub.subscription.type}"
            )
            await send_message(
                f"400: Bad request on {endpoint}. Invalid subscription type.",
                BOT_ADMIN_CHANNEL,
            )
            raise HTTPException(status_code=400)

        _ = asyncio.create_task(task_func(event_sub))
        return Response(status_code=202)
    except HTTPException as e:
        raise e
    except Exception as e:
        await handle_error(e, f"500: Internal server error on {endpoint}")
        raise HTTPException(status_code=500) from e


def _get_twitch_url(user_login: str) -> str:
    """Generate Twitch channel URL from user login."""
    return f"https://www.twitch.tv/{user_login}"


def _get_live_alerts_mention(channel_id: int) -> str | None:
    """Get the live alerts role mention if channel is the stream alerts channel."""
    return f"<@&{LIVE_ALERTS_ROLE}>" if channel_id == STREAM_ALERTS_CHANNEL else None


def _is_main_broadcaster(broadcaster_id: str | int) -> bool:
    """Check if the broadcaster is the main broadcaster."""
    return str(broadcaster_id) == TWITCH_BROADCASTER_ID


def _extract_alert_data(alert: dict[str, Any]) -> tuple[int, int, str, str]:
    """Extract relevant data from alert dictionary."""
    channel_id = alert.get("channel_id", 0)
    message_id = alert.get("message_id", 0)
    stream_id = alert.get("stream_id", "")
    stream_started_at = alert.get("stream_started_at", "")
    return channel_id, message_id, stream_id, stream_started_at


async def _wait_for_stream_info(broadcaster_id: int):
    """Poll for stream info until it's available."""
    stream_info = await get_stream_info(broadcaster_id)
    while not stream_info:
        await asyncio.sleep(1)
        stream_info = await get_stream_info(broadcaster_id)
    return stream_info


async def _handle_broadcaster_stream_start(
    broadcaster_id: int, stream_info, is_main_broadcaster: bool
) -> None:
    """Send welcome messages when the main broadcaster goes live."""
    if not is_main_broadcaster:
        return None

    _ = asyncio.create_task(shoutout_queue.activate())
    await twitch_send_message(
        str(broadcaster_id),
        "NilavHcalam is here valinmArrive",
    )
    await twitch_send_message(
        str(broadcaster_id),
        f"{stream_info.user_name} is now live! Streaming {stream_info.game_name}: {stream_info.title}",
    )


def _create_stream_online_embed(stream_info, user_info: User | None) -> discord.Embed:
    """Create the Discord embed for a live stream notification."""
    url = _get_twitch_url(stream_info.user_login)
    raw_thumb_url = stream_info.thumbnail_url.replace("{width}x{height}", "400x225")
    cache_busted_thumb_url = f"{raw_thumb_url}?cb={int(pendulum.now().timestamp())}"

    return (
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


def _create_stream_watch_button(stream_info) -> View:
    """Create the view with a 'Watch Stream' button."""
    url = _get_twitch_url(stream_info.user_login)
    view = View(timeout=None)
    view.add_item(
        discord.ui.Button(label="Watch Stream", style=discord.ButtonStyle.link, url=url)
    )
    return view


async def _save_live_alert(
    broadcaster_id: int, channel: int, message_id: int, stream_info
) -> None:
    """Save the live alert to parquet storage and start update task."""
    alert: LiveAlert = {
        "id": broadcaster_id,
        "channel_id": channel,
        "message_id": message_id,
        "stream_id": int(stream_info.id),
        "stream_started_at": stream_info.started_at,
    }

    try:
        upsert_row_to_parquet(alert, LIVE_ALERTS)
        _ = asyncio.create_task(
            update_alert(
                broadcaster_id,
                channel,
                message_id,
                int(stream_info.id),
                stream_info.started_at,
            )
        )
    except Exception as e:
        await handle_error(
            e,
            f"Failed to insert live alert message into parquet for broadcaster {broadcaster_id}",
        )


async def _stream_online_task(event_sub: StreamOnlineEventSub) -> None:
    broadcaster_id = int(event_sub.event.broadcaster_user_id)
    try:
        stream_info = await _wait_for_stream_info(broadcaster_id)
        user_info = await get_user(broadcaster_id)

        is_main_broadcaster = stream_info.user_login == BROADCASTER_USERNAME
        channel = STREAM_ALERTS_CHANNEL if is_main_broadcaster else PROMO_CHANNEL

        await _handle_broadcaster_stream_start(
            broadcaster_id, stream_info, is_main_broadcaster
        )

        content = _get_live_alerts_mention(channel)

        embed = _create_stream_online_embed(stream_info, user_info)
        view = _create_stream_watch_button(stream_info)

        message_id = await send_embed(embed, channel, view, content=content)
        if message_id is None:
            await send_message(
                f"Failed to send live alert message\nbroadcaster_id: {broadcaster_id}\nchannel_id: {channel}",
                BOT_ADMIN_CHANNEL,
            )
            logger.error(f"Failed to send embed for broadcaster {broadcaster_id}")
            return None

        await _save_live_alert(broadcaster_id, channel, message_id, stream_info)

    except Exception as e:
        await handle_error(e, f"Error in _stream_online_task for {broadcaster_id}")


def _cancel_ad_break_task_if_needed(broadcaster_user_login: str) -> None:
    """Cancel ad break notification task for the broadcaster if it exists."""
    if broadcaster_user_login == BROADCASTER_USERNAME:
        shoutout_queue.deactivate()
        _cancel_task_if_exists(_ad_break_notification_tasks, broadcaster_user_login)


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
        await handle_error(
            e, f"Failed to fetch VOD info for broadcaster {broadcaster_id}"
        )
        return None


def _create_offline_embed(
    event_sub: StreamOfflineEventSub,
    user_info: User | None,
    channel_info: Channel | None,
    vod_info: Video | None,
    stream_started_at: str | None,
) -> discord.Embed:
    """Create the offline embed with all necessary information."""
    url = _get_twitch_url(event_sub.event.broadcaster_user_login)
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
        await handle_error(e, "Error editing offline embed")


async def _cleanup_live_alert(broadcaster_id: int) -> None:
    """Remove the live alert from storage."""
    try:
        delete_row_from_parquet(broadcaster_id, LIVE_ALERTS)
    except Exception as e:
        await handle_error(
            e, f"Failed to delete live alert for broadcaster {broadcaster_id}"
        )


async def _stream_offline_task(event_sub: StreamOfflineEventSub) -> None:
    broadcaster_id = int(event_sub.event.broadcaster_user_id)
    try:
        _cancel_ad_break_task_if_needed(event_sub.event.broadcaster_user_login)

        user_info, channel_info, alert = await _fetch_stream_data(broadcaster_id)
        if alert is None:
            return None

        channel_id, message_id, stream_id, stream_started_at = _extract_alert_data(
            alert
        )

        vod_info = await _get_vod_info(broadcaster_id, stream_id)

        content = _get_live_alerts_mention(channel_id)

        embed = _create_offline_embed(
            event_sub, user_info, channel_info, vod_info, stream_started_at
        )

        await _update_offline_message(message_id, embed, channel_id, content)
        await _cleanup_live_alert(broadcaster_id)

    except Exception as e:
        await handle_error(e, f"Error in _stream_offline_task for {broadcaster_id}")


async def _channel_chat_message_task(event_sub: ChannelChatMessageEventSub) -> None:
    user_command_dict: dict[
        str, Callable[[ChannelChatMessageEventSub, str], Awaitable[None]]
    ] = {
        "lurk": lurk,
        "discord": discord_command,
        "kofi": kofi,
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
            return None
        text_without_prefix = event_sub.event.message.text[1:]
        command_parts = text_without_prefix.split(" ", 1)
        command = command_parts[0].lower()
        args = command_parts[1] if len(command_parts) > 1 else ""

        if (
            event_sub.event.source_broadcaster_user_id is not None
            and event_sub.event.source_broadcaster_user_id
            != event_sub.event.broadcaster_user_id
        ):
            return None

        async def default_command(
            event_sub: ChannelChatMessageEventSub, args: str
        ) -> None:
            """Default no-op command handler."""
            pass

        await user_command_dict.get(command, default_command)(event_sub, args)
    except Exception as e:
        await handle_error(e, "Error processing Twitch chat webhook task")


async def _channel_follow_task(event_sub: ChannelFollowEventSub) -> None:
    try:
        await twitch_send_message(
            event_sub.event.broadcaster_user_id,
            f"Thank you for following, {event_sub.event.user_name}! valinmHeart Your support means a lot to me! I hope you enjoy your stay! valinmHeart",
        )
    except Exception as e:
        await handle_error(e, "Error processing Twitch follow webhook task")


_ad_break_notification_tasks: dict[str, asyncio.Task] = {}


def _cancel_task_if_exists(task_dict: dict[str, asyncio.Task], key: str) -> None:
    """Cancel an async task if it exists in the given dictionary."""
    existing_task = task_dict.get(key)
    if existing_task and not existing_task.done():
        existing_task.cancel()


def _register_ad_break_task(broadcaster_id: str, task: asyncio.Task) -> None:
    """Register an ad break notification task and set up auto-cleanup."""
    _ad_break_notification_tasks[broadcaster_id] = task
    task.add_done_callback(
        lambda t, bid=broadcaster_id: _ad_break_notification_tasks.pop(bid, None)
    )


async def _schedule_next_ad_break_notification(broadcaster_id: str) -> None:
    try:
        ad_schedule = await get_ad_schedule(int(broadcaster_id))
        if not ad_schedule:
            return None

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
        await handle_error(e, "Error scheduling next ad break notification")


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

        broadcaster_id = event_sub.event.broadcaster_user_id
        _cancel_task_if_exists(_ad_break_notification_tasks, broadcaster_id)

        task = asyncio.create_task(_schedule_next_ad_break_notification(broadcaster_id))
        _register_ad_break_task(broadcaster_id, task)
    except Exception as e:
        await handle_error(e, "Error processing Twitch ad break webhook task")


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
        await handle_error(e, "500: Internal server error on /twitch/oauth/callback")
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
        await handle_error(
            e, "500: Internal server error on /twitch/oauth/callback/broadcaster"
        )
        raise HTTPException(status_code=500) from e


@twitch_router.post("/webhook/twitch")
async def stream_online_webhook(request: Request) -> Response:
    return await process_webhook(
        request,
        "/webhook/twitch",
        StreamOnlineEventSub,
        "stream.online",
        _stream_online_task,
    )


@twitch_router.post("/webhook/twitch/offline")
async def stream_offline_webhook(request: Request) -> Response:
    return await process_webhook(
        request,
        "/webhook/twitch/offline",
        StreamOfflineEventSub,
        "stream.offline",
        _stream_offline_task,
    )


@twitch_router.post("/webhook/twitch/chat")
async def channel_chat_message_webhook(request: Request) -> Response:
    return await process_webhook(
        request,
        "/webhook/twitch/chat",
        ChannelChatMessageEventSub,
        "channel.chat.message",
        _channel_chat_message_task,
    )


@twitch_router.post("/webhook/twitch/follow")
async def channel_follow_webhook(request: Request) -> Response:
    return await process_webhook(
        request,
        "/webhook/twitch/follow",
        ChannelFollowEventSub,
        "channel.follow",
        _channel_follow_task,
    )


@twitch_router.post("/webhook/twitch/adbreak")
async def channel_ad_break_begin_webhook(request: Request) -> Response:
    return await process_webhook(
        request,
        "/webhook/twitch/adbreak",
        ChannelAdBreakBeginEventSub,
        "channel.ad_break.begin",
        _channel_ad_break_begin_task,
    )


async def _channel_raid_task(event_sub: ChannelRaidEventSub) -> None:
    try:
        if _is_main_broadcaster(event_sub.event.from_broadcaster_user_id):
            twitch_url = _get_twitch_url(event_sub.event.to_broadcaster_user_login)
            await twitch_send_message(
                event_sub.event.from_broadcaster_user_id,
                f"We just raided {event_sub.event.to_broadcaster_user_name}. In case you got left behind, you can find them here: {twitch_url} valinmHeart",
            )
        elif _is_main_broadcaster(event_sub.event.to_broadcaster_user_id):
            await twitch_send_message(
                event_sub.event.to_broadcaster_user_id,
                f"!so {event_sub.event.from_broadcaster_user_login}",
            )
    except Exception as e:
        await handle_error(e, "Error processing Twitch raid webhook task")


@twitch_router.post("/webhook/twitch/raid")
async def channel_raid_webhook(request: Request) -> Response:
    return await process_webhook(
        request,
        "/webhook/twitch/raid",
        ChannelRaidEventSub,
        "channel.raid",
        _channel_raid_task,
    )


async def _channel_moderate_task(event_sub: ChannelModerateEventSub) -> None:
    try:
        if event_sub.event.action != "raid" or not _is_main_broadcaster(
            event_sub.event.broadcaster_user_id
        ):
            return None
        await twitch_send_message(
            event_sub.event.broadcaster_user_id,
            "Have a great rest of your day! valinmHeart Don't forget to stay hydrated and take care of yourself! valinmHeart",
        )
    except Exception as e:
        await handle_error(e, "Error processing Twitch moderate webhook task")


@twitch_router.post("/webhook/twitch/moderate")
async def channel_moderate_webhook(request: Request) -> Response:
    return await process_webhook(
        request,
        "/webhook/twitch/moderate",
        ChannelModerateEventSub,
        "channel.moderate",
        _channel_moderate_task,
    )
