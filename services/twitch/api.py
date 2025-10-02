import asyncio
import itertools
import logging
import os
from typing import List, Optional

import discord
import pendulum
import polars as pl
import sentry_sdk
from discord.ui import View
from dotenv import load_dotenv

from constants import (
    BOT_ADMIN_CHANNEL,
    LIVE_ALERTS_ROLE,
    STREAM_ALERTS_CHANNEL,
    TokenType,
)
from models import (
    AdSchedule,
    AdScheduleResponse,
    Channel,
    ChannelResponse,
    Stream,
    StreamResponse,
    Subscription,
    SubscriptionResponse,
    User,
    UserResponse,
    Video,
    VideoResponse,
)
from services.helper.helper import (
    delete_row_from_parquet,
    edit_embed,
    get_age,
    parse_rfc3339,
    send_message,
)
from services.helper.twitch import call_twitch

load_dotenv()

APP_URL = os.getenv("APP_URL")
TWITCH_WEBHOOK_SECRET = os.getenv("TWITCH_WEBHOOK_SECRET")

logger = logging.getLogger(__name__)


@sentry_sdk.trace()
async def get_subscriptions() -> Optional[List[Subscription]]:
    all_subscriptions: List[Subscription] = []
    cursor: Optional[str] = None

    url = "https://api.twitch.tv/helix/eventsub/subscriptions"
    while True:
        params = {"status": "enabled"}
        if cursor:
            params["after"] = cursor
        response = await call_twitch("GET", url, params)
        if (
            response is None
            or response.status_code < 200
            or response.status_code >= 300
        ):
            logger.warning(
                f"Error fetching subscriptions: {response.status_code if response else 'No response'}"
            )
            await send_message(
                f"Failed to fetch subscriptions: {response.status_code if response else 'No response'} {response.text if response else ''}",
                BOT_ADMIN_CHANNEL,
            )
            return

        subscription_info_response = SubscriptionResponse.model_validate(
            response.json()
        )
        data = subscription_info_response.data
        if not data:
            break

        all_subscriptions.extend(data)
        cursor = subscription_info_response.pagination.cursor
        if not cursor:
            break

    return all_subscriptions


@sentry_sdk.trace()
async def get_user(id: int) -> Optional[User]:
    url = f"https://api.twitch.tv/helix/users?id={id}"
    response = await call_twitch("GET", url)
    if response is None or response.status_code < 200 or response.status_code >= 300:
        logger.warning(
            f"Failed to fetch user info: {response.status_code if response else 'No response'}"
        )
        await send_message(
            f"Failed to fetch user info: {response.status_code if response else 'No response'} {response.text if response else ''}",
            BOT_ADMIN_CHANNEL,
        )
        return
    user_info_response = UserResponse.model_validate(response.json())
    return user_info_response.data[0] if user_info_response.data else None


@sentry_sdk.trace()
async def get_user_by_username(username: str) -> Optional[User]:
    url = f"https://api.twitch.tv/helix/users?login={username}"
    response = await call_twitch("GET", url)
    if response is None or response.status_code < 200 or response.status_code >= 300:
        logger.warning(
            f"Failed to fetch user info: {response.status_code if response else 'No response'}"
        )
        await send_message(
            f"Failed to fetch user info: {response.status_code if response else 'No response'} {response.text if response else ''}",
            BOT_ADMIN_CHANNEL,
        )
        return
    user_info_response = UserResponse.model_validate(response.json())
    return user_info_response.data[0] if user_info_response.data else None


@sentry_sdk.trace()
async def subscribe_to_user(username: str) -> bool:
    user = await get_user_by_username(username)
    if not user:
        logger.warning(f"User not found: {username}")
        await send_message(f"User not found: {username}", BOT_ADMIN_CHANNEL)
        return False

    user_id = user.id

    url = "https://api.twitch.tv/helix/eventsub/subscriptions"
    body = {
        "type": "stream.online",
        "version": "1",
        "condition": {"broadcaster_user_id": user_id},
        "transport": {
            "method": "webhook",
            "callback": f"{APP_URL}/webhook/twitch",
            "secret": TWITCH_WEBHOOK_SECRET,
        },
    }
    response = await call_twitch("POST", url, body)
    if response is None or response.status_code < 200 or response.status_code >= 300:
        logger.warning(
            f"Failed to subscribe to online event: {response.status_code if response else 'No response'}"
        )
        await send_message(
            f"Failed to subscribe to online event: {response.status_code if response else 'No response'} {response.text if response else ''}",
            BOT_ADMIN_CHANNEL,
        )
        return False

    body = {
        "type": "stream.offline",
        "version": "1",
        "condition": {"broadcaster_user_id": user_id},
        "transport": {
            "method": "webhook",
            "callback": f"{APP_URL}/webhook/twitch/offline",
            "secret": TWITCH_WEBHOOK_SECRET,
        },
    }
    response = await call_twitch("POST", url, body)
    if response is None or response.status_code < 200 or response.status_code >= 300:
        logger.warning(
            f"Failed to subscribe to offline event: {response.status_code if response else 'No response'}"
        )
        await send_message(
            f"Failed to subscribe to offline event: {response.status_code if response else 'No response'} {response.text if response else ''}",
            BOT_ADMIN_CHANNEL,
        )
        return False

    return True


@sentry_sdk.trace()
async def get_users(ids: List[str]) -> Optional[List[User]]:
    batches_iterator = itertools.batched(ids, 100)
    batches_list = [list(batch) for batch in batches_iterator]

    users: List[User] = []

    for batch in batches_list:
        if not batch:
            continue
        url = f"https://api.twitch.tv/helix/users?id={'&id='.join(batch)}"
        response = await call_twitch("GET", url)
        if (
            response is None
            or response.status_code < 200
            or response.status_code >= 300
        ):
            logger.warning(
                f"Failed batch fetch of user infos: {response.status_code if response else 'No response'}"
            )
            await send_message(
                f"Failed to fetch users infos: {response.status_code if response else 'No response'} {response.text if response else ''}",
                BOT_ADMIN_CHANNEL,
            )
            return
        user_info_response = UserResponse.model_validate(response.json())
        users.extend(user_info_response.data)

    return users


@sentry_sdk.trace()
async def get_channel(id: int) -> Optional[Channel]:
    url = f"https://api.twitch.tv/helix/channels?broadcaster_id={id}"
    response = await call_twitch("GET", url)
    if response is None or response.status_code < 200 or response.status_code >= 300:
        logger.warning(
            f"Failed to fetch channel info: {response.status_code if response else 'No response'}"
        )
        await send_message(
            f"Failed to fetch channel info: {response.status_code if response else 'No response'} {response.text if response else ''}",
            BOT_ADMIN_CHANNEL,
        )
        return
    channel_info_response = ChannelResponse.model_validate(response.json())
    return channel_info_response.data[0] if channel_info_response.data else None


@sentry_sdk.trace()
async def get_stream_info(broadcaster_id: int) -> Optional[Stream]:
    url = f"https://api.twitch.tv/helix/streams?user_id={broadcaster_id}"
    response = await call_twitch("GET", url)
    if response is None or response.status_code < 200 or response.status_code >= 300:
        logger.warning(
            f"Failed to fetch stream info: {response.status_code if response else 'No response'}"
        )
        await send_message(
            f"Failed to fetch stream info: {response.status_code if response else 'No response'} {response.text if response else ''}",
            BOT_ADMIN_CHANNEL,
        )
        return
    stream_info_response = StreamResponse.model_validate(response.json())
    return stream_info_response.data[0] if stream_info_response.data else None


@sentry_sdk.trace()
async def get_stream_vod(user_id: int, stream_id: int) -> Optional[Video]:
    url = f"https://api.twitch.tv/helix/videos?user_id={user_id}&type=archive"
    response = await call_twitch("GET", url)
    if response is None or response.status_code < 200 or response.status_code >= 300:
        logger.warning(
            f"Failed to fetch VOD info: {response.status_code if response else 'No response'}"
        )
        await send_message(
            f"Failed to fetch VOD info: {response.status_code if response else 'No response'} {response.text if response else ''}",
            BOT_ADMIN_CHANNEL,
        )
        return
    video_info_response = VideoResponse.model_validate(response.json())
    return next(
        (
            video
            for video in video_info_response.data
            if video.stream_id == str(stream_id)
        ),
        None,
    )


@sentry_sdk.trace()
async def get_ad_schedule(broadcaster_id: int) -> Optional[AdSchedule]:
    url = f"https://api.twitch.tv/helix/channels/ads?broadcaster_id={broadcaster_id}"
    response = await call_twitch("GET", url, None, TokenType.Broadcaster)
    if response is None or response.status_code < 200 or response.status_code >= 300:
        logger.warning(
            f"Failed to fetch ad schedule: {response.status_code if response else 'No response'}"
        )
        await send_message(
            f"Failed to fetch ad schedule: {response.status_code if response else 'No response'} {response.text if response else ''}",
            BOT_ADMIN_CHANNEL,
        )
        return
    ad_schedule_response = AdScheduleResponse.model_validate(response.json())
    return ad_schedule_response.data[0] if ad_schedule_response.data else None


@sentry_sdk.trace()
async def trigger_offline_sequence(
    broadcaster_id: int,
    stream_id: int,
    stream_info: Optional[Stream],
    now: pendulum.DateTime,
    user_info: Optional[User],
    url: str,
    age: str,
    message_id: int,
    channel_id: int,
    content: Optional[str],
    channel: Optional[Channel],
) -> None:
    if (stream_info and stream_info.user_login == "valinmalach") or (
        user_info and user_info.login == "valinmalach"
    ):
        from controller.twitch import _ad_break_notification_tasks
        from services.twitch.shoutout_queue import shoutout_queue

        await shoutout_queue.deactivate()
        existing_task = _ad_break_notification_tasks.get(str(broadcaster_id))
        if existing_task and not existing_task.done():
            existing_task.cancel()

    vod_info = None
    try:
        vod_info = await get_stream_vod(broadcaster_id, stream_id)
    except Exception as e:
        sentry_sdk.capture_exception(e)
        logger.warning(
            f"Failed to fetch VOD info for broadcaster_id={broadcaster_id}: {e}",
        )
        await send_message(
            f"Failed to fetch VOD info for {broadcaster_id}: {e}",
            BOT_ADMIN_CHANNEL,
        )

    if not vod_info:
        logger.warning(
            f"No VOD info found for broadcaster_id={broadcaster_id}, stream_id={stream_id}",
        )

    embed = (
        discord.Embed(
            description=f"**{stream_info.title if stream_info else vod_info.title if vod_info else channel.title if channel else 'Unknown'}**",
            color=0x9046FF,
            timestamp=now,
        )
        .set_author(
            name=f"{stream_info.user_name if stream_info else user_info.display_name if user_info else 'Unknown'} was live",
            icon_url=user_info.profile_image_url if user_info else None,
            url=url,
        )
        .add_field(
            name="**Game**",
            value=f"{stream_info.game_name if stream_info else channel.game_name if channel else 'Unknown'}",
            inline=True,
        )
        .set_footer(
            text=f"Online for {age} | Offline at",
        )
    )
    if vod_info:
        vod_url = vod_info.url
        embed = embed.add_field(
            name="**VOD**",
            value=f"[**Click to view**]({vod_url})",
            inline=True,
        )
    try:
        await edit_embed(message_id, embed, channel_id, content=content)
    except discord.NotFound:
        logger.warning(
            f"Message not found when editing offline embed for message_id={message_id}; aborting"
        )
        delete_row_from_parquet(broadcaster_id, "data/live_alerts.parquet")
    except Exception as e:
        logger.warning(
            f"Error encountered while editing offline embed; Continuing without aborting...\n{e}"
        )


@sentry_sdk.trace()
async def update_alert(
    broadcaster_id: int,
    channel_id: int,
    message_id: int,
    stream_id: int,
    stream_started_at: str,
) -> None:
    try:
        await asyncio.sleep(60)

        content = (
            f"<@&{LIVE_ALERTS_ROLE}>" if channel_id == STREAM_ALERTS_CHANNEL else None
        )
        started_at = parse_rfc3339(stream_started_at)
        started_at_timestamp = f"<t:{int(started_at.timestamp())}:f>"

        df = pl.read_parquet("data/live_alerts.parquet")
        alert_row = df.filter(pl.col("id") == broadcaster_id)
        if alert_row.height == 0:
            logger.warning(
                f"No live alert record found for broadcaster_id={broadcaster_id}; exiting"
            )
            return
        stream_info = await get_stream_info(broadcaster_id)
        user_info = await get_user(broadcaster_id)
        channel_info = await get_channel(broadcaster_id)
        if alert_row.height == 0 or stream_info is None:
            url = f"https://www.twitch.tv/{user_info.login if user_info else channel_info.broadcaster_login if channel_info else ''}"
            now = pendulum.now()
            age = get_age(started_at, limit_units=2)
            await trigger_offline_sequence(
                broadcaster_id,
                stream_id,
                stream_info,
                now,
                user_info,
                url,
                age,
                message_id,
                channel_id,
                content,
                channel_info,
            )
            return
        while alert_row.height != 0 and stream_info is not None:
            alert = alert_row.row(0, named=True)
            url = f"https://www.twitch.tv/{stream_info.user_login}"
            now = pendulum.now()
            age = get_age(started_at, limit_units=2)
            if alert.get("stream_id", "") != stream_id or stream_info.id != str(
                stream_id
            ):
                await trigger_offline_sequence(
                    broadcaster_id,
                    stream_id,
                    stream_info,
                    now,
                    user_info,
                    url,
                    age,
                    message_id,
                    channel_id,
                    content,
                    channel_info,
                )
                return
            raw_thumb_url = stream_info.thumbnail_url.replace(
                "{width}x{height}", "400x225"
            )
            cache_busted_thumb_url = (
                f"{raw_thumb_url}?cb={int(pendulum.now().timestamp())}"
            )
            embed = (
                discord.Embed(
                    description=f"[**{stream_info.title}**]({url})",
                    color=0x9046FF,
                    timestamp=now,
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
                .add_field(
                    name="**Started At**",
                    value=started_at_timestamp,
                    inline=True,
                )
                .set_image(url=cache_busted_thumb_url)
                .set_footer(
                    text=f"Online for {age} | Last updated",
                )
            )
            view = View(timeout=None)
            view.add_item(
                discord.ui.Button(
                    label="Watch Stream", style=discord.ButtonStyle.link, url=url
                )
            )
            try:
                await edit_embed(message_id, embed, channel_id, view, content=content)
            except discord.NotFound:
                logger.warning(
                    f"Message not found when editing offline embed for message_id={message_id}; aborting"
                )
                delete_row_from_parquet(broadcaster_id, "data/live_alerts.parquet")
                return
            except Exception as e:
                logger.warning(
                    f"Error on live embed edit; Continuing without aborting: {e}"
                )
            await asyncio.sleep(60)
            df = pl.read_parquet("data/live_alerts.parquet")
            alert_row = df.filter(pl.col("id") == broadcaster_id)
            stream_info = await get_stream_info(broadcaster_id)

    except Exception as e:
        logger.warning(
            f"Error updating live alert message for broadcaster_id={broadcaster_id}: {e}"
        )
        sentry_sdk.capture_exception(e)
        await send_message(
            f"Failed to update live alert message: {e}",
            BOT_ADMIN_CHANNEL,
        )
