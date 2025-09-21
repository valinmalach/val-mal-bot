import asyncio
import itertools
import logging
import os
from typing import List, Optional

import discord
import httpx
import pendulum
import polars as pl
import sentry_sdk
from discord.ui import View
from dotenv import load_dotenv

from constants import BOT_ADMIN_CHANNEL, LIVE_ALERTS_ROLE, STREAM_ALERTS_CHANNEL
from models import (
    ChannelInfo,
    ChannelInfoResponse,
    StreamInfo,
    StreamInfoResponse,
    SubscriptionInfo,
    SubscriptionInfoResponse,
    UserInfo,
    UserInfoResponse,
    VideoInfo,
    VideoInfoResponse,
)
from services import (
    delete_row_from_parquet,
    edit_embed,
    get_age,
    parse_rfc3339,
    send_message,
)
from services.twitch_token_manager import token_manager

load_dotenv()

TWITCH_CLIENT_ID = os.getenv("TWITCH_CLIENT_ID")
TWITCH_WEBHOOK_SECRET = os.getenv("TWITCH_WEBHOOK_SECRET")

logger = logging.getLogger(__name__)


@sentry_sdk.trace()
async def refresh_access_token() -> bool:
    return await token_manager.refresh_access_token()


@sentry_sdk.trace()
async def get_subscriptions() -> Optional[List[SubscriptionInfo]]:
    logger.info("Retrieving Twitch EventSub subscriptions list")
    if not token_manager.access_token:
        refresh_success = await refresh_access_token()
        if not refresh_success:
            return

    all_subscriptions: List[SubscriptionInfo] = []
    cursor: Optional[str] = None

    url = "https://api.twitch.tv/helix/eventsub/subscriptions"
    headers = {
        "Client-ID": TWITCH_CLIENT_ID,
        "Authorization": f"Bearer {token_manager.access_token}",
    }
    logger.info(f"Subscriptions endpoint URL: {url}")
    while True:
        logger.info(f"Fetching subscriptions page with cursor={cursor}")
        params = {"status": "enabled"}
        if cursor:
            params["after"] = cursor

        response = httpx.get(
            url,
            headers=headers,
            params=params,
        )
        logger.info(f"Subscriptions API response status={response.status_code}")
        if response.status_code == 401:
            logger.warning(
                "Unauthorized when fetching subscriptions, refreshing token..."
            )
            if await refresh_access_token():
                headers["Authorization"] = f"Bearer {token_manager.access_token}"
                response = httpx.get(
                    url,
                    headers=headers,
                    params=params,
                )
            else:
                return

        if response.status_code < 200 or response.status_code >= 300:
            logger.warning(f"Error fetching subscriptions: {response.status_code}")
            await send_message(
                f"Failed to fetch subscriptions: {response.status_code} {response.text}",
                BOT_ADMIN_CHANNEL,
            )
            return

        subscription_info_response = SubscriptionInfoResponse.model_validate(
            response.json()
        )
        data = subscription_info_response.data
        logger.info(f"Received {len(data)} subscriptions")
        if not data:
            break

        all_subscriptions.extend(data)
        cursor = subscription_info_response.pagination.cursor
        logger.info(f"Next pagination cursor={cursor}")
        if not cursor:
            break

    return all_subscriptions


@sentry_sdk.trace()
async def get_user(id: int) -> Optional[UserInfo]:
    logger.info(f"Retrieving Twitch user info for id={id}")
    if not token_manager.access_token:
        refresh_success = await refresh_access_token()
        if not refresh_success:
            return

    url = f"https://api.twitch.tv/helix/users?id={id}"
    logger.info(f"Requesting user endpoint: {url}")
    headers = {
        "Client-ID": TWITCH_CLIENT_ID,
        "Authorization": f"Bearer {token_manager.access_token}",
    }
    response = httpx.get(url, headers=headers)
    logger.info(f"User endpoint response status={response.status_code}")
    if response.status_code == 401:
        logger.warning("Unauthorized fetching user, refreshing token...")
        if await refresh_access_token():
            headers["Authorization"] = f"Bearer {token_manager.access_token}"
            response = httpx.get(url, headers=headers)
        else:
            return
    if response.status_code < 200 or response.status_code >= 300:
        logger.warning(f"Failed to fetch user info: {response.status_code}")
        await send_message(
            f"Failed to fetch user info: {response.status_code} {response.text}",
            BOT_ADMIN_CHANNEL,
        )
        return
    user_info_response = UserInfoResponse.model_validate(response.json())
    logger.info(f"Parsed user_info count={len(user_info_response.data)}")
    return user_info_response.data[0] if user_info_response.data else None


@sentry_sdk.trace()
async def get_user_by_username(username: str) -> Optional[UserInfo]:
    logger.info(f"Retrieving Twitch user info for username={username}")
    if not token_manager.access_token:
        refresh_success = await refresh_access_token()
        if not refresh_success:
            return

    url = f"https://api.twitch.tv/helix/users?login={username}"
    logger.info(f"Requesting user endpoint: {url}")
    headers = {
        "Client-ID": TWITCH_CLIENT_ID,
        "Authorization": f"Bearer {token_manager.access_token}",
    }
    response = httpx.get(url, headers=headers)
    logger.info(f"User endpoint response status={response.status_code}")
    if response.status_code == 401:
        logger.warning("Unauthorized fetching user, refreshing token...")
        if await refresh_access_token():
            headers["Authorization"] = f"Bearer {token_manager.access_token}"
            response = httpx.get(url, headers=headers)
        else:
            return
    if response.status_code < 200 or response.status_code >= 300:
        logger.warning(f"Failed to fetch user info: {response.status_code}")
        await send_message(
            f"Failed to fetch user info: {response.status_code} {response.text}",
            BOT_ADMIN_CHANNEL,
        )
        return
    user_info_response = UserInfoResponse.model_validate(response.json())
    logger.info(f"Parsed user_info count={len(user_info_response.data)}")
    return user_info_response.data[0] if user_info_response.data else None


@sentry_sdk.trace()
async def subscribe_to_user(username: str) -> bool:
    logger.info(f"Retrieving Twitch user info for username={username}")
    if not token_manager.access_token:
        refresh_success = await refresh_access_token()
        if not refresh_success:
            return False

    logger.info(f"Getting user: {username}")
    user = await get_user_by_username(username)
    if not user:
        logger.warning(f"User not found: {username}")
        await send_message(f"User not found: {username}", BOT_ADMIN_CHANNEL)
        return False
    logger.info(f"Subscribing to user: {user.display_name} (id={user.id})")

    user_id = user.id

    url = "https://api.twitch.tv/helix/eventsub/subscriptions"
    headers = {
        "Client-ID": TWITCH_CLIENT_ID,
        "Authorization": f"Bearer {token_manager.access_token}",
    }

    logger.info(f"Subscribing to stream.online for user={user.display_name}")
    body = {
        "type": "stream.online",
        "version": "1",
        "condition": {"broadcaster_user_id": user_id},
        "transport": {
            "method": "webhook",
            "callback": "https://valin.loclx.io/webhook/twitch",
            "secret": TWITCH_WEBHOOK_SECRET,
        },
    }
    response = httpx.post(url, headers=headers, json=body)
    if response.status_code == 401:
        logger.warning("Unauthorized subscribing to online event, refreshing token...")
        if not await refresh_access_token():
            return False
        headers["Authorization"] = f"Bearer {token_manager.access_token}"
        response = httpx.post(url, headers=headers, json=body)
    if response.status_code < 200 or response.status_code >= 300:
        logger.warning(f"Failed to subscribe to online event: {response.status_code}")
        await send_message(
            f"Failed to subscribe to online event: {response.status_code} {response.text}",
            BOT_ADMIN_CHANNEL,
        )
        return False
    logger.info(f"Subscribed to stream.online for user: {user.display_name}")

    logger.info(f"Subscribing to stream.offline for user={user.display_name}")
    body = {
        "type": "stream.offline",
        "version": "1",
        "condition": {"broadcaster_user_id": user_id},
        "transport": {
            "method": "webhook",
            "callback": "https://valin.loclx.io/webhook/twitch/offline",
            "secret": TWITCH_WEBHOOK_SECRET,
        },
    }
    response = httpx.post(url, headers=headers, json=body)
    if response.status_code == 401:
        logger.warning("Unauthorized subscribing to offline event, refreshing token...")
        if not await refresh_access_token():
            return False
        headers["Authorization"] = f"Bearer {token_manager.access_token}"
        response = httpx.post(url, headers=headers, json=body)
    if response.status_code < 200 or response.status_code >= 300:
        logger.warning(f"Failed to subscribe to offline event: {response.status_code}")
        await send_message(
            f"Failed to subscribe to offline event: {response.status_code} {response.text}",
            BOT_ADMIN_CHANNEL,
        )
        return False
    logger.info(f"Subscribed to stream.offline for user: {user.display_name}")

    return True


@sentry_sdk.trace()
async def get_users(ids: List[str]) -> Optional[List[UserInfo]]:
    logger.info(f"Retrieving Twitch user infos in batches for ids={ids}")
    if not token_manager.access_token:
        refresh_success = await refresh_access_token()
        if not refresh_success:
            return

    # Split ids into batches of 100
    batches_iterator = itertools.batched(ids, 100)
    batches_list = [list(batch) for batch in batches_iterator]
    logger.info(f"Split into {len(batches_list)} batches")

    users: List[UserInfo] = []

    for batch in batches_list:
        logger.info(f"Requesting batch: {batch}")
        if not batch:
            continue
        url = f"https://api.twitch.tv/helix/users?id={'&id='.join(batch)}"
        headers = {
            "Client-ID": TWITCH_CLIENT_ID,
            "Authorization": f"Bearer {token_manager.access_token}",
        }
        response = httpx.get(url, headers=headers)
        logger.info(f"Batch users API response status={response.status_code}")
        if response.status_code == 401:
            logger.warning("Unauthorized on batch users, refreshing token...")
            if await refresh_access_token():
                headers["Authorization"] = f"Bearer {token_manager.access_token}"
                response = httpx.get(url, headers=headers)
            else:
                return
        if response.status_code < 200 or response.status_code >= 300:
            logger.warning(f"Failed batch fetch of user infos: {response.status_code}")
            await send_message(
                f"Failed to fetch users infos: {response.status_code} {response.text}",
                BOT_ADMIN_CHANNEL,
            )
            return
        user_info_response = UserInfoResponse.model_validate(response.json())
        logger.info(f"Batch parsed {len(user_info_response.data)} users")
        users.extend(user_info_response.data)

    return users


@sentry_sdk.trace()
async def get_channel(id: int) -> Optional[ChannelInfo]:
    logger.info(f"Retrieving Twitch channel info for broadcaster_id={id}")
    if not token_manager.access_token:
        refresh_success = await refresh_access_token()
        if not refresh_success:
            return

    url = f"https://api.twitch.tv/helix/channels?broadcaster_id={id}"
    logger.info(f"Requesting channel endpoint: {url}")
    headers = {
        "Client-ID": TWITCH_CLIENT_ID,
        "Authorization": f"Bearer {token_manager.access_token}",
    }
    response = httpx.get(url, headers=headers)
    logger.info(f"Channel endpoint response status={response.status_code}")
    if response.status_code == 401:
        logger.warning("Unauthorized fetching channel, refreshing token...")
        if await refresh_access_token():
            headers["Authorization"] = f"Bearer {token_manager.access_token}"
            response = httpx.get(url, headers=headers)
        else:
            return
    if response.status_code < 200 or response.status_code >= 300:
        logger.warning(f"Failed to fetch channel info: {response.status_code}")
        await send_message(
            f"Failed to fetch channel info: {response.status_code} {response.text}",
            BOT_ADMIN_CHANNEL,
        )
        return
    channel_info_response = ChannelInfoResponse.model_validate(response.json())
    logger.info(f"Parsed channel info: {len(channel_info_response.data)} entries")
    return channel_info_response.data[0] if channel_info_response.data else None


@sentry_sdk.trace()
async def get_stream_info(broadcaster_id: int) -> Optional[StreamInfo]:
    logger.info(f"Retrieving Twitch stream info for broadcaster_id={broadcaster_id}")
    if not token_manager.access_token:
        refresh_success = await refresh_access_token()
        if not refresh_success:
            return

    url = f"https://api.twitch.tv/helix/streams?user_id={broadcaster_id}"
    logger.info(f"Requesting stream info endpoint: {url}")
    headers = {
        "Client-ID": TWITCH_CLIENT_ID,
        "Authorization": f"Bearer {token_manager.access_token}",
    }
    response = httpx.get(url, headers=headers)
    logger.info(f"Stream info API response status={response.status_code}")
    if response.status_code == 401:
        logger.warning("Unauthorized fetching stream info, refreshing token...")
        if await refresh_access_token():
            headers["Authorization"] = f"Bearer {token_manager.access_token}"
            response = httpx.get(url, headers=headers)
        else:
            return
    if response.status_code < 200 or response.status_code >= 300:
        logger.warning(f"Failed to fetch stream info: {response.status_code}")
        await send_message(
            f"Failed to fetch stream info: {response.status_code} {response.text}",
            BOT_ADMIN_CHANNEL,
        )
        return
    stream_info_response = StreamInfoResponse.model_validate(response.json())
    logger.info(f"Parsed stream info items: {len(stream_info_response.data)}")
    return stream_info_response.data[0] if stream_info_response.data else None


@sentry_sdk.trace()
async def get_stream_vod(user_id: int, stream_id: int) -> Optional[VideoInfo]:
    logger.info(
        f"Retrieving VOD list for user_id={user_id} to find stream_id={stream_id}"
    )
    if not token_manager.access_token:
        refresh_success = await refresh_access_token()
        if not refresh_success:
            return

    url = f"https://api.twitch.tv/helix/videos?user_id={user_id}&type=archive"
    logger.info(f"Requesting VOD endpoint: {url}")
    headers = {
        "Client-ID": TWITCH_CLIENT_ID,
        "Authorization": f"Bearer {token_manager.access_token}",
    }
    response = httpx.get(url, headers=headers)
    logger.info(f"VOD API response status={response.status_code}")
    if response.status_code == 401:
        logger.warning("Unauthorized fetching VOD, refreshing token...")
        if await refresh_access_token():
            headers["Authorization"] = f"Bearer {token_manager.access_token}"
            response = httpx.get(url, headers=headers)
        else:
            return
    if response.status_code < 200 or response.status_code >= 300:
        logger.warning(f"Failed to fetch VOD info: {response.status_code}")
        await send_message(
            f"Failed to fetch stream info: {response.status_code} {response.text}",
            BOT_ADMIN_CHANNEL,
        )
        return
    video_info_response = VideoInfoResponse.model_validate(response.json())
    logger.info(f"Parsed {len(video_info_response.data)} video entries")
    return next(
        (
            video
            for video in video_info_response.data
            if video.stream_id == str(stream_id)
        ),
        None,
    )


@sentry_sdk.trace()
async def update_alert(
    broadcaster_id: int,
    channel_id: int,
    message_id: int,
    stream_id: int,
    stream_started_at: str,
) -> None:
    logger.info(
        f"Updating live alert embed in Discord for broadcaster_id={broadcaster_id}, message_id={message_id}",
    )
    try:
        await asyncio.sleep(60)
        df = pl.read_parquet("data/live_alerts.parquet")
        alert_row = df.filter(pl.col("id") == broadcaster_id)
        if alert_row.height == 0:
            logger.warning(
                f"No live alert record found for broadcaster_id={broadcaster_id}; exiting"
            )
            return
        stream_info = await get_stream_info(broadcaster_id)
        logger.info(f"Fetched stream_info for update: {stream_info}")
        user_info = await get_user(broadcaster_id)
        logger.info(f"Fetched user_info for update: {user_info}")
        while alert_row.height != 0 and stream_info is not None:
            alert = alert_row.row(0, named=True)
            logger.info(
                f"Live alert record found. Checking if stream_id changed (current={stream_info.id}, original={stream_id})",
            )
            content = (
                f"<@&{LIVE_ALERTS_ROLE}>"
                if channel_id == STREAM_ALERTS_CHANNEL
                else None
            )
            url = f"https://www.twitch.tv/{stream_info.user_login}"
            started_at = parse_rfc3339(stream_started_at)
            started_at_timestamp = f"<t:{int(started_at.timestamp())}:f>"
            now = pendulum.now()
            age = get_age(started_at, limit_units=2)
            if alert.get("stream_id", "") != stream_id or stream_info.id != str(
                stream_id
            ):
                logger.info(
                    f"Stream ID changed; building offline VOD embed for previous stream_id={stream_id}",
                )
                vod_info = None
                logger.info(
                    f"Beginning VOD lookup for broadcaster_id={broadcaster_id}, stream_id={stream_id}",
                )
                try:
                    vod_info = await get_stream_vod(broadcaster_id, stream_id)
                    if vod_info:
                        logger.info(f"VOD info found: {vod_info}")
                    else:
                        logger.warning(
                            f"No VOD info found for broadcaster_id={broadcaster_id}, stream_id={stream_id}",
                        )
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
                        f"No VOD info found for broadcaster_id={broadcaster_id}",
                    )

                logger.info(
                    f"Building offline embed for previous stream_id={stream_id}"
                )
                embed = (
                    discord.Embed(
                        description=f"**{stream_info.title}**",
                        color=0x9046FF,
                        timestamp=now,
                    )
                    .set_author(
                        name=f"{stream_info.user_name} was live",
                        icon_url=user_info.profile_image_url if user_info else None,
                        url=url,
                    )
                    .add_field(
                        name="**Game**",
                        value=f"{stream_info.game_name}",
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
                logger.info("Editing embed message to display offline VOD")
                try:
                    await edit_embed(message_id, embed, channel_id, content=content)
                    logger.info(
                        f"Successfully edited embed for offline event message_id={message_id}",
                    )
                    break
                except discord.NotFound:
                    logger.warning(
                        f"Message not found when editing offline embed for message_id={message_id}; aborting"
                    )
                    delete_row_from_parquet(broadcaster_id, "data/live_alerts.parquet")
                    break
                except Exception as e:
                    logger.warning(
                        f"Error encountered while editing offline embed; Continuing without aborting...\n{e}"
                    )
                return
            logger.info(f"Building live update embed for ongoing stream_id={stream_id}")
            # Cache-bust thumbnail URL to force Discord to refresh the image
            raw_thumb_url = stream_info.thumbnail_url.replace(
                "{width}x{height}", "400x225"
            )
            cache_busted_thumb_url = (
                f"{raw_thumb_url}?cb={int(pendulum.now().timestamp())}"
            )
            logger.info(f"Using cache-busted thumbnail URL: {cache_busted_thumb_url}")
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
            logger.info(
                f"Editing embed message for live update message_id={message_id}"
            )
            try:
                await edit_embed(message_id, embed, channel_id, view, content=content)
                logger.info(
                    f"Successfully edited embed for live update message_id={message_id}"
                )
                break
            except discord.NotFound:
                logger.warning(
                    f"Message not found when editing offline embed for message_id={message_id}; aborting"
                )
                delete_row_from_parquet(broadcaster_id, "data/live_alerts.parquet")
                break
            except Exception as e:
                logger.warning(
                    f"Error on live embed edit; Continuing without aborting: {e}"
                )
            logger.info("Sleeping for 60 seconds before next update cycle")
            await asyncio.sleep(60)
            logger.info(
                f"Fetching updated live_alert record after sleep for broadcaster_id={broadcaster_id}"
            )
            df = pl.read_parquet("data/live_alerts.parquet")
            alert_row = df.filter(pl.col("id") == broadcaster_id)
            stream_info = await get_stream_info(broadcaster_id)
            logger.info(f"Fetched updated stream_info for next cycle: {stream_info}")

    except Exception as e:
        logger.warning(
            f"Error updating live alert message for broadcaster_id={broadcaster_id}: {e}"
        )
        sentry_sdk.capture_exception(e)
        await send_message(
            f"Failed to update live alert message: {e}",
            BOT_ADMIN_CHANNEL,
        )
