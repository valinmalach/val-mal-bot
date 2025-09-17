import asyncio
import itertools
import logging
import os
from datetime import datetime
from typing import List, Optional

import discord
import requests
import sentry_sdk
from discord.ui import View
from dotenv import load_dotenv

from constants import BOT_ADMIN_CHANNEL, LIVE_ALERTS_ROLE, STREAM_ALERTS_CHANNEL
from init import xata_client
from models import (
    AuthResponse,
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
from services import edit_embed, get_age, parse_rfc3339, send_message

load_dotenv()

TWITCH_CLIENT_ID = os.getenv("TWITCH_CLIENT_ID")
TWITCH_CLIENT_SECRET = os.getenv("TWITCH_CLIENT_SECRET")
TWITCH_WEBHOOK_SECRET = os.getenv("TWITCH_WEBHOOK_SECRET")
access_token = ""

logger = logging.getLogger(__name__)


@sentry_sdk.trace()
async def refresh_access_token() -> bool:
    logger.info("Refreshing Twitch OAuth token by requesting new access token")
    global access_token
    url = f"https://id.twitch.tv/oauth2/token?client_id={TWITCH_CLIENT_ID}&client_secret={TWITCH_CLIENT_SECRET}&grant_type=client_credentials"
    logger.info("Posting to token endpoint: %s", url)

    response = requests.post(url)
    logger.info(
        "Token endpoint response status=%s, body=%s",
        response.status_code,
        response.text,
    )
    if response.status_code != 200:
        logger.error("Token refresh failed with status=%s", response.status_code)
        await send_message(
            f"Failed to refresh access token: {response.status_code} {response.text}",
            BOT_ADMIN_CHANNEL,
        )
        return False
    auth_response = AuthResponse.model_validate(response.json())
    logger.info("Token refresh returned token_type=%s", auth_response.token_type)
    if auth_response.token_type == "bearer":
        access_token = auth_response.access_token
        logger.info("Access token updated successfully")
        return True
    else:
        logger.error("Unexpected token type received: %s", auth_response.token_type)
        await send_message(
            f"Unexpected token type: {auth_response.token_type}", BOT_ADMIN_CHANNEL
        )
        return False


@sentry_sdk.trace()
async def get_subscriptions() -> Optional[List[SubscriptionInfo]]:
    logger.info("Retrieving Twitch EventSub subscriptions list")
    global access_token
    if not access_token:
        refresh_success = await refresh_access_token()
        if not refresh_success:
            return

    all_subscriptions: List[SubscriptionInfo] = []
    cursor: Optional[str] = None

    url = "https://api.twitch.tv/helix/eventsub/subscriptions"
    headers = {
        "Client-ID": TWITCH_CLIENT_ID,
        "Authorization": f"Bearer {access_token}",
    }
    logger.info("Subscriptions endpoint URL: %s", url)
    while True:
        logger.info("Fetching subscriptions page with cursor=%s", cursor)
        params = {"status": "enabled"}
        if cursor:
            params["after"] = cursor

        response = requests.get(
            url,
            headers=headers,
            params=params,
        )
        logger.info("Subscriptions API response status=%s", response.status_code)
        if response.status_code == 401:
            logger.warning(
                "Unauthorized when fetching subscriptions, refreshing token..."
            )
            if await refresh_access_token():
                headers["Authorization"] = f"Bearer {access_token}"
                response = requests.get(
                    url,
                    headers=headers,
                    params=params,
                )
            else:
                return

        if response.status_code != 200:
            logger.error("Error fetching subscriptions: %s", response.status_code)
            await send_message(
                f"Failed to fetch subscriptions: {response.status_code} {response.text}",
                BOT_ADMIN_CHANNEL,
            )
            return

        subscription_info_response = SubscriptionInfoResponse.model_validate(
            response.json()
        )
        data = subscription_info_response.data
        logger.info("Received %d subscriptions", len(data))
        if not data:
            break

        all_subscriptions.extend(data)
        cursor = subscription_info_response.pagination.cursor
        logger.info("Next pagination cursor=%s", cursor)
        if not cursor:
            break

    return all_subscriptions


@sentry_sdk.trace()
async def get_user(id: str) -> Optional[UserInfo]:
    logger.info("Retrieving Twitch user info for id=%s", id)
    global access_token
    if not access_token:
        refresh_success = await refresh_access_token()
        if not refresh_success:
            return

    url = f"https://api.twitch.tv/helix/users?id={id}"
    logger.info("Requesting user endpoint: %s", url)
    headers = {
        "Client-ID": TWITCH_CLIENT_ID,
        "Authorization": f"Bearer {access_token}",
    }
    response = requests.get(url, headers=headers)
    logger.info("User endpoint response status=%s", response.status_code)
    if response.status_code == 401:
        logger.warning("Unauthorized fetching user, refreshing token...")
        if await refresh_access_token():
            headers["Authorization"] = f"Bearer {access_token}"
            response = requests.get(url, headers=headers)
        else:
            return
    if response.status_code != 200:
        logger.error("Failed to fetch user info: %s", response.status_code)
        await send_message(
            f"Failed to fetch user info: {response.status_code} {response.text}",
            BOT_ADMIN_CHANNEL,
        )
        return
    user_info_response = UserInfoResponse.model_validate(response.json())
    logger.info("Parsed user_info count=%d", len(user_info_response.data))
    return user_info_response.data[0] if user_info_response.data else None


@sentry_sdk.trace()
async def get_user_by_username(username: str) -> Optional[UserInfo]:
    logger.info("Retrieving Twitch user info for username=%s", username)
    global access_token
    if not access_token:
        refresh_success = await refresh_access_token()
        if not refresh_success:
            return

    url = f"https://api.twitch.tv/helix/users?login={username}"
    logger.info("Requesting user endpoint: %s", url)
    headers = {
        "Client-ID": TWITCH_CLIENT_ID,
        "Authorization": f"Bearer {access_token}",
    }
    response = requests.get(url, headers=headers)
    logger.info("User endpoint response status=%s", response.status_code)
    if response.status_code == 401:
        logger.warning("Unauthorized fetching user, refreshing token...")
        if await refresh_access_token():
            headers["Authorization"] = f"Bearer {access_token}"
            response = requests.get(url, headers=headers)
        else:
            return
    if response.status_code != 200:
        logger.error("Failed to fetch user info: %s", response.status_code)
        await send_message(
            f"Failed to fetch user info: {response.status_code} {response.text}",
            BOT_ADMIN_CHANNEL,
        )
        return
    user_info_response = UserInfoResponse.model_validate(response.json())
    logger.info("Parsed user_info count=%d", len(user_info_response.data))
    return user_info_response.data[0] if user_info_response.data else None


@sentry_sdk.trace()
async def subscribe_to_user(username: str) -> bool:
    logger.info("Retrieving Twitch user info for username=%s", username)
    global access_token
    if not access_token:
        refresh_success = await refresh_access_token()
        if not refresh_success:
            return False

    logger.info("Getting user: %s", username)
    user = await get_user_by_username(username)
    if not user:
        logger.error("User not found: %s", username)
        return False
    logger.info("Subscribing to user: %s (id=%s)", user.display_name, user.id)

    user_id = user.id

    url = "https://api.twitch.tv/helix/eventsub/subscriptions"
    headers = {
        "Client-ID": TWITCH_CLIENT_ID,
        "Authorization": f"Bearer {access_token}",
    }

    logger.info("Subscribing to stream.online for user=%s", user.display_name)
    body = {
        "type": "stream.online",
        "version": "1",
        "condition": {
            "broadcaster_user_id": user_id
        },
        "transport": {
            "method": "webhook",
            "callback": "https://valin.loclx.io/webhook/twitch",
            "secret": TWITCH_WEBHOOK_SECRET
        }
    }
    response = requests.post(url, headers=headers, json=body)
    if response.status_code == 401:
        logger.warning("Unauthorized subscribing to online event, refreshing token...")
        if not await refresh_access_token():
            return False
        headers["Authorization"] = f"Bearer {access_token}"
        response = requests.post(url, headers=headers, json=body)
    if response.status_code != 200:
        logger.error("Failed to subscribe to online event: %s", response.status_code)
        await send_message(
            f"Failed to subscribe to online event: {response.status_code} {response.text}",
            BOT_ADMIN_CHANNEL,
        )
        return False
    logger.info("Subscribed to stream.online for user: %s", user.display_name)

    logger.info("Subscribing to stream.offline for user=%s", user.display_name)
    body = {
        "type": "stream.offline",
        "version": "1",
        "condition": {
            "broadcaster_user_id": user_id
        },
        "transport": {
            "method": "webhook",
            "callback": "https://valin.loclx.io/webhook/twitch/offline",
            "secret": TWITCH_WEBHOOK_SECRET
        }
    }
    response = requests.post(url, headers=headers, json=body)
    if response.status_code == 401:
        logger.warning("Unauthorized subscribing to offline event, refreshing token...")
        if not await refresh_access_token():
            return False
        headers["Authorization"] = f"Bearer {access_token}"
        response = requests.post(url, headers=headers, json=body)
    if response.status_code != 200:
        logger.error("Failed to subscribe to offline event: %s", response.status_code)
        await send_message(
            f"Failed to subscribe to offline event: {response.status_code} {response.text}",
            BOT_ADMIN_CHANNEL,
        )
        return False
    logger.info("Subscribed to stream.offline for user: %s", user.display_name)

    return True


@sentry_sdk.trace()
async def get_users(ids: List[str]) -> Optional[List[UserInfo]]:
    logger.info("Retrieving Twitch user infos in batches for ids=%s", ids)
    global access_token
    if not access_token:
        refresh_success = await refresh_access_token()
        if not refresh_success:
            return

    # Split ids into batches of 100
    batches_iterator = itertools.batched(ids, 100)
    batches_list = [list(batch) for batch in batches_iterator]
    logger.info("Split into %d batches", len(batches_list))

    users: List[UserInfo] = []

    for batch in batches_list:
        logger.info("Requesting batch: %s", batch)
        if not batch:
            continue
        url = f"https://api.twitch.tv/helix/users?id={'&id='.join(batch)}"
        headers = {
            "Client-ID": TWITCH_CLIENT_ID,
            "Authorization": f"Bearer {access_token}",
        }
        response = requests.get(url, headers=headers)
        logger.info("Batch users API response status=%s", response.status_code)
        if response.status_code == 401:
            logger.warning("Unauthorized on batch users, refreshing token...")
            if await refresh_access_token():
                headers["Authorization"] = f"Bearer {access_token}"
                response = requests.get(url, headers=headers)
            else:
                return
        if response.status_code != 200:
            logger.error("Failed batch fetch of user infos: %s", response.status_code)
            await send_message(
                f"Failed to fetch users infos: {response.status_code} {response.text}",
                BOT_ADMIN_CHANNEL,
            )
            return
        user_info_response = UserInfoResponse.model_validate(response.json())
        logger.info("Batch parsed %d users", len(user_info_response.data))
        users.extend(user_info_response.data)

    return users


@sentry_sdk.trace()
async def get_channel(id: str) -> Optional[ChannelInfo]:
    logger.info("Retrieving Twitch channel info for broadcaster_id=%s", id)
    global access_token
    if not access_token:
        refresh_success = await refresh_access_token()
        if not refresh_success:
            return

    url = f"https://api.twitch.tv/helix/channels?broadcaster_id={id}"
    logger.info("Requesting channel endpoint: %s", url)
    headers = {
        "Client-ID": TWITCH_CLIENT_ID,
        "Authorization": f"Bearer {access_token}",
    }
    response = requests.get(url, headers=headers)
    logger.info("Channel endpoint response status=%s", response.status_code)
    if response.status_code == 401:
        logger.warning("Unauthorized fetching channel, refreshing token...")
        if await refresh_access_token():
            headers["Authorization"] = f"Bearer {access_token}"
            response = requests.get(url, headers=headers)
        else:
            return
    if response.status_code != 200:
        logger.error("Failed to fetch channel info: %s", response.status_code)
        await send_message(
            f"Failed to fetch channel info: {response.status_code} {response.text}",
            BOT_ADMIN_CHANNEL,
        )
        return
    channel_info_response = ChannelInfoResponse.model_validate(response.json())
    logger.info("Parsed channel info: %s entries", len(channel_info_response.data))
    return channel_info_response.data[0] if channel_info_response.data else None


@sentry_sdk.trace()
async def get_stream_info(broadcaster_id: str) -> Optional[StreamInfo]:
    logger.info("Retrieving Twitch stream info for broadcaster_id=%s", broadcaster_id)
    global access_token
    if not access_token:
        refresh_success = await refresh_access_token()
        if not refresh_success:
            return

    url = f"https://api.twitch.tv/helix/streams?user_id={broadcaster_id}"
    logger.info("Requesting stream info endpoint: %s", url)
    headers = {
        "Client-ID": TWITCH_CLIENT_ID,
        "Authorization": f"Bearer {access_token}",
    }
    response = requests.get(url, headers=headers)
    logger.info("Stream info API response status=%s", response.status_code)
    if response.status_code == 401:
        logger.warning("Unauthorized fetching stream info, refreshing token...")
        if await refresh_access_token():
            headers["Authorization"] = f"Bearer {access_token}"
            response = requests.get(url, headers=headers)
        else:
            return
    if response.status_code != 200:
        logger.error("Failed to fetch stream info: %s", response.status_code)
        await send_message(
            f"Failed to fetch stream info: {response.status_code} {response.text}",
            BOT_ADMIN_CHANNEL,
        )
        return
    stream_info_response = StreamInfoResponse.model_validate(response.json())
    logger.info("Parsed stream info items: %d", len(stream_info_response.data))
    return stream_info_response.data[0] if stream_info_response.data else None


@sentry_sdk.trace()
async def get_stream_vod(user_id: str, stream_id: str) -> Optional[VideoInfo]:
    logger.info(
        "Retrieving VOD list for user_id=%s to find stream_id=%s", user_id, stream_id
    )
    global access_token
    if not access_token:
        refresh_success = await refresh_access_token()
        if not refresh_success:
            return

    url = f"https://api.twitch.tv/helix/videos?user_id={user_id}&type=archive"
    logger.info("Requesting VOD endpoint: %s", url)
    headers = {
        "Client-ID": TWITCH_CLIENT_ID,
        "Authorization": f"Bearer {access_token}",
    }
    response = requests.get(url, headers=headers)
    logger.info("VOD API response status=%s", response.status_code)
    if response.status_code == 401:
        logger.warning("Unauthorized fetching VOD, refreshing token...")
        if await refresh_access_token():
            headers["Authorization"] = f"Bearer {access_token}"
            response = requests.get(url, headers=headers)
        else:
            return
    if response.status_code != 200:
        logger.error("Failed to fetch VOD info: %s", response.status_code)
        await send_message(
            f"Failed to fetch stream info: {response.status_code} {response.text}",
            BOT_ADMIN_CHANNEL,
        )
        return
    video_info_response = VideoInfoResponse.model_validate(response.json())
    logger.info("Parsed %d video entries", len(video_info_response.data))
    return next(
        (video for video in video_info_response.data if video.stream_id == stream_id),
        None,
    )


@sentry_sdk.trace()
async def update_alert(
    broadcaster_id: str,
    channel_id: int,
    message_id: int,
    stream_id: str,
    stream_started_at: str,
) -> None:
    logger.info(
        "Updating live alert embed in Discord for broadcaster_id=%s, message_id=%s",
        broadcaster_id,
        message_id,
    )
    try:
        await asyncio.sleep(60)
        # retry on connection errors
        while True:
            try:
                logger.info(
                    "Fetching live_alert record for broadcaster_id=%s", broadcaster_id
                )
                alert = xata_client.records().get("live_alerts", broadcaster_id)
                break
            except Exception:
                logger.warning("Error fetching live_alert; retrying after sleep")
                await asyncio.sleep(1)
        stream_info = await get_stream_info(broadcaster_id)
        logger.info("Fetched stream_info for update: %s", stream_info)
        user_info = await get_user(broadcaster_id)
        logger.info("Fetched user_info for update: %s", user_info)
        while alert.is_success() and stream_info is not None:
            logger.info(
                "Live alert record found. Checking if stream_id changed (current=%s, original=%s)",
                stream_info.id,
                stream_id,
            )
            content = (
                f"<@&{LIVE_ALERTS_ROLE}>"
                if channel_id == STREAM_ALERTS_CHANNEL
                else None
            )
            url = f"https://www.twitch.tv/{stream_info.user_login}"
            started_at = parse_rfc3339(stream_started_at)
            started_at_timestamp = f"<t:{int(started_at.timestamp())}:f>"
            now = datetime.now()
            age = get_age(started_at, limit_units=2)
            if alert.get("stream_id", "") != stream_id or stream_info.id != stream_id:
                logger.info(
                    "Stream ID changed; building offline VOD embed for previous stream_id=%s",
                    stream_id,
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

                logger.info(
                    "Building offline embed for previous stream_id=%s", stream_id
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
                # retry on Discord Server Error
                while True:
                    try:
                        await edit_embed(message_id, embed, channel_id, content=content)
                        logger.info(
                            "Successfully edited embed for offline event message_id=%s",
                            message_id,
                        )
                        break
                    except Exception:
                        logger.warning(
                            "Error encountered while editing offline embed; retrying..."
                        )
                        await asyncio.sleep(1)
                return
            logger.info(
                "Building live update embed for ongoing stream_id=%s", stream_id
            )
            # Cache-bust thumbnail URL to force Discord to refresh the image
            raw_thumb_url = stream_info.thumbnail_url.replace(
                "{width}x{height}", "400x225"
            )
            cache_busted_thumb_url = (
                f"{raw_thumb_url}?cb={int(datetime.now().timestamp())}"
            )
            logger.info("Using cache-busted thumbnail URL: %s", cache_busted_thumb_url)
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
                "Editing embed message for live update message_id=%s", message_id
            )
            # retry on Discord Server Error
            while True:
                try:
                    await edit_embed(
                        message_id, embed, channel_id, view, content=content
                    )
                    logger.info(
                        "Successfully edited embed for live update message_id=%s",
                        message_id,
                    )
                    break
                except Exception:
                    logger.warning("Error on live embed edit; retrying after sleep")
                    await asyncio.sleep(1)
            logger.info("Sleeping for 60 seconds before next update cycle")
            await asyncio.sleep(60)
            # retry on connection errors
            while True:
                logger.info(
                    "Fetching updated live_alert record after sleep for broadcaster_id=%s",
                    broadcaster_id,
                )
                try:
                    alert = xata_client.records().get("live_alerts", broadcaster_id)
                    break
                except Exception:
                    logger.warning(
                        "Error fetching alert post-sleep; retrying after sleep"
                    )
                    await asyncio.sleep(1)
            stream_info = await get_stream_info(broadcaster_id)
            logger.info("Fetched updated stream_info for next cycle: %s", stream_info)

    except Exception as e:
        logger.error(
            "Error updating live alert message for broadcaster_id=%s: %s",
            broadcaster_id,
            e,
        )
        sentry_sdk.capture_exception(e)
        await send_message(
            f"Failed to update live alert message: {e}",
            BOT_ADMIN_CHANNEL,
        )
