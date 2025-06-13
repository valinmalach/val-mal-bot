import asyncio
import itertools
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
access_token = ""


@sentry_sdk.trace()
async def refresh_access_token() -> bool:
    global access_token
    url = f"https://id.twitch.tv/oauth2/token?client_id={TWITCH_CLIENT_ID}&client_secret={TWITCH_CLIENT_SECRET}&grant_type=client_credentials"

    response = requests.post(url)
    if response.status_code != 200:
        await send_message(
            f"Failed to refresh access token: {response.status_code} {response.text}",
            BOT_ADMIN_CHANNEL,
        )
        return False
    auth_response = AuthResponse.model_validate(response.json())
    if auth_response.token_type == "bearer":
        access_token = auth_response.access_token
        return True
    else:
        await send_message(
            f"Unexpected token type: {auth_response.token_type}", BOT_ADMIN_CHANNEL
        )
        return False


@sentry_sdk.trace()
async def get_subscriptions() -> Optional[List[SubscriptionInfo]]:
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
    while True:
        params = {"status": "enabled"}
        if cursor:
            params["after"] = cursor

        response = requests.get(
            url,
            headers=headers,
            params=params,
        )
        if response.status_code == 401:
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
            await send_message(
                f"Failed to fetch subscriptions: {response.status_code} {response.text}",
                BOT_ADMIN_CHANNEL,
            )
            return

        subscription_info_response = SubscriptionInfoResponse.model_validate(
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
async def get_user(id: str) -> Optional[UserInfo]:
    global access_token
    if not access_token:
        refresh_success = await refresh_access_token()
        if not refresh_success:
            return

    url = f"https://api.twitch.tv/helix/users?id={id}"
    headers = {
        "Client-ID": TWITCH_CLIENT_ID,
        "Authorization": f"Bearer {access_token}",
    }
    response = requests.get(url, headers=headers)
    if response.status_code == 401:
        if await refresh_access_token():
            headers["Authorization"] = f"Bearer {access_token}"
            response = requests.get(url, headers=headers)
        else:
            return
    if response.status_code != 200:
        await send_message(
            f"Failed to fetch user info: {response.status_code} {response.text}",
            BOT_ADMIN_CHANNEL,
        )
        return
    user_info_response = UserInfoResponse.model_validate(response.json())
    return user_info_response.data[0] if user_info_response.data else None


@sentry_sdk.trace()
async def get_users(ids: List[str]) -> Optional[List[UserInfo]]:
    global access_token
    if not access_token:
        refresh_success = await refresh_access_token()
        if not refresh_success:
            return

    # Split ids into batches of 100
    batches_iterator = itertools.batched(ids, 100)
    batches_list = [list(batch) for batch in batches_iterator]

    users: List[UserInfo] = []

    for batch in batches_list:
        if not batch:
            continue
        url = f"https://api.twitch.tv/helix/users?id={'&id='.join(batch)}"
        headers = {
            "Client-ID": TWITCH_CLIENT_ID,
            "Authorization": f"Bearer {access_token}",
        }
        response = requests.get(url, headers=headers)
        if response.status_code == 401:
            if await refresh_access_token():
                headers["Authorization"] = f"Bearer {access_token}"
                response = requests.get(url, headers=headers)
            else:
                return
        if response.status_code != 200:
            await send_message(
                f"Failed to fetch users infos: {response.status_code} {response.text}",
                BOT_ADMIN_CHANNEL,
            )
            return
        user_info_response = UserInfoResponse.model_validate(response.json())
        users.extend(user_info_response.data)

    return users


@sentry_sdk.trace()
async def get_channel(id: str) -> Optional[ChannelInfo]:
    global access_token
    if not access_token:
        refresh_success = await refresh_access_token()
        if not refresh_success:
            return

    url = f"https://api.twitch.tv/helix/channels?broadcaster_id={id}"
    headers = {
        "Client-ID": TWITCH_CLIENT_ID,
        "Authorization": f"Bearer {access_token}",
    }
    response = requests.get(url, headers=headers)
    if response.status_code == 401:
        if await refresh_access_token():
            headers["Authorization"] = f"Bearer {access_token}"
            response = requests.get(url, headers=headers)
        else:
            return
    if response.status_code != 200:
        await send_message(
            f"Failed to fetch channel info: {response.status_code} {response.text}",
            BOT_ADMIN_CHANNEL,
        )
        return
    channel_info_response = ChannelInfoResponse.model_validate(response.json())
    return channel_info_response.data[0] if channel_info_response.data else None


@sentry_sdk.trace()
async def get_stream_info(broadcaster_id: str) -> Optional[StreamInfo]:
    global access_token
    if not access_token:
        refresh_success = await refresh_access_token()
        if not refresh_success:
            return

    url = f"https://api.twitch.tv/helix/streams?user_id={broadcaster_id}"
    headers = {
        "Client-ID": TWITCH_CLIENT_ID,
        "Authorization": f"Bearer {access_token}",
    }
    response = requests.get(url, headers=headers)
    if response.status_code == 401:
        if await refresh_access_token():
            headers["Authorization"] = f"Bearer {access_token}"
            response = requests.get(url, headers=headers)
        else:
            return
    if response.status_code != 200:
        await send_message(
            f"Failed to fetch stream info: {response.status_code} {response.text}",
            BOT_ADMIN_CHANNEL,
        )
        return
    stream_info_response = StreamInfoResponse.model_validate(response.json())
    return stream_info_response.data[0] if stream_info_response.data else None


@sentry_sdk.trace()
async def get_stream_vod(user_id: str, stream_id: str) -> Optional[VideoInfo]:
    global access_token
    if not access_token:
        refresh_success = await refresh_access_token()
        if not refresh_success:
            return

    url = f"https://api.twitch.tv/helix/videos?user_id={user_id}&type=archive"
    headers = {
        "Client-ID": TWITCH_CLIENT_ID,
        "Authorization": f"Bearer {access_token}",
    }
    response = requests.get(url, headers=headers)
    if response.status_code == 401:
        if await refresh_access_token():
            headers["Authorization"] = f"Bearer {access_token}"
            response = requests.get(url, headers=headers)
        else:
            return
    if response.status_code != 200:
        await send_message(
            f"Failed to fetch stream info: {response.status_code} {response.text}",
            BOT_ADMIN_CHANNEL,
        )
        return
    video_info_response = VideoInfoResponse.model_validate(response.json())
    return next(
        (video for video in video_info_response.data if video.stream_id == stream_id),
        None,
    )


@sentry_sdk.trace()
async def update_alert(broadcaster_id: str, channel_id: int, message_id: int) -> None:
    try:
        # retry on connection errors
        while True:
            try:
                alert = xata_client.records().get("live_alerts", broadcaster_id)
                break
            except ConnectionError:
                await asyncio.sleep(60)
        stream_info = await get_stream_info(broadcaster_id)
        user_info = await get_user(broadcaster_id)
        while alert.is_success() and stream_info is not None:
            content = (
                f"<@&{LIVE_ALERTS_ROLE}>"
                if channel_id == STREAM_ALERTS_CHANNEL
                else None
            )
            url = f"https://www.twitch.tv/{stream_info.user_login}"
            started_at = parse_rfc3339(stream_info.started_at)
            started_at_timestamp = f"<t:{int(started_at.timestamp())}:f>"
            now = datetime.now()
            age = get_age(started_at, limit_units=2)
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
                .set_image(
                    url=stream_info.thumbnail_url.replace("{width}x{height}", "400x225")
                )
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
            await edit_embed(message_id, embed, channel_id, view, content=content)
            await asyncio.sleep(300)
            # retry on connection errors
            while True:
                try:
                    alert = xata_client.records().get("live_alerts", broadcaster_id)
                    break
                except ConnectionError:
                    await asyncio.sleep(60)
            stream_info = await get_stream_info(broadcaster_id)

    except Exception as e:
        sentry_sdk.capture_exception(e)
        await send_message(
            f"Failed to update live alert message: {e}",
            BOT_ADMIN_CHANNEL,
        )
