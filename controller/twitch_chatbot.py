import asyncio
import logging
import os
from typing import Any, Awaitable, Callable

import sentry_sdk
from dotenv import load_dotenv
from fastapi import APIRouter, HTTPException, Request, Response

from constants import (
    BOT_ADMIN_CHANNEL,
    HMAC_PREFIX,
    TWITCH_MESSAGE_ID,
    TWITCH_MESSAGE_SIGNATURE,
    TWITCH_MESSAGE_TIMESTAMP,
    TWITCH_MESSAGE_TYPE,
)
from models import ChannelChatMessageEventSub
from services import (
    call_twitch,
    get_channel,
    get_hmac,
    get_hmac_message,
    get_user_by_username,
    send_message,
    verify_message,
)
from services.twitch_shoutout_queue import shoutout_queue
from services.twitch_token_manager import token_manager

load_dotenv()

TWITCH_CLIENT_ID = os.getenv("TWITCH_CLIENT_ID")
TWITCH_WEBHOOK_SECRET = os.getenv("TWITCH_WEBHOOK_SECRET")
TWITCH_BOT_USER_ID = os.getenv("TWITCH_BOT_USER_ID")

twitch_chatbot_router = APIRouter()

logger = logging.getLogger(__name__)


@sentry_sdk.trace()
async def refresh_access_token() -> bool:
    return await token_manager.refresh_access_token()


@sentry_sdk.trace()
async def check_mod(event_sub: ChannelChatMessageEventSub) -> bool:
    has_mod = any(
        badge.set_id in {"moderator", "broadcaster"}
        for badge in event_sub.event.badges or []
    )
    broadcaster_id = event_sub.event.broadcaster_user_id
    if not has_mod:
        message = "Only moderators can use this command."
        await twitch_send_message(broadcaster_id, message)
        return False
    return True


@sentry_sdk.trace()
async def twitch_send_message(broadcaster_id: str, message: str) -> None:
    try:
        if not token_manager.access_token:
            refresh_success = await refresh_access_token()
            if not refresh_success:
                logger.warning("No access token available and failed to refresh")
                await send_message(
                    "No access token available and failed to refresh",
                    BOT_ADMIN_CHANNEL,
                )
                return

        url = "https://api.twitch.tv/helix/chat/messages"
        data = {
            "broadcaster_id": broadcaster_id,
            "sender_id": TWITCH_BOT_USER_ID,
            "message": message,
            "for_source_only": False,
        }
        response = await call_twitch("POST", url, data)
        if (
            response is None
            or response.status_code < 200
            or response.status_code >= 300
        ):
            logger.warning(
                f"Failed to send message: {response.status_code if response else 'No response'}"
            )
            await send_message(
                f"Failed to send message: {response.status_code if response else 'No response'} {response.text if response else ''}",
                BOT_ADMIN_CHANNEL,
            )
            return
    except Exception as e:
        logger.error(f"Error sending Twitch message: {e}")
        sentry_sdk.capture_exception(e)
        await send_message(f"Error sending Twitch message: {e}", BOT_ADMIN_CHANNEL)


@sentry_sdk.trace()
async def lurk(event_sub: ChannelChatMessageEventSub, _: str) -> None:
    broadcaster_id = event_sub.event.broadcaster_user_id
    chatter_name = event_sub.event.chatter_user_name
    message = f"{chatter_name} has gone to lurk. Eat, drink, sleep, water your pets, feed your plants. Make sure to take care of yourself and stay safe while you're away!"
    await twitch_send_message(broadcaster_id, message)


@sentry_sdk.trace()
async def discord(event_sub: ChannelChatMessageEventSub, _: str) -> None:
    broadcaster_id = event_sub.event.broadcaster_user_id
    message = "https://discord.gg/tkJyNJH2k7 Come join us and hang out! This is also where all my updates on streams and whatnot go"
    await twitch_send_message(broadcaster_id, message)


@sentry_sdk.trace()
async def kofi(event_sub: ChannelChatMessageEventSub, _: str) -> None:
    broadcaster_id = event_sub.event.broadcaster_user_id
    message = "Idk why you would want to donate, but here: https://ko-fi.com/valinmalach But always remember to take care of yourselves first!"
    await twitch_send_message(broadcaster_id, message)


@sentry_sdk.trace()
async def megathon(event_sub: ChannelChatMessageEventSub, _: str) -> None:
    broadcaster_id = event_sub.event.broadcaster_user_id
    message = "I'm holding a megathon until 31st October! Click here to see the goals: https://x.com/ValinMalach/status/1949087837296726406"
    await twitch_send_message(broadcaster_id, message)
    message = "Subs, bits, donos to my kofi and throne all contribute to the goals! https://ko-fi.com/valinmalach https://throne.com/valinmalach"
    await twitch_send_message(broadcaster_id, message)


@sentry_sdk.trace()
async def raid(event_sub: ChannelChatMessageEventSub, _: str) -> None:
    broadcaster_id = event_sub.event.broadcaster_user_id
    message = "valinmArrive valinmRaid Valin Raid valinmArrive valinmRaid Valin Raid valinmArrive valinmRaid Your Fallen Angel is here valinmHeart valinmHeart"
    await twitch_send_message(broadcaster_id, message)
    message = "DinoDance DinoDance Valin Raid DinoDance DinoDance Valin Raid DinoDance DinoDance Your Fallen Angel is here <3 <3"
    await twitch_send_message(broadcaster_id, message)


@sentry_sdk.trace()
async def socials(event_sub: ChannelChatMessageEventSub, _: str) -> None:
    broadcaster_id = event_sub.event.broadcaster_user_id
    message = "Twitter: https://twitter.com/ValinMalach Bluesky: https://bsky.app/profile/valinmalach.bsky.social"
    await twitch_send_message(broadcaster_id, message)


@sentry_sdk.trace()
async def throne(event_sub: ChannelChatMessageEventSub, _: str) -> None:
    broadcaster_id = event_sub.event.broadcaster_user_id
    message = "There's really only one thing on it for now lol... https://throne.com/valinmalach If I do add more, they will all be for stream!"
    await twitch_send_message(broadcaster_id, message)


@sentry_sdk.trace()
async def unlurk(event_sub: ChannelChatMessageEventSub, _: str) -> None:
    broadcaster_id = event_sub.event.broadcaster_user_id
    chatter_name = event_sub.event.chatter_user_name
    message = f"{chatter_name} has returned from their lurk. Welcome back! Hope you had a good break and are ready to hang out again!"
    await twitch_send_message(broadcaster_id, message)


@sentry_sdk.trace()
async def hug(event_sub: ChannelChatMessageEventSub, args: str) -> None:
    target = args.split(" ", 1)[0] if args else ""
    broadcaster_id = event_sub.event.broadcaster_user_id
    chatter_name = event_sub.event.chatter_user_name
    if not target:
        message = f"{chatter_name} gives everyone a big warm hug. How sweet! <3"
        await twitch_send_message(broadcaster_id, message)
        return
    message = f"{chatter_name} gives {target} a big warm hug. How sweet! <3"
    await twitch_send_message(broadcaster_id, message)


@sentry_sdk.trace()
async def shoutout(event_sub: ChannelChatMessageEventSub, args: str) -> None:
    if not await check_mod(event_sub):
        return

    broadcaster_id = event_sub.event.broadcaster_user_id
    target = (
        args.split(" ", 1)[0] if args else ""
    ) or event_sub.event.broadcaster_user_login
    if target.startswith("@"):
        target = target[1:]

    user = await get_user_by_username(target)
    target_channel = await get_channel(int(user.id)) if user else None
    if not target_channel:
        message = "User not found."
        await twitch_send_message(broadcaster_id, message)
        return

    if shoutout_queue.activated:
        shoutout_queue.add_to_queue(target)

    message = f"Go follow {target_channel.broadcaster_name} at https://www.twitch.tv/{target_channel.broadcaster_login}. They were last seen playing {target_channel.game_name}."
    await twitch_send_message(broadcaster_id, message)


@sentry_sdk.trace()
async def everything(event_sub: ChannelChatMessageEventSub, args: str) -> None:
    if not await check_mod(event_sub):
        return

    await discord(event_sub, args)
    await socials(event_sub, args)
    await kofi(event_sub, args)
    await throne(event_sub, args)
    await megathon(event_sub, args)
    await raid(event_sub, args)


@sentry_sdk.trace()
async def _twitch_chat_webhook_task(event_sub: ChannelChatMessageEventSub) -> None:
    user_command_dict: dict[
        str, Callable[[ChannelChatMessageEventSub, str], Awaitable[None]]
    ] = {
        "lurk": lurk,
        "discord": discord,
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


@twitch_chatbot_router.post("/webhook/twitch/chat")
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

        event_sub = ChannelChatMessageEventSub.model_validate(body)
        if event_sub.subscription.type != "channel.chat.message":
            logger.warning(
                f"400: Bad request. Invalid subscription type: {event_sub.subscription.type}"
            )
            await send_message(
                "400: Bad request on /webhook/twitch. Invalid subscription type.",
                BOT_ADMIN_CHANNEL,
            )
            raise HTTPException(status_code=400)

        asyncio.create_task(_twitch_chat_webhook_task(event_sub))

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
