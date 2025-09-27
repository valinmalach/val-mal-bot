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
    discord,
    everything,
    get_hmac,
    get_hmac_message,
    hug,
    kofi,
    lurk,
    megathon,
    raid,
    send_message,
    shoutout,
    socials,
    throne,
    unlurk,
    verify_message,
)

load_dotenv()

TWITCH_CLIENT_ID = os.getenv("TWITCH_CLIENT_ID")
TWITCH_WEBHOOK_SECRET = os.getenv("TWITCH_WEBHOOK_SECRET")
TWITCH_BOT_USER_ID = os.getenv("TWITCH_BOT_USER_ID")

twitch_chatbot_router = APIRouter()

logger = logging.getLogger(__name__)

# Ad start message
# A 3 minute ad break is starting! Thank you for sticking with us through this break! valinmArrive Ads help support my content. Consider subscribing to remove ads and support the stream!

# Ad end message
# The ad break is finishing now! valinmArrive

# Raid start
# Have a great rest of your day! valinmHeart Don't forget to stay hydrated and take care of yourself! valinmHeart

# Post-raid message
# We just raided ${raidtargetname}. In case you got left behind, you can find them here: https://www.twitch.tv/${raidtargetlogin}

# On follow message
# Thank you for following! valinmHeart Your support means a lot to me! valinmHeart


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
async def twitch_chat_webhook(request: Request) -> Response:
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
