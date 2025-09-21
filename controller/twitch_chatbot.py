import asyncio
import logging
import os
from typing import Any

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
from models import StreamChatEventSub
from services import (
    get_hmac,
    get_hmac_message,
    send_message,
    verify_message,
)

load_dotenv()

TWITCH_WEBHOOK_SECRET = os.getenv("TWITCH_WEBHOOK_SECRET")

twitch_chatbot_router = APIRouter()

logger = logging.getLogger(__name__)


@sentry_sdk.trace()
async def _twitch_chat_webhook_task(event_sub: StreamChatEventSub) -> None:
    pass


@twitch_chatbot_router.post("/webhook/twitch/chat")
async def twitch_webhook(request: Request) -> Response:
    logger.info("Webhook received: twitch_webhook start")
    try:
        headers = request.headers
        logger.info(f"Headers parsed: {dict(headers)}")
        body: dict[str, Any] = await request.json()
        logger.info(f"Body JSON parsed: {body}")

        if headers.get(TWITCH_MESSAGE_TYPE) == "webhook_callback_verification":
            challenge = body.get("challenge", "")
            logger.info(
                f"Responding to callback verification with challenge={challenge}"
            )
            return Response(challenge or "", status_code=200)

        if headers.get(TWITCH_MESSAGE_TYPE, "").lower() == "revocation":
            subscription: dict[str, Any] = body.get("subscription", {})
            logger.info(f"{subscription.get('type', 'unknown')} notifications revoked!")
            logger.info(f"reason: {subscription.get('status', 'No reason provided')}")
            condition = subscription.get("condition", {})
            logger.info(f"condition: {condition}")
            await send_message(
                f"Revoked {subscription.get('type', 'unknown')} notifications for condition: {condition} because {subscription.get('status', 'No reason provided')}",
                BOT_ADMIN_CHANNEL,
            )
            return Response(status_code=204)

        twitch_message_id = headers.get(TWITCH_MESSAGE_ID, "")
        twitch_message_timestamp = headers.get(TWITCH_MESSAGE_TIMESTAMP, "")
        body_str = (await request.body()).decode()
        logger.info("Request raw body retrieved")
        message = get_hmac_message(
            twitch_message_id, twitch_message_timestamp, body_str
        )
        logger.info("HMAC message constructed")
        secret_hmac = HMAC_PREFIX + get_hmac(TWITCH_WEBHOOK_SECRET, message)
        logger.info(f"Computed secret_hmac={secret_hmac}")

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
        logger.info("Signature verified")

        event_sub = StreamChatEventSub.model_validate(body)
        logger.info(f"Event subscription parsed: type={event_sub.subscription.type}")
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
