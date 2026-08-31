import logging
import time
from collections import OrderedDict
from collections.abc import Callable, Coroutine
from typing import Any

import pendulum
from fastapi import APIRouter, HTTPException, Request, Response
from pydantic import BaseModel, ValidationError

from background import fire_and_forget
from config import settings
from constants import (
    HMAC_PREFIX,
    TWITCH_MESSAGE_ID,
    TWITCH_MESSAGE_SIGNATURE,
    TWITCH_MESSAGE_TIMESTAMP,
    TWITCH_MESSAGE_TYPE,
)
from errors import notify, report
from models import (
    ChannelAdBreakBeginEventSub,
    ChannelChatMessageEventSub,
    ChannelFollowEventSub,
    ChannelModerateEventSub,
    ChannelRaidEventSub,
    StreamOfflineEventSub,
    StreamOnlineEventSub,
)
from services import (
    get_hmac,
    get_hmac_message,
    parse_rfc3339,
    verify_message,
)
from services.twitch import events

logger = logging.getLogger(__name__)

twitch_router = APIRouter()


# An EventSub payload is a few kilobytes. The whole body has to be read to
# compute the signature over it, so it is counted as it arrives and abandoned
# past this.
_MAX_BODY_BYTES = 256 * 1024


# Twitch's own guidance for replay: a delivery whose timestamp is far from now
# is not one Twitch is sending, it is one somebody kept. Applied in both
# directions, since a clock ahead is as wrong as a clock behind.
_MESSAGE_WINDOW_SECONDS = 600

# Ids of deliveries that reached a handler. Only those, so a delivery Twitch
# retries because this end failed still gets through, while a retry of one that
# worked does not run it twice. Process-local is enough and not a compromise:
# the Discord gateway connection lives in this process, so a second replica
# would double every bot action.
_HANDLED_LIMIT = 1024
_handled: OrderedDict[str, float] = OrderedDict()


def _is_stale(sent: pendulum.DateTime) -> bool:
    return abs((pendulum.now("UTC") - sent).total_seconds()) > _MESSAGE_WINDOW_SECONDS


def _claim(message_id: str) -> bool:
    """Take this delivery, or say that something already has it.

    Claiming and checking are one step: there is an await between here and the
    dispatch, so two copies of one delivery could otherwise both get past a
    check that only looked.
    """
    cutoff = time.monotonic() - _MESSAGE_WINDOW_SECONDS
    while _handled and next(iter(_handled.values())) < cutoff:
        _handled.popitem(last=False)
    if message_id in _handled:
        return False
    _handled[message_id] = time.monotonic()
    while len(_handled) > _HANDLED_LIMIT:
        _handled.popitem(last=False)
    return True


def _release(message_id: str) -> None:
    """Give a claim back, so a delivery this end failed can still be retried."""
    _handled.pop(message_id, None)


async def _bounded_body(request: Request, endpoint: str) -> bytes:
    """The request body, refusing to hold more than ``_MAX_BODY_BYTES`` of it.

    Counts while streaming rather than trusting Content-Length, which a chunked
    request does not send at all. Caches onto the request the way Starlette's own
    ``body()`` does, so the later ``json()`` reads what was counted here.
    """
    cached: bytes | None = getattr(request, "_body", None)
    if cached is not None:
        return cached

    chunks: list[bytes] = []
    size = 0
    async for chunk in request.stream():
        size += len(chunk)
        if size > _MAX_BODY_BYTES:
            logger.warning("413: Payload too large on %s", endpoint)
            await notify(f"413: Oversized request on {endpoint}, refused part-read.")
            raise HTTPException(status_code=413)
        chunks.append(chunk)

    body = b"".join(chunks)
    request._body = body
    return body


async def validate_call(request: Request, endpoint: str) -> Response | None:
    headers = request.headers

    # Twitch signs every message type, so the signature is checked before the
    # request is read as anything. Branching first left the handshake echoing
    # attacker text and a revocation putting attacker text, mentions included,
    # into the admin channel -- neither of which needed a signature at all.
    try:
        body_str = (await _bounded_body(request, endpoint)).decode()
    except UnicodeDecodeError:
        # Twitch sends UTF-8 JSON, so this cannot be a signed request. Answering
        # it as one keeps it out of the 500 handler, which would report every
        # malformed byte an unauthenticated caller cared to send.
        logger.warning("403: Forbidden. Body is not UTF-8 on %s", endpoint)
        await notify(f"403: Forbidden request on {endpoint}. Body is not UTF-8.")
        raise HTTPException(status_code=403) from None

    message = get_hmac_message(
        headers.get(TWITCH_MESSAGE_ID, ""),
        headers.get(TWITCH_MESSAGE_TIMESTAMP, ""),
        body_str,
    )
    secret_hmac = HMAC_PREFIX + get_hmac(settings.twitch_webhook_secret, message)

    if not verify_message(secret_hmac, headers.get(TWITCH_MESSAGE_SIGNATURE, "")):
        logger.warning("403: Forbidden. Signature does not match.")
        await notify(f"403: Forbidden request on {endpoint}. Signature does not match.")
        raise HTTPException(status_code=403)

    # Only after the signature matched: an unauthenticated caller must not be
    # able to raise a notice by sending a stale timestamp.
    try:
        sent = parse_rfc3339(headers.get(TWITCH_MESSAGE_TIMESTAMP, ""))
    except ValueError:
        logger.warning("403: Unreadable message timestamp on %s", endpoint)
        await notify(f"403: Forbidden request on {endpoint}. Unreadable timestamp.")
        raise HTTPException(status_code=403) from None

    if _is_stale(sent):
        logger.warning("403: Stale delivery on %s, sent %s", endpoint, sent)
        await notify(
            f"403: Forbidden request on {endpoint}. Timestamp outside the"
            f" {_MESSAGE_WINDOW_SECONDS}s window, which is also what a wrong"
            f" clock on this host looks like.",
            key=f"stale-delivery:{endpoint}",
        )
        raise HTTPException(status_code=403)

    message_type = headers.get(TWITCH_MESSAGE_TYPE, "").lower()
    try:
        parsed = await request.json()
    except ValueError as e:
        # The signature already matched, so this is Twitch sending something
        # unparseable rather than an intruder. 4xx because the retry would carry
        # the same bytes and fail the same way.
        logger.warning("400: Body is not JSON on %s", endpoint)
        await notify(f"400: Bad request on {endpoint}. Body is not JSON.")
        raise HTTPException(status_code=400) from e

    if not isinstance(parsed, dict):
        logger.warning("400: Body is not a JSON object on %s", endpoint)
        await notify(f"400: Bad request on {endpoint}. Body is not a JSON object.")
        raise HTTPException(status_code=400)
    body: dict[str, Any] = parsed

    if message_type == "webhook_callback_verification":
        challenge = body.get("challenge")
        # text/plain explicitly: the value is echoed straight back, and nothing
        # that reads the reply should be free to interpret it as markup.
        return Response(
            challenge if isinstance(challenge, str) else "",
            status_code=200,
            media_type="text/plain",
        )

    if message_type == "revocation":
        raw = body.get("subscription")
        subscription: dict[str, Any] = raw if isinstance(raw, dict) else {}
        await notify(
            f"Revoked {subscription.get('type', 'unknown')} notifications for"
            f" condition: {subscription.get('condition', {})} because"
            f" {subscription.get('status', 'No reason provided')}"
        )
        return Response(status_code=204)


async def process_webhook[E: BaseModel](
    request: Request,
    endpoint: str,
    event_model: type[E],
    task_func: Callable[[E], Coroutine[Any, Any, None]],
) -> Response:
    """Validate, parse and dispatch one EventSub notification.

    The type parameter ties a route's model to its handler, so a pair that
    disagree stops type-checking; both were unannotated, and therefore Any.
    Which event the route serves is asserted by the model itself, whose
    subscription type is a Literal.
    """
    try:
        validation = await validate_call(request, endpoint)
        if validation:
            return validation

        # Twitch says a notification may arrive twice. 202 rather than an error:
        # this end does have it, and saying so is what stops the retries.
        message_id = request.headers.get(TWITCH_MESSAGE_ID, "")
        if not _claim(message_id):
            logger.info(
                "Duplicate %s on %s, not dispatched again", message_id, endpoint
            )
            return Response(status_code=202)

        try:
            body: dict[str, Any] = await request.json()
            event_sub = event_model.model_validate(body)
            fire_and_forget(task_func(event_sub), name=endpoint)
        except BaseException:
            _release(message_id)
            raise
        return Response(status_code=202)
    except HTTPException:
        raise
    except ValidationError as e:
        # 4xx rather than 5xx deliberately: Twitch retries a 5xx, and a payload
        # this model rejects will be rejected identically every time, so the
        # retries only cost deliveries against the subscription's failure budget.
        logger.warning(
            f"400: {event_model.__name__} rejected the payload on {endpoint}"
        )
        await notify(
            f"400: Bad request on {endpoint}."
            f" Payload did not match {event_model.__name__}."
        )
        raise HTTPException(status_code=400) from e
    except Exception as e:
        await report(e, f"500: Internal server error on {endpoint}")
        raise HTTPException(status_code=500) from e


@twitch_router.post("/webhook/twitch")
async def stream_online_webhook(request: Request) -> Response:
    return await process_webhook(
        request,
        "/webhook/twitch",
        StreamOnlineEventSub,
        events.stream_online,
    )


@twitch_router.post("/webhook/twitch/offline")
async def stream_offline_webhook(request: Request) -> Response:
    return await process_webhook(
        request,
        "/webhook/twitch/offline",
        StreamOfflineEventSub,
        events.stream_offline,
    )


@twitch_router.post("/webhook/twitch/chat")
async def channel_chat_message_webhook(request: Request) -> Response:
    return await process_webhook(
        request,
        "/webhook/twitch/chat",
        ChannelChatMessageEventSub,
        events.channel_chat_message,
    )


@twitch_router.post("/webhook/twitch/follow")
async def channel_follow_webhook(request: Request) -> Response:
    return await process_webhook(
        request,
        "/webhook/twitch/follow",
        ChannelFollowEventSub,
        events.channel_follow,
    )


@twitch_router.post("/webhook/twitch/adbreak")
async def channel_ad_break_begin_webhook(request: Request) -> Response:
    return await process_webhook(
        request,
        "/webhook/twitch/adbreak",
        ChannelAdBreakBeginEventSub,
        events.channel_ad_break_begin,
    )


@twitch_router.post("/webhook/twitch/raid")
async def channel_raid_webhook(request: Request) -> Response:
    return await process_webhook(
        request,
        "/webhook/twitch/raid",
        ChannelRaidEventSub,
        events.channel_raid,
    )


@twitch_router.post("/webhook/twitch/moderate")
async def channel_moderate_webhook(request: Request) -> Response:
    return await process_webhook(
        request,
        "/webhook/twitch/moderate",
        ChannelModerateEventSub,
        events.channel_moderate,
    )
