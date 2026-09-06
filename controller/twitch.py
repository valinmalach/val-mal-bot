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
#
# Twice the freshness window, and that is provably enough rather than a guess:
# freshness is measured on the wall clock and this on the monotonic one, so a
# host clock behind Twitch's would leave a gap where a delivery is still fresh
# but no longer remembered - except that a clock more than one window out
# refuses everything as stale anyway, so the usable skew cannot exceed a window.
_HANDLED_TTL_SECONDS = _MESSAGE_WINDOW_SECONDS * 2

# A safety valve, not the eviction policy; the TTL is. The chat route claims one
# id per chat line, so the old 1024 was reached at under two lines a second and
# then forgot ids inside their own window, which is precisely a redelivery being
# handled twice. Reaching even this says the traffic broke the assumption, so it
# is counted and said out loud rather than silently dropping the oldest.
_HANDLED_LIMIT = 20_000
_handled: OrderedDict[str, float] = OrderedDict()
_forgotten_early = 0


def _is_stale(sent: pendulum.DateTime) -> bool:
    return abs((pendulum.now("UTC") - sent).total_seconds()) > _MESSAGE_WINDOW_SECONDS


def _claim(message_id: str) -> bool:
    """Take this delivery, or say that something already has it.

    Checking and taking are one step so that adding an await between them later
    cannot let two copies of one delivery both past a check that only looked.
    """
    global _forgotten_early
    cutoff = time.monotonic() - _HANDLED_TTL_SECONDS
    while _handled and next(iter(_handled.values())) < cutoff:
        _handled.popitem(last=False)
    if message_id in _handled:
        return False
    _handled[message_id] = time.monotonic()
    while len(_handled) > _HANDLED_LIMIT:
        _handled.popitem(last=False)
        _forgotten_early += 1
    return True


async def _report_forgotten(endpoint: str) -> None:
    """Say so when the cap bit, because then a redelivery can be handled twice."""
    global _forgotten_early
    if not _forgotten_early:
        return
    dropped, _forgotten_early = _forgotten_early, 0
    await notify(
        f"Replay cache full on {endpoint}: {dropped} delivery id(s) forgotten"
        f" inside their window, so a redelivery of one could run twice.",
        key="replay-cache-full",
    )


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

    # Freshness applies to notifications alone, and deliberately sits below the
    # handshake. A wrong clock refusing events is recoverable; a wrong clock that
    # also refuses webhook_callback_verification would block the resubscribe that
    # repairs it, and five of the seven subscriptions cannot be recreated from
    # this repo at all.
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
            f" clock on this host looks like. Twitch counts this as a failed"
            f" delivery, and enough of them revoke the subscription.",
            key=f"stale-delivery:{endpoint}",
        )
        raise HTTPException(status_code=403)


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
        if not message_id:
            # Twitch always sends one, and it is inside the HMAC. Refusing beats
            # claiming "": that collapses every id-less delivery into one and
            # throws the rest away saying nothing.
            logger.warning("400: Delivery carried no message id on %s", endpoint)
            await notify(f"400: Bad request on {endpoint}. No message id.")
            raise HTTPException(status_code=400)

        await _report_forgotten(endpoint)
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
        # 4xx rather than 5xx because a payload this end cannot read is not a
        # server fault, and a full exception report for one is noise. Not for the
        # reason first given here: Twitch documents no 4xx/5xx distinction at all,
        # and revocation counts anything that is not a 2xx, so this spends the
        # subscription's failure budget exactly as a 500 would.
        # The field, not just the model: what this most often catches is this
        # end's model falling behind Twitch's payload, and the name of the
        # field that moved is the whole diagnosis.
        where = "; ".join(
            ".".join(str(part) for part in err["loc"]) for err in e.errors()[:3]
        )
        logger.warning(
            "400: %s rejected the payload on %s at %s",
            event_model.__name__,
            endpoint,
            where,
        )
        await notify(
            f"400: Bad request on {endpoint}. Payload did not match"
            f" {event_model.__name__} at: {where}"
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
