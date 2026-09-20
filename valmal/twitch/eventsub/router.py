import logging
from collections.abc import Callable, Coroutine
from typing import Any, NoReturn, get_args

from fastapi import APIRouter, HTTPException, Request, Response
from pydantic import BaseModel, ValidationError
from starlette.datastructures import Headers

from valmal.core.background import fire_and_forget
from valmal.core.errors import notify, report
from valmal.core.settings import settings
from valmal.twitch.eventsub import events, replay
from valmal.twitch.eventsub.constants import (
    HMAC_PREFIX,
    TWITCH_MESSAGE_ID,
    TWITCH_MESSAGE_SIGNATURE,
    TWITCH_MESSAGE_TIMESTAMP,
    TWITCH_MESSAGE_TYPE,
)
from valmal.twitch.eventsub.signature import get_hmac, get_hmac_message, verify_message
from valmal.twitch.models.eventsub.channel_ad_break_begin import (
    ChannelAdBreakBeginEventSub,
)
from valmal.twitch.models.eventsub.channel_chat_message import (
    ChannelChatMessageEventSub,
)
from valmal.twitch.models.eventsub.channel_follow import ChannelFollowEventSub
from valmal.twitch.models.eventsub.channel_moderate import ChannelModerateEventSub
from valmal.twitch.models.eventsub.channel_points_custom_reward_redemption_add import (
    ChannelPointsCustomRewardRedemptionAddEventSub,
)
from valmal.twitch.models.eventsub.channel_raid import ChannelRaidEventSub
from valmal.twitch.models.eventsub.stream_offline import StreamOfflineEventSub
from valmal.twitch.models.eventsub.stream_online import StreamOnlineEventSub
from valmal.twitch.timestamps import parse_rfc3339

logger = logging.getLogger(__name__)

twitch_router = APIRouter()


# An EventSub payload is a few kilobytes. The whole body has to be read to
# compute the signature over it, so it is counted as it arrives and abandoned
# past this.
_MAX_BODY_BYTES = 256 * 1024


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


async def _authenticate(request: Request, endpoint: str) -> None:
    """Verify the request's HMAC signature, or raise 403.

    Twitch signs every message type, so the signature is checked before the
    request is read as anything. Branching first left the handshake echoing
    attacker text and a revocation putting attacker text, mentions included,
    into the admin channel -- neither of which needed a signature at all.
    """
    headers = request.headers
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


async def _answer_non_notification(
    message_type: str, body: dict[str, Any]
) -> Response | None:
    """The handshake and revocation arms, or None for an ordinary notification."""
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

    return None


async def _admit(headers: Headers, endpoint: str) -> None:
    """Check a notification's freshness, or raise 403.

    Freshness applies to notifications alone, and deliberately sits below the
    handshake. A wrong clock refusing events is recoverable; a wrong clock that
    also refuses webhook_callback_verification would block the resubscribe that
    repairs it, and six of the eight subscriptions cannot be recreated from
    this repo at all.
    """
    try:
        sent = parse_rfc3339(headers.get(TWITCH_MESSAGE_TIMESTAMP, ""))
    except ValueError:
        logger.warning("403: Unreadable message timestamp on %s", endpoint)
        await notify(f"403: Forbidden request on {endpoint}. Unreadable timestamp.")
        raise HTTPException(status_code=403) from None

    if replay.is_stale(sent):
        logger.warning("403: Stale delivery on %s, sent %s", endpoint, sent)
        await notify(
            f"403: Forbidden request on {endpoint}. Timestamp outside the"
            f" {replay.MESSAGE_WINDOW_SECONDS}s window, which is also what a wrong"
            f" clock on this host looks like. Twitch counts this as a failed"
            f" delivery, and enough of them revoke the subscription.",
            key=f"stale-delivery:{endpoint}",
        )
        raise HTTPException(status_code=403)


async def validate_call(request: Request, endpoint: str) -> dict[str, Any] | Response:
    """Either an answer already given, or the body of a notification to dispatch.

    Returning the body rather than None is what stops the caller parsing it a
    second time and re-asserting a shape it did not check: this is the function
    that proved it is an object, so this is the one that can say so.
    """
    await _authenticate(request, endpoint)
    headers = request.headers

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

    answer = await _answer_non_notification(message_type, body)
    if answer is not None:
        return answer

    await _admit(headers, endpoint)
    return body


async def _refuse_payload(
    e: ValidationError, event_model: type[BaseModel], endpoint: str
) -> NoReturn:
    """Log, notify and raise the 400 for a payload that failed to validate.

    4xx rather than 5xx because a payload this end cannot read is not a server
    fault, and a full exception report for one is noise. Not because it saves
    retries: Twitch documents no 4xx/5xx distinction, and revocation counts anything
    that is not a 2xx, so this spends the failure budget exactly as a 500 would.
    """
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


async def process_webhook[E: BaseModel](
    request: Request,
    endpoint: str,
    event_model: type[E],
    task_func: Callable[[E], Coroutine[Any, Any, None]],
) -> Response:
    """Validate, parse and dispatch one EventSub notification.

    The type parameter ties a route's model to its handler, so a pair that
    disagree stops type-checking. Which event the route serves is asserted by the
    model itself, whose subscription type is a Literal.
    """
    try:
        validated = await validate_call(request, endpoint)
        if isinstance(validated, Response):
            return validated

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

        await replay.report_forgotten(endpoint)
        if not replay.claim(message_id):
            logger.info(
                "Duplicate %s on %s, not dispatched again", message_id, endpoint
            )
            return Response(status_code=202)

        try:
            event_sub = event_model.model_validate(validated)
            fire_and_forget(task_func(event_sub), name=endpoint)
        except BaseException:
            replay.release(message_id)
            raise
        return Response(status_code=202)
    except HTTPException:
        raise
    except ValidationError as e:
        await _refuse_payload(e, event_model, endpoint)
    except Exception as e:
        await report(e, f"500: Internal server error on {endpoint}")
        raise HTTPException(status_code=500) from e


# Which webhook path serves each EventSub type. Derived from the routes below
# rather than listed beside them, so it cannot drift from them. Read by
# valmal/twitch/eventsub/migrate.py, which is handed this rather than importing
# it, since nothing under valmal/twitch/ imports a router.
WEBHOOK_PATHS: dict[str, str] = {}


def _subscription_type(event_model: type[BaseModel]) -> str:
    """The EventSub type a model serves, read off the Literal that asserts it.

    Raises at import, which is the point: a model that stopped declaring exactly
    one type would otherwise leave its route out of WEBHOOK_PATHS, and a type
    missing from that map is one the migration declines to touch.
    """
    subscription = event_model.model_fields["subscription"].annotation
    if not isinstance(subscription, type) or not issubclass(subscription, BaseModel):
        raise TypeError(f"{event_model.__name__}.subscription is not a model")

    declared = get_args(subscription.model_fields["type"].annotation)
    if len(declared) != 1 or not isinstance(declared[0], str):
        raise TypeError(f"{subscription.__name__}.type is not a single-value Literal")
    return declared[0]


def _route[E: BaseModel](
    path: str,
    event_model: type[E],
    task_func: Callable[[E], Coroutine[Any, Any, None]],
) -> None:
    """Register one webhook route, with the path written once.

    Generic for the reason process_webhook is: a route whose model and handler
    disagree has to fail type-checking rather than at the first delivery. The path
    is written once, where the decorator and the endpoint the notices name would
    otherwise repeat it and could drift apart.
    """

    async def webhook(request: Request) -> Response:
        return await process_webhook(request, path, event_model, task_func)

    # Refused rather than overwritten: a plain assignment would let two models
    # declaring the same type make the migration repoint every subscription of it at
    # whichever route registered last, leaving the other route out of the map and
    # never migrated.
    subscription_type = _subscription_type(event_model)
    if subscription_type in WEBHOOK_PATHS:
        raise ValueError(
            f"{subscription_type} is already routed to"
            f" {WEBHOOK_PATHS[subscription_type]}, so {path} would replace it"
        )
    WEBHOOK_PATHS[subscription_type] = path

    # Applied as a call rather than as a decorator: every route's function is
    # named "webhook", so each needs a name of its own for url_for and the
    # OpenAPI operation ids to stay distinct.
    twitch_router.post(path, name=path)(webhook)


_route("/webhook/twitch", StreamOnlineEventSub, events.stream_online)
_route("/webhook/twitch/offline", StreamOfflineEventSub, events.stream_offline)
_route("/webhook/twitch/chat", ChannelChatMessageEventSub, events.channel_chat_message)
_route("/webhook/twitch/follow", ChannelFollowEventSub, events.channel_follow)
_route(
    "/webhook/twitch/adbreak",
    ChannelAdBreakBeginEventSub,
    events.channel_ad_break_begin,
)
_route("/webhook/twitch/raid", ChannelRaidEventSub, events.channel_raid)
_route("/webhook/twitch/moderate", ChannelModerateEventSub, events.channel_moderate)
_route(
    "/webhook/twitch/redemption",
    ChannelPointsCustomRewardRedemptionAddEventSub,
    events.channel_points_custom_reward_redemption_add,
)
