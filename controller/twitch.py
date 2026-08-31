import asyncio
import logging
import time
from typing import Any

from fastapi import APIRouter, HTTPException, Request, Response
from fastapi.responses import RedirectResponse

from background import fire_and_forget
from config import settings
from constants import (
    HMAC_PREFIX,
    TWITCH_MESSAGE_ID,
    TWITCH_MESSAGE_SIGNATURE,
    TWITCH_MESSAGE_TIMESTAMP,
    TWITCH_MESSAGE_TYPE,
    TokenType,
)
from errors import notify, report
from models import (
    ChannelAdBreakBeginEventSub,
    ChannelChatMessageEventSub,
    ChannelFollowEventSub,
    ChannelModerateEventSub,
    ChannelRaidEventSub,
    RefreshResponse,
    Stream,
    StreamOfflineEventSub,
    StreamOnlineEventSub,
    TokenValidationResponse,
)
from services import (
    get_hmac,
    get_hmac_message,
    get_stream,
    get_user,
    verify_message,
)
from services.config import config
from services.helper.http_client import http_client_manager
from services.twitch import live_alert, stream_session
from services.twitch.chat import say, say_template
from services.twitch.commands import dispatch
from services.twitch.helix import HelixError
from services.twitch.oauth import (
    authorization_url,
    callback_uri,
    configured_scopes,
    consume_authorization,
    expected_user_id,
)
from services.twitch.token_manager import token_manager

logger = logging.getLogger(__name__)

twitch_router = APIRouter()

# Twitch announces the stream before Helix lists it. Bounded by the clock rather
# than by a count of attempts: a lookup that is timing out takes most of a minute
# on its own, so thirty of those is a quarter of an hour, not thirty seconds.
_STREAM_WAIT_SECONDS = 60

# An EventSub payload is a few kilobytes. The whole body has to be read to
# compute the signature over it, so the size is capped before that happens.
_MAX_BODY_BYTES = 256 * 1024


async def validate_call(request: Request, endpoint: str) -> Response | None:
    headers = request.headers

    # Twitch signs every message type, so the signature is checked before the
    # request is read as anything. Branching first left the handshake echoing
    # attacker text and a revocation putting attacker text, mentions included,
    # into the admin channel -- neither of which needed a signature at all.
    declared = headers.get("content-length")
    if declared is not None and declared.isdigit() and int(declared) > _MAX_BODY_BYTES:
        # A chunked request declares no length and gets past this; the platform
        # in front is the backstop for that. This stops the honest large POST.
        logger.warning("413: Payload too large on %s", endpoint)
        await notify(f"413: Oversized request on {endpoint}, rejected unread.")
        raise HTTPException(status_code=413)

    body_str = (await request.body()).decode()
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
    body: dict[str, Any] = await request.json()

    if message_type == "webhook_callback_verification":
        challenge = body.get("challenge", "")
        return Response(challenge or "", status_code=200)

    if message_type == "revocation":
        subscription: dict[str, Any] = body.get("subscription", {})
        await notify(
            f"Revoked {subscription.get('type', 'unknown')} notifications for"
            f" condition: {subscription.get('condition', {})} because"
            f" {subscription.get('status', 'No reason provided')}"
        )
        return Response(status_code=204)


async def process_webhook(
    request: Request, endpoint: str, event_model, expected_type: str, task_func
) -> Response:
    try:
        validation = await validate_call(request, endpoint)
        if validation:
            return validation

        body: dict[str, Any] = await request.json()
        event_sub = event_model.model_validate(body)
        if event_sub.subscription.type != expected_type:
            logger.warning(
                f"400: Bad request. Invalid subscription type: {event_sub.subscription.type}"
            )
            await notify(f"400: Bad request on {endpoint}. Invalid subscription type.")
            raise HTTPException(status_code=400)

        fire_and_forget(task_func(event_sub), name=endpoint)
        return Response(status_code=202)
    except HTTPException:
        raise
    except Exception as e:
        await report(e, f"500: Internal server error on {endpoint}")
        raise HTTPException(status_code=500) from e


async def _wait_for_stream_info(
    broadcaster_id: int,
) -> tuple[Stream | None, HelixError | None]:
    """Poll until Twitch admits the stream is up, tolerating a failed lookup.

    Bounded, because Twitch delivers stream.online once and never again: waiting
    forever and giving up both lose the alert, but only one of them says so. The
    last lookup error comes back too, so giving up can tell "Twitch says they
    are offline" from "Twitch could not be reached".
    """
    deadline = time.monotonic() + _STREAM_WAIT_SECONDS
    last_error: HelixError | None = None

    while True:
        try:
            stream_info = await get_stream(broadcaster_id)
        except HelixError as e:
            last_error = e
            logger.warning(f"Stream lookup failed for {broadcaster_id}: {e}")
            stream_info = None

        if stream_info:
            return stream_info, None
        # Checked after the attempt, so a slow first lookup still gets its turn.
        if time.monotonic() >= deadline:
            return None, last_error
        await asyncio.sleep(1)


async def _stream_online_task(event_sub: StreamOnlineEventSub) -> None:
    broadcaster_id = int(event_sub.event.broadcaster_user_id)
    try:
        stream_info, lookup_error = await _wait_for_stream_info(broadcaster_id)
        if stream_info is None:
            reason = (
                f"the last lookup failed: {lookup_error}"
                if lookup_error is not None
                else "Twitch went on reporting them offline"
            )
            await notify(
                f"Gave up after {_STREAM_WAIT_SECONDS}s waiting for Twitch to confirm"
                f" broadcaster {broadcaster_id} is live, so no alert was posted and no"
                f" stream session was started: {reason}",
                key=f"stream-online-gave-up:{broadcaster_id}",
            )
            return

        try:
            user_info = await get_user(broadcaster_id)
        except HelixError as e:
            # Decoration. stream.online does not come again, so the alert must
            # not depend on it.
            await notify(
                f"Announcing broadcaster {broadcaster_id} without their profile: {e}",
                key=f"stream-online-profile:{broadcaster_id}",
            )
            user_info = None

        is_main = stream_info.user_login == config.setting("broadcaster_username")
        channel = (
            config.channel("stream_alerts") if is_main else config.channel("promo")
        )

        # began does not raise: it guards each of its own chat lines, because
        # the alert below must survive a Twitch that will not take a greeting.
        await stream_session.began(broadcaster_id, stream_info)

        await live_alert.announce(broadcaster_id, stream_info, user_info, channel)

    except Exception as e:  # noqa: BLE001
        await report(e, f"Error in _stream_online_task for {broadcaster_id}")


async def _stream_offline_task(event_sub: StreamOfflineEventSub) -> None:
    broadcaster_id = int(event_sub.event.broadcaster_user_id)
    try:
        # The payload names no stream, so this handler cannot tell which one
        # ended. It wakes the updater, which can. See docs/adr/0001.
        await live_alert.wake(broadcaster_id)

    except Exception as e:  # noqa: BLE001
        await report(e, f"Error in _stream_offline_task for {broadcaster_id}")


async def _channel_chat_message_task(event_sub: ChannelChatMessageEventSub) -> None:
    try:
        if not event_sub.event.message.text.startswith("!"):
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

        await dispatch(event_sub, command, args)
    except Exception as e:  # noqa: BLE001
        await report(e, "Error processing Twitch chat webhook task")


async def _channel_follow_task(event_sub: ChannelFollowEventSub) -> None:
    try:
        await say_template(
            event_sub.event.broadcaster_user_id,
            "twitch_follow_thanks",
            user=event_sub.event.user_name,
        )
    except Exception as e:  # noqa: BLE001
        await report(e, "Error processing Twitch follow webhook task")


async def _channel_ad_break_begin_task(event_sub: ChannelAdBreakBeginEventSub) -> None:
    broadcaster_id = event_sub.event.broadcaster_user_id
    try:
        ad_duration = event_sub.event.duration_seconds

        # Each step stands alone: a dropped "ads starting" used to take the
        # "ads over" message and the next break's warning down with it.
        await say_template(
            broadcaster_id, "twitch_ad_break_start", minutes=ad_duration // 60
        )
        await asyncio.sleep(ad_duration)
        await say_template(broadcaster_id, "twitch_ad_break_end")

        stream_session.schedule_ad_break_warning(broadcaster_id)
    except Exception as e:  # noqa: BLE001
        await report(e, "Error processing Twitch ad break webhook task")


async def _validate_oauth_identity(
    auth_response: RefreshResponse,
    token_type: TokenType,
    endpoint: str,
) -> TokenValidationResponse:
    response = await http_client_manager.request(
        "GET",
        "https://id.twitch.tv/oauth2/validate",
        headers={"Authorization": f"OAuth {auth_response.access_token}"},
    )
    if response.status_code < 200 or response.status_code >= 300:
        logger.error(
            "OAuth token validation failed with status=%s", response.status_code
        )
        await notify(
            f"Failed to validate the Twitch token from {endpoint}: "
            f"{response.status_code} {response.text}"
        )
        raise HTTPException(status_code=500, detail="Twitch token validation failed")

    validation = TokenValidationResponse.model_validate(response.json())
    expected_id = expected_user_id(token_type)
    missing_scopes = sorted(set(configured_scopes()) - set(validation.scopes))

    problems = []
    if validation.client_id != settings.twitch_client_id:
        problems.append("the token belongs to a different Twitch application")
    if validation.user_id != expected_id:
        problems.append(
            f"expected Twitch user ID {expected_id}, but {validation.login} "
            f"authorized as ID {validation.user_id}"
        )
    if missing_scopes:
        problems.append(f"missing scopes: {', '.join(missing_scopes)}")

    if problems:
        message = f"Rejected {token_type.value} authorization: {'; '.join(problems)}"
        logger.warning(message)
        await notify(message)
        raise HTTPException(status_code=400, detail=message)
    return validation


async def _oauth_callback_common(
    code: str | None,
    state: str,
    endpoint: str,
    token_type: TokenType,
    error: str | None,
    error_description: str | None,
) -> tuple[RefreshResponse, TokenValidationResponse]:
    if not consume_authorization(token_type, state):
        logger.warning("400: Bad request on %s. Invalid OAuth state.", endpoint)
        await notify(f"400: Bad request on {endpoint}. Invalid or expired OAuth state.")
        raise HTTPException(status_code=400, detail="Invalid or expired OAuth state")

    if error:
        detail = error_description or error
        logger.warning("Twitch authorization denied on %s: %s", endpoint, detail)
        raise HTTPException(
            status_code=400, detail=f"Twitch authorization denied: {detail}"
        )
    if not code:
        raise HTTPException(status_code=400, detail="Missing Twitch authorization code")

    params = {
        "client_id": settings.twitch_client_id,
        "client_secret": settings.twitch_client_secret,
        "code": code,
        "grant_type": "authorization_code",
        "redirect_uri": callback_uri(token_type),
    }
    response = await http_client_manager.request(
        "POST", "https://id.twitch.tv/oauth2/token", data=params
    )

    if response.status_code < 200 or response.status_code >= 300:
        logger.error(
            f"Token exchange failed with status={response.status_code}, response={response.text}"
        )
        await notify(
            f"Failed to exchange token: {response.status_code} {response.text}"
        )
        raise HTTPException(status_code=500)

    auth_response = RefreshResponse.model_validate(response.json())
    if auth_response.token_type != "bearer":
        logger.error(
            f"Token exchange failed: unexpected token type {auth_response.token_type}"
        )
        await notify(
            f"Failed to exchange token: unexpected token type {auth_response.token_type}"
        )
        raise HTTPException(status_code=500)
    validation = await _validate_oauth_identity(auth_response, token_type, endpoint)
    return auth_response, validation


@twitch_router.get("/twitch/oauth/start/{identity}")
async def twitch_oauth_start(identity: str, state: str) -> Response:
    try:
        token_type = TokenType(identity)
        return RedirectResponse(authorization_url(token_type, state), status_code=302)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from None


@twitch_router.get("/twitch/oauth/callback")
async def twitch_oauth_callback(
    state: str,
    code: str | None = None,
    error: str | None = None,
    error_description: str | None = None,
) -> Response:
    try:
        auth_response, validation = await _oauth_callback_common(
            code,
            state,
            "/twitch/oauth/callback",
            TokenType.User,
            error,
            error_description,
        )
        await token_manager.set_user_access_token(auth_response)
        return Response(
            f"Authorization successful for bot account {validation.login} "
            f"({validation.user_id}). You can close this tab.",
            status_code=200,
        )
    except HTTPException:
        raise
    except Exception as e:
        await report(e, "500: Internal server error on /twitch/oauth/callback")
        raise HTTPException(status_code=500) from e


@twitch_router.get("/twitch/oauth/callback/broadcaster")
async def twitch_oauth_callback_broadcaster(
    state: str,
    code: str | None = None,
    error: str | None = None,
    error_description: str | None = None,
) -> Response:
    try:
        auth_response, validation = await _oauth_callback_common(
            code,
            state,
            "/twitch/oauth/callback/broadcaster",
            TokenType.Broadcaster,
            error,
            error_description,
        )
        await token_manager.set_broadcaster_access_token(auth_response)
        return Response(
            f"Authorization successful for broadcaster account {validation.login} "
            f"({validation.user_id}). You can close this tab.",
            status_code=200,
        )
    except HTTPException:
        raise
    except Exception as e:
        await report(
            e, "500: Internal server error on /twitch/oauth/callback/broadcaster"
        )
        raise HTTPException(status_code=500) from e


@twitch_router.post("/webhook/twitch")
async def stream_online_webhook(request: Request) -> Response:
    return await process_webhook(
        request,
        "/webhook/twitch",
        StreamOnlineEventSub,
        "stream.online",
        _stream_online_task,
    )


@twitch_router.post("/webhook/twitch/offline")
async def stream_offline_webhook(request: Request) -> Response:
    return await process_webhook(
        request,
        "/webhook/twitch/offline",
        StreamOfflineEventSub,
        "stream.offline",
        _stream_offline_task,
    )


@twitch_router.post("/webhook/twitch/chat")
async def channel_chat_message_webhook(request: Request) -> Response:
    return await process_webhook(
        request,
        "/webhook/twitch/chat",
        ChannelChatMessageEventSub,
        "channel.chat.message",
        _channel_chat_message_task,
    )


@twitch_router.post("/webhook/twitch/follow")
async def channel_follow_webhook(request: Request) -> Response:
    return await process_webhook(
        request,
        "/webhook/twitch/follow",
        ChannelFollowEventSub,
        "channel.follow",
        _channel_follow_task,
    )


@twitch_router.post("/webhook/twitch/adbreak")
async def channel_ad_break_begin_webhook(request: Request) -> Response:
    return await process_webhook(
        request,
        "/webhook/twitch/adbreak",
        ChannelAdBreakBeginEventSub,
        "channel.ad_break.begin",
        _channel_ad_break_begin_task,
    )


async def _channel_raid_task(event_sub: ChannelRaidEventSub) -> None:
    try:
        if stream_session.is_main_broadcaster(event_sub.event.from_broadcaster_user_id):
            twitch_url = (
                f"https://www.twitch.tv/{event_sub.event.to_broadcaster_user_login}"
            )
            await say_template(
                event_sub.event.from_broadcaster_user_id,
                "twitch_raid_out",
                name=event_sub.event.to_broadcaster_user_name,
                url=twitch_url,
            )
        elif stream_session.is_main_broadcaster(event_sub.event.to_broadcaster_user_id):
            await say(
                event_sub.event.to_broadcaster_user_id,
                f"!so {event_sub.event.from_broadcaster_user_login}",
                "the incoming raid shoutout",
            )
    except Exception as e:  # noqa: BLE001
        await report(e, "Error processing Twitch raid webhook task")


@twitch_router.post("/webhook/twitch/raid")
async def channel_raid_webhook(request: Request) -> Response:
    return await process_webhook(
        request,
        "/webhook/twitch/raid",
        ChannelRaidEventSub,
        "channel.raid",
        _channel_raid_task,
    )


async def _channel_moderate_task(event_sub: ChannelModerateEventSub) -> None:
    try:
        if event_sub.event.action != "raid" or not stream_session.is_main_broadcaster(
            event_sub.event.broadcaster_user_id
        ):
            return
        await say_template(event_sub.event.broadcaster_user_id, "twitch_raid_farewell")
    except Exception as e:  # noqa: BLE001
        await report(e, "Error processing Twitch moderate webhook task")


@twitch_router.post("/webhook/twitch/moderate")
async def channel_moderate_webhook(request: Request) -> Response:
    return await process_webhook(
        request,
        "/webhook/twitch/moderate",
        ChannelModerateEventSub,
        "channel.moderate",
        _channel_moderate_task,
    )
