"""One pass of an alert updater: what it concludes, and carrying that out.

Split from ``live_alert`` so that module is left with the updater's lifecycle -
starting one, waking it, restoring them after a restart. Nothing here knows a
task exists; it is handed the alert it is working on and answers with an
:class:`Action`.

``_decide`` is the whole rule and is pure, because this is where the subtle
cases live and they are worth reading in one place. New conditions go there,
not in the I/O around it.
"""

import logging
from collections.abc import Callable
from enum import Enum, auto

import aiohttp
import discord
import pendulum

from db import repository
from db.models import LiveAlert
from errors import notify, report
from models.twitch_api_responses.channel import Channel
from models.twitch_api_responses.stream import Stream
from models.twitch_api_responses.user import User
from models.twitch_api_responses.video import Video
from services.duration import get_age
from services.send import edit_embed
from services.twitch.api import (
    get_channel,
    get_stream,
    get_stream_vod,
    get_user,
    live_stream,
)
from services.twitch.helix import HelixError
from services.twitch.live_alert_embeds import (
    live_embed,
    offline_embed,
    twitch_url,
    watch_button,
)

logger = logging.getLogger(__name__)


class Action(Enum):
    """What one cycle concluded it should do."""

    REFRESH = auto()  # the stream is live; update the embed
    CLOSE = auto()  # this alert is finished; write the offline embed and retire it
    RETRY = auto()  # nothing could be concluded; try again next cycle
    STOP = auto()  # nothing left to maintain


def _owns_row(alert: LiveAlert | None, message_id: int, stream_id: int) -> bool:
    """Whether the stored alert is still the one this updater was started for."""
    return (
        alert is not None
        and alert.message_id == message_id
        and alert.stream_id == stream_id
    )


def _decide(
    alert: LiveAlert | None,
    message_id: int,
    stream_id: int,
    stream: Stream | None,
) -> Action:
    """The whole rule for one cycle, with every input already in hand.

    Pure, because this is where the subtle cases live and they are worth reading
    in one place.
    """
    if alert is None:
        return Action.STOP

    if not _owns_row(alert, message_id, stream_id):
        # Superseded. This message still shows a live stream, so close it, but
        # the row belongs to the newer alert and its delete will not match.
        return Action.CLOSE

    live = live_stream(stream)
    if live is None or live.id != str(stream_id):
        return Action.CLOSE

    return Action.REFRESH


def _is_transient_edit_error(e: Exception) -> bool:
    """Discord edits fail transiently on 5xx replies and on dropped sockets.

    aiohttp because that is what discord.py sends on: a dropped socket arrives
    as one of its ClientError subclasses, not as anything discord.py names.
    """
    if isinstance(e, discord.HTTPException):
        return 500 <= e.status < 600
    return isinstance(e, (aiohttp.ClientError, TimeoutError))


async def _forget_row(broadcaster_id: int, message_id: int) -> None:
    """Drop the row this updater owns; a newer alert's row is left in place."""
    try:
        await repository.delete_live_alert(broadcaster_id, message_id=message_id)
    except Exception as e:  # noqa: BLE001
        await report(
            e, f"Failed to delete live alert record for broadcaster_id={broadcaster_id}"
        )


async def _vod(broadcaster_id: int, stream_id: int) -> Video | None:
    if not stream_id:
        return None
    try:
        return await get_stream_vod(broadcaster_id, stream_id)
    except Exception as e:  # noqa: BLE001
        await report(e, f"Failed to fetch VOD info for broadcaster_id={broadcaster_id}")
        return None


async def _edit_or_retry(
    message_id: int,
    channel_id: int,
    broadcaster_id: int,
    embed: discord.Embed,
    kind: str,
    on_error: Action,
    report_context: Callable[[Exception], str],
    on_success: Action,
    content: str | None = None,
    view: discord.ui.View | None = None,
    forget_on_success: bool = False,
) -> Action:
    """Attempt a Discord message edit and interpret the outcome.

    Shared by ``_refresh`` and ``_close``: not-found forgets the row and
    stops, a transient error retries. Everything else -- the embed, whether a
    button accompanies it, the report text and what a caller does once a
    failure is not transient -- is theirs to supply, because the two do not
    agree on that last outcome (``on_error``) and never did.
    """
    try:
        edited = await edit_embed(message_id, embed, channel_id, view, content=content)
    except discord.NotFound:
        logger.warning(
            f"Message not found when editing {kind} embed for message_id={message_id}; clearing alert"
        )
        await _forget_row(broadcaster_id, message_id)
        return Action.STOP
    except Exception as e:  # noqa: BLE001
        if _is_transient_edit_error(e):
            logger.warning(
                f"Transient error when editing {kind} embed for message_id={message_id}; will retry next cycle: {e}"
            )
            return Action.RETRY
        await report(e, report_context(e))
        return on_error

    if not edited:
        return Action.RETRY

    if forget_on_success:
        await _forget_row(broadcaster_id, message_id)
    return on_success


async def _refresh(
    broadcaster_id: int,
    channel_id: int,
    message_id: int,
    stream: Stream,
    user_info: User | None,
    age: str,
    started_at_timestamp: str,
    content: str | None,
) -> Action:
    try:
        url = twitch_url(stream.user_login)
    except ValueError as e:
        await report(
            e, f"Failed to build the live alert URL for broadcaster_id={broadcaster_id}"
        )
        return Action.RETRY

    embed = live_embed(
        stream, user_info, url, age, started_at_timestamp, pendulum.now()
    )

    def _report_context(e: Exception) -> str:
        return (
            f"Discord HTTP error {e.status} when editing live embed for message_id={message_id}"
            if isinstance(e, discord.HTTPException)
            else f"Error editing live embed for message_id={message_id}"
        )

    return await _edit_or_retry(
        message_id,
        channel_id,
        broadcaster_id,
        embed,
        kind="live",
        on_error=Action.RETRY,
        report_context=_report_context,
        on_success=Action.REFRESH,
        content=content,
        view=watch_button(url),
    )


async def _gather_close_info(
    broadcaster_id: int,
    stream_id: int,
    stream: Stream | None,
    user_info: User | None,
) -> tuple[Stream | None, Channel | None, str]:
    """What ``_close`` needs to build the offline embed: channel info (with
    its own Helix fallback), which stream still counts as this alert's own,
    and the login to link to.
    """
    try:
        channel_info = await get_channel(broadcaster_id)
    except HelixError as e:
        # Only a fallback for the title and game, which the VOD usually supplies
        # anyway. Not worth abandoning the close over, but worth saying.
        await notify(
            f"Closing the live alert for broadcaster {broadcaster_id} without channel"
            f" details: {e}",
            key=f"live-alert-close-channel:{broadcaster_id}",
        )
        channel_info = None

    # A live stream that is not the one this alert announced describes a
    # different broadcast; borrowing its title would retitle this message.
    own_stream = stream if stream is not None and stream.id == str(stream_id) else None

    if user_info:
        login = user_info.login
    elif channel_info:
        login = channel_info.broadcaster_login
    else:
        login = ""

    try:
        url = twitch_url(login)
    except ValueError as e:
        # Retrying would not help: a malformed login is a permanent property
        # of this cycle's Helix data, not a transient failure, and _close's
        # whole job is to guarantee the alert actually closes. Degrades to
        # the same generic link an unresolved login already produces.
        await notify(
            f"Closing the live alert for broadcaster {broadcaster_id} with an"
            f" unusable login ({e}); using a generic link instead.",
            key=f"live-alert-close-login:{broadcaster_id}",
        )
        url = twitch_url("")

    return own_stream, channel_info, url


async def _close(
    broadcaster_id: int,
    channel_id: int,
    message_id: int,
    stream_id: int,
    stream: Stream | None,
    user_info: User | None,
    age: str,
    content: str | None,
) -> Action:
    """Swap the live embed for the offline one and retire the alert."""
    own_stream, channel_info, url = await _gather_close_info(
        broadcaster_id, stream_id, stream, user_info
    )

    vod = await _vod(broadcaster_id, stream_id)
    embed = offline_embed(
        own_stream,
        vod,
        channel_info,
        user_info,
        url,
        age,
        pendulum.now(),
    )

    # The stream is over: forgetting the row on success is what keeps a
    # restart from resurrecting an updater for a dead stream.
    return await _edit_or_retry(
        message_id,
        channel_id,
        broadcaster_id,
        embed,
        kind="offline",
        on_error=Action.STOP,
        report_context=lambda _: (
            f"Error editing offline embed for message_id={message_id}"
        ),
        on_success=Action.STOP,
        content=content,
        forget_on_success=True,
    )


async def _fetch_profile(broadcaster_id: int) -> User | None:
    """The broadcaster's profile for the embed, degrading to None rather than losing the alert over it."""
    try:
        return await get_user(broadcaster_id)
    except HelixError as e:
        # The avatar and display name are decoration; the embed renders without
        # them, and losing the alert over them would be worse. Still worth
        # saying: nobody reads the logs, and the admin channel is watched.
        await notify(
            f"Updating the live alert for broadcaster {broadcaster_id} without the"
            f" broadcaster's profile: {e}",
            key=f"live-alert-profile:{broadcaster_id}",
        )
        return None


async def _dispatch(
    action: Action,
    broadcaster_id: int,
    channel_id: int,
    message_id: int,
    stream_id: int,
    stream: Stream | None,
    user_info: User | None,
    age: str,
    started_at_timestamp: str,
    content: str | None,
) -> Action:
    """Carry out whatever ``_decide`` concluded, once STOP is already handled."""
    if action is Action.CLOSE:
        return await _close(
            broadcaster_id,
            channel_id,
            message_id,
            stream_id,
            stream,
            user_info,
            age,
            content,
        )

    if stream is None:
        # Unreachable: _decide only answers REFRESH for a live stream. A guard
        # rather than an assert, because -O strips an assert and would leave
        # _refresh taking a None it is not typed for; a cycle that concluded
        # nothing is the honest reading if the rule ever changes underneath.
        return Action.RETRY

    return await _refresh(
        broadcaster_id,
        channel_id,
        message_id,
        stream,
        user_info,
        age,
        started_at_timestamp,
        content,
    )


async def cycle(
    broadcaster_id: int,
    channel_id: int,
    message_id: int,
    stream_id: int,
    started_at: pendulum.DateTime,
    started_at_timestamp: str,
    content: str | None,
) -> Action:
    alert = await repository.get_live_alert(broadcaster_id)

    # Once the row has moved on, Helix has nothing to add: the alert is closed
    # either way, and a newer stream must not lend it a title. A lookup that
    # fails raises, and the loop above reads that as an inconclusive cycle.
    owns_row = _owns_row(alert, message_id, stream_id)
    stream = await get_stream(broadcaster_id) if owns_row else None

    action = _decide(alert, message_id, stream_id, stream)
    if action is Action.STOP:
        logger.info(
            f"No live alert record left for broadcaster_id={broadcaster_id}; stopping updates for message_id={message_id}"
        )
        return action

    user_info = await _fetch_profile(broadcaster_id)
    age = get_age(started_at, limit_units=2)

    return await _dispatch(
        action,
        broadcaster_id,
        channel_id,
        message_id,
        stream_id,
        stream,
        user_info,
        age,
        started_at_timestamp,
        content,
    )
