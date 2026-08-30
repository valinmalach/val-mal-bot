"""The Discord message announcing a stream, and the task that keeps it current.

One alert per broadcaster. The updater started for an alert is the only thing
that closes it: a ``stream.offline`` webhook carries no stream id, so it cannot
tell which stream ended and can only wake the updater. See
``docs/adr/0001-alert-updater-is-the-only-closer.md``.
"""

import asyncio
import contextlib
import logging
from enum import Enum, auto

import discord
import pendulum
from discord.ui import View

from background import fire_and_forget
from db import LiveAlert, repository
from errors import notify, report
from models import Channel, Stream, User, Video
from services.config import config
from services.helper.helper import edit_embed, get_age, parse_rfc3339, send_embed
from services.helper.http_client import is_transient_network_error
from services.twitch import stream_session
from services.twitch.api import get_channel, get_stream, get_stream_vod, get_user
from services.twitch.helix import HelixError

logger = logging.getLogger(__name__)

_INTERVAL_SECONDS = 60

# Stop after this many cycles that concluded nothing, so a stuck alert cannot
# poll (and report) forever; the record is left behind for the next restart.
_MAX_INCONCLUSIVE_CYCLES = 5

# Keyed by message: an alert is identified by the message it maintains, and a
# broadcaster can briefly have a replaced alert whose updater is still winding up.
_update_tasks: dict[int, asyncio.Task] = {}
_wakeups: dict[int, asyncio.Event] = {}


class _Action(Enum):
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
) -> _Action:
    """The whole rule for one cycle, with every input already in hand.

    Pure, because this is where the subtle cases live and they are worth reading
    in one place.
    """
    if alert is None:
        return _Action.STOP

    if not _owns_row(alert, message_id, stream_id):
        # Superseded. This message still shows a live stream, so close it, but
        # the row belongs to the newer alert and its delete will not match.
        return _Action.CLOSE

    if stream is None or stream.id != str(stream_id):
        return _Action.CLOSE

    return _Action.REFRESH


def _twitch_url(user_login: str) -> str:
    return f"https://www.twitch.tv/{user_login}"


def _mention(channel_id: int) -> str | None:
    """The live-alerts role ping, which only the stream alerts channel gets."""
    return (
        f"<@&{config.role('live_alerts')}>"
        if channel_id == config.channel("stream_alerts")
        else None
    )


def _watch_button(url: str) -> View:
    view = View(timeout=None)
    view.add_item(
        discord.ui.Button(
            label=config.template("stream_watch_button"),
            style=discord.ButtonStyle.link,
            url=url,
        )
    )
    return view


def _announcement_embed(stream: Stream, user_info: User | None) -> discord.Embed:
    """The alert as first posted, timestamped at the stream's start."""
    url = _twitch_url(stream.user_login)
    raw_thumb_url = stream.thumbnail_url.replace("{width}x{height}", "400x225")

    return (
        discord.Embed(
            description=f"[**{stream.title}**]({url})",
            color=config.color("embed_color_stream"),
            timestamp=parse_rfc3339(stream.started_at),
        )
        .set_author(
            name=config.template("stream_live_title", name=stream.user_name),
            icon_url=user_info.profile_image_url if user_info else None,
            url=url,
        )
        .add_field(
            name=config.template("stream_field_game"),
            value=f"{stream.game_name}",
            inline=True,
        )
        .add_field(
            name=config.template("stream_field_viewers"),
            value=f"{stream.viewer_count}",
            inline=True,
        )
        .set_image(url=f"{raw_thumb_url}?cb={int(pendulum.now().timestamp())}")
    )


def _live_embed(
    stream: Stream,
    user_info: User | None,
    url: str,
    age: str,
    started_at_timestamp: str,
    now: pendulum.DateTime,
) -> discord.Embed:
    """The alert as refreshed, carrying how long the stream has been up."""
    raw_thumb_url = stream.thumbnail_url.replace("{width}x{height}", "400x225")

    return (
        discord.Embed(
            description=f"[**{stream.title}**]({url})",
            color=config.color("embed_color_stream"),
            timestamp=now,
        )
        .set_author(
            name=config.template("stream_live_title", name=stream.user_name),
            icon_url=user_info.profile_image_url if user_info else None,
            url=url,
        )
        .add_field(
            name=config.template("stream_field_game"),
            value=f"{stream.game_name}",
            inline=True,
        )
        .add_field(
            name=config.template("stream_field_viewers"),
            value=f"{stream.viewer_count}",
            inline=True,
        )
        .add_field(
            name=config.template("stream_field_started_at"),
            value=started_at_timestamp,
            inline=True,
        )
        .set_image(url=f"{raw_thumb_url}?cb={int(pendulum.now().timestamp())}")
        .set_footer(text=config.template("stream_footer_online", age=age))
    )


def _title(stream: Stream | None, vod: Video | None, channel: Channel | None) -> str:
    if stream:
        return stream.title
    if vod:
        return vod.title
    return channel.title if channel else "Unknown"


def _display_name(stream: Stream | None, user_info: User | None) -> str:
    if stream:
        return stream.user_name
    return user_info.display_name if user_info else "Unknown"


def _game(stream: Stream | None, channel: Channel | None) -> str:
    if stream:
        return stream.game_name
    return channel.game_name if channel else "Unknown"


def _offline_embed(
    stream: Stream | None,
    vod: Video | None,
    channel: Channel | None,
    user_info: User | None,
    url: str,
    age: str,
    now: pendulum.DateTime,
) -> discord.Embed:
    """The alert as left behind once the stream is over."""
    embed = (
        discord.Embed(
            description=f"**{_title(stream, vod, channel)}**",
            color=config.color("embed_color_stream"),
            timestamp=now,
        )
        .set_author(
            name=config.template(
                "stream_offline_title", name=_display_name(stream, user_info)
            ),
            icon_url=user_info.profile_image_url if user_info else None,
            url=url,
        )
        .add_field(
            name=config.template("stream_field_game"),
            value=_game(stream, channel),
            inline=True,
        )
        .set_footer(text=config.template("stream_footer_offline", age=age))
    )

    if vod:
        embed = embed.add_field(
            name=config.template("stream_field_vod"),
            value=config.template("stream_field_vod_value", url=vod.url),
            inline=True,
        )

    return embed


def _is_transient_edit_error(e: Exception) -> bool:
    """Discord edits fail transiently on 5xx replies and on dropped sockets."""
    if isinstance(e, discord.HTTPException) and 500 <= e.status < 600:
        return True
    if is_transient_network_error(e):
        return True
    return "clientoserror" in f"{type(e).__name__} {e}".lower()


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


async def _refresh(
    broadcaster_id: int,
    channel_id: int,
    message_id: int,
    stream: Stream,
    user_info: User | None,
    age: str,
    started_at_timestamp: str,
    content: str | None,
) -> _Action:
    url = _twitch_url(stream.user_login)
    embed = _live_embed(
        stream, user_info, url, age, started_at_timestamp, pendulum.now()
    )

    try:
        edited = await edit_embed(
            message_id, embed, channel_id, _watch_button(url), content=content
        )
    except discord.NotFound:
        logger.warning(
            f"Message not found when editing live embed for message_id={message_id}; clearing alert"
        )
        await _forget_row(broadcaster_id, message_id)
        return _Action.STOP
    except Exception as e:  # noqa: BLE001
        if _is_transient_edit_error(e):
            logger.warning(
                f"Transient error when editing live embed for message_id={message_id}; will retry next cycle: {e}"
            )
            return _Action.RETRY
        context = (
            f"Discord HTTP error {e.status} when editing live embed for message_id={message_id}"
            if isinstance(e, discord.HTTPException)
            else f"Error editing live embed for message_id={message_id}"
        )
        await report(e, context)
        return _Action.RETRY

    return _Action.REFRESH if edited else _Action.RETRY


async def _close(
    broadcaster_id: int,
    channel_id: int,
    message_id: int,
    stream_id: int,
    stream: Stream | None,
    user_info: User | None,
    age: str,
    content: str | None,
) -> _Action:
    """Swap the live embed for the offline one and retire the alert."""
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

    vod = await _vod(broadcaster_id, stream_id)
    embed = _offline_embed(
        own_stream,
        vod,
        channel_info,
        user_info,
        _twitch_url(login),
        age,
        pendulum.now(),
    )

    try:
        edited = await edit_embed(message_id, embed, channel_id, content=content)
    except discord.NotFound:
        logger.warning(
            f"Message not found when editing offline embed for message_id={message_id}; clearing alert"
        )
        await _forget_row(broadcaster_id, message_id)
        return _Action.STOP
    except Exception as e:  # noqa: BLE001
        if _is_transient_edit_error(e):
            logger.warning(
                f"Transient network error when editing offline embed for message_id={message_id}: {e}"
            )
            return _Action.RETRY
        await report(e, f"Error editing offline embed for message_id={message_id}")
        return _Action.STOP

    if not edited:
        return _Action.RETRY

    # The stream is over: without this the record outlives it and every restart
    # resurrects an updater for a dead stream.
    await _forget_row(broadcaster_id, message_id)
    return _Action.STOP


async def _cycle(
    broadcaster_id: int,
    channel_id: int,
    message_id: int,
    stream_id: int,
    started_at: pendulum.DateTime,
    started_at_timestamp: str,
    content: str | None,
) -> _Action:
    alert = await repository.get_live_alert(broadcaster_id)

    # Once the row has moved on, Helix has nothing to add: the alert is closed
    # either way, and a newer stream must not lend it a title. A lookup that
    # fails raises, and the loop above reads that as an inconclusive cycle.
    owns_row = _owns_row(alert, message_id, stream_id)
    stream = await get_stream(broadcaster_id) if owns_row else None

    action = _decide(alert, message_id, stream_id, stream)
    if action is _Action.STOP:
        logger.info(
            f"No live alert record left for broadcaster_id={broadcaster_id}; stopping updates for message_id={message_id}"
        )
        return action

    try:
        user_info = await get_user(broadcaster_id)
    except HelixError as e:
        # The avatar and display name are decoration; the embed renders without
        # them, and losing the alert over them would be worse. Still worth
        # saying: nobody reads the logs, and the admin channel is watched.
        await notify(
            f"Updating the live alert for broadcaster {broadcaster_id} without the"
            f" broadcaster's profile: {e}",
            key=f"live-alert-profile:{broadcaster_id}",
        )
        user_info = None

    age = get_age(started_at, limit_units=2)

    if action is _Action.CLOSE:
        if owns_row and stream is None:
            # Only a stream Helix confirms is gone stands the session down; a
            # superseded alert closes while its broadcaster may still be live.
            stream_session.ended(broadcaster_id)

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

    # REFRESH only reaches here with a live stream, which _decide guaranteed.
    assert stream is not None
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


async def _run(
    broadcaster_id: int,
    channel_id: int,
    message_id: int,
    stream_id: int,
    stream_started_at: str,
) -> None:
    try:
        content = _mention(channel_id)
        started_at = parse_rfc3339(stream_started_at)
        started_at_timestamp = f"<t:{int(started_at.timestamp())}:f>"
        wakeup = _wakeups.setdefault(message_id, asyncio.Event())

        inconclusive = 0
        while True:
            # A wake cuts the wait short, so an offline webhook is acted on at
            # once rather than up to a full interval later.
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(wakeup.wait(), timeout=_INTERVAL_SECONDS)
            wakeup.clear()

            try:
                action = await _cycle(
                    broadcaster_id,
                    channel_id,
                    message_id,
                    stream_id,
                    started_at,
                    started_at_timestamp,
                    content,
                )
            except Exception as e:  # noqa: BLE001
                # A cycle that raised concluded nothing, and must not take the
                # updater with it; the cap below still stops a hopeless one.
                await report(
                    e,
                    f"Error in the live alert update cycle for broadcaster_id={broadcaster_id}",
                )
                action = _Action.RETRY

            if action is _Action.STOP:
                return

            inconclusive = inconclusive + 1 if action is _Action.RETRY else 0
            if inconclusive >= _MAX_INCONCLUSIVE_CYCLES:
                # The one place this loop speaks: it stays quiet while retrying
                # and says so once when it stops, so an outage costs a message
                # rather than one a minute.
                await notify(
                    f"Gave up updating the live alert for broadcaster"
                    f" {broadcaster_id} after {inconclusive} cycles that concluded"
                    f" nothing (message_id={message_id}). The message is left as it"
                    f" stands and the record is kept, so a restart or the next"
                    f" stream.offline picks it up again.",
                    key=f"live-alert-gave-up:{broadcaster_id}",
                )
                return

    except Exception as e:  # noqa: BLE001
        await report(
            e, f"Error updating live alert message for broadcaster_id={broadcaster_id}"
        )


def _forget_updater(message_id: int, finished: asyncio.Task) -> None:
    if _update_tasks.get(message_id) is finished:
        del _update_tasks[message_id]
        _wakeups.pop(message_id, None)


def _start(
    broadcaster_id: int,
    channel_id: int,
    message_id: int,
    stream_id: int,
    stream_started_at: str,
) -> None:
    """Start the updater for one alert, unless it already has one.

    on_ready fires again on every gateway resume, which would otherwise stack a
    second updater per alert on each reconnect.
    """
    existing = _update_tasks.get(message_id)
    if existing is not None and not existing.done():
        logger.info(f"Live alert updater already running for message_id={message_id}")
        return

    # Created here, not in _run, so a wake issued before the task first runs
    # still lands.
    _wakeups[message_id] = asyncio.Event()
    task = fire_and_forget(
        _run(broadcaster_id, channel_id, message_id, stream_id, stream_started_at),
        name=f"live-alert-{message_id}",
    )
    _update_tasks[message_id] = task
    task.add_done_callback(
        lambda finished, mid=message_id: _forget_updater(mid, finished)
    )


async def announce(
    broadcaster_id: int,
    stream: Stream,
    user_info: User | None,
    channel_id: int,
) -> None:
    """Post the alert for a stream that has just gone live, and start its updater."""
    message_id = await send_embed(
        _announcement_embed(stream, user_info),
        channel_id,
        _watch_button(_twitch_url(stream.user_login)),
        content=_mention(channel_id),
    )
    if message_id is None:
        logger.error(f"Failed to send embed for broadcaster {broadcaster_id}")
        await notify(
            f"Failed to send live alert message\nbroadcaster_id: {broadcaster_id}\nchannel_id: {channel_id}",
            key=f"live-alert-send:{broadcaster_id}",
        )
        return

    try:
        await repository.upsert_live_alert(
            broadcaster_id,
            channel_id,
            message_id,
            int(stream.id),
            parse_rfc3339(stream.started_at),
        )
        _start(
            broadcaster_id, channel_id, message_id, int(stream.id), stream.started_at
        )
    except Exception as e:  # noqa: BLE001
        await report(e, f"Failed to store live alert for broadcaster {broadcaster_id}")


async def wake(broadcaster_id: int) -> None:
    """Have this broadcaster's alert re-check itself now instead of on its interval.

    Safe to call at any time: the cycle decides what to do, so a wake for a
    stream that has already been replaced closes only the message it owns.
    """
    alert = await repository.get_live_alert(broadcaster_id)
    if alert is None:
        logger.info(
            f"No live alert to wake for broadcaster_id={broadcaster_id}; nothing to do"
        )
        return

    # Starting is a no-op when an updater is already running, so this also
    # repairs a row whose updater died or gave up.
    _start(
        alert.broadcaster_id,
        alert.channel_id,
        alert.message_id,
        alert.stream_id,
        alert.stream_started_at.isoformat(),
    )
    wakeup = _wakeups.get(alert.message_id)
    if wakeup is not None:
        wakeup.set()


async def restore_all() -> None:
    """Start an updater for every stored alert, staggered to spread the Helix calls."""
    for alert in await repository.list_live_alerts():
        _start(
            alert.broadcaster_id,
            alert.channel_id,
            alert.message_id,
            alert.stream_id,
            alert.stream_started_at.isoformat(),
        )
        await asyncio.sleep(1)
