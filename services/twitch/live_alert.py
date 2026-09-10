"""The Discord message announcing a stream, and the task that keeps it current.

One alert per broadcaster. The updater started for an alert is the only thing
that closes it: a ``stream.offline`` webhook carries no stream id, so it cannot
tell which stream ended and can only wake the updater. See
``docs/adr/0001-alert-updater-is-the-only-closer.md``.
"""

import asyncio
import contextlib
import logging

import pendulum

from background import fire_and_forget
from db import repository
from db.models import LiveAlert
from errors import notify, report
from models import Stream, User
from services.send import send_embed
from services.twitch.live_alert_cycle import Action, cycle
from services.twitch.live_alert_embeds import (
    announcement_embed,
    mention,
    twitch_url,
    watch_button,
)
from services.twitch.signature import parse_rfc3339

logger = logging.getLogger(__name__)

_INTERVAL_SECONDS = 60

# Stop after this many cycles that concluded nothing, so a stuck alert cannot
# poll (and report) forever; the record is left behind for the next restart.
_MAX_INCONCLUSIVE_CYCLES = 5

# Keyed by message: an alert is identified by the message it maintains, and a
# broadcaster can briefly have a replaced alert whose updater is still winding up.
_update_tasks: dict[int, asyncio.Task] = {}
_wakeups: dict[int, asyncio.Event] = {}


async def _run(
    broadcaster_id: int,
    channel_id: int,
    message_id: int,
    stream_id: int,
    stream_started_at: str,
) -> None:
    try:
        content = mention(channel_id)
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
                action = await cycle(
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
                action = Action.RETRY

            if action is Action.STOP:
                return

            inconclusive = inconclusive + 1 if action is Action.RETRY else 0
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
        announcement_embed(stream, user_info),
        channel_id,
        watch_button(twitch_url(stream.user_login)),
        content=mention(channel_id),
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


def _stored_start(alert: LiveAlert) -> str:
    """The stored start as an aware timestamp, whatever the driver handed back.

    pendulum.instance reads a naive datetime as UTC, which is what the column
    means; parse_rfc3339 will not accept one without a zone.
    """
    return pendulum.instance(alert.stream_started_at).isoformat()


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
        _stored_start(alert),
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
            _stored_start(alert),
        )
        await asyncio.sleep(1)
