"""What each EventSub notification makes the bot do.

Lives here rather than beside the routes because none of it touches HTTP: it
orchestrates the alert, the session, chat and Helix, and only happens to be
triggered by a webhook.
"""

import asyncio
import logging
import time

from errors import notify, report
from models import (
    ChannelAdBreakBeginEventSub,
    ChannelChatMessageEventSub,
    ChannelFollowEventSub,
    ChannelModerateEventSub,
    ChannelRaidEventSub,
    Stream,
    StreamOfflineEventSub,
    StreamOnlineEventSub,
)
from services import get_stream, get_user
from services.config import config
from services.twitch import live_alert, stream_session
from services.twitch.chat import say, say_template
from services.twitch.commands import dispatch
from services.twitch.helix import HelixError

logger = logging.getLogger(__name__)

# Twitch announces the stream before Helix lists it. Bounded by the clock rather
# than by a count of attempts: a lookup that is timing out takes most of a minute
# on its own, so thirty of those is a quarter of an hour, not thirty seconds.
_STREAM_WAIT_SECONDS = 60


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


async def stream_online(event_sub: StreamOnlineEventSub) -> None:
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


async def stream_offline(event_sub: StreamOfflineEventSub) -> None:
    broadcaster_id = int(event_sub.event.broadcaster_user_id)
    try:
        # The payload names no stream, so this handler cannot tell which one
        # ended. It wakes the updater, which can. See docs/adr/0001.
        await live_alert.wake(broadcaster_id)

    except Exception as e:  # noqa: BLE001
        await report(e, f"Error in _stream_offline_task for {broadcaster_id}")


async def channel_chat_message(event_sub: ChannelChatMessageEventSub) -> None:
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


async def channel_follow(event_sub: ChannelFollowEventSub) -> None:
    try:
        await say_template(
            event_sub.event.broadcaster_user_id,
            "twitch_follow_thanks",
            user=event_sub.event.user_name,
        )
    except Exception as e:  # noqa: BLE001
        await report(e, "Error processing Twitch follow webhook task")


async def channel_ad_break_begin(event_sub: ChannelAdBreakBeginEventSub) -> None:
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


async def channel_raid(event_sub: ChannelRaidEventSub) -> None:
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


async def channel_moderate(event_sub: ChannelModerateEventSub) -> None:
    try:
        if event_sub.event.action != "raid" or not stream_session.is_main_broadcaster(
            event_sub.event.broadcaster_user_id
        ):
            return
        await say_template(event_sub.event.broadcaster_user_id, "twitch_raid_farewell")
    except Exception as e:  # noqa: BLE001
        await report(e, "Error processing Twitch moderate webhook task")
