"""The main broadcaster being live: at most one at a time.

Owns what only applies while that one account is streaming — the shoutout queue
and the ad-break warning — which a live alert does not, since alerts exist for
every broadcaster the bot announces.
"""

import asyncio
import logging

import pendulum

from background import fire_and_forget
from errors import report
from models import Stream
from services.config import config
from services.twitch.api import get_ad_schedule
from services.twitch.chat import say_template
from services.twitch.shoutout_queue import shoutout_queue

logger = logging.getLogger(__name__)

# Keyed by broadcaster because the EventSub subscription is, even though only
# the session's own warning is ever stood down.
_ad_break_tasks: dict[str, asyncio.Task] = {}


def is_main_broadcaster(broadcaster_id: str | int) -> bool:
    return str(broadcaster_id) == config.setting("twitch_broadcaster_id")


async def began(broadcaster_id: int, stream: Stream) -> None:
    """Greet chat and bring the session's helpers up. Does not raise.

    Each line stands alone, and neither can fail the caller: the live alert is
    posted after this, and a greeting Twitch refused is not worth losing it over.
    """
    if not is_main_broadcaster(broadcaster_id):
        return

    fire_and_forget(shoutout_queue.activate(), name="shoutout-queue")
    await say_template(broadcaster_id, "twitch_stream_greeting")
    await say_template(
        broadcaster_id,
        "twitch_stream_announce",
        name=stream.user_name,
        game=stream.game_name,
        title=stream.title,
    )


def ended(broadcaster_id: str | int) -> None:
    """Stand the session's helpers down. A no-op for any other broadcaster."""
    if not is_main_broadcaster(broadcaster_id):
        return

    shoutout_queue.deactivate()
    cancel_ad_break_warning(broadcaster_id)


def cancel_ad_break_warning(broadcaster_id: str | int) -> None:
    task = _ad_break_tasks.get(str(broadcaster_id))
    if task and not task.done():
        task.cancel()


def _forget_ad_break_task(broadcaster_id: str, finished: asyncio.Task) -> None:
    # Identity-checked: a cancelled task finishes after its replacement is
    # registered, and an unconditional pop would drop the live one.
    if _ad_break_tasks.get(broadcaster_id) is finished:
        del _ad_break_tasks[broadcaster_id]


def schedule_ad_break_warning(broadcaster_id: str) -> None:
    """Replace this broadcaster's pending warning with one for the next ad break."""
    cancel_ad_break_warning(broadcaster_id)

    task = asyncio.create_task(_warn_before_next_ad(broadcaster_id))
    _ad_break_tasks[broadcaster_id] = task
    task.add_done_callback(
        lambda finished, bid=broadcaster_id: _forget_ad_break_task(bid, finished)
    )


async def _warn_before_next_ad(broadcaster_id: str) -> None:
    try:
        ad_schedule = await get_ad_schedule(int(broadcaster_id))
        if not ad_schedule:
            return

        notify_time = pendulum.from_timestamp(ad_schedule.next_ad_at).subtract(
            minutes=5
        )
        wait_seconds = (notify_time - pendulum.now(tz=pendulum.UTC)).total_seconds()
        if wait_seconds > 0:
            await asyncio.sleep(wait_seconds)
            await say_template(broadcaster_id, "twitch_ad_break_warning")
    except asyncio.CancelledError:
        logger.info(
            "Cancelled ad break notification task for broadcaster_id=%s", broadcaster_id
        )
        raise
    except Exception as e:  # noqa: BLE001
        await report(e, "Error scheduling next ad break notification")
