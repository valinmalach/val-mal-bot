import asyncio
import contextlib
import logging
from typing import ClassVar, Self, cast

import httpx
import pendulum

from background import fire_and_forget
from errors import notify, report
from models import User
from services.twitch.api import get_user, send_shoutout
from services.twitch.helix import HelixError

logger = logging.getLogger(__name__)

# Twitch: 1 shoutout to the same channel every 60 minutes; stay strictly above that window.
_MIN_SAME_TARGET_COOLDOWN_SECONDS = 61 * 60
# Helix: 1 shoutout every 2 minutes (global); small buffer after success.
_GLOBAL_SHOUTOUT_INTERVAL_SECONDS = 125
# A lookup that failed says nothing about the target, so hold it back long
# enough that a sustained outage costs a handful of attempts an hour.
_LOOKUP_RETRY_BACKOFF_SECONDS = 300


class TwitchShoutoutQueue:
    _instance: ClassVar[TwitchShoutoutQueue | None] = None
    _shoutout_queue: ClassVar[list[tuple[str, str]]] = []
    _last_shoutout_by_target_id: ClassVar[dict[str, pendulum.DateTime]] = {}
    _next_attempt_allowed_by_target_id: ClassVar[dict[str, pendulum.DateTime]] = {}

    def __new__(cls) -> Self:
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cast("Self", cls._instance)

    def add_to_queue(self, login: str, user_id: str) -> None:
        """Queue a target, refusing what the drainer could not act on.

        The one caller passes fields off a validated Helix model, so this only
        fires on a caller that does not - and finding that out here beats
        finding it out as an int() failure inside the drainer.
        """
        # isdecimal, not isdigit: isdigit passes a superscript that int() then
        # rejects inside the drainer, after the pair has left the queue.
        # isascii because a Twitch id is ASCII - int() would take an
        # Arabic-Indic numeral quite happily.
        if not login or not user_id.isascii() or not user_id.isdecimal():
            fire_and_forget(
                notify(
                    f"Refused a shoutout for {login!r} (id {user_id!r}):"
                    f" not a Twitch login and numeric id.",
                    key="shoutout-bad-target",
                ),
                name="shoutout-bad-target",
            )
            return

        if all(uid != user_id for _, uid in self._shoutout_queue):
            self._shoutout_queue.append((login, user_id))

    def _can_shoutout_target(self, user_id: str) -> bool:
        now = pendulum.now()
        next_ok = self._next_attempt_allowed_by_target_id.get(user_id)
        if next_ok is not None and now < next_ok:
            return False
        last = self._last_shoutout_by_target_id.get(user_id)
        if last is None:
            return True
        return (now - last).total_seconds() >= _MIN_SAME_TARGET_COOLDOWN_SECONDS

    def _get_next_available_pair(self) -> tuple[str, str] | None:
        return next(
            (
                (login, uid)
                for login, uid in self._shoutout_queue
                if self._can_shoutout_target(uid)
            ),
            None,
        )

    def _wait_until_from_429(self, response: httpx.Response) -> pendulum.DateTime:
        now = pendulum.now()
        if ra := response.headers.get("Retry-After"):
            with contextlib.suppress(ValueError):
                return now.add(seconds=int(ra))
        if rr := response.headers.get("Ratelimit-Reset"):
            with contextlib.suppress(ValueError):
                reset = pendulum.from_timestamp(int(rr), tz=pendulum.UTC)
                if reset > now:
                    return reset
        return now.add(seconds=_MIN_SAME_TARGET_COOLDOWN_SECONDS)

    async def drain(self) -> None:
        """Send queued shoutouts for as long as the process runs.

        One task, started once, rather than one per stream. Tying its lifetime
        to the stream is what used to need a generation counter: a drainer told
        to stand down could still be inside a two-minute sleep when the next
        stream started one alongside it.

        Each pass asks the session, rather than draining whatever it finds. The
        queue is only fed while a session is live and is emptied when one ends,
        but a target whose lookup failed re-queues itself afterwards, and
        without this that straggler would be shouted out into an offline chat.
        """
        # Deferred: the session imports this module to empty the queue when a
        # stream ends, so importing it at module level would be a cycle. By the
        # time anything calls this, every module is loaded.
        from services.twitch import stream_session

        while True:
            try:
                if not stream_session.is_live():
                    await asyncio.sleep(5)
                    continue
                await self._drain_once()
            except Exception as e:  # noqa: BLE001
                # One pass, not the queue. The Helix failures inside handle
                # themselves; this is for what was never anticipated, and
                # without it one of those ended shoutouts for the whole stream.
                await report(e, "Shoutout queue: a pass failed unexpectedly")
                await asyncio.sleep(5)

    async def _drain_once(self) -> None:
        """Send at most one shoutout, or wait. Its own unit so a failure is too."""
        if len(self._shoutout_queue) == 0:
            await asyncio.sleep(5)
            return

        pair = self._get_next_available_pair()

        if pair is None:
            await asyncio.sleep(5)
            return

        login, user_id_str = pair
        self._shoutout_queue.remove(pair)

        user = await self._look_up(login, user_id_str)
        if user is not None:
            await self._send(login, user_id_str, user)

    async def _look_up(self, login: str, user_id_str: str) -> User | None:
        """The target, or None with the reason already said."""
        try:
            user = await get_user(int(user_id_str))
        except HelixError as e:
            # One unreachable lookup must not end the queue, nor spin on it. The
            # back-off has to outlast the wait that follows it: setting it to the
            # interval the loop then slept for meant it had already expired by
            # the time the loop looked again. No shoutout went out, so the global
            # interval does not apply here and the queue is free to try a
            # different target.
            await notify(
                f"Could not look up {login} for a shoutout: {e}",
                key=f"shoutout-lookup:{user_id_str}",
            )
            self._next_attempt_allowed_by_target_id[user_id_str] = pendulum.now().add(
                seconds=_LOOKUP_RETRY_BACKOFF_SECONDS
            )
            self.add_to_queue(login, user_id_str)
            return None

        if not user:
            logger.warning("User id %r (%r) not found for shoutout", user_id_str, login)
            await notify(
                f"User {login} not found for shoutout",
                key=f"shoutout-not-found:{user_id_str}",
            )
            return None

        return user

    async def _send(self, login: str, user_id_str: str, user: User) -> None:
        """Shout the target out, and wait out the global interval either way."""
        try:
            await send_shoutout(user.id)
        except HelixError as e:
            if e.status == 429 and e.response is not None:
                wait_until = self._wait_until_from_429(e.response)
                self._next_attempt_allowed_by_target_id[user_id_str] = wait_until
                self.add_to_queue(login, user_id_str)
                logger.warning(
                    "Shoutout rate limited for %r (id=%r), re-queued;"
                    " next attempt after %s",
                    login,
                    user_id_str,
                    wait_until,
                )
            else:
                await report(e, f"Failed to send shoutout to {login}")
        else:
            self._last_shoutout_by_target_id[user_id_str] = pendulum.now()
            self._next_attempt_allowed_by_target_id.pop(user_id_str, None)

        await asyncio.sleep(_GLOBAL_SHOUTOUT_INTERVAL_SECONDS)

    def clear(self) -> None:
        """Drop what is pending, because it belonged to a stream that is over.

        Not `_last_shoutout_by_target_id`: that mirrors a cooldown Twitch keeps
        for an hour whatever this process does, so forgetting it would only earn
        a 429 on the next stream.
        """
        self._shoutout_queue.clear()
        self._next_attempt_allowed_by_target_id.clear()


shoutout_queue = TwitchShoutoutQueue()
