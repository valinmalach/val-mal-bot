"""Who deleted a message, read back from the audit log."""

from collections.abc import Callable, Iterable
from datetime import timedelta

import discord
import pendulum
from discord import Member, User
from discord.ext.commands import Bot

from valmal.core.errors import notify

# Discord writes no audit entry when people delete their own messages, and folds
# a moderator's repeat deletions of one author in one channel into one entry by
# raising its count, so the newest entry is usually about some earlier deletion.
# A deletion is attributed only to a recent entry about it that still has a
# counted deletion no earlier event claimed; otherwise the deleter is left
# unnamed. Claims live in memory, so for one window after a restart an earlier
# moderator's entry can still be claimed.
_AUDIT_WINDOW = timedelta(minutes=5)
# A ceiling, not a fetch size: the lookup asks Discord only for entries inside the
# window, which is usually none or a few.
_AUDIT_LOOKBACK = 100

# Not a `type` statement: PEP 695 syntax blinds Sourcery to the whole file.
Matches = Callable[[discord.AuditLogEntry], bool]


def about_message(channel_id: int, author_id: int) -> Matches:
    def matches(entry: discord.AuditLogEntry) -> bool:
        channel = getattr(entry.extra, "channel", None)
        return (
            channel is not None
            and channel.id == channel_id
            and getattr(entry.target, "id", None) == author_id
        )

    return matches


def about_purge(channel_id: int, count: int) -> Matches:
    """A bulk deletion's entry targets the channel itself and counts its messages,
    which usually tells two purges of one channel apart."""
    return lambda entry: (
        getattr(entry.target, "id", None) == channel_id
        and getattr(entry.extra, "count", None) == count
    )


async def recent_entries(
    bot: Bot, guild_id: int | None, action: discord.AuditLogAction
) -> list[discord.AuditLogEntry]:
    if guild_id is None:
        return []
    guild = bot.get_guild(guild_id)
    if guild is None:
        return []
    entries = [
        entry
        async for entry in guild.audit_logs(
            limit=_AUDIT_LOOKBACK,
            action=action,
            after=pendulum.now() - _AUDIT_WINDOW,
            # Oldest first is what sends `after` to Discord; newest first
            # fetches a whole page and filters it here. It is also the order
            # the deletions' events arrive in, so each claims its own entry.
            oldest_first=True,
        )
    ]
    if len(entries) >= _AUDIT_LOOKBACK:
        # Oldest first, so what a full page leaves out is the newest entries,
        # the one for this deletion among them.
        await notify(
            f"The audit log held {_AUDIT_LOOKBACK} or more {action.name} entries"
            " from the last five minutes, so some deletions may be logged"
            " without who deleted them.",
            key=f"audit-lookback:{action.name}",
        )
    return entries


class Claims:
    def __init__(self) -> None:
        # Entry id -> how many of its counted deletions events have claimed.
        self._attributed: dict[int, int] = {}

    def deleter(
        self, entries: Iterable[discord.AuditLogEntry], matches: Matches, size: int
    ) -> User | Member | None:
        """The first matching entry with `size` counted deletions still unclaimed.

        `size` is how many this event accounts for: one message, or a whole purge.
        No await between the check and the claim, so two deletions handled at once
        cannot both take the same one. Records are kept for twice the lookup
        window, so an entry a lookup can still fetch has not lost its claims.
        """
        cutoff = pendulum.now() - 2 * _AUDIT_WINDOW
        self._attributed = {
            entry_id: claimed
            for entry_id, claimed in self._attributed.items()
            if discord.utils.snowflake_time(entry_id) >= cutoff
        }
        for entry in entries:
            count: int = getattr(entry.extra, "count", None) or 1
            claimed = self._attributed.get(entry.id, 0) + size
            if matches(entry) and claimed <= count:
                self._attributed[entry.id] = claimed
                return entry.user
        return None
