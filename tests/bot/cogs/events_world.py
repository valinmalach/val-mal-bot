import itertools
from collections.abc import AsyncIterator, Callable
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import discord
import pendulum
import pytest

from tests.bot.audit.support import message
from valmal.bot import audit
from valmal.bot.cogs import events
from valmal.bot.cogs.events import Events
from valmal.core.config import config

NOW = pendulum.datetime(2026, 6, 15, 12)

AUDIT_FUNCTIONS = (
    "member_joined",
    "member_left",
    "banned",
    "unbanned",
    "invite_created",
    "invite_deleted",
    "role_added",
    "role_removed",
    "nickname_changed",
    "pfp_changed",
    "timed_out",
    "timeout_lifted",
    "message_edited",
    "pin_changed",
    "message_deleted",
    "message_deleted_uncached",
    "bulk_deleted",
    "command_failed",
)


class EventsWorld:
    """Every collaborator of the Events cog, recorded and scriptable."""

    def __init__(self) -> None:
        self.audit: list[tuple[str, tuple[Any, ...], dict[str, Any]]] = []
        self.order: list[str] = []
        self.stored: list[tuple[Any, ...]] = []
        self.usernames: list[tuple[int, str]] = []
        self.removed_users: list[int] = []
        self.deleted: list[int] = []
        self.deleted_batches: list[set[int]] = []
        self.rows: dict[int, Any] = {}
        self.fail: set[str] = set()
        self.reported: list[str] = []
        self.sent_embeds: list[tuple[discord.Embed, int]] = []
        self.audit_entries: list[Any] = []
        self.audit_asked: list[dict[str, Any]] = []
        self.guilds: dict[int, Any] = {}
        self.channels: dict[int, Any] = {}
        self.bot_user = SimpleNamespace(id=1)
        self.reply: str | None = None

    def calls(self, name: str) -> list[tuple[tuple[Any, ...], dict[str, Any]]]:
        return [(a, k) for n, a, k in self.audit if n == name]

    @property
    def names(self) -> list[str]:
        return [n for n, _, _ in self.audit]

    async def entries(self, **kwargs: Any) -> AsyncIterator[Any]:
        self.audit_asked.append(kwargs)
        for entry in self.audit_entries:
            yield entry

    def store(self, *args: Any) -> None:
        self.stored.append(args)

    def rename(self, user_id: int, name: str) -> None:
        self.usernames.append((user_id, name))

    def remove_user(self, user_id: int) -> None:
        self.removed_users.append(user_id)

    def delete(self, message_id: int) -> None:
        self.deleted.append(message_id)

    def delete_batch(self, message_ids: set[int]) -> None:
        self.deleted_batches.append(message_ids)

    def auto_response(self, content: str) -> str | None:
        return self.reply


def now(tz: object = None) -> pendulum.DateTime:
    return NOW


def install(monkeypatch: pytest.MonkeyPatch) -> EventsWorld:
    """Replace everything the Events cog reaches for, for the life of one test."""
    world = EventsWorld()

    def recorder(name: str) -> Any:
        async def record(*args: Any, **kwargs: Any) -> None:
            world.order.append(name)
            world.audit.append((name, args, kwargs))

        return record

    def failing(name: str, action: Callable[..., None]) -> Any:
        async def run(*args: Any) -> None:
            world.order.append(name)
            if name in world.fail:
                raise ConnectionError(name)
            action(*args)

        return run

    async def get_message(message_id: int) -> Any:
        return world.rows.get(message_id)

    async def send_embed(embed: discord.Embed, channel_id: int, *a: Any) -> int:
        world.order.append("send")
        world.sent_embeds.append((embed, channel_id))
        return 1

    async def report(exc: Exception, context: str, **_: object) -> None:
        world.reported.append(context)

    repository = events.repository
    for name in AUDIT_FUNCTIONS:
        monkeypatch.setattr(audit, name, recorder(name))
    monkeypatch.setattr(repository, "upsert_message", failing("store", world.store))
    monkeypatch.setattr(
        repository, "upsert_username", failing("username", world.rename)
    )
    monkeypatch.setattr(
        repository, "delete_user", failing("delete_user", world.remove_user)
    )
    monkeypatch.setattr(
        repository, "delete_message", failing("delete_message", world.delete)
    )
    monkeypatch.setattr(
        repository, "delete_messages", failing("delete_messages", world.delete_batch)
    )
    monkeypatch.setattr(repository, "get_message", get_message)
    monkeypatch.setattr(events, "send_embed", send_embed)
    monkeypatch.setattr(events, "report", report)
    monkeypatch.setattr(pendulum, "now", now)
    monkeypatch.setattr(config, "auto_response", world.auto_response)
    monkeypatch.setattr(config, "_channels", {"welcome": 900})
    monkeypatch.setattr(
        config,
        "_settings",
        {"guild_id": 42, "embed_color_welcome": 1, "embed_color_goodbye": 2},
    )
    monkeypatch.setattr(
        config,
        "_templates",
        {"discord_welcome": "welcome {mention}", "discord_goodbye": "bye {mention}"},
    )
    return world


def cog(world: EventsWorld) -> Events:
    def get_guild(guild_id: int) -> Any:
        return world.guilds.get(guild_id)

    def get_channel(channel_id: int) -> Any:
        return world.channels.get(channel_id)

    bot = MagicMock()
    bot.user = world.bot_user
    bot.get_guild = get_guild
    bot.get_channel = get_channel
    return Events(bot)


def guild_with_log(world: EventsWorld, guild_id: int = 5) -> None:
    guild = MagicMock(spec=discord.Guild)
    guild.audit_logs = world.entries
    world.guilds[guild_id] = guild


# Real snowflakes from NOW on, since an attribution is dropped by its entry's age.
_entry_ids = itertools.count(discord.utils.time_snowflake(NOW))


def entry(
    user: Any,
    *,
    channel_id: int = 55,
    target_id: int | None = None,
    count: int = 1,
    id: int | None = None,
) -> Any:
    """An audit entry for a deletion in a channel, of a message by target_id."""
    return SimpleNamespace(
        id=next(_entry_ids) if id is None else id,
        user=user,
        extra=SimpleNamespace(channel=SimpleNamespace(id=channel_id), count=count),
        target=None if target_id is None else SimpleNamespace(id=target_id),
    )


def bulk_entry(user: Any, *, channel_id: int = 55, count: int = 1) -> Any:
    """An audit entry for a bulk deletion, which targets the channel itself."""
    return SimpleNamespace(
        id=next(_entry_ids),
        user=user,
        extra=SimpleNamespace(count=count),
        target=SimpleNamespace(id=channel_id),
    )


def sent(**kwargs: Any) -> Any:
    """A message as the gateway hands it over, with a channel it can answer in."""
    made = message(**kwargs)
    made.id = 9
    made.guild = SimpleNamespace(id=5)
    made.attachments = []
    made.channel.id = 55
    made.channel.send = AsyncMock()
    return made
