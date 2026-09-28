import asyncio
from collections.abc import Awaitable, Callable, Iterable
from datetime import timedelta

import discord
import pendulum
from discord import (
    Embed,
    Guild,
    Invite,
    Member,
    Message,
    RawBulkMessageDeleteEvent,
    RawMemberRemoveEvent,
    RawMessageDeleteEvent,
    RawMessageUpdateEvent,
    User,
)
from discord.ext.commands import Bot, Cog, CommandError, Context
from pendulum import DateTime

from valmal.bot import audit
from valmal.bot.duration import get_ordinal_suffix
from valmal.bot.present import get_discriminator, get_pfp
from valmal.bot.send import send_embed
from valmal.core.config import config
from valmal.core.errors import report
from valmal.db import repository

# Discord writes no audit entry when people delete their own messages, and folds
# a moderator's repeat deletions into one entry by raising its count, so the
# newest entry is usually about some earlier deletion. Only a recent entry that is
# about this deletion, and new or counted up since it last named anyone, names the
# deleter; otherwise the deleter is left unnamed rather than guessed.
_AUDIT_WINDOW = timedelta(minutes=5)
_AUDIT_LOOKBACK = 10

# Not a `type` statement: PEP 695 syntax blinds Sourcery to the whole file.
_Matches = Callable[[discord.AuditLogEntry], bool]


def _about_message(channel_id: int, author_id: int | None) -> _Matches:
    """An entry for this channel, and for this author when the author is known."""

    def matches(entry: discord.AuditLogEntry) -> bool:
        channel = getattr(entry.extra, "channel", None)
        return (
            channel is not None
            and channel.id == channel_id
            and (author_id is None or getattr(entry.target, "id", None) == author_id)
        )

    return matches


def _about_purge(channel_id: int) -> _Matches:
    """A bulk deletion's entry targets the channel itself."""
    return lambda entry: getattr(entry.target, "id", None) == channel_id


class Events(Cog):
    def __init__(self, bot: Bot) -> None:
        self.bot = bot
        # Entry id -> the count it had when it last named a deleter.
        self._attributed: dict[int, int] = {}

    def _deleter(
        self, entries: Iterable[discord.AuditLogEntry], matches: _Matches
    ) -> User | Member | None:
        """The first matching entry that records a deletion not yet attributed.

        No await between the check and the record, so two deletions handled at
        once cannot both claim one entry. An entry older than the window is never
        fetched again, so its record is dropped.
        """
        cutoff = pendulum.now() - _AUDIT_WINDOW
        self._attributed = {
            entry_id: count
            for entry_id, count in self._attributed.items()
            if discord.utils.snowflake_time(entry_id) >= cutoff
        }
        for entry in entries:
            count: int = getattr(entry.extra, "count", None) or 1
            if matches(entry) and count > self._attributed.get(entry.id, 0):
                self._attributed[entry.id] = count
                return entry.user
        return None

    async def _safe_db_operation(
        self, operation: str, write: Awaitable[object]
    ) -> None:
        """Run a database write, reporting failures instead of raising.

        Takes the call already made rather than a function and its arguments, so
        pyright checks them as ordinary arguments.
        """
        try:
            await write
        except Exception as e:  # noqa: BLE001
            await report(e, f"Failed to {operation}")

    async def _recent_audit_entries(
        self, guild_id: int | None, action: discord.AuditLogAction
    ) -> list[discord.AuditLogEntry]:
        if guild_id is None:
            return []
        guild = self.bot.get_guild(guild_id)
        if guild is None:
            return []
        return [
            entry
            async for entry in guild.audit_logs(
                limit=_AUDIT_LOOKBACK,
                action=action,
                after=pendulum.now() - _AUDIT_WINDOW,
                oldest_first=False,
            )
        ]

    async def _store_message(self, message: Message) -> None:
        guild = message.guild
        await self._safe_db_operation(
            f"store message {message.id}",
            repository.upsert_message(
                message.id,
                message.content,
                config.setting("guild_id") if guild is None else guild.id,
                message.author.id,
                message.channel.id,
                [attachment.url for attachment in message.attachments],
            ),
        )

    @Cog.listener()
    async def on_message(self, message: Message) -> None:
        if self._is_bot_message(message):
            return

        # The reply goes first so it does not wait on the database, and the store
        # sits in finally so a send that fails still records the message.
        try:
            reply = config.auto_response(message.content)
            if reply is not None:
                # No mentions: anyone can trigger a reply, and it may hold a role
                # mention.
                await message.channel.send(
                    reply, allowed_mentions=discord.AllowedMentions.none()
                )
        finally:
            await self._store_message(message)

    @Cog.listener()
    async def on_member_join(self, member: Member) -> None:
        url = get_pfp(member)
        embed = Embed(
            description=config.template("discord_welcome", mention=member.mention),
            color=config.color("embed_color_welcome"),
            timestamp=pendulum.now(),
        ).set_author(name=f"{member.name}{get_discriminator(member)}", icon_url=url)
        embed = embed.set_image(url=url)
        # None for a guild the gateway sent without one, which no ordinal
        # can be made of; the footer is decoration, so it is left off.
        if member.guild.member_count is not None:
            embed = embed.set_footer(
                text=f"{get_ordinal_suffix(member.guild.member_count)} member"
            )
        await send_embed(
            embed,
            config.channel("welcome"),
        )

        await audit.member_joined(member)

        await self._safe_db_operation(
            f"insert user {member.name} ({member.id})",
            repository.upsert_username(member.id, member.name),
        )

    @Cog.listener()
    async def on_raw_member_remove(self, payload: RawMemberRemoveEvent) -> None:
        member = payload.user
        url = get_pfp(member)
        embed = Embed(
            description=config.template("discord_goodbye", mention=member.mention),
            color=config.color("embed_color_goodbye"),
            timestamp=pendulum.now(),
        ).set_author(name=f"{member.name}{get_discriminator(member)}", icon_url=url)
        embed = embed.set_image(url=url)
        await send_embed(
            embed,
            config.channel("welcome"),
        )

        await audit.member_left(member)

        await self._safe_db_operation(
            f"remove user {member.name} ({member.id})",
            repository.delete_user(member.id),
        )

    @Cog.listener()
    async def on_command_error(self, ctx: Context[Bot], error: CommandError) -> None:
        await audit.command_failed(ctx, error)

    @Cog.listener()
    async def on_user_update(self, before: User, after: User) -> None:
        """A new global avatar, which on_member_update cannot see.

        discord.py's member copy shares its User with the live member and updates
        it in place, so both sides of a member update hold the new avatar. This
        event's `before` is a real copy. It counts only where the guild shows it:
        a member with a guild avatar of their own looks no different.
        """
        if before.avatar == after.avatar:
            return
        guild = self.bot.get_guild(config.setting("guild_id"))
        member = guild.get_member(after.id) if guild is not None else None
        if member is not None and member.guild_avatar is None:
            await audit.pfp_changed(member)

    @Cog.listener()
    async def on_member_update(self, before: Member, after: Member) -> None:
        if before.guild_avatar != after.guild_avatar:
            await audit.pfp_changed(after)

        added = [role for role in after.roles if role not in before.roles]
        removed = [role for role in before.roles if role not in after.roles]
        if added:
            await audit.role_added(after, added)
        if removed:
            await audit.role_removed(after, removed)

        if before.nick != after.nick:
            await audit.nickname_changed(
                after,
                before.name if before.nick is None else before.nick,
                after.name if after.nick is None else after.nick,
            )

        await self._handle_timeout_changes(before, after)

    async def _handle_timeout_changes(self, before: Member, after: Member) -> None:
        """A timeout that has already expired is not one, so both ends are compared."""
        before_until = (
            pendulum.instance(before.timed_out_until)
            if before.timed_out_until
            else None
        )
        after_until = (
            pendulum.instance(after.timed_out_until) if after.timed_out_until else None
        )
        was_timed_out = self._is_currently_timed_out(before_until)
        is_timed_out = self._is_currently_timed_out(after_until)

        if not was_timed_out and is_timed_out and after_until:
            await audit.timed_out(after, after_until)
        elif was_timed_out and not is_timed_out:
            await audit.timeout_lifted(after)

    def _is_currently_timed_out(self, timeout_until: DateTime | None) -> bool:
        """Check if a member is currently timed out."""
        return timeout_until is not None and timeout_until > pendulum.now()

    def _is_bot_message(self, *messages: Message | None) -> bool:
        """Whether any of these is a message the bot itself sent.

        Variadic because the three callers hold different things: a live
        Message, an edit payload's before and after, and a delete payload's
        cached copy alone. Spelling it three times is how the delete path came
        to check only half of what the edit path checks.
        """
        return any(m is not None and m.author == self.bot.user for m in messages)

    @Cog.listener()
    async def on_raw_message_edit(self, payload: RawMessageUpdateEvent) -> None:
        if self._is_bot_message(payload.message, payload.cached_message):
            return

        before = payload.cached_message
        after = payload.message

        if before and before.pinned != after.pinned:
            await audit.pin_changed(after)

        # Message.content is a slot set in __init__, so reading it cannot
        # raise; a payload without it fails inside discord.py before dispatch.
        before_content = (
            before.content if before else await self._get_message_content(after.id)
        )

        if before_content == after.content:
            return

        await audit.message_edited(after, before_content)
        await self._store_message(after)

    @Cog.listener()
    async def on_raw_message_delete(self, payload: RawMessageDeleteEvent) -> None:
        if self._is_bot_message(payload.cached_message):
            return

        lookup = self._recent_audit_entries(
            payload.guild_id, discord.AuditLogAction.message_delete
        )
        channel = self.bot.get_channel(payload.channel_id)
        message = payload.cached_message

        if message is None:
            # The stored row is the only place an uncached message's author is.
            # Neither lookup needs the other, since the entries are matched to the
            # author afterwards, so they run together.
            entries, stored = await asyncio.gather(
                lookup, repository.get_message(payload.message_id)
            )
            await audit.message_deleted_uncached(
                content=stored.contents if stored is not None else None,
                message_id=payload.message_id,
                deleted_by=self._deleter(
                    entries,
                    _about_message(
                        payload.channel_id,
                        stored.author_id if stored is not None else None,
                    ),
                ),
                channel=channel,
            )
        else:
            entries = await lookup
            await audit.message_deleted(
                content=message.content,
                attachments=message.attachments,
                message_id=payload.message_id,
                author=message.author,
                deleted_by=self._deleter(
                    entries, _about_message(payload.channel_id, message.author.id)
                ),
                channel=channel,
            )

        await self._safe_db_operation(
            f"delete message {payload.message_id}",
            repository.delete_message(payload.message_id),
        )

    @Cog.listener()
    async def on_raw_bulk_message_delete(
        self, payload: RawBulkMessageDeleteEvent
    ) -> None:
        entries = await self._recent_audit_entries(
            payload.guild_id, discord.AuditLogAction.message_bulk_delete
        )

        await audit.bulk_deleted(
            count=len(payload.message_ids),
            deleted_by=self._deleter(entries, _about_purge(payload.channel_id)),
            channel=self.bot.get_channel(payload.channel_id),
        )

        await self._safe_db_operation(
            f"delete {len(payload.message_ids)} bulk-deleted messages",
            repository.delete_messages(payload.message_ids),
        )

    @Cog.listener()
    async def on_member_ban(self, guild: Guild, user: User | Member) -> None:
        await audit.banned(user)

    @Cog.listener()
    async def on_member_unban(self, guild: Guild, user: User | Member) -> None:
        await audit.unbanned(user)

    @Cog.listener()
    async def on_invite_create(self, invite: Invite) -> None:
        await audit.invite_created(invite)

    @Cog.listener()
    async def on_invite_delete(self, invite: Invite) -> None:
        await audit.invite_deleted(invite)

    async def _get_message_content(self, message_id: int) -> str | None:
        """None when there is no row, which is not the same as a row storing "".

        An attachment-only message is stored empty, and calling that a cache miss
        both mislabels the delete and makes an edit that changed nothing look like
        one that did.
        """
        stored = await repository.get_message(message_id)
        return stored.contents if stored is not None else None


async def setup(bot: Bot) -> None:
    await bot.add_cog(Events(bot))
