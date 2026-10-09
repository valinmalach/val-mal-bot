import asyncio
from collections import Counter
from collections.abc import Awaitable, Generator, Iterable
from contextlib import contextmanager

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
from valmal.bot.deleters import Claims, about_message, about_purge, recent_entries
from valmal.bot.duration import get_ordinal_suffix
from valmal.bot.present import get_discriminator, get_pfp
from valmal.bot.send import send_embed
from valmal.core.config import config
from valmal.core.errors import report
from valmal.db import repository


class Events(Cog):
    def __init__(self, bot: Bot) -> None:
        self.bot = bot
        self._claims = Claims()
        # A store can land after its message's delete, so a delete marks the ids
        # with a handler in flight that may still store them, and the store takes
        # its row back out. Kept only while that handler runs.
        self._storing: Counter[int] = Counter()
        self._deleted: set[int] = set()

    @contextmanager
    def _may_store(self, message_id: int) -> Generator[None]:
        """Entered before the handler's first await, so a later delete sees it."""
        self._storing[message_id] += 1
        try:
            yield
        finally:
            self._storing[message_id] -= 1
            if not self._storing[message_id]:
                del self._storing[message_id]
                self._deleted.discard(message_id)

    def _mark_deleted(self, message_ids: Iterable[int]) -> None:
        self._deleted.update(i for i in message_ids if i in self._storing)

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
        if message.id in self._deleted:
            await self._safe_db_operation(
                f"delete message {message.id}",
                repository.delete_message(message.id),
            )

    @Cog.listener()
    async def on_message(self, message: Message) -> None:
        if self._is_bot_message(message):
            return

        with self._may_store(message.id):
            reply = config.auto_response(message.content)
            if reply is None:
                await self._store_message(message)
                return

            # Together: the reply does not wait on the database, and the row does not
            # wait on Discord, where a delete handled meanwhile would find nothing to
            # remove. A failure is raised once both have finished, the store's first.
            results = await asyncio.gather(
                self._store_message(message),
                # No mentions: anyone can trigger a reply, and it may hold a role mention.
                message.channel.send(
                    reply, allowed_mentions=discord.AllowedMentions.none()
                ),
                return_exceptions=True,
            )
            for result in results:
                if isinstance(result, BaseException):
                    raise result

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

        with self._may_store(payload.message.id):
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
        self._mark_deleted([payload.message_id])

        lookup = recent_entries(
            self.bot, payload.guild_id, discord.AuditLogAction.message_delete
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
                # Without an author any entry in the channel would fit, so nobody
                # is named rather than a guess.
                deleted_by=None
                if stored is None
                else self._claims.deleter(
                    entries, about_message(payload.channel_id, stored.author_id), 1
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
                deleted_by=self._claims.deleter(
                    entries, about_message(payload.channel_id, message.author.id), 1
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
        self._mark_deleted(payload.message_ids)
        entries = await recent_entries(
            self.bot, payload.guild_id, discord.AuditLogAction.message_bulk_delete
        )

        await audit.bulk_deleted(
            count=len(payload.message_ids),
            deleted_by=self._claims.deleter(
                entries,
                about_purge(payload.channel_id, len(payload.message_ids)),
                len(payload.message_ids),
            ),
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
