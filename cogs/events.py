from collections.abc import Awaitable

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

from db import repository
from errors import report
from services import audit
from services.config import config
from services.duration import get_ordinal_suffix
from services.present import get_discriminator, get_pfp
from services.send import send_embed


class Events(Cog):
    def __init__(self, bot: Bot) -> None:
        self.bot = bot

    async def _safe_db_operation(
        self, operation: str, write: Awaitable[object]
    ) -> None:
        """Run a database write, reporting failures instead of raising.

        Takes the call already made rather than a function and its arguments.
        Passed as `*args: object` they were checked against nothing, so
        upsert_message's six could be reordered and pyright would agree; written
        out at the call site they are checked as ordinary arguments.
        """
        try:
            await write
        except Exception as e:  # noqa: BLE001
            await report(e, f"Failed to {operation}")

    async def _get_audit_user(
        self, guild_id: int | None, action: discord.AuditLogAction
    ) -> User | Member | None:
        """Fetch the first matching audit log entry user for a given action."""
        if guild_id is None:
            return None
        guild = self.bot.get_guild(guild_id)
        if guild is None:
            return None
        async for entry in guild.audit_logs(limit=1, action=action):
            return entry.user
        return None

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
        try:
            if self._is_bot_message(message):
                return

            await self._store_message(message)

            reply = config.auto_response(message.content)
            if reply is not None:
                await message.channel.send(reply)
        except Exception as e:  # noqa: BLE001
            await report(e, "Fatal error with on_message event")

    @Cog.listener()
    async def on_member_join(self, member: Member) -> None:
        try:
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
        except Exception as e:  # noqa: BLE001
            await report(e, "Fatal error with on_member_join event")

    @Cog.listener()
    async def on_raw_member_remove(self, payload: RawMemberRemoveEvent) -> None:
        try:
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
        except Exception as e:  # noqa: BLE001
            await report(e, "Fatal error with on_raw_member_remove event")

    @Cog.listener()
    async def on_command_error(self, ctx: Context, error: CommandError) -> None:
        try:
            await audit.command_failed(ctx, error)
        except Exception as e:  # noqa: BLE001
            await report(e, "Fatal error with on_command_error event")

    @Cog.listener()
    async def on_member_update(self, before: Member, after: Member) -> None:
        try:
            if get_pfp(before) != get_pfp(after):
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
        except Exception as e:  # noqa: BLE001
            await report(e, "Fatal error with on_member_update event")

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
        try:
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

        except Exception as e:  # noqa: BLE001
            await report(e, "Fatal error with on_raw_message_edit event")

    @Cog.listener()
    async def on_raw_message_delete(self, payload: RawMessageDeleteEvent) -> None:
        try:
            if self._is_bot_message(payload.cached_message):
                return

            user_who_deleted = await self._get_audit_user(
                payload.guild_id, discord.AuditLogAction.message_delete
            )
            channel = self.bot.get_channel(payload.channel_id)
            message = payload.cached_message

            if message is None:
                await audit.message_deleted_uncached(
                    content=await self._get_message_content(payload.message_id),
                    message_id=payload.message_id,
                    deleted_by=user_who_deleted,
                    channel=channel,
                )
            else:
                await audit.message_deleted(
                    content=message.content,
                    attachments=message.attachments,
                    message_id=payload.message_id,
                    author=message.author,
                    deleted_by=user_who_deleted,
                    channel=channel,
                )

            await self._safe_db_operation(
                f"delete message {payload.message_id}",
                repository.delete_message(payload.message_id),
            )
        except Exception as e:  # noqa: BLE001
            await report(e, "Fatal error with on_raw_message_delete event")

    @Cog.listener()
    async def on_raw_bulk_message_delete(
        self, payload: RawBulkMessageDeleteEvent
    ) -> None:
        try:
            user_who_deleted = await self._get_audit_user(
                payload.guild_id, discord.AuditLogAction.message_bulk_delete
            )

            await audit.bulk_deleted(
                count=len(payload.message_ids),
                deleted_by=user_who_deleted,
                channel=self.bot.get_channel(payload.channel_id),
            )

            for message_id in payload.message_ids:
                await self._safe_db_operation(
                    f"delete message {message_id}",
                    repository.delete_message(message_id),
                )
        except Exception as e:  # noqa: BLE001
            await report(e, "Fatal error with on_raw_bulk_message_delete event")

    @Cog.listener()
    async def on_member_ban(self, guild: Guild, user: User | Member) -> None:
        try:
            await audit.banned(user)
        except Exception as e:  # noqa: BLE001
            await report(e, "Fatal error with on_member_ban event")

    @Cog.listener()
    async def on_member_unban(self, guild: Guild, user: User | Member) -> None:
        try:
            await audit.unbanned(user)
        except Exception as e:  # noqa: BLE001
            await report(e, "Fatal error with on_member_unban event")

    @Cog.listener()
    async def on_invite_create(self, invite: Invite) -> None:
        try:
            await audit.invite_created(invite)
        except Exception as e:  # noqa: BLE001
            await report(e, "Fatal error with on_invite_create event")

    @Cog.listener()
    async def on_invite_delete(self, invite: Invite) -> None:
        try:
            await audit.invite_deleted(invite)
        except Exception as e:  # noqa: BLE001
            await report(e, "Fatal error with on_invite_delete event")

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
