import io
import logging
import traceback
from typing import Literal

import discord
import orjson
import pendulum
import polars as pl
from discord import (
    CategoryChannel,
    Embed,
    ForumChannel,
    Guild,
    Invite,
    Member,
    Message,
    Object,
    RawBulkMessageDeleteEvent,
    RawMemberRemoveEvent,
    RawMessageDeleteEvent,
    RawMessageUpdateEvent,
    Role,
    StageChannel,
    TextChannel,
    Thread,
    User,
    VoiceChannel,
)
from discord.abc import PrivateChannel
from discord.ext.commands import Bot, Cog, CommandError, Context
from pendulum import DateTime

from constants import (
    AUDIT_LOGS_CHANNEL,
    BOT_ADMIN_CHANNEL,
    DEFAULT_MISSING_CONTENT,
    GUILD_ID,
    MESSAGES,
    UNKNOWN_USER,
    USERS,
    WELCOME_CHANNEL,
    ErrorDetails,
    UserRecord,
)
from services import (
    delete_row_from_parquet,
    get_age,
    get_channel_mention,
    get_discriminator,
    get_ordinal_suffix,
    get_pfp,
    read_parquet_cached,
    send_embed,
    send_message,
    upsert_row_to_parquet,
)

logger = logging.getLogger(__name__)


class Events(Cog):
    def __init__(self, bot: Bot) -> None:
        self.bot = bot

    def _create_error_details(self, exception: Exception) -> ErrorDetails:
        """Create standardized error details dictionary from an exception."""
        return {
            "type": type(exception).__name__,
            "message": str(exception),
            "args": exception.args,
            "traceback": traceback.format_exc(),
        }

    async def _handle_error(
        self, exception: Exception, context: str, should_raise: bool = False
    ) -> None:
        """Centralized error handling and logging."""
        error_details = self._create_error_details(exception)
        error_msg = f"{context} - Type: {error_details['type']}, Message: {error_details['message']}, Args: {error_details['args']}"
        logger.error(f"{error_msg}\nTraceback:\n{error_details['traceback']}")
        await self._send_error_message(error_msg, error_details["traceback"])
        if should_raise:
            raise

    async def _safe_parquet_operation(
        self, operation: str, func, *args, **kwargs
    ) -> None:
        """Safely execute parquet operations with error handling."""
        try:
            func(*args, **kwargs)
        except Exception as e:
            await self._handle_error(e, f"Failed to {operation}")

    def _base_embed(self, description: str, color: int) -> Embed:
        """Create a base embed with common settings (description, color, timestamp)."""
        return Embed(description=description, color=color, timestamp=pendulum.now())

    def _set_author(
        self, embed: Embed, name: str, discriminator: str, url: str | None
    ) -> Embed:
        """Set the author on an embed using name+discriminator and avatar URL."""
        return embed.set_author(name=f"{name}{discriminator}", icon_url=url)

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

    async def _get_message_object(self, message: Message) -> dict:
        guild = message.guild
        guild_id = GUILD_ID if guild is None else guild.id
        return {
            "id": message.id,
            "contents": message.content,
            "guild_id": guild_id,
            "author_id": message.author.id,
            "channel_id": message.channel.id,
            "attachment_urls": orjson.dumps(
                [attachment.url for attachment in message.attachments]
            ).decode("utf-8"),
        }

    async def _send_error_message(
        self,
        error_msg: str,
        traceback_str: str,
    ) -> None:
        traceback_buffer = io.BytesIO(traceback_str.encode("utf-8"))
        traceback_file = discord.File(traceback_buffer, filename="traceback.txt")
        await send_message(
            error_msg,
            BOT_ADMIN_CHANNEL,
            file=traceback_file,
        )

    @Cog.listener()
    async def on_message(self, message: Message) -> None:
        try:
            if message.author == self.bot.user:
                return

            message_obj = await self._get_message_object(message)
            await self._safe_parquet_operation(
                f"save message {message.id} in parquet",
                upsert_row_to_parquet,
                message_obj,
                MESSAGES,
            )

            content = message.content.lower()
            if content == "ping":
                await message.channel.send("pong")
            elif content == "plap":
                await message.channel.send("clank")
        except Exception as e:
            await self._handle_error(e, "Fatal error with on_message event")

    @Cog.listener()
    async def on_member_join(self, member: Member) -> None:
        try:
            discriminator, url = await self._get_user_data(member)
            embed = self._base_embed(
                f"**Welcome to Malachar, {member.mention}**", 0x9B59B6
            )
            embed = self._set_author(embed, member.name, discriminator, url)
            embed = embed.set_footer(
                text=f"{get_ordinal_suffix(member.guild.member_count)} member"
            ).set_image(url=url)
            await send_embed(
                embed,
                WELCOME_CHANNEL,
            )

            age = get_age(pendulum.instance(member.created_at))
            embed = self._base_embed(
                f"{member.mention} {member.name}{discriminator}", 0x43B582
            )
            embed = embed.set_author(name="Member Joined", icon_url=url)
            embed = (
                embed.set_thumbnail(url=url)
                .add_field(name="**Account Age**", value=age, inline=False)
                .set_footer(text=f"ID: {member.id}")
            )
            await send_embed(
                embed,
                AUDIT_LOGS_CHANNEL,
            )

            user: UserRecord = {
                "id": member.id,
                "username": member.name,
                "birthday": None,
                "isBirthdayLeap": None,
            }
            await self._safe_parquet_operation(
                f"insert user {member.name} ({member.id}) in parquet",
                upsert_row_to_parquet,
                user,
                USERS,
            )
        except Exception as e:
            await self._handle_error(e, "Fatal error with on_member_join event")

    async def _get_user_data(self, user: User | Member) -> tuple[str, str]:
        return get_discriminator(user), get_pfp(user)

    @Cog.listener()
    async def on_raw_member_remove(self, payload: RawMemberRemoveEvent) -> None:
        try:
            member = payload.user
            discriminator, url = await self._get_user_data(member)
            embed = self._base_embed(
                f"**{member.mention} has left. Goodbye!**", 0x992D22
            )
            embed = self._set_author(embed, member.name, discriminator, url)
            embed = embed.set_image(url=url)
            await send_embed(
                embed,
                WELCOME_CHANNEL,
            )

            triple_nl = (
                "" if isinstance(member, Member) and member.roles[1:] else "\n\n\n"
            )
            embed = self._base_embed(
                f"{member.mention} {member.name}{discriminator}{triple_nl}", 0xFF470F
            )
            embed = embed.set_author(name="Member Left", icon_url=url)
            embed = embed.set_thumbnail(url=url).set_footer(text=f"ID: {member.id}")
            if isinstance(member, Member) and member.roles[1:]:
                embed = embed.add_field(
                    name="**Roles**",
                    value=" ".join([f"{role.mention}" for role in member.roles[1:]]),
                    inline=False,
                )
            await send_embed(embed, AUDIT_LOGS_CHANNEL)

            await self._safe_parquet_operation(
                f"remove user {member.name} ({member.id}) from parquet",
                delete_row_from_parquet,
                member.id,
                USERS,
            )
        except Exception as e:
            await self._handle_error(e, "Fatal error with on_raw_member_remove event")

    @Cog.listener()
    async def on_command_error(self, ctx: Context, error: CommandError) -> None:
        channel_mention = get_channel_mention(ctx.channel)
        message = f"Command not found: {ctx.message.content}\nSent by: {ctx.author.mention} in {channel_mention}\n{error}"
        await send_message(message, AUDIT_LOGS_CHANNEL)

    @Cog.listener()
    async def on_member_update(self, before: Member, after: Member) -> None:
        try:
            discriminator, url = await self._get_user_data(after)

            await self._handle_pfp_change(before, after, discriminator, url)
            await self._handle_role_changes(before, after, discriminator, url)
            await self._handle_nickname_change(before, after, discriminator, url)
            await self._handle_timeout_changes(before, after, discriminator, url)
        except Exception as e:
            await self._handle_error(e, "Fatal error with on_member_update event")

    async def _handle_pfp_change(
        self, before: Member, after: Member, discriminator: str, url: str
    ) -> None:
        """Handle profile picture changes."""
        before_url = get_pfp(before)
        if url != before_url:
            await self._log_pfp_change(after, discriminator, url)

    async def _handle_role_changes(
        self, before: Member, after: Member, discriminator: str, url: str
    ) -> None:
        """Handle role additions and removals."""
        roles_before, roles_after = before.roles, after.roles
        if roles_diff := list(set(roles_before) ^ set(roles_after)):
            add = len(roles_after) > len(roles_before)
            await self._log_role_change(
                after,
                discriminator,
                url,
                roles_diff,
                add,
            )

    async def _handle_nickname_change(
        self, before: Member, after: Member, discriminator: str, url: str
    ) -> None:
        """Handle nickname changes."""
        if before.nick != after.nick:
            before_nick = before.name if before.nick is None else before.nick
            after_nick = after.name if after.nick is None else after.nick
            await self._log_nickname_change(
                after, discriminator, url, before_nick, after_nick
            )

    async def _handle_timeout_changes(
        self, before: Member, after: Member, discriminator: str, url: str
    ) -> None:
        """Handle timeout and untimeout events."""
        before_timed_out_until = (
            pendulum.instance(before.timed_out_until)
            if before.timed_out_until
            else None
        )
        after_timed_out_until = (
            pendulum.instance(after.timed_out_until) if after.timed_out_until else None
        )
        before_timed_out = self._is_currently_timed_out(before_timed_out_until)
        after_timed_out = self._is_currently_timed_out(after_timed_out_until)

        if not before_timed_out and after_timed_out and after_timed_out_until:
            await self._log_timeout(after, discriminator, url, after_timed_out_until)
        elif before_timed_out and not after_timed_out:
            await self._log_untimeout(after, discriminator, url)

    def _is_currently_timed_out(self, timeout_until: DateTime | None) -> bool:
        """Check if a member is currently timed out."""
        return timeout_until is not None and timeout_until > pendulum.now()

    async def _is_bot_message(self, payload: RawMessageUpdateEvent) -> bool:
        """Check if the message was sent by the bot."""
        return payload.message.author == self.bot.user or (
            payload.cached_message is not None
            and payload.cached_message.author == self.bot.user
        )

    async def _get_before_content(self, before: Message | None, message_id: int) -> str:
        """Get the content of the message before editing."""
        try:
            if before:
                return before.content
            return await self._get_message_content(message_id)
        except KeyError:
            return DEFAULT_MISSING_CONTENT

    def _truncate_content(self, content: str, max_length: int = 1024) -> str:
        """Truncate content if it exceeds the maximum length."""
        if len(content) > max_length:
            return f"{content[: max_length - 3]}..."
        return content

    async def _log_message_edit(
        self,
        after: Message,
        before_content: str,
        after_content: str,
        discriminator: str,
        url: str,
    ) -> None:
        """Log the message edit to the audit logs channel."""
        channel_mention = get_channel_mention(after.channel)
        message = f"**Message edited in {channel_mention}** [Jump to Message]({after.jump_url})"

        before_content = self._truncate_content(before_content)
        after_content = self._truncate_content(after_content)

        embed = self._base_embed(message, 0x337FD5)
        embed = self._set_author(embed, after.author.name, discriminator, url)
        embed = (
            embed.set_footer(text=f"User ID: {after.author.id}")
            .add_field(name="**Before**", value=f"{before_content}", inline=False)
            .add_field(name="**After**", value=f"{after_content}", inline=False)
        )
        await send_embed(embed, AUDIT_LOGS_CHANNEL)

    async def _update_message_in_parquet(self, message: Message) -> None:
        """Update the message in the parquet file."""
        message_obj = await self._get_message_object(message)
        await self._safe_parquet_operation(
            f"upsert message {message.id} in parquet",
            upsert_row_to_parquet,
            message_obj,
            MESSAGES,
        )

    @Cog.listener()
    async def on_raw_message_edit(self, payload: RawMessageUpdateEvent) -> None:
        try:
            if await self._is_bot_message(payload):
                return

            before = payload.cached_message
            after = payload.message

            discriminator, url = await self._get_user_data(after.author)

            if before and before.pinned != after.pinned:
                await self._log_message_pin(after, discriminator, url)

            before_content = await self._get_before_content(before, after.id)
            after_content = after.content

            if before_content == after_content:
                return

            await self._log_message_edit(
                after, before_content, after_content, discriminator, url
            )
            await self._update_message_in_parquet(after)

        except Exception as e:
            await self._handle_error(e, "Fatal error with on_raw_message_edit event")

    @Cog.listener()
    async def on_raw_message_delete(self, payload: RawMessageDeleteEvent) -> None:
        try:
            if (
                payload.cached_message is not None
                and payload.cached_message.author == self.bot.user
            ):
                return

            user_who_deleted = await self._get_audit_user(
                payload.guild_id, discord.AuditLogAction.message_delete
            )

            channel = self.bot.get_channel(payload.channel_id)

            message = payload.cached_message
            if not message:
                await self._log_deleted_missing_message(
                    payload.message_id, user_who_deleted, channel
                )
                return

            author = message.author

            await self._log_message_delete(
                message, payload.message_id, author, user_who_deleted, channel
            )

            await self._safe_parquet_operation(
                f"delete message {payload.message_id} from parquet",
                delete_row_from_parquet,
                payload.message_id,
                MESSAGES,
            )
        except Exception as e:
            await self._handle_error(e, "Fatal error with on_raw_message_delete event")

    @Cog.listener()
    async def on_raw_bulk_message_delete(
        self, payload: RawBulkMessageDeleteEvent
    ) -> None:
        try:
            user_who_deleted = await self._get_audit_user(
                payload.guild_id, discord.AuditLogAction.message_bulk_delete
            )

            channel_mention = get_channel_mention(
                self.bot.get_channel(payload.channel_id)
            )
            description = f"**Bulk Delete in {channel_mention}, {len(payload.message_ids)} messages deleted**"

            if user_who_deleted is None:
                discriminator = ""
                url = None
                user_who_deleted_name = UNKNOWN_USER
            else:
                discriminator, url = await self._get_user_data(user_who_deleted)
                user_who_deleted_name = user_who_deleted.name

            embed = self._base_embed(description, 0x337FD5)
            embed = self._set_author(embed, user_who_deleted_name, discriminator, url)
            await send_embed(embed, AUDIT_LOGS_CHANNEL)

            for message_id in payload.message_ids:
                await self._safe_parquet_operation(
                    f"delete message {message_id} from parquet",
                    delete_row_from_parquet,
                    message_id,
                    MESSAGES,
                )
        except Exception as e:
            await self._handle_error(
                e, "Fatal error with on_raw_bulk_message_delete event"
            )

    async def _log_ban_unban(
        self, user: User | Member, action: Literal["ban", "unban"]
    ) -> None:
        discriminator, url = await self._get_user_data(user)
        embed = self._base_embed(
            f"{user.mention} {user.name}{discriminator}",
            0xFF470F if action == "ban" else 0x337FD5,
        )
        embed = embed.set_author(name=f"User {action.capitalize()}ed", icon_url=url)
        embed = embed.set_thumbnail(url=url).set_footer(text=f"ID: {user.id}")
        await send_embed(embed, AUDIT_LOGS_CHANNEL)

    @Cog.listener()
    async def on_member_ban(self, guild: Guild, user: User | Member) -> None:
        try:
            await self._log_ban_unban(user, "ban")
        except Exception as e:
            await self._handle_error(e, "Fatal error with on_member_ban event")

    @Cog.listener()
    async def on_member_unban(self, guild: Guild, user: User | Member) -> None:
        try:
            await self._log_ban_unban(user, "unban")
        except Exception as e:
            await self._handle_error(e, "Fatal error with on_member_unban event")

    @Cog.listener()
    async def on_invite_create(self, invite: Invite) -> None:
        try:
            guild_name, guild_icon = await self._get_guild_name_and_icon_from_invite(
                invite
            )
            channel_mention = get_channel_mention(invite.channel)
            inviter_mention = f" by {invite.inviter.mention}" if invite.inviter else ""
            expiry = (
                f"<t:{int(invite.expires_at.timestamp())}:R>"
                if invite.expires_at
                else "Never"
            )
            description = f"**Invite [{invite.code}]({invite.url}) to {channel_mention} created by {inviter_mention}**\nExpires: {expiry}"
            embed = self._base_embed(description, 0x337FD5)
            embed = embed.set_author(name=f"{guild_name}", icon_url=guild_icon)
            await send_embed(embed, AUDIT_LOGS_CHANNEL)
        except Exception as e:
            await self._handle_error(e, "Fatal error with on_invite_create event")

    @Cog.listener()
    async def on_invite_delete(self, invite: Invite) -> None:
        try:
            guild_name, guild_icon = await self._get_guild_name_and_icon_from_invite(
                invite
            )
            description = f"**Invite [{invite.code}]({invite.url}) deleted**"
            embed = self._base_embed(description, 0xFF470F)
            embed = embed.set_author(name=f"{guild_name}", icon_url=guild_icon)
            await send_embed(embed, AUDIT_LOGS_CHANNEL)
        except Exception as e:
            await self._handle_error(e, "Fatal error with on_invite_delete event")

    async def _get_message_content(self, message_id: int) -> str:
        df = await read_parquet_cached(MESSAGES)
        message_row = df.filter(pl.col("id") == message_id)
        if message_row.height != 0:
            return message_row.row(0, named=True)["contents"]
        return DEFAULT_MISSING_CONTENT

    async def _log_role_change(
        self, member: Member, discriminator: str, url: str, roles: list[Role], add: bool
    ) -> None:
        roles_str = " ".join([role.mention for role in roles])
        message = f"**{member.mention} was {'given' if add else 'removed from'} the role{'' if len(roles) == 1 else 's'} {roles_str}**"
        embed = self._base_embed(message, 0x337FD5)
        embed = self._set_author(embed, member.name, discriminator, url)
        embed = embed.set_footer(text=f"ID: {member.id}")
        await send_embed(
            embed,
            AUDIT_LOGS_CHANNEL,
        )

    async def _log_nickname_change(
        self, member: Member, discriminator: str, url: str, before: str, after: str
    ) -> None:
        embed = self._base_embed(
            f"**{member.mention} changed their nickname**", 0x337FD5
        )
        embed = self._set_author(embed, member.name, discriminator, url)
        embed = (
            embed.set_footer(text=f"ID: {member.id}")
            .add_field(name="**Before**", value=f"{before}", inline=False)
            .add_field(name="**After**", value=f"{after}", inline=False)
        )
        await send_embed(
            embed,
            AUDIT_LOGS_CHANNEL,
        )

    async def _log_pfp_change(
        self, member: Member, discriminator: str, url: str
    ) -> None:
        embed = self._base_embed(
            f"**{member.mention} changed their profile picture**", 0x337FD5
        )
        embed = self._set_author(embed, member.name, discriminator, url)
        embed = embed.set_thumbnail(url=url).set_footer(text=f"ID: {member.id}")
        await send_embed(
            embed,
            AUDIT_LOGS_CHANNEL,
        )

    async def _log_timeout(
        self, member: Member, discriminator: str, url: str, timeout: DateTime
    ) -> None:
        expiry = f"<t:{int(timeout.timestamp())}:R>"
        embed = self._base_embed(
            f"**{member.mention} has been timed out**\nExpires {expiry}", 0x337FD5
        )
        embed = self._set_author(embed, member.name, discriminator, url)
        embed = embed.set_footer(text=f"ID: {member.id}")
        await send_embed(
            embed,
            AUDIT_LOGS_CHANNEL,
        )

    async def _log_untimeout(
        self, member: Member, discriminator: str, url: str
    ) -> None:
        embed = self._base_embed(
            f"**{member.mention}'s timeout has been removed**", 0x337FD5
        )
        embed = self._set_author(embed, member.name, discriminator, url)
        embed = embed.set_footer(text=f"ID: {member.id}")
        await send_embed(
            embed,
            AUDIT_LOGS_CHANNEL,
        )

    async def _log_message_pin(
        self, message: Message, discriminator: str, url: str
    ) -> None:
        channel_mention = get_channel_mention(message.channel)
        description = f"**Message {'pinned' if message.pinned else 'unpinned'} in {channel_mention}** [Jump to Message]({message.jump_url})"
        embed = self._base_embed(description, 0x337FD5)
        embed = self._set_author(embed, message.author.name, discriminator, url)
        embed = embed.set_footer(text=f"User ID: {message.author.id}")
        await send_embed(
            embed,
            AUDIT_LOGS_CHANNEL,
        )

    async def _log_deleted_missing_message(
        self,
        message_id: int,
        user: User | Member | None,
        channel: (
            VoiceChannel
            | StageChannel
            | ForumChannel
            | TextChannel
            | CategoryChannel
            | Thread
            | PrivateChannel
            | None
        ),
    ) -> None:
        message_content = await self._get_message_content(message_id)

        if user is None:
            discriminator = ""
            url = None
            user_mention = UNKNOWN_USER
            user_name = UNKNOWN_USER
            user_id = "Unknown ID"
        else:
            discriminator, url = await self._get_user_data(user)
            user_mention = user.mention
            user_name = user.name
            user_id = user.id

        channel_mention = get_channel_mention(channel)
        description = f"**Message deleted by {user_mention} in {channel_mention}**"
        embed = self._base_embed(description, 0xFF470F)
        embed = self._set_author(embed, user_name, discriminator, url)
        embed = embed.add_field(
            name="**Message**", value=f"{message_content}", inline=False
        ).set_footer(text=f"Deleter: {user_id} | Message ID: {message_id}")
        await send_embed(embed, AUDIT_LOGS_CHANNEL)

        await self._safe_parquet_operation(
            f"delete message {message_id} from parquet",
            delete_row_from_parquet,
            message_id,
            MESSAGES,
        )

    async def _log_message_delete(
        self,
        message: Message,
        message_id: int,
        author: User | Member,
        user_who_deleted: User | Member | None,
        channel: (
            VoiceChannel
            | StageChannel
            | ForumChannel
            | TextChannel
            | CategoryChannel
            | Thread
            | PrivateChannel
            | None
        ),
    ) -> None:
        try:
            message_content = message.content
        except KeyError:
            message_content = await self._get_message_content(message_id)

        user_who_deleted_mention = (
            "" if user_who_deleted is None else f" by {user_who_deleted.mention}"
        )
        channel_mention = get_channel_mention(channel)
        description = f"**Message sent by {author.mention} deleted{user_who_deleted_mention} in {channel_mention}**"
        discriminator, url = await self._get_user_data(author)
        embed = self._base_embed(description, 0xFF470F)
        embed = self._set_author(embed, author.name, discriminator, url)
        embed = embed.set_footer(text=f"Author: {author.id} | Message ID: {message_id}")
        if message_content:
            message_content = self._truncate_content(message_content)
            embed = embed.add_field(
                name="**Message**", value=f"{message_content}", inline=False
            )
        await send_embed(embed, AUDIT_LOGS_CHANNEL)
        await self._log_message_attachments_delete(
            message, message_id, author, channel, discriminator, url
        )

    async def _log_message_attachments_delete(
        self,
        message: Message,
        message_id: int,
        author: User | Member,
        channel: (
            VoiceChannel
            | StageChannel
            | ForumChannel
            | TextChannel
            | CategoryChannel
            | Thread
            | PrivateChannel
            | None
        ),
        discriminator: str,
        url: str,
    ) -> None:
        if message.attachments:
            channel_mention = get_channel_mention(channel)
            for attachment in message.attachments:
                embed = self._base_embed(
                    f"**Attachment sent by {author.mention} deleted in {channel_mention}**",
                    0xFF470F,
                )
                embed = self._set_author(embed, author.name, discriminator, url)
                embed = embed.set_footer(
                    text=f"Author: {author.id} | Message ID: {message_id}"
                ).set_image(url=attachment.url)
                await send_embed(embed, AUDIT_LOGS_CHANNEL)

    async def _get_guild_name_and_icon_from_invite(
        self, invite: Invite
    ) -> tuple[str, str | None]:
        guild = invite.guild
        guild_name = (
            guild.name if guild and not isinstance(guild, Object) else "Unknown Guild"
        )
        guild_icon = (
            guild.icon.url
            if guild and not isinstance(guild, Object) and guild.icon
            else None
        )
        return guild_name, guild_icon


async def setup(bot: Bot) -> None:
    await bot.add_cog(Events(bot))
