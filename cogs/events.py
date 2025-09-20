import discord
import orjson
import pendulum
import polars as pl
import sentry_sdk
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
    WELCOME_CHANNEL,
)
from services import (
    delete_row_from_parquet,
    get_age,
    get_channel_mention,
    get_discriminator,
    get_ordinal_suffix,
    get_pfp,
    send_embed,
    send_message,
    upsert_row_to_parquet,
)


class Events(Cog):
    def __init__(self, bot: Bot) -> None:
        self.bot = bot

    @Cog.listener()
    @sentry_sdk.trace()
    async def on_message(self, message: Message) -> None:
        try:
            if message.author == self.bot.user:
                return

            guild = message.guild
            guild_id = GUILD_ID if guild is None else guild.id
            message_obj = {
                "id": message.id,
                "contents": message.content,
                "guild_id": guild_id,
                "author_id": message.author.id,
                "channel_id": message.channel.id,
                "attachment_urls": orjson.dumps(
                    [attachment.url for attachment in message.attachments]
                ).decode("utf-8"),
            }
            try:
                success, error = upsert_row_to_parquet(
                    message_obj,
                    "data/messages.parquet",
                )
                if not success:
                    await send_message(
                        f"Failed to save message {message.id} in parquet: {error}",
                        BOT_ADMIN_CHANNEL,
                    )
            except Exception as e:
                sentry_sdk.capture_exception(e)
                await send_message(
                    f"Failed to save message {message.id}: {e}",
                    BOT_ADMIN_CHANNEL,
                )

            content = message.content.lower()
            if content == "ping":
                await message.channel.send("pong")
            elif content == "plap":
                await message.channel.send("clank")
        except Exception as e:
            sentry_sdk.capture_exception(e)
            await send_message(
                f"Fatal error with on_message event: {e}",
                BOT_ADMIN_CHANNEL,
            )

    @Cog.listener()
    @sentry_sdk.trace()
    async def on_member_join(self, member: Member) -> None:
        try:
            discriminator = get_discriminator(member)
            url = get_pfp(member)
            embed = (
                Embed(
                    description=f"**Welcome to Malachar, {member.mention}**",
                    color=0x9B59B6,
                    timestamp=pendulum.now(),
                )
                .set_author(
                    name=f"{member.name}{discriminator}",
                    icon_url=url,
                )
                .set_footer(
                    text=f"{get_ordinal_suffix(member.guild.member_count)} member"
                )
                .set_image(url=url)
            )
            await send_embed(
                embed,
                WELCOME_CHANNEL,
            )

            age = get_age(pendulum.instance(member.created_at))
            embed = (
                Embed(
                    description=f"{member.mention} {member.name}{discriminator}",
                    color=0x43B582,
                    timestamp=pendulum.now(),
                )
                .set_author(
                    name="Member Joined",
                    icon_url=url,
                )
                .set_thumbnail(
                    url=url,
                )
                .add_field(
                    name="**Account Age**",
                    value=age,
                    inline=False,
                )
                .set_footer(text=f"ID: {member.id}")
            )
            await send_embed(
                embed,
                AUDIT_LOGS_CHANNEL,
            )

            user = {
                "id": member.id,
                "username": member.name,
                "birthday": None,
                "isBirthdayLeap": None,
            }
            success, error = upsert_row_to_parquet(
                user,
                "data/users.parquet",
            )
            if not success:
                await send_message(
                    f"Failed to insert user {member.name} ({member.id}) in parquet: {error}",
                    BOT_ADMIN_CHANNEL,
                )
        except Exception as e:
            sentry_sdk.capture_exception(e)
            await send_message(
                f"Fatal error with on_member_join event: {e}",
                BOT_ADMIN_CHANNEL,
            )

    @Cog.listener()
    @sentry_sdk.trace()
    async def on_raw_member_remove(self, payload: RawMemberRemoveEvent) -> None:
        try:
            member = payload.user
            discriminator = get_discriminator(member)
            url = get_pfp(member)
            embed = (
                Embed(
                    description=f"**{member.mention} has left. Goodbye!**",
                    color=0x992D22,
                    timestamp=pendulum.now(),
                )
                .set_author(
                    name=f"{member.name}{discriminator}",
                    icon_url=url,
                )
                .set_image(url=url)
            )
            await send_embed(
                embed,
                WELCOME_CHANNEL,
            )

            triple_nl = (
                "" if isinstance(member, Member) and member.roles[1:] else "\n\n\n"
            )
            embed = (
                Embed(
                    description=f"{member.mention} {member.name}{discriminator}{triple_nl}",
                    color=0xFF470F,
                    timestamp=pendulum.now(),
                )
                .set_author(
                    name="Member Left",
                    icon_url=url,
                )
                .set_thumbnail(
                    url=url,
                )
                .set_footer(text=f"ID: {member.id}")
            )
            if isinstance(member, Member) and member.roles[1:]:
                embed = embed.add_field(
                    name="**Roles**",
                    value=" ".join([f"{role.mention}" for role in member.roles[1:]]),
                    inline=False,
                )
            await send_embed(embed, AUDIT_LOGS_CHANNEL)

            success, error = delete_row_from_parquet(
                member.id,
                "data/users.parquet",
            )
            if not success:
                await send_message(
                    f"Failed to remove user {member.name} ({member.id}) from parquet: {error}",
                    BOT_ADMIN_CHANNEL,
                )
        except Exception as e:
            sentry_sdk.capture_exception(e)
            await send_message(
                f"Fatal error with on_raw_member_remove event: {e}",
                BOT_ADMIN_CHANNEL,
            )

    @Cog.listener()
    @sentry_sdk.trace()
    async def on_command_error(self, ctx: Context, error: CommandError) -> None:
        channel_mention = get_channel_mention(ctx.channel)
        message = f"Command not found: {ctx.message.content}\nSent by: {ctx.author.mention} in {channel_mention}\n{error}"
        await send_message(message, AUDIT_LOGS_CHANNEL)

    @Cog.listener()
    @sentry_sdk.trace()
    async def on_member_update(self, before: Member, after: Member) -> None:
        try:
            discriminator = get_discriminator(after)
            url = get_pfp(after)

            before_url = get_pfp(before)
            if url != before_url:
                await self._log_pfp_change(after, discriminator, url)

            roles_before, roles_after = before.roles, after.roles
            roles_diff = list(set(roles_before) ^ set(roles_after))
            if len(roles_diff):
                add = len(roles_after) > len(roles_before)
                await self._log_role_change(
                    after,
                    discriminator,
                    url,
                    roles_diff,
                    add,
                )

            if before.nick != after.nick:
                before_nick = before.name if before.nick is None else before.nick
                after_nick = after.name if after.nick is None else after.nick
                await self._log_nickname_change(
                    after, discriminator, url, before_nick, after_nick
                )

            if (
                before.timed_out_until is None
                or before.timed_out_until <= pendulum.now()
            ) and (
                after.timed_out_until is not None
                and after.timed_out_until > pendulum.now()
            ):
                await self._log_timeout(
                    after, discriminator, url, pendulum.instance(after.timed_out_until)
                )

            elif (
                before.timed_out_until is not None
                and before.timed_out_until > pendulum.now()
            ) and (
                after.timed_out_until is None or after.timed_out_until <= pendulum.now()
            ):
                await self._log_untimeout(after, discriminator, url)
        except Exception as e:
            sentry_sdk.capture_exception(e)
            await send_message(
                f"Fatal error with on_member_update event: {e}",
                BOT_ADMIN_CHANNEL,
            )

    @Cog.listener()
    @sentry_sdk.trace()
    async def on_raw_message_edit(self, payload: RawMessageUpdateEvent) -> None:
        try:
            if payload.message.author == self.bot.user or (
                payload.cached_message is not None
                and payload.cached_message.author == self.bot.user
            ):
                return

            before = payload.cached_message
            after = payload.message

            discriminator = get_discriminator(after.author)
            url = get_pfp(after.author)

            if before and before.pinned != after.pinned:
                await self._log_message_pin(after, discriminator, url)

            channel_mention = get_channel_mention(after.channel)

            try:
                if before:
                    before_content = before.content
                else:
                    before_content = await self._get_message_content(after.id)

                after_content = after.content
            except KeyError:
                message = f"Embed-only edit detected. Audit log not supported.\nMessage ID: {after.id}\nChannel: {channel_mention}\n[Jump to Message]({after.jump_url})"
                await send_message(
                    message,
                    AUDIT_LOGS_CHANNEL,
                )
                return

            if before_content == after_content:
                return

            before_content = (
                f"{before_content[:1021]}..."
                if len(before_content) > 1024
                else before_content
            )
            after_content = (
                f"{after_content[:1021]}..."
                if len(after_content) > 1024
                else after_content
            )

            message = f"**Message edited in {channel_mention}** [Jump to Message]({after.jump_url})"
            embed = (
                Embed(
                    description=message,
                    color=0x337FD5,
                    timestamp=pendulum.now(),
                )
                .set_author(
                    name=f"{after.author.name}{discriminator}",
                    icon_url=url,
                )
                .set_footer(text=f"User ID: {after.author.id}")
                .add_field(name="**Before**", value=f"{before_content}", inline=False)
                .add_field(name="**After**", value=f"{after_content}", inline=False)
            )
            await send_embed(
                embed,
                AUDIT_LOGS_CHANNEL,
            )

            try:
                guild = after.guild
                guild_id = GUILD_ID if guild is None else guild.id
                after_message_obj = {
                    "id": after.id,
                    "contents": after.content,
                    "guild_id": guild_id,
                    "author_id": after.author.id,
                    "channel_id": after.channel.id,
                    "attachment_urls": orjson.dumps(
                        [attachment.url for attachment in after.attachments]
                    ).decode("utf-8"),
                }

                success, error = upsert_row_to_parquet(
                    after_message_obj,
                    "data/messages.parquet",
                )
                if not success:
                    await send_message(
                        f"Failed to upsert message {after.id} in parquet: {error}",
                        BOT_ADMIN_CHANNEL,
                    )
            except Exception as e:
                sentry_sdk.capture_exception(e)
                await send_message(
                    f"Failed to upsert message {after.id} in parquet: {e}",
                    BOT_ADMIN_CHANNEL,
                )
        except Exception as e:
            sentry_sdk.capture_exception(e)
            await send_message(
                f"Fatal error with on_raw_message_edit event: {e}",
                BOT_ADMIN_CHANNEL,
            )

    @Cog.listener()
    @sentry_sdk.trace()
    async def on_raw_message_delete(self, payload: RawMessageDeleteEvent) -> None:
        try:
            if payload.cached_message is not None and payload.cached_message.author == self.bot.user:
                return

            user_who_deleted = None
            if payload.guild_id is not None:
                guild = self.bot.get_guild(payload.guild_id)
                if guild is not None:
                    async for entry in guild.audit_logs(
                        limit=1, action=discord.AuditLogAction.message_delete
                    ):
                        user_who_deleted = entry.user

            channel = self.bot.get_channel(payload.channel_id)

            message = payload.cached_message
            if not message:
                await self._log_deleted_missing_message(
                    payload.message_id, user_who_deleted, channel
                )
                return

            author = message.author
            discriminator = get_discriminator(author)
            url = get_pfp(author)

            await self._log_message_delete(
                message,
                payload.message_id,
                author,
                user_who_deleted,
                channel,
                discriminator,
                url,
            )

            try:
                success, error = delete_row_from_parquet(
                    payload.message_id,
                    "data/messages.parquet",
                )
                if not success:
                    await send_message(
                        f"Failed to delete message {payload.message_id} from parquet: {error}",
                        BOT_ADMIN_CHANNEL,
                    )
            except Exception as e:
                sentry_sdk.capture_exception(e)
                await send_message(
                    f"Failed to delete message {payload.message_id} from parquet: {e}",
                    BOT_ADMIN_CHANNEL,
                )
        except Exception as e:
            sentry_sdk.capture_exception(e)
            await send_message(
                f"Fatal error with on_raw_message_delete event: {e}",
                BOT_ADMIN_CHANNEL,
            )

    @Cog.listener()
    @sentry_sdk.trace()
    async def on_raw_bulk_message_delete(
        self, payload: RawBulkMessageDeleteEvent
    ) -> None:
        try:
            user_who_deleted = None
            if payload.guild_id is not None:
                guild = self.bot.get_guild(payload.guild_id)
                if guild is not None:
                    async for entry in guild.audit_logs(
                        limit=1, action=discord.AuditLogAction.message_bulk_delete
                    ):
                        user_who_deleted = entry.user

            channel_mention = get_channel_mention(
                self.bot.get_channel(payload.channel_id)
            )
            description = f"**Bulk Delete in {channel_mention}, {len(payload.message_ids)} messages deleted**"

            if user_who_deleted is None:
                discriminator = ""
                url = None
                user_who_deleted_name = "Unknown User"
            else:
                discriminator = get_discriminator(user_who_deleted)
                url = get_pfp(user_who_deleted)
                user_who_deleted_name = user_who_deleted.name

            embed = Embed(
                description=description,
                color=0x337FD5,
                timestamp=pendulum.now(),
            ).set_author(
                name=f"{user_who_deleted_name}{discriminator}",
                icon_url=url,
            )
            await send_embed(embed, AUDIT_LOGS_CHANNEL)

            for message_id in payload.message_ids:
                try:
                    success, error = delete_row_from_parquet(
                        message_id,
                        "data/messages.parquet",
                    )
                    if not success:
                        await send_message(
                            f"Failed to delete message {message_id} from parquet: {error}",
                            BOT_ADMIN_CHANNEL,
                        )
                except Exception as e:
                    sentry_sdk.capture_exception(e)
                    await send_message(
                        f"Failed to delete message {message_id} from parquet: {e}",
                        BOT_ADMIN_CHANNEL,
                    )
        except Exception as e:
            sentry_sdk.capture_exception(e)
            await send_message(
                f"Fatal error with on_raw_bulk_message_delete event: {e}",
                BOT_ADMIN_CHANNEL,
            )

    @Cog.listener()
    @sentry_sdk.trace()
    async def on_member_ban(self, guild: Guild, user: User | Member) -> None:
        try:
            discriminator = get_discriminator(user)
            url = get_pfp(user)

            embed = (
                Embed(
                    description=f"{user.mention} {user.name}{discriminator}",
                    color=0xFF470F,
                    timestamp=pendulum.now(),
                )
                .set_author(
                    name="User Banned",
                    icon_url=url,
                )
                .set_thumbnail(
                    url=url,
                )
                .set_footer(text=f"ID: {user.id}")
            )
            await send_embed(embed, AUDIT_LOGS_CHANNEL)
        except Exception as e:
            sentry_sdk.capture_exception(e)
            await send_message(
                f"Fatal error with on_member_ban event: {e}",
                BOT_ADMIN_CHANNEL,
            )

    @Cog.listener()
    @sentry_sdk.trace()
    async def on_member_unban(self, guild: Guild, user: User | Member) -> None:
        try:
            discriminator = get_discriminator(user)
            url = get_pfp(user)

            embed = (
                Embed(
                    description=f"{user.mention} {user.name}{discriminator}",
                    color=0x337FD5,
                    timestamp=pendulum.now(),
                )
                .set_author(
                    name="User Unbanned",
                    icon_url=url,
                )
                .set_thumbnail(
                    url=url,
                )
                .set_footer(text=f"ID: {user.id}")
            )
            await send_embed(embed, AUDIT_LOGS_CHANNEL)
        except Exception as e:
            sentry_sdk.capture_exception(e)
            await send_message(
                f"Fatal error with on_member_unban event: {e}",
                BOT_ADMIN_CHANNEL,
            )

    @Cog.listener()
    @sentry_sdk.trace()
    async def on_invite_create(self, invite: Invite) -> None:
        try:
            guild = invite.guild
            guild_name = (
                guild.name
                if guild and not isinstance(guild, Object)
                else "Unknown Guild"
            )
            guild_icon = (
                guild.icon.url
                if guild and not isinstance(guild, Object) and guild.icon
                else None
            )
            channel_mention = get_channel_mention(invite.channel)
            inviter_mention = f" by {invite.inviter.mention}" if invite.inviter else ""
            expiry = (
                f"<t:{int(invite.expires_at.timestamp())}:R>"
                if invite.expires_at
                else "Never"
            )
            description = f"**Invite [{invite.code}]({invite.url}) to {channel_mention} created by {inviter_mention}**\nExpires: {expiry}"
            embed = Embed(
                description=description,
                color=0x337FD5,
                timestamp=pendulum.now(),
            ).set_author(
                name=f"{guild_name}",
                icon_url=guild_icon,
            )
            await send_embed(embed, AUDIT_LOGS_CHANNEL)
        except Exception as e:
            sentry_sdk.capture_exception(e)
            await send_message(
                f"Fatal error with on_invite_create event: {e}",
                BOT_ADMIN_CHANNEL,
            )

    @Cog.listener()
    @sentry_sdk.trace()
    async def on_invite_delete(self, invite: Invite) -> None:
        try:
            guild = invite.guild
            guild_name = (
                guild.name
                if guild and not isinstance(guild, Object)
                else "Unknown Guild"
            )
            guild_icon = (
                guild.icon.url
                if guild and not isinstance(guild, Object) and guild.icon
                else None
            )
            description = f"**Invite [{invite.code}]({invite.url}) deleted**"
            embed = Embed(
                description=description,
                color=0xFF470F,
                timestamp=pendulum.now(),
            ).set_author(
                name=f"{guild_name}",
                icon_url=guild_icon,
            )
            await send_embed(embed, AUDIT_LOGS_CHANNEL)
        except Exception as e:
            sentry_sdk.capture_exception(e)
            await send_message(
                f"Fatal error with on_invite_delete event: {e}",
                BOT_ADMIN_CHANNEL,
            )

    @sentry_sdk.trace()
    async def _get_message_content(self, message_id: int) -> str:
        df = pl.read_parquet("data/messages.parquet")
        message_row = df.filter(pl.col("id") == message_id)
        if message_row.height != 0:
            return message_row.row(0, named=True)["content"]
        return DEFAULT_MISSING_CONTENT

    @sentry_sdk.trace()
    async def _log_role_change(
        self, member: Member, discriminator: str, url: str, roles: list[Role], add: bool
    ) -> None:
        roles_str = " ".join([role.mention for role in roles])
        message = f"**{member.mention} was {'given' if add else 'removed from'} the role{'' if len(roles) == 1 else 's'} {roles_str}**"
        embed = (
            Embed(
                description=message,
                color=0x337FD5,
                timestamp=pendulum.now(),
            )
            .set_author(
                name=f"{member.name}{discriminator}",
                icon_url=url,
            )
            .set_footer(text=f"ID: {member.id}")
        )
        await send_embed(
            embed,
            AUDIT_LOGS_CHANNEL,
        )

    @sentry_sdk.trace()
    async def _log_nickname_change(
        self, member: Member, discriminator: str, url: str, before: str, after: str
    ) -> None:
        embed = (
            Embed(
                description=f"**{member.mention} changed their nickname**",
                color=0x337FD5,
                timestamp=pendulum.now(),
            )
            .set_author(
                name=f"{member.name}{discriminator}",
                icon_url=url,
            )
            .set_footer(text=f"ID: {member.id}")
            .add_field(name="**Before**", value=f"{before}", inline=False)
            .add_field(name="**After**", value=f"{after}", inline=False)
        )
        await send_embed(
            embed,
            AUDIT_LOGS_CHANNEL,
        )

    @sentry_sdk.trace()
    async def _log_pfp_change(
        self, member: Member, discriminator: str, url: str
    ) -> None:
        embed = (
            Embed(
                description=f"**{member.mention} changed their profile picture**",
                color=0x337FD5,
                timestamp=pendulum.now(),
            )
            .set_author(
                name=f"{member.name}{discriminator}",
                icon_url=url,
            )
            .set_thumbnail(
                url=url,
            )
            .set_footer(text=f"ID: {member.id}")
        )
        await send_embed(
            embed,
            AUDIT_LOGS_CHANNEL,
        )

    @sentry_sdk.trace()
    async def _log_timeout(
        self, member: Member, discriminator: str, url: str, timeout: DateTime
    ) -> None:
        expiry = f"<t:{int(timeout.timestamp())}:R>"
        embed = (
            Embed(
                description=f"**{member.mention} has been timed out**\nExpires {expiry}",
                color=0x337FD5,
                timestamp=pendulum.now(),
            )
            .set_author(
                name=f"{member.name}{discriminator}",
                icon_url=url,
            )
            .set_footer(text=f"ID: {member.id}")
        )
        await send_embed(
            embed,
            AUDIT_LOGS_CHANNEL,
        )

    @sentry_sdk.trace()
    async def _log_untimeout(
        self, member: Member, discriminator: str, url: str
    ) -> None:
        embed = (
            Embed(
                description=f"**{member.mention}'s timeout has been removed**",
                color=0x337FD5,
                timestamp=pendulum.now(),
            )
            .set_author(
                name=f"{member.name}{discriminator}",
                icon_url=url,
            )
            .set_footer(text=f"ID: {member.id}")
        )
        await send_embed(
            embed,
            AUDIT_LOGS_CHANNEL,
        )

    @sentry_sdk.trace()
    async def _log_message_pin(
        self, message: Message, discriminator: str, url: str
    ) -> None:
        channel_mention = get_channel_mention(message.channel)
        description = f"**Message {'pinned' if message.pinned else 'unpinned'} in {channel_mention}** [Jump to Message]({message.jump_url})"
        embed = (
            Embed(
                description=description,
                color=0x337FD5,
                timestamp=pendulum.now(),
            )
            .set_author(
                name=f"{message.author.name}{discriminator}",
                icon_url=url,
            )
            .set_footer(text=f"User ID: {message.author.id}")
        )
        await send_embed(
            embed,
            AUDIT_LOGS_CHANNEL,
        )

    @sentry_sdk.trace()
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
            user_mention = "Unknown User"
            user_name = "Unknown User"
            user_id = "Unknown ID"
        else:
            discriminator = get_discriminator(user)
            url = get_pfp(user)
            user_mention = user.mention
            user_name = user.name
            user_id = user.id

        channel_mention = get_channel_mention(channel)
        description = f"**Message deleted by {user_mention} in {channel_mention}**"
        embed = (
            Embed(
                description=description,
                color=0xFF470F,
                timestamp=pendulum.now(),
            )
            .set_author(
                name=f"{user_name}{discriminator}",
                icon_url=url,
            )
            .add_field(
                name="**Message**",
                value=f"{message_content}",
                inline=False,
            )
            .set_footer(text=f"Deleter: {user_id} | Message ID: {message_id}")
        )
        await send_embed(embed, AUDIT_LOGS_CHANNEL)

        try:
            success, error = delete_row_from_parquet(
                message_id,
                "data/messages.parquet",
            )
            if not success:
                await send_message(
                    f"Failed to delete message {message_id} from parquet: {error}",
                    BOT_ADMIN_CHANNEL,
                )
        except Exception as e:
            sentry_sdk.capture_exception(e)
            await send_message(
                f"Failed to delete message {message_id} from parquet: {e}",
                BOT_ADMIN_CHANNEL,
            )

    @sentry_sdk.trace()
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
        discriminator: str,
        url: str,
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
        embed = (
            Embed(
                description=description,
                color=0xFF470F,
                timestamp=pendulum.now(),
            )
            .set_author(
                name=f"{author.name}{discriminator}",
                icon_url=url,
            )
            .set_footer(text=f"Author: {author.id} | Message ID: {message_id}")
        )
        if message_content:
            message_content = (
                f"{message_content[:1021]}..."
                if len(message_content) > 1024
                else message_content
            )
            embed = embed.add_field(
                name="**Message**", value=f"{message_content}", inline=False
            )
        await send_embed(embed, AUDIT_LOGS_CHANNEL)
        await self._log_message_attachments_delete(
            message, message_id, author, channel, discriminator, url
        )

    @sentry_sdk.trace()
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
                embed = (
                    Embed(
                        description=f"**Attachment sent by {author.mention} deleted in {channel_mention}**",
                        color=0xFF470F,
                        timestamp=pendulum.now(),
                    )
                    .set_author(
                        name=f"{author.name}{discriminator}",
                        icon_url=url,
                    )
                    .set_footer(text=f"Author: {author.id} | Message ID: {message_id}")
                    .set_image(url=attachment.url)
                )
                await send_embed(embed, AUDIT_LOGS_CHANNEL)


async def setup(bot: Bot) -> None:
    await bot.add_cog(Events(bot))
