import random
from datetime import datetime

import discord
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
    RawReactionActionEvent,
    Role,
    User,
)
from discord.ext.commands import Bot, Cog, CommandError, Context

from constants import AUDIT_LOGS_CHANNEL, MESSAGE_REACTION_ROLE_MAP, WELCOME_CHANNEL
from helper import (
    get_age,
    get_discriminator,
    get_ordinal_suffix,
    get_pfp,
    send_embed,
    send_message,
)


class Events(Cog):
    def __init__(self, bot: Bot):
        self.bot = bot

    @Cog.listener()
    async def on_message(self, message: Message):
        if message.author == self.bot.user:
            return

        content = message.content.lower()
        if content == "ping":
            await message.channel.send("pong")
        elif content == "plap":
            await message.channel.send("clank")

        if message.author.id == 1131782416260935810 and random.random() < 0.1:
            await message.reply(
                "Fuck you, Weiss\n\nRegards, Valin", mention_author=True
            )

    @Cog.listener()
    async def on_member_join(self, member: Member):
        await send_message(
            f"{member.mention} has joined. Welcome!\nYou're the {get_ordinal_suffix(member.guild.member_count)} member!",
            self.bot,
            WELCOME_CHANNEL,
        )

        discriminator = get_discriminator(member)
        url = get_pfp(member)
        age = get_age(member.created_at)
        embed = (
            Embed(
                description=f"{member.mention} {member.name}{discriminator}",
                color=0x43B582,
                timestamp=datetime.now(),
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
            self.bot,
            AUDIT_LOGS_CHANNEL,
        )

    @Cog.listener()
    async def on_raw_member_remove(self, payload: RawMemberRemoveEvent):
        member = payload.user
        await send_message(
            f"{member.mention} has left the server. Goodbye!", self.bot, WELCOME_CHANNEL
        )

        discriminator = get_discriminator(member)
        url = get_pfp(member)
        triple_nl = "" if member.roles[1:] else "\n\n\n"
        embed = (
            Embed(
                description=f"{member.mention} {member.name}{discriminator}{triple_nl}",
                color=0xFF470F,
                timestamp=datetime.now(),
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
        if member.roles[1:]:
            embed = embed.add_field(
                name="**Roles**",
                value=" ".join([f"{role.mention}" for role in member.roles[1:]]),
                inline=False,
            )
        await send_embed(embed, self.bot, AUDIT_LOGS_CHANNEL)

    @Cog.listener()
    async def on_command_error(self, ctx: Context, error: CommandError):
        message = f"Command not found: {ctx.message.content}\nSent by: {ctx.author.mention} in {ctx.channel.mention}\n{error}"
        await send_message(message, self.bot, AUDIT_LOGS_CHANNEL)

    @Cog.listener()
    async def on_raw_reaction_add(self, payload: RawReactionActionEvent):
        await self._toggle_role(payload, True)

    @Cog.listener()
    async def on_raw_reaction_remove(self, payload: RawReactionActionEvent):
        await self._toggle_role(payload, False)

    @Cog.listener()
    async def on_member_update(self, before: Member, after: Member):
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
            before.timed_out_until is None or before.timed_out_until <= datetime.now()
        ) and (
            after.timed_out_until is not None and after.timed_out_until > datetime.now()
        ):
            await self._log_timeout(after, discriminator, url, after.timed_out_until)

        elif (
            before.timed_out_until is not None
            and before.timed_out_until > datetime.now()
        ) and (
            after.timed_out_until is None or after.timed_out_until <= datetime.now()
        ):
            await self._log_untimeout(after, discriminator, url)

    @Cog.listener()
    async def on_raw_message_edit(self, payload: RawMessageUpdateEvent):
        before = payload.cached_message
        after = payload.message

        discriminator = get_discriminator(after.author)
        url = get_pfp(after.author)

        if before and before.pinned != after.pinned:
            await self._log_pin_change(after, discriminator, url)

        try:
            before_content = (
                before.content if before else "`Old message content not found in cache`"
            )
            after_content = after.content
        except KeyError:
            message = f"Embed-only edit detected. Audit log not supported.\nMessage ID: {after.id}\nChannel: {after.channel.mention}\n[Jump to Message]({after.jump_url})"
            send_message(
                message,
                self.bot,
                AUDIT_LOGS_CHANNEL,
            )
            return

        if before_content == after_content:
            return

        message = f"**Message edited in {after.channel.mention}** [Jump to Message]({after.jump_url})"
        embed = (
            Embed(
                description=message,
                color=0x337FD5,
                timestamp=datetime.now(),
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
            self.bot,
            AUDIT_LOGS_CHANNEL,
        )

    @Cog.listener()
    async def on_raw_message_delete(self, payload: RawMessageDeleteEvent):
        message = payload.cached_message
        author = message.author
        discriminator = get_discriminator(author)
        url = get_pfp(author)

        try:
            message_content = (
                message.content if message else "`Message content not found in cache`"
            )
        except KeyError:
            message_content = "`Message content not found in cache`"

        guild = self.bot.get_guild(payload.guild_id)
        async for entry in guild.audit_logs(
            limit=1, action=discord.AuditLogAction.message_delete
        ):
            user_who_deleted = entry.user

        channel = self.bot.get_channel(payload.channel_id)
        description = f"**Message sent by {author.mention} deleted by {user_who_deleted} in {channel.mention}**\n{message_content}"
        embed = (
            Embed(
                description=description,
                color=0xFF470F,
                timestamp=datetime.now(),
            )
            .set_author(
                name=f"{author.name}{discriminator}",
                icon_url=url,
            )
            .set_footer(text=f"Author: {author.id} | Message ID: {payload.message_id}")
        )
        await send_embed(embed, self.bot, AUDIT_LOGS_CHANNEL)

        if message.attachments:
            for attachment in message.attachments:
                embed = (
                    Embed(
                        description=f"**Attachment sent by {author.mention} deleted in {channel.mention}**",
                        color=0xFF470F,
                        timestamp=datetime.now(),
                    )
                    .set_author(
                        name=f"{author.name}{discriminator}",
                        icon_url=url,
                    )
                    .set_footer(
                        text=f"Author: {author.id} | Message ID: {payload.message_id}"
                    )
                    .set_image(url=attachment.url)
                )
                await send_embed(embed, self.bot, AUDIT_LOGS_CHANNEL)

    @Cog.listener()
    async def on_raw_bulk_message_delete(self, payload: RawBulkMessageDeleteEvent):
        guild = self.bot.get_guild(payload.guild_id)
        description = f"**Bulk Delete in {self.bot.get_channel(payload.channel_id).mention}, {len(payload.message_ids)} messages deleted**"
        embed = Embed(
            description=description,
            color=0x337FD5,
            timestamp=datetime.now(),
        ).set_author(
            name=f"{guild.name}",
            icon_url=guild.icon.url,
        )
        await send_embed(embed, self.bot, AUDIT_LOGS_CHANNEL)

    @Cog.listener()
    async def on_member_ban(self, guild: Guild, user: User | Member):
        discriminator = get_discriminator(user)
        url = get_pfp(user)

        embed = (
            Embed(
                description=f"{user.mention} {user.name}{discriminator}",
                color=0xFF470F,
                timestamp=datetime.now(),
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
        await send_embed(embed, self.bot, AUDIT_LOGS_CHANNEL)

    @Cog.listener()
    async def on_member_unban(self, guild: Guild, user: User | Member):
        discriminator = get_discriminator(user)
        url = get_pfp(user)

        embed = (
            Embed(
                description=f"{user.mention} {user.name}{discriminator}",
                color=0x337FD5,
                timestamp=datetime.now(),
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
        await send_embed(embed, self.bot, AUDIT_LOGS_CHANNEL)

    @Cog.listener()
    async def on_invite_create(self, invite: Invite):
        guild = invite.guild
        channel = invite.channel
        expiry = get_age(invite.expires_at) if invite.expires_at else "Never"
        description = f"**Invite [{invite.code}]({invite.url}) to {channel.mention} created by {invite.inviter.mention}**\nExpires in: {expiry}"
        embed = Embed(
            description=description,
            color=0x337FD5,
            timestamp=datetime.now(),
        ).set_author(
            name=f"{guild.name}",
            icon_url=guild.icon.url,
        )
        await send_embed(embed, self.bot, AUDIT_LOGS_CHANNEL)

    @Cog.listener()
    async def on_invite_delete(self, invite: Invite):
        guild = invite.guild
        description = f"**Invite [{invite.code}]({invite.url}) deleted**"
        embed = Embed(
            description=description,
            color=0xFF470F,
            timestamp=datetime.now(),
        ).set_author(
            name=f"{guild.name}",
            icon_url=guild.icon.url,
        )
        await send_embed(embed, self.bot, AUDIT_LOGS_CHANNEL)

    async def _log_role_change(
        self, member: Member, discriminator: str, url: str, roles: list[Role], add: bool
    ):
        roles_str = " ".join([role.mention for role in roles])
        message = f"**{member.mention} was {"given" if add else "removed from"} the role{"" if len(roles) == 1 else "s"} {roles_str}**"
        embed = (
            Embed(
                description=message,
                color=0x337FD5,
                timestamp=datetime.now(),
            )
            .set_author(
                name=f"{member.name}{discriminator}",
                icon_url=url,
            )
            .set_footer(text=f"ID: {member.id}")
        )
        await send_embed(
            embed,
            self.bot,
            AUDIT_LOGS_CHANNEL,
        )

    async def _log_nickname_change(
        self, member: Member, discriminator: str, url: str, before: str, after: str
    ):
        embed = (
            Embed(
                description=f"**{member.mention} changed their nickname**",
                color=0x337FD5,
                timestamp=datetime.now(),
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
            self.bot,
            AUDIT_LOGS_CHANNEL,
        )

    async def _log_pfp_change(self, member: Member, discriminator: str, url: str):
        embed = (
            Embed(
                description=f"**{member.mention} changed their profile picture**",
                color=0x337FD5,
                timestamp=datetime.now(),
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
            self.bot,
            AUDIT_LOGS_CHANNEL,
        )

    async def _log_timeout(
        self, member: Member, discriminator: str, url: str, timeout: datetime
    ):
        expiry = get_age(timeout)
        embed = (
            Embed(
                description=f"**{member.mention} has been timed out**\nExpires in: {expiry}",
                color=0x337FD5,
                timestamp=datetime.datetime.now(),
            )
            .set_author(
                name=f"{member.name}{discriminator}",
                icon_url=url,
            )
            .set_footer(text=f"ID: {member.id}")
        )
        await send_embed(
            embed,
            self.bot,
            AUDIT_LOGS_CHANNEL,
        )

    async def _log_untimeout(self, member: Member, discriminator: str, url: str):
        embed = (
            Embed(
                description=f"**{member.mention}'s timeout has been removed**",
                color=0x337FD5,
                timestamp=datetime.now(),
            )
            .set_author(
                name=f"{member.name}{discriminator}",
                icon_url=url,
            )
            .set_footer(text=f"ID: {member.id}")
        )
        await send_embed(
            embed,
            self.bot,
            AUDIT_LOGS_CHANNEL,
        )

    async def _log_message_pin(self, message: Message, discriminator: str, url: str):
        description = f"**Message {"pinned" if message.pinned else "unpinned"} in {message.channel.mention}** [Jump to Message]({message.jump_url})"
        embed = (
            Embed(
                description=description,
                color=0x337FD5,
                timestamp=datetime.now(),
            )
            .set_author(
                name=f"{message.author.name}{discriminator}",
                icon_url=url,
            )
            .set_footer(text=f"User ID: {message.author.id}")
        )
        await send_embed(
            embed,
            self.bot,
            AUDIT_LOGS_CHANNEL,
        )

    def _get_member_role_from_payload(self, payload: RawReactionActionEvent):
        guild = self.bot.get_guild(payload.guild_id)
        if not guild:
            return None, None

        member = guild.get_member(payload.user_id)
        if not member:
            return None, None

        role_map = MESSAGE_REACTION_ROLE_MAP.get(payload.message_id)
        if not role_map:
            return None, None

        role_name = role_map.get(payload.emoji.name)
        if not role_name:
            return None, None

        role = discord.utils.get(guild.roles, name=role_name)
        return (member, role) if role else (None, None)

    async def _toggle_role(self, payload: RawReactionActionEvent, add: bool):
        member, role = self._get_member_role_from_payload(payload)
        if not member or not role:
            return

        if add:
            await member.add_roles(role)
        else:
            await member.remove_roles(role)


async def setup(bot: Bot):
    await bot.add_cog(Events(bot))
