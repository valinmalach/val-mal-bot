from datetime import datetime

import discord
from discord import Embed, Member, Message, RawReactionActionEvent, Role
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
    async def on_member_remove(self, member: Member):
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
        member, role = self._get_member_role_from_payload(payload)
        if not member or not role:
            return

        await self._toggle_role(member, role, True)

        discriminator = get_discriminator(member)
        url = get_pfp(member)
        embed = (
            Embed(
                description=f"**{member.mention} was given the {role.mention} role**",
                color=0x337FD5,
                timestamp=datetime.now(),
            )
            .set_author(
                name=f"{member.name} {discriminator}",
                icon_url=url,
            )
            .set_footer(text=f"ID: {member.id}")
        )
        await send_embed(
            embed,
            self.bot,
            AUDIT_LOGS_CHANNEL,
        )

    @Cog.listener()
    async def on_raw_reaction_remove(self, payload: RawReactionActionEvent):
        member, role = self._get_member_role_from_payload(payload)
        if not member or not role:
            return

        await self._toggle_role(member, role, False)

        discriminator = get_discriminator(member)
        url = get_pfp(member)
        embed = (
            Embed(
                description=f"**{member.mention} was removed from the {role.mention} role**",
                color=0x337FD5,
                timestamp=datetime.now(),
            )
            .set_author(
                name=f"{member.name} {discriminator}",
                icon_url=url,
            )
            .set_footer(text=f"ID: {member.id}")
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

    async def _toggle_role(self, member: Member, role: Role, add: bool):
        if add:
            await member.add_roles(role)
        else:
            await member.remove_roles(role)


async def setup(bot: Bot):
    await bot.add_cog(Events(bot))
