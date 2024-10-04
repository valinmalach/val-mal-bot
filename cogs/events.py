import discord
from discord.ext import commands

from send_discord_message import send_discord_message


class Events(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self.member = discord.Member
        self.guild = discord.guild

    @commands.Cog.listener()
    async def on_message(self, message: discord.Message):
        if message.author == self.bot.user:
            return

        if message.content == "ping":
            await message.channel.send("pong")

        if message.content == "plap":
            await message.channel.send("clank")

    @commands.Cog.listener()
    async def on_member_join(self, member: discord.Member):
        channel = self.bot.get_channel(1285276874645438544)  # welcome channel
        await channel.send(f"{member.mention} has joined. Welcome!")

    @commands.Cog.listener()
    async def on_raw_reaction_add(self, payload: discord.RawReactionActionEvent):
        if payload.message_id == 1291772906841571500:  # rules reaction message id
            guild_id = payload.guild_id
            guild = discord.utils.find(lambda g: g.id == guild_id, self.bot.guilds)

            if payload.emoji.name == "✅":
                role = discord.utils.get(guild.roles, name="Followers")

            member = discord.utils.find(
                lambda m: m.id == payload.user_id, guild.members
            )
            if member is not None:
                await member.add_roles(role)

    @commands.Cog.listener()
    async def on_raw_reaction_remove(self, payload: discord.RawReactionActionEvent):
        if payload.message_id == 1291772906841571500:
            guild_id = payload.guild_id
            guild = discord.utils.find(lambda g: g.id == guild_id, self.bot.guilds)

            if payload.emoji.name == "✅":
                role = discord.utils.get(guild.roles, name="Followers")

            member = discord.utils.find(
                lambda m: m.id == payload.user_id, guild.members
            )
            if member is not None:
                await member.remove_roles(role)


async def setup(bot: commands.Bot):
    await bot.add_cog(Events(bot))
