import discord
from discord.ext import commands


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


async def setup(bot: commands.Bot):
    await bot.add_cog(Events(bot))
