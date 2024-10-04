import discord
from discord.ext import commands

from send_discord_message import send_discord_message


class Events(commands.Cog):
    def __init__(self, bot):
        self.bot = bot
        self.member = discord.Member
        self.guild = discord.guild

    @commands.Cog.listener()
    async def on_message(self, message):
        if message.author == self.bot.user:
            return

        if message.content == "ping":
            await message.channel.send("pong")

        if message.content == "plap":
            await message.channel.send("clank")
