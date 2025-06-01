import discord
import sentry_sdk
from discord import CategoryChannel, ForumChannel
from discord.abc import PrivateChannel
from discord.ext.commands import Bot

from constants import BOT_ADMIN_CHANNEL, GUILD_ID

MY_GUILD = discord.Object(id=GUILD_ID)


class MyBot(Bot):
    def __init__(self, *, command_prefix: str, intents: discord.Intents) -> None:
        super().__init__(command_prefix=command_prefix, intents=intents)
        self.case_insensitive = True

    async def setup_hook(self) -> None:
        self.tree.copy_global_to(guild=MY_GUILD)
        await self.tree.sync(guild=MY_GUILD)


bot = MyBot(command_prefix="$", intents=discord.Intents.all())


@bot.event
@sentry_sdk.trace()
async def on_ready() -> None:
    channel = bot.get_channel(BOT_ADMIN_CHANNEL)
    if channel is None or isinstance(
        channel, (ForumChannel, CategoryChannel, PrivateChannel)
    ):
        return
    await channel.send("Started successfully!")
