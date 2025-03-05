import discord
from discord import Member, Message, RawReactionActionEvent
from discord.ext.commands import Bot, Cog


class Events(Cog):
    def __init__(self, bot: Bot):
        self.bot = bot
        # Message_id, reaction_emoji, role_name mapping
        self.message_reaction_role_map = {
            1291772906841571500: {"✅": "🙇Followers"},  # Rules message
            1292348915257053367: {  # Ping roles message
                "📢": "📢Announcements",
                "🔴": "🔴Live Alerts",
                "❗": "❗Ping Role",
                "🦋": "🦋Bluesky",
            },
            1292349441763971123: {"🔞": "🔞NSFW Access"},  # NSFW Access role message
            1292349494666465282: {  # Pronouns roles message
                "🙋‍♂️": "🙋‍♂️He/Him",
                "🙋‍♀️": "🙋‍♀️She/Her",
                "🙋": "🙋They/Them",
                "❓": "❓Other/Ask",
            },
            1292350341521739837: {  # Streamer, Gamer, Artist roles message
                "📽️": "📽️Streamer",
                "🎮": "🎮Gamer",
                "🎨": "🎨Artist",
            },
            1292357707365351445: {  # DMs Open, Ask to DM, DMs Closed roles message
                "🟩": "🟩DMs Open",
                "🟨": "🟨Ask to DM",
                "🟥": "🟥DMs Closed",
            },
        }

    @Cog.listener()
    async def on_message(self, message: Message):
        if message.author == self.bot.user:
            return

        if message.content == "ping":
            await message.channel.send("pong")

        if message.content == "plap":
            await message.channel.send("clank")

    @Cog.listener()
    async def on_member_join(self, member: Member):
        channel = self.bot.get_channel(1285276874645438544)  # welcome channel
        await channel.send(f"{member.mention} has joined. Welcome!")

    @Cog.listener()
    async def on_raw_reaction_add(self, payload: RawReactionActionEvent):
        await self._toggle_role(payload, True)

    @Cog.listener()
    async def on_raw_reaction_remove(self, payload: RawReactionActionEvent):
        await self._toggle_role(payload, False)

    async def _toggle_role(self, payload: RawReactionActionEvent, add: bool):
        guild = self.bot.get_guild(payload.guild_id)
        if guild is None:
            return

        member = guild.get_member(payload.user_id)
        if member is None:
            return

        message_id = payload.message_id

        role_mapping = self.message_reaction_role_map.get(message_id)
        if not role_mapping:
            return

        role_name = role_mapping.get(payload.emoji.name)
        if not role_name:
            return

        role = discord.utils.get(guild.roles, name=role_name)

        if role is None:
            return

        if add:
            await member.add_roles(role)
        else:
            await member.remove_roles(role)


async def setup(bot: Bot):
    await bot.add_cog(Events(bot))
