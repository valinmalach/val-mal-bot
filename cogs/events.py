import datetime

import discord
from dateutil import relativedelta
from discord import Member, Message, RawReactionActionEvent
from discord.ext.commands import Bot, Cog, CommandError, Context

from constants import AUDIT_LOGS_CHANNEL, WELCOME_CHANNEL


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

        content = message.content.lower()
        if content == "ping":
            await message.channel.send("pong")
        elif content == "plap":
            await message.channel.send("clank")

    @Cog.listener()
    async def on_member_join(self, member: Member):
        # Welcome message
        channel = self.bot.get_channel(WELCOME_CHANNEL)
        member_count = str(member.guild.member_count)
        if member_count[-1] == "1" and member_count != "11":
            suffix = "st"
        elif member_count[-1] == "2" and member_count != "12":
            suffix = "nd"
        elif member_count[-1] == "3" and member_count != "13":
            suffix = "rd"
        else:
            suffix = "th"
        await channel.send(
            f"{member.mention} has joined. Welcome!\nYou're the {member_count}{suffix} member!"
        )

        # Audit log
        url = (
            member.avatar.url
            if member.avatar is not None
            else member.default_avatar.url
        )
        age = relativedelta.relativedelta(
            datetime.datetime.now(datetime.timezone.utc), member.created_at
        )
        year, month, day, hour, minute, second = (
            age.years,
            age.months,
            age.days,
            age.hours,
            age.minutes,
            age.seconds,
        )
        week = day // 7
        year_ln = f"{year} years, " if year > 1 else f"{year} year, "
        month_ln = f"{month} months, " if month > 1 else f"{month} month, "
        week_ln = f"{week} weeks, " if week > 1 else f"{week} week, "
        hour_ln = f"{hour} hrs, " if hour > 1 else f"{hour} hr, "
        minute_ln = f"{minute} mins, " if minute > 1 else f"{minute} min, "
        second_ln = f"{second} secs" if second > 1 else f"{second} sec"
        if month >= 1 or year >= 1:
            day_ln = f"{day} days" if day > 1 else f"{day} day"
            age_ln = (
                f"{year_ln}{month_ln}{day_ln}" if year >= 1 else f"{month_ln}{day_ln}"
            )
        else:
            day = day % 7
            day_ln = f"{day} days, " if day > 1 else f"{day} day, "
            if week > 0:
                age_ln = f"{week_ln}{day_ln}{hour_ln}{minute_ln}{second_ln}"
            elif day > 0:
                age_ln = f"{day_ln}{hour_ln}{minute_ln}{second_ln}"
            elif hour > 0:
                age_ln = f"{hour_ln}{minute_ln}{second_ln}"
            elif minute > 0:
                age_ln = f"{minute_ln}{second_ln}"
            else:
                age_ln = f"{second_ln}"
        if member.discriminator == "0":
            discriminator = ""
        else:
            discriminator = f"#{member.discriminator}"
        await self.bot.get_channel(AUDIT_LOGS_CHANNEL).send(
            embed=(
                discord.Embed(
                    description=f"{member.mention} {member.name}{discriminator}",
                    color=0x43B582,
                    timestamp=datetime.datetime.now(),
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
                    value=f"{age_ln}",
                    inline=False,
                )
                .set_footer(text=f"ID: {member.id}")
            )
        )

    @Cog.listener()
    async def on_member_remove(self, member: Member):
        # Goodbye message
        channel = self.bot.get_channel(WELCOME_CHANNEL)
        await channel.send(f"{member.mention} has left the server. Goodbye!")

        # Audit log
        url = (
            member.avatar.url
            if member.avatar is not None
            else member.default_avatar.url
        )
        triple_nl = "" if member.roles[1:] else "\n\n\n"
        if member.discriminator == "0":
            discriminator = ""
        else:
            discriminator = f"#{member.discriminator}"
        embed = (
            discord.Embed(
                description=f"{member.mention} {member.name}{discriminator}{triple_nl}",
                color=0xFF470F,
                timestamp=datetime.datetime.now(),
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
        await self.bot.get_channel(AUDIT_LOGS_CHANNEL).send(embed=embed)

    @Cog.listener()
    async def on_command_error(self, ctx: Context, error: CommandError):
        self.bot.get_channel(AUDIT_LOGS_CHANNEL).send(
            f"Command not found: {ctx.message.content}\n"
            + f"Sent by: {ctx.author.mention} in {ctx.channel.mention}\n"
            + f"{error}"
        )

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
