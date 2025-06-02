import discord
from discord import Interaction
from discord.ui import Button, View

from constants import (
    FOLLOWER_ROLE,
    FOOD_CHANNEL,
    PETS_CHANNEL,
    PROMO_CHANNEL,
    RANTING_CHANNEL,
)
from services import roles_button_pressed


class RulesView(View):
    def __init__(self) -> None:
        super().__init__(timeout=None)

    @discord.ui.button(emoji="✅")
    async def accept_rules(self, interaction: Interaction, button: Button) -> None:
        await roles_button_pressed(interaction, button)


RULES_EMBED = (
    discord.Embed(
        title="Server Rules",
        description="Please read and follow the server rules:",
        color=discord.Color.dark_blue(),
    )
    .add_field(
        name="",
        value="* This is an 18+ only server. Anyone discovered to be under 18 will be banned PERMANENTLY. No appeals will be entertained. I don't care if your 18th birthday is tomorrow. You can wait a day before you join my community. If you are banned pre-18, your ban WILL NOT be lifted once you hit 18. Permanent means permanent.",
        inline=False,
    )
    .add_field(
        name="",
        value="* Although this is an 18+ server, this does not give anybody free rein to just say anything anywhere. Keep the appropriate topics in their appropriate channels. I.e. Please keep NSFW stuff to NSFW channels. They're there for a reason. I may be horny af, but other people might not be. Let's keep things safe and comfy for everybody here, ok? 🙂",
        inline=False,
    )
    .add_field(
        name="",
        value=f"* No doxxing or sharing of personal information (birthdays and other significant dates are exceptions). Barring <#{FOOD_CHANNEL}> and <#{PETS_CHANNEL}> and possibly scenery, no sharing of irl pictures, videos or anything. This might be a bit overkill, but I have known people to have shared an innocent irl picture without realising there's doxxable info in the background. I just want to ensure a completely dox-free environment.",
        inline=False,
    )
    .add_field(
        name="",
        value="* No inappropriate names/profile pics.",
        inline=False,
    )
    .add_field(
        name="",
        value="* No spamming.",
        inline=False,
    )
    .add_field(
        name="",
        value="* Try to keep things in their relevant channels.",
        inline=False,
    )
    .add_field(
        name="",
        value="* Please restrict messages to English as much as possible! I would like to keep all messages in a language that I understand.",
        inline=False,
    )
    .add_field(
        name="",
        value="* Please do not discuss sensitive topics including, but not limited to, politics and religion on this server.",
        inline=False,
    )
    .add_field(
        name="",
        value=f"* Avoid discussions about severe mental health issues as it may be triggering for others. If you are in need of help, please seek professional help. I am not a therapist, and this isn't the place for that. Exceptions apply to <#{RANTING_CHANNEL}> **to a certain extent**. Everything has its limits. The channel can and will be removed if it's abused.",
        inline=False,
    )
    .add_field(
        name="",
        value="* Speaking of <#{RANTING_CHANNEL}>, spoiler when necessary. Especially any topics that might even be remotely triggering. If you are unsure, just spoiler it. There's no harm in spoilering.",
        inline=False,
    )
    .add_field(
        name="",
        value="* Don't be a dick, a bigot, a Karen, etc. Use common sense and show common decency. Be respectful to one another, and above all: CONSENT, CONSENT, CONSENT. For anything and everything.",
        inline=False,
    )
    .add_field(
        name="",
        value="* No discrimination or bigotry whatsoever, be it racism, homophobia, transphobia, etc. Depending on circumstances, it MAY be acceptable if it's painfully obvious to be a joke. But if other people (or I) start to feel offended/uncomfortable, you will get a strike.",
        inline=False,
    )
    .add_field(
        name="",
        value="* Speaking of strikes, barring insta-ban offences, you are allowed 3 strikes. 1 strike and you will be given a warning. 2 strikes and you will be timed out for a day. 3 strikes and you will be banned. I may or may not allow more strikes to be given to you depending on the level of offence.",
        inline=False,
    )
    .add_field(
        name="",
        value="* I'm generally a very lenient and non-confrontational person, so I will allow loads of shit, but the moment someone else feels uncomfortable, or if someone/mods/me tells you to stop. You stop. No questions or arguments.",
        inline=False,
    )
    .add_field(
        name="",
        value="* My Twitch and Discord will have shared bans/strikes. Any offences in one or the other will reflect in both.",
        inline=False,
    )
    .add_field(
        name="",
        value=f"* No self-promo outside of the <#{PROMO_CHANNEL}> channel.",
        inline=False,
    )
    .add_field(
        name="",
        value="* This is a weird rule, but no calling cats cars. It pisses me off to no end. I've been told it's 'cute' and \"it's like saying 'forgor' instead of 'forgot'\". No it's fucking not. There's a difference. Because 'forgor' isn't a real word, but 'car' is a real word. It's confusing, it makes my brain hurt. So, please, just don't. If it's a GENUINE typo, of course I won't get angry. But if you INTENTIONALLY typo it, you're just purposefully making things confusing.",
        inline=False,
    )
    .add_field(
        name="",
        value=f"**Click the button below to accept the rules to get the <@&{FOLLOWER_ROLE}> role and gain access to the server. By clicking the button, I expect you to have read and understood the rules.**",
        inline=False,
    )
)
