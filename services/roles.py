"""Giving and taking a Discord role from the roles panel."""

import discord
from discord import Interaction, Member, PartialEmoji, Role
from discord.ui import Button

from init import bot
from services.config import config


def get_member_role(
    guild_id: int, user_id: int, emoji: PartialEmoji
) -> tuple[Member | None, Role | None]:
    guild = bot.get_guild(guild_id)
    if not guild:
        return None, None

    member = guild.get_member(user_id)
    if not member:
        return None, None

    role_name = config.role_name_for_emoji(emoji.name)
    if not role_name:
        return None, None

    role = discord.utils.get(guild.roles, name=role_name)
    return (member, role) if role else (None, None)


async def toggle_role(
    guild_id: int, user_id: int, emoji: PartialEmoji
) -> tuple[bool, Role] | None:
    member, role = get_member_role(guild_id, user_id, emoji)
    if not member or not role:
        return None

    if member.get_role(role.id) is None:
        await member.add_roles(role)
        return True, role
    else:
        await member.remove_roles(role)
        return False, role


async def roles_button_pressed(interaction: Interaction, button: Button) -> None:
    guild_id = interaction.guild_id
    member_id = interaction.user.id
    emoji = button.emoji
    if not guild_id or not emoji:
        await interaction.response.send_message(
            config.template("discord_role_error"),
            ephemeral=True,
        )
        return
    res = await toggle_role(guild_id, member_id, emoji)
    if res is None:
        await interaction.response.send_message(
            config.template("discord_role_error"),
            ephemeral=True,
        )
        return
    success, role = res
    if not success:
        await interaction.response.send_message(
            config.template("discord_role_removed", role=role.mention),
            ephemeral=True,
        )
        return
    await interaction.response.send_message(
        config.template("discord_role_added", role=role.mention),
        ephemeral=True,
    )
