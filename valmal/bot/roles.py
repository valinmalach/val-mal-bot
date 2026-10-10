"""Giving and taking a Discord role from the roles panel."""

import asyncio
from weakref import WeakValueDictionary

import discord
from discord import Guild, Interaction, Member, Role
from discord.ui import Button, View

from valmal.bot.client import bot
from valmal.core.background import fire_and_forget
from valmal.core.config import config
from valmal.core.errors import notify, report

# One change at a time per member and role, read fresh: the cached member catches up
# only when the gateway's update arrives, so a quick second press saw the role as it
# was and toggled it the same way again.
_toggling: WeakValueDictionary[tuple[int, int], asyncio.Lock] = WeakValueDictionary()


def _unmanageable(guild: Guild, role: Role) -> str | None:
    """Why Discord would refuse the bot granting or revoking this role, if it would."""
    me = guild.me
    if not me.guild_permissions.manage_roles:
        return "the bot lacks the Manage Roles permission"
    if role.is_default() or role.managed:
        return "it is @everyone or managed by an integration"
    if role >= me.top_role:
        return "it is at or above the bot's top role"
    return None


def get_member_role(
    guild_id: int, user_id: int, custom_id: str
) -> tuple[Member | None, Role | None]:
    guild = bot.get_guild(guild_id)
    if not guild:
        return None, None

    member = guild.get_member(user_id)
    if not member:
        return None, None

    stored = config.role_for_custom_id(custom_id)
    if not stored:
        return None, None

    role = guild.get_role(stored.role_id)
    if not role:
        # Configured but gone from the guild -- worth a notice, unlike a member
        # who simply isn't there, which is silent below. Fired rather than
        # awaited, so the presser's reply does not wait on an admin-channel send.
        fire_and_forget(
            notify(
                f"discord_role {stored.key!r} points at role id"
                f" {stored.role_id}, which no longer exists in the guild.",
                key=f"discord-role-missing:{stored.key}",
            ),
            name="notify",
        )
        return None, None

    if reason := _unmanageable(guild, role):
        # The reason is in the key so that fixing one cause does not hold back
        # the notice for the next.
        fire_and_forget(
            notify(
                f"discord_role {stored.key!r} points at role id {role.id},"
                f" which the bot cannot manage: {reason}.",
                key=f"discord-role-unmanageable:{stored.key}:{reason}",
            ),
            name="notify",
        )
        return None, None

    return member, role


async def toggle_role(
    guild_id: int, user_id: int, custom_id: str
) -> tuple[bool, Role] | None:
    member, role = get_member_role(guild_id, user_id, custom_id)
    if not member or not role:
        return None

    lock = _toggling.setdefault((member.id, role.id), asyncio.Lock())
    try:
        async with lock:
            current = await member.guild.fetch_member(member.id)
            if current.get_role(role.id) is None:
                await current.add_roles(role)
                return True, role
            await current.remove_roles(role)
            return False, role
    except discord.HTTPException as e:
        # Fired for the same reason as the notices above.
        fire_and_forget(
            report(e, f"Failed to toggle role id {role.id}"),
            name="report",
        )
        return None


async def roles_button_pressed(interaction: Interaction, button: Button[View]) -> None:
    # Before the role change, which is an API call of its own: an interaction
    # left unanswered for about three seconds fails for the presser.
    await interaction.response.defer(ephemeral=True, thinking=True)
    guild_id = interaction.guild_id
    custom_id = button.custom_id

    # A button with no custom_id and a toggle that could not be made
    # are one answer to the presser: the reason is theirs to act on in neither case.
    res = (
        await toggle_role(guild_id, interaction.user.id, custom_id)
        if guild_id and custom_id
        else None
    )
    if res is None:
        await interaction.followup.send(
            config.template("discord_role_error"),
            ephemeral=True,
        )
        return

    added, role = res
    await interaction.followup.send(
        config.template(
            "discord_role_added" if added else "discord_role_removed",
            role=role.mention,
        ),
        ephemeral=True,
    )
