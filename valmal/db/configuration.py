"""The configuration tables, read whole once at startup into ``ConfigCache``.

The bot's other statements are in ``repository.py``; the same rules hold here
(ADR 0005): every statement is a constant, and every read lists its columns.
"""

from dataclasses import dataclass

from valmal.db import rows
from valmal.db.enums import AutoResponseMatch, SettingValueType
from valmal.db.pool import transaction

__all__ = ["Configuration", "load_configuration"]

LIST_CHANNELS = """
SELECT key, channel_id, description, created_at, updated_at FROM discord_channel
"""

LIST_ROLES = """
SELECT key, role_id, name, emoji, custom_id, embed_key, position, assignable,
       description, created_at, updated_at
FROM discord_role
"""

LIST_SETTINGS = """
SELECT key, value, value_type, description, created_at, updated_at FROM app_setting
"""

LIST_TEMPLATES = """
SELECT key, content, description, created_at, updated_at FROM message_template
"""

LIST_EMBEDS = """
SELECT key, title, description, color, channel_key, position, created_at, updated_at
FROM discord_embed
"""

LIST_EMBED_FIELDS = """
SELECT id, embed_key, position, name, value, inline, created_at, updated_at
FROM discord_embed_field
"""

LIST_AUTO_RESPONSES = """
SELECT id, trigger, response, match_type, case_sensitive, enabled,
       created_at, updated_at
FROM discord_auto_response
"""

LIST_COMMANDS = """
SELECT name, handler, description, mod_only, enabled, cooldown_seconds, position,
       created_at, updated_at
FROM twitch_command
"""

LIST_COMMAND_RESPONSES = """
SELECT id, command_name, position, message, created_at, updated_at
FROM twitch_command_response
"""

LIST_COMMAND_COMPONENTS = """
SELECT id, parent_name, child_name, position, created_at, updated_at
FROM twitch_command_component
"""


@dataclass(frozen=True, slots=True)
class Configuration:
    """Every configuration table, read in one transaction."""

    channels: tuple[rows.DiscordChannel, ...]
    roles: tuple[rows.DiscordRole, ...]
    settings: tuple[rows.AppSetting, ...]
    templates: tuple[rows.MessageTemplate, ...]
    embeds: tuple[rows.DiscordEmbed, ...]
    embed_fields: tuple[rows.DiscordEmbedField, ...]
    auto_responses: tuple[rows.DiscordAutoResponse, ...]
    commands: tuple[rows.TwitchCommand, ...]
    command_responses: tuple[rows.TwitchCommandResponse, ...]
    command_components: tuple[rows.TwitchCommandComponent, ...]


async def load_configuration() -> Configuration:
    async with transaction() as connection:
        channels = await connection.fetch(LIST_CHANNELS)
        roles = await connection.fetch(LIST_ROLES)
        settings = await connection.fetch(LIST_SETTINGS)
        templates = await connection.fetch(LIST_TEMPLATES)
        embeds = await connection.fetch(LIST_EMBEDS)
        fields = await connection.fetch(LIST_EMBED_FIELDS)
        autos = await connection.fetch(LIST_AUTO_RESPONSES)
        commands = await connection.fetch(LIST_COMMANDS)
        responses = await connection.fetch(LIST_COMMAND_RESPONSES)
        components = await connection.fetch(LIST_COMMAND_COMPONENTS)
    return Configuration(
        channels=tuple(rows.build(rows.DiscordChannel, r) for r in channels),
        roles=tuple(rows.build(rows.DiscordRole, r) for r in roles),
        settings=tuple(
            rows.build(rows.AppSetting, r, value_type=SettingValueType(r["value_type"]))
            for r in settings
        ),
        templates=tuple(rows.build(rows.MessageTemplate, r) for r in templates),
        embeds=tuple(rows.build(rows.DiscordEmbed, r) for r in embeds),
        embed_fields=tuple(rows.build(rows.DiscordEmbedField, r) for r in fields),
        auto_responses=tuple(
            rows.build(
                rows.DiscordAutoResponse,
                r,
                match_type=AutoResponseMatch(r["match_type"]),
            )
            for r in autos
        ),
        commands=tuple(rows.build(rows.TwitchCommand, r) for r in commands),
        command_responses=tuple(
            rows.build(rows.TwitchCommandResponse, r) for r in responses
        ),
        command_components=tuple(
            rows.build(rows.TwitchCommandComponent, r) for r in components
        ),
    )
