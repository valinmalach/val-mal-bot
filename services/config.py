"""In-memory snapshot of the configuration tables.

Loaded once at startup and refreshable, so an edit made in the database, or
later through a frontend, takes effect without a restart. Accessors are
synchronous because the call sites are everywhere and mostly not async.
"""

import json
import logging
import re
from typing import Any

from sqlalchemy import select

from db import (
    AppSetting,
    AutoResponseMatch,
    DiscordAutoResponse,
    DiscordChannel,
    DiscordEmbed,
    DiscordEmbedField,
    DiscordRole,
    MessageTemplate,
    SettingValueType,
    TwitchCommand,
    TwitchCommandComponent,
    TwitchCommandResponse,
)
from db.session import session_scope
from errors import notify_soon

logger = logging.getLogger(__name__)

_PLACEHOLDER = re.compile(r"\{(channel|role):([a-z0-9_]+)\}")
_FORMAT_FIELD = re.compile(r"\{([^{}]*)\}")


def _field_name(field: str) -> str:
    """The value a replacement field reads, without conversion or format spec."""
    name = re.split(r"[!:]", field, maxsplit=1)[0]
    return re.split(r"[.\[]", name, maxsplit=1)[0].strip()


def safe_format(text: str, values: dict[str, Any]) -> str:
    """``str.format`` over text nobody validated: a database row, not source.

    A brace naming nothing that was passed is left as written, so a template
    holding literal braces still renders, and text that cannot be formatted at
    all is sent as-is rather than not at all.
    """
    protected = _FORMAT_FIELD.sub(
        lambda match: (
            match.group(0)
            if _field_name(match.group(1)) in values
            else "{{" + match.group(1) + "}}"
        ),
        text,
    )
    try:
        return protected.format(**values)
    except (IndexError, KeyError, ValueError) as e:
        notify_soon(
            f"Could not format template text, so it went out with its braces"
            f" as written: {e}. Text: {text[:200]}",
            # The whole text, not a prefix: two rows sharing an opening line
            # would otherwise be held back as each other. It is a database row,
            # never user input, so the number of distinct keys is bounded by the
            # number of rows.
            key=f"template-unformattable:{text}",
        )
        return text


class ConfigCache:
    def __init__(self) -> None:
        self._channels: dict[str, int] = {}
        self._roles: dict[str, DiscordRole] = {}
        self._roles_by_emoji: dict[str, DiscordRole] = {}
        self._settings: dict[str, Any] = {}
        self._templates: dict[str, str] = {}
        self._embeds: dict[str, DiscordEmbed] = {}
        self._embed_fields: dict[str, list[DiscordEmbedField]] = {}
        self._auto_responses: list[DiscordAutoResponse] = []
        self._commands: dict[str, TwitchCommand] = {}
        self._command_responses: dict[str, list[str]] = {}
        self._command_components: dict[str, list[str]] = {}
        self._loaded = False

    @property
    def loaded(self) -> bool:
        return self._loaded

    async def load(self) -> None:
        """Read every configuration table into memory, replacing what is held."""
        async with session_scope() as session:
            channels = (await session.execute(select(DiscordChannel))).scalars().all()
            roles = (await session.execute(select(DiscordRole))).scalars().all()
            settings = (await session.execute(select(AppSetting))).scalars().all()
            templates = (await session.execute(select(MessageTemplate))).scalars().all()
            embeds = (await session.execute(select(DiscordEmbed))).scalars().all()
            fields = (await session.execute(select(DiscordEmbedField))).scalars().all()
            autos = (await session.execute(select(DiscordAutoResponse))).scalars().all()
            commands = (await session.execute(select(TwitchCommand))).scalars().all()
            responses = (
                (await session.execute(select(TwitchCommandResponse))).scalars().all()
            )
            components = (
                (await session.execute(select(TwitchCommandComponent))).scalars().all()
            )

        self._channels = {c.key: c.channel_id for c in channels}
        self._roles = {r.key: r for r in roles}
        self._roles_by_emoji = {r.emoji: r for r in roles if r.emoji}
        self._settings = {s.key: _coerce(s) for s in settings}
        self._templates = {t.key: t.content for t in templates}
        self._embeds = {e.key: e for e in embeds}

        self._embed_fields = {}
        for field in sorted(fields, key=lambda f: f.position):
            self._embed_fields.setdefault(field.embed_key, []).append(field)

        self._auto_responses = [a for a in autos if a.enabled]
        self._commands = {c.name: c for c in commands if c.enabled}

        self._command_responses = {}
        for response in sorted(responses, key=lambda r: r.position):
            self._command_responses.setdefault(response.command_name, []).append(
                response.message
            )

        self._command_components = {}
        for component in sorted(components, key=lambda c: c.position):
            self._command_components.setdefault(component.parent_name, []).append(
                component.child_name
            )

        self._loaded = True
        logger.info(
            "Configuration loaded: %d channels, %d roles, %d settings, %d templates",
            len(self._channels),
            len(self._roles),
            len(self._settings),
            len(self._templates),
        )

    def channel(self, key: str) -> int:
        try:
            return self._channels[key]
        except KeyError:
            raise KeyError(f"No discord_channel row keyed {key!r}") from None

    def role(self, key: str) -> int:
        try:
            return self._roles[key].role_id
        except KeyError:
            raise KeyError(f"No discord_role row keyed {key!r}") from None

    def role_name(self, key: str) -> str:
        return self._roles[key].name

    def role_name_for_emoji(self, emoji: str) -> str | None:
        found = self._roles_by_emoji.get(emoji)
        return found.name if found else None

    def setting(self, key: str, default: Any = None) -> Any:
        return self._settings.get(key, default)

    def color(self, key: str, default: int = 0x337FD5) -> int:
        value = self._settings.get(key, default)
        return int(value) if value is not None else default

    def template(self, key: str, **values: Any) -> str:
        """Render a message template, resolving channel and role placeholders."""
        content = self._templates.get(key)
        if content is None:
            notify_soon(
                f"No message_template row keyed {key!r}, so the bot sent nothing"
                f" where that message should have been.",
                key=f"template-missing:{key}",
            )
            return ""
        # Placeholders resolve first: str.format reads {channel:promo} as a
        # format spec and raises KeyError on the brace it does not own.
        rendered = self.render(content)
        return safe_format(rendered, values) if values else rendered

    def render(self, text: str) -> str:
        """Turn {channel:key} and {role:key} into Discord mentions."""

        def replace(match: re.Match[str]) -> str:
            kind, key = match.group(1), match.group(2)
            if kind == "channel":
                return f"<#{self.channel(key)}>"
            return f"<@&{self.role(key)}>"

        return _PLACEHOLDER.sub(replace, text)

    def embed(self, key: str) -> DiscordEmbed | None:
        return self._embeds.get(key)

    def embed_fields(self, key: str) -> list[DiscordEmbedField]:
        return self._embed_fields.get(key, [])

    def embed_keys(self) -> list[str]:
        return [e.key for e in sorted(self._embeds.values(), key=lambda e: e.position)]

    def roles_for_embed(self, key: str) -> list[DiscordRole]:
        return sorted(
            (r for r in self._roles.values() if r.embed_key == key),
            key=lambda r: r.position,
        )

    def auto_response(self, content: str) -> str | None:
        """The canned reply for a message, or None if nothing matches."""
        for row in self._auto_responses:
            subject = content if row.case_sensitive else content.lower()
            trigger = row.trigger if row.case_sensitive else row.trigger.lower()
            matched = (
                (row.match_type is AutoResponseMatch.EXACT and subject == trigger)
                or (
                    row.match_type is AutoResponseMatch.PREFIX
                    and subject.startswith(trigger)
                )
                or (row.match_type is AutoResponseMatch.CONTAINS and trigger in subject)
            )
            if matched:
                return row.response
        return None

    def command(self, name: str) -> TwitchCommand | None:
        return self._commands.get(name)

    def command_responses(self, name: str) -> list[str]:
        return self._command_responses.get(name, [])

    def command_components(self, name: str) -> list[str]:
        return self._command_components.get(name, [])


def _coerce(setting: AppSetting) -> Any:
    if setting.value is None:
        return None
    if setting.value_type is SettingValueType.INTEGER:
        return int(setting.value)
    if setting.value_type is SettingValueType.BOOLEAN:
        return setting.value.strip().lower() in {"1", "true", "yes"}
    if setting.value_type is SettingValueType.JSON:
        return json.loads(setting.value)
    return setting.value


config = ConfigCache()


def has_configured_role(key: str):
    """An app command check against a role whose ID lives in the database.

    Resolved when the command runs, so the ID can change without a redeploy --
    unlike app_commands.checks.has_role, which needs it at decoration time.
    """
    from discord import Interaction, Member, app_commands

    def predicate(interaction: Interaction) -> bool:
        member = interaction.user
        if not isinstance(member, Member):
            return False
        role_id = config.role(key)
        return any(role.id == role_id for role in member.roles)

    return app_commands.check(predicate)
