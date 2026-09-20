"""In-memory snapshot of the configuration tables.

Loaded once at startup, in setup_hook(). load() is public and awaitable so a
future admin command or periodic task can reload it, but nothing calls it
again today -- an edit made in the database takes effect on the next
restart, not before it. Accessors are synchronous because the call sites are
everywhere and mostly not async.
"""

import json
import logging
import re
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any, TypeVar

from sqlalchemy import select

from errors import notify_soon
from valmal.db.models import (
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
from valmal.db.session import session_scope

logger = logging.getLogger(__name__)

_PLACEHOLDER = re.compile(r"\{(channel|role):([a-z0-9_]+)\}")
_FORMAT_FIELD = re.compile(r"\{([^{}]*)\}")

# Only reached when a colour key is missing from the settings table entirely;
# a key that exists answers with its own row. It deliberately matches the
# seeded embed_color_info, so a caller that names nothing looks like the rest.
_FALLBACK_COLOR = 0x337FD5


@dataclass(frozen=True)
class RenderedField:
    name: str
    value: str
    inline: bool


@dataclass(frozen=True)
class RenderedEmbed:
    """A stored embed with its placeholders already resolved.

    Not the table rows: handing those out left every caller to remember the
    render() call, and one that forgot sent a literal ``{channel:key}``.
    """

    title: str | None
    description: str | None
    color: int | None
    channel_key: str | None
    fields: tuple[RenderedField, ...]


def _field_name(field: str) -> str:
    """The value a replacement field reads, without conversion or format spec."""
    name = re.split(r"[!:]", field, maxsplit=1)[0]
    return re.split(r"[.\[]", name, maxsplit=1)[0].strip()


def safe_format(text: str, values: dict[str, Any]) -> str:
    """``str.format`` over text nobody validated: a database row, not source.

    A brace naming nothing that was passed is left as written, so a template
    holding literal braces still renders, and text that cannot be formatted at
    all is sent as-is rather than not at all.

    A ``{channel:x}``/``{role:x}`` left behind by render() is always one of
    those literal braces, never a real field: render() runs first and reserves
    that shape, so it must not be read as a field named "channel"/"role" just
    because the caller happens to pass a value under that name too. But a row
    that doubles its own braces around that shape (``{{role:x}}``) already
    means it literally, and doubling it again breaks str.format's own escape.
    """

    def protect(match: re.Match[str]) -> str:
        field = match.group(1)
        already_escaped = (
            match.start() > 0
            and text[match.start() - 1] == "{"
            and match.end() < len(text)
            and text[match.end()] == "}"
        )
        if already_escaped:
            return match.group(0)
        if _field_name(field) not in values or _PLACEHOLDER.fullmatch(match.group(0)):
            return "{{" + field + "}}"
        return match.group(0)

    protected = _FORMAT_FIELD.sub(protect, text)
    try:
        return protected.format(**values)
    except (IndexError, KeyError, ValueError, AttributeError, TypeError) as e:
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
        self._roles_by_custom_id: dict[str, DiscordRole] = {}
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
        self._roles_by_custom_id = {r.custom_id: r for r in roles if r.custom_id}
        self._settings = {s.key: _coerce(s) for s in settings}
        self._templates = {t.key: t.content for t in templates}
        self._embeds = {e.key: e for e in embeds}

        self._embed_fields = {}
        for field in sorted(fields, key=lambda f: f.position):
            self._embed_fields.setdefault(field.embed_key, []).append(field)

        # By id: the first row that matches wins, and Postgres promises no order
        # without an ORDER BY -- an UPDATE can move a row -- so which of two
        # overlapping replies fired would change when an admin edited one.
        self._auto_responses = [
            a for a in sorted(autos, key=lambda a: a.id or 0) if a.enabled
        ]
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

    def role_for_custom_id(self, custom_id: str) -> DiscordRole | None:
        return self._roles_by_custom_id.get(custom_id)

    def setting(self, key: str, default: Any = None) -> Any:
        return self._settings.get(key, default)

    def color(self, key: str, default: int = _FALLBACK_COLOR) -> int:
        value = self._settings.get(key, default)
        return int(value) if value is not None else default

    def template(self, key: str, **values: Any) -> str:
        """Render a message template, resolving channel and role placeholders.

        A missing row, a stale channel/role slug, or a malformed field (e.g.
        a compound reference like {mention.foo} against a plain string) all
        degrade to an admin notice instead of raising, per notify_soon's own
        docstring.
        """
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
        rendered = self.render(content, source=f"message_template:{key}")
        return safe_format(rendered, values) if values else rendered

    def render(self, text: str, *, source: str) -> str:
        """Turn {channel:key} and {role:key} into Discord mentions.

        ``source`` names the template/embed this text came from, so the
        admin notice for a stale slug says what to fix -- not just which
        slug, since two different rows can share one dangling placeholder.
        A slug with no row is left as the literal placeholder, like
        safe_format leaves an unformattable brace, rather than raising.

        A doubled ``{{channel:key}}``/``{{role:key}}`` is left untouched
        rather than resolved, mirroring str.format's own ``{{``/``}}``
        escape: a caller that goes on to safe_format (template()) gets it
        unescaped there; embed() and auto_response() do not, and leave the
        doubled braces as written.
        """

        def replace(match: re.Match[str]) -> str:
            already_escaped = (
                match.start() > 0
                and text[match.start() - 1] == "{"
                and match.end() < len(text)
                and text[match.end()] == "}"
            )
            if already_escaped:
                return match.group(0)
            kind, key = match.group(1), match.group(2)
            try:
                value = self.channel(key) if kind == "channel" else self.role(key)
            except KeyError:
                notify_soon(
                    f"{source} references {match.group(0)}, which has no row,"
                    f" so it went out as written.",
                    key=f"render-missing-{kind}:{key}:{source}",
                )
                return match.group(0)
            return f"<#{value}>" if kind == "channel" else f"<@&{value}>"

        return _PLACEHOLDER.sub(replace, text)

    def embed(self, key: str) -> RenderedEmbed | None:
        """A stored embed with channel and role placeholders resolved, or None."""
        stored = self._embeds.get(key)
        if stored is None:
            return None
        source = f"discord_embed:{key}"
        fields: list[RenderedField] = []
        for field in self._embed_fields.get(key, []):
            field_source = f"discord_embed_field:{key}:{field.position}"
            fields.append(
                RenderedField(
                    name=self.render(field.name, source=field_source),
                    value=self.render(field.value, source=field_source),
                    inline=field.inline,
                )
            )
        return RenderedEmbed(
            title=self.render(stored.title, source=source) if stored.title else None,
            description=(
                self.render(stored.description, source=source)
                if stored.description
                else None
            ),
            color=stored.color,
            channel_key=stored.channel_key,
            fields=tuple(fields),
        )

    def embed_keys(self) -> list[str]:
        # Key breaks a tie: positions are not unique, and a tie left to the order
        # the database returned rows in is an order that changes on an UPDATE.
        return [
            e.key
            for e in sorted(self._embeds.values(), key=lambda e: (e.position, e.key))
        ]

    def embed_keys_for_channel(self, channel_key: str) -> list[str]:
        """Keys of the embeds destined for one channel, in stored order.

        Read off the stored rows: embed() would render every embed to answer it,
        and a stale slug in one bound for another channel is not this caller's
        to hear about.
        """
        return [
            k for k in self.embed_keys() if self._embeds[k].channel_key == channel_key
        ]

    def roles_for_embed(self, key: str) -> list[DiscordRole]:
        return sorted(
            (r for r in self._roles.values() if r.embed_key == key),
            key=lambda r: (r.position, r.key),
        )

    def auto_response(self, content: str) -> str | None:
        """The canned reply for a message, placeholders resolved, or None."""
        for row in self._auto_responses:
            subject = content if row.case_sensitive else content.lower()
            trigger = row.trigger if row.case_sensitive else row.trigger.lower()
            if (
                (row.match_type is AutoResponseMatch.EXACT and subject == trigger)
                or (
                    row.match_type is AutoResponseMatch.PREFIX
                    and subject.startswith(trigger)
                )
                or (row.match_type is AutoResponseMatch.CONTAINS and trigger in subject)
            ):
                return self.render(
                    row.response, source=f"discord_auto_response:{row.trigger}"
                )
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

# Not a PEP 695 parameter list: Sourcery 1.45 silently analyses nothing in a
# file that has one, so the custom rules stop guarding it and say so by
# reporting clean.
_CheckT = TypeVar("_CheckT")


def has_configured_role(key: str) -> Callable[[_CheckT], _CheckT]:
    """An app command check against a role whose ID lives in the database.

    Resolved when the command runs, so the ID can change without a redeploy --
    unlike app_commands.checks.has_role, which needs it at decoration time.
    """
    from discord import Interaction, Member, app_commands

    def predicate(interaction: Interaction) -> bool:
        member = interaction.user
        if not isinstance(member, Member):
            return False
        try:
            role_id = config.role(key)
        except KeyError as missing:
            # discord.py hands only an AppCommandError to the tree's error handler,
            # so the KeyError for a role row that is gone would leave the person on
            # a spinner and tell nobody. Not a CheckFailure either: that reads as
            # a refusal, and this is a configuration fault to report.
            raise app_commands.AppCommandError(str(missing)) from missing
        return any(role.id == role_id for role in member.roles)

    return app_commands.check(predicate)
