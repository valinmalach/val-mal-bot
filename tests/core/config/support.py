"""Fakes and row builders shared by the ConfigCache tests."""

from typing import Any

from valmal.db import rows
from valmal.db.configuration import Configuration
from valmal.db.enums import SettingValueType
from valmal.db.rows import AppSetting, DiscordRole

# What the notices fixture records: each notice's text and its repeat key.
Notices = list[tuple[str, str | None]]


# Which Configuration field each row type is loaded into.
_FIELDS: dict[type, str] = {
    rows.DiscordChannel: "channels",
    rows.DiscordRole: "roles",
    rows.AppSetting: "settings",
    rows.MessageTemplate: "templates",
    rows.DiscordEmbed: "embeds",
    rows.DiscordEmbedField: "embed_fields",
    rows.DiscordAutoResponse: "auto_responses",
    rows.TwitchCommand: "commands",
    rows.TwitchCommandResponse: "command_responses",
    rows.TwitchCommandComponent: "command_components",
}


def configuration(*given: object) -> Configuration:
    """What load_configuration() would return for these rows."""
    grouped: dict[str, list[Any]] = {name: [] for name in _FIELDS.values()}
    for row in given:
        grouped[_FIELDS[type(row)]].append(row)
    return Configuration(**{name: tuple(found) for name, found in grouped.items()})


async def database_down() -> Configuration:
    raise ConnectionError("database is down")


def role(key: str, role_id: int, **fields: Any) -> DiscordRole:
    return DiscordRole(key=key, role_id=role_id, name=key.title(), **fields)


def setting(key: str, value: str | None, kind: SettingValueType) -> AppSetting:
    return AppSetting(key=key, value=value, value_type=kind)
