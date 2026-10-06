from datetime import UTC, datetime
from typing import Any

import pytest

from tests.db.support import Database
from valmal.db import configuration
from valmal.db.enums import AutoResponseMatch, SettingValueType

pytestmark = pytest.mark.anyio

STAMP = datetime(2026, 1, 1, tzinfo=UTC)
STAMPS = {"created_at": STAMP, "updated_at": STAMP}

READS = [
    configuration.LIST_CHANNELS,
    configuration.LIST_ROLES,
    configuration.LIST_SETTINGS,
    configuration.LIST_TEMPLATES,
    configuration.LIST_EMBEDS,
    configuration.LIST_EMBED_FIELDS,
    configuration.LIST_AUTO_RESPONSES,
    configuration.LIST_COMMANDS,
    configuration.LIST_COMMAND_RESPONSES,
    configuration.LIST_COMMAND_COMPONENTS,
]


def answers(**by_read: list[dict[str, Any]]) -> list[list[dict[str, Any]]]:
    """One answer per read, in READS order; a read not named gets no rows."""
    names = [name for name, value in vars(configuration).items() if value in READS]
    order = sorted(names, key=lambda name: READS.index(getattr(configuration, name)))
    return [by_read.get(name, []) for name in order]


async def test_every_table_is_read_once_in_one_transaction(database: Database) -> None:
    """One transaction, so a configuration edit mid-load cannot be half seen."""
    await configuration.load_configuration()

    assert [call[1] for call in database.calls] == READS
    assert database.transactions == 1


async def test_stored_enum_values_come_back_as_their_enums(
    database: Database,
) -> None:
    database.answers = answers(
        LIST_SETTINGS=[
            {"key": "k", "value": "1", "value_type": "integer", "description": None}
            | STAMPS
        ],
        LIST_AUTO_RESPONSES=[
            {
                "id": 1,
                "trigger": "hi",
                "response": "hello",
                "match_type": "prefix",
                "case_sensitive": False,
                "enabled": True,
            }
            | STAMPS
        ],
    )

    loaded = await configuration.load_configuration()

    assert loaded.settings[0].value_type is SettingValueType.INTEGER
    assert loaded.auto_responses[0].match_type is AutoResponseMatch.PREFIX


async def test_each_table_lands_in_its_own_field(database: Database) -> None:
    database.answers = answers(
        LIST_CHANNELS=[
            {"key": "audit_logs", "channel_id": 5, "description": None} | STAMPS
        ],
        LIST_COMMANDS=[
            {
                "name": "so",
                "handler": "shoutout",
                "description": None,
                "mod_only": True,
                "enabled": True,
                "cooldown_seconds": 0,
                "position": 0,
            }
            | STAMPS
        ],
    )

    loaded = await configuration.load_configuration()

    assert [c.channel_id for c in loaded.channels] == [5]
    assert [c.name for c in loaded.commands] == ["so"]
    assert loaded.roles == () and loaded.embed_fields == ()


async def test_a_database_failure_reaches_the_caller(database: Database) -> None:
    database.failure = ConnectionError("database is down")

    with pytest.raises(ConnectionError):
        await configuration.load_configuration()
