"""Retire the public Discord invite, and drop kofi/throne from !everything.

The permanent invite is gone: it was drawing bot accounts, so joining is now by
asking a person. !kofi and !throne stay as commands and just stop being part of
the !everything fanout.

Revision ID: 0006
Revises: 0005
"""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects.postgresql import insert

revision: str = "0006"
down_revision: str | None = "0005"
branch_labels: str | None = None
depends_on: str | None = None

_twitch_command_response = sa.table(
    "twitch_command_response",
    sa.column("command_name", sa.String),
    sa.column("position", sa.Integer),
    sa.column("message", sa.Text),
)
_twitch_command_component = sa.table(
    "twitch_command_component",
    sa.column("parent_name", sa.String),
    sa.column("child_name", sa.String),
    sa.column("position", sa.Integer),
)

_OLD_DISCORD = "https://discord.gg/tkJyNJH2k7 Come join us and hang out! This is also where all my updates on streams and whatnot go"
_NEW_DISCORD = "Due to an influx of bots, the Discord link is no longer public. Moots and friends can DM me or a mod directly to join the server. Sorry for the inconvenience!"

# The positions 0002 gave them; !everything sorts on position, so removing two
# leaves gaps and no reordering.
_DROPPED_COMPONENTS = [
    {"parent_name": "everything", "child_name": "kofi", "position": 2},
    {"parent_name": "everything", "child_name": "throne", "position": 3},
]


def _set_discord_message(new: str, old: str) -> None:
    # Guarded on the text it replaces, so a message edited in the database stays.
    op.execute(
        sa.update(_twitch_command_response)
        .where(
            _twitch_command_response.c.command_name == "discord",
            _twitch_command_response.c.position == 0,
            _twitch_command_response.c.message == old,
        )
        .values(message=new)
    )


def upgrade() -> None:
    _set_discord_message(_NEW_DISCORD, _OLD_DISCORD)
    op.execute(
        sa.delete(_twitch_command_component).where(
            _twitch_command_component.c.parent_name == "everything",
            _twitch_command_component.c.child_name.in_(["kofi", "throne"]),
        )
    )


def downgrade() -> None:
    _set_discord_message(_OLD_DISCORD, _NEW_DISCORD)
    op.execute(
        insert(_twitch_command_component)
        .values(_DROPPED_COMPONENTS)
        .on_conflict_do_nothing()
    )
