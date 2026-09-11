"""Register !aso and !unaso, and the line !unaso answers with.

Both are mod-only, matching !so: they curate a list that outlives the stream.
Only removal gets a template — adding someone is answered by the shoutout it
produces, and naming someone already on the list changes nothing and so says
nothing.

Revision ID: 0008
Revises: 0007
"""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects.postgresql import insert

revision: str = "0008"
down_revision: str | None = "0007"
branch_labels: str | None = None
depends_on: str | None = None

_twitch_command = sa.table(
    "twitch_command",
    sa.column("name", sa.String),
    sa.column("handler", sa.String),
    sa.column("description", sa.String),
    sa.column("mod_only", sa.Boolean),
    sa.column("enabled", sa.Boolean),
    sa.column("cooldown_seconds", sa.Integer),
    sa.column("position", sa.Integer),
)

_message_template = sa.table(
    "message_template",
    sa.column("key", sa.String),
    sa.column("content", sa.Text),
    sa.column("description", sa.String),
)

_COMMAND_ROWS = [
    {
        "name": "aso",
        "handler": "auto_shoutout",
        "description": "Add a channel to the autoshoutout list and shout it out",
        "mod_only": True,
        "enabled": True,
        "cooldown_seconds": 0,
        "position": 10,
    },
    {
        "name": "unaso",
        "handler": "un_auto_shoutout",
        "description": "Take a channel off the autoshoutout list",
        "mod_only": True,
        "enabled": True,
        "cooldown_seconds": 0,
        "position": 11,
    },
]

_TEMPLATE_ROWS = [
    {
        "key": "twitch_autoshoutout_removed",
        "content": "{name} will no longer be shouted out automatically.",
        "description": "!unaso when a row was actually removed",
    },
]


def upgrade() -> None:
    # Tolerates a conflict: a database seeded after these rows existed has them.
    op.execute(insert(_twitch_command).values(_COMMAND_ROWS).on_conflict_do_nothing())
    op.execute(
        insert(_message_template).values(_TEMPLATE_ROWS).on_conflict_do_nothing()
    )


def downgrade() -> None:
    op.execute(
        sa.delete(_message_template).where(
            _message_template.c.key.in_([row["key"] for row in _TEMPLATE_ROWS])
        )
    )
    # twitch_command_response and twitch_command_component are ON DELETE
    # CASCADE, so removing the command rows takes anything hung off them.
    op.execute(
        sa.delete(_twitch_command).where(
            _twitch_command.c.name.in_([row["name"] for row in _COMMAND_ROWS])
        )
    )
