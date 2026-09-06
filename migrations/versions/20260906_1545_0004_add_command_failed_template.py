"""Add the template a failed slash command answers with.

Nothing caught what a command body did not: there was no on_app_command_error
handler, so a command that raised outside its own try left the person who ran it
on a spinner and told nobody. The handler needs something to say.

Revision ID: 0004
Revises: 0003
"""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects.postgresql import insert

revision: str = "0004"
down_revision: str | None = "0003"
branch_labels: str | None = None
depends_on: str | None = None

_message_template = sa.table(
    "message_template",
    sa.column("key", sa.String),
    sa.column("content", sa.Text),
    sa.column("description", sa.String),
)

_ROWS = [
    {
        "key": "command_failed",
        "content": "Something went wrong running that command. It has been reported.",
        "description": "Any slash command that raised without handling it itself",
    },
]


def upgrade() -> None:
    # Tolerates a conflict: a database seeded after this row existed already has it.
    op.execute(insert(_message_template).values(_ROWS).on_conflict_do_nothing())


def downgrade() -> None:
    op.execute(
        sa.delete(_message_template).where(
            _message_template.c.key.in_([row["key"] for row in _ROWS])
        )
    )
