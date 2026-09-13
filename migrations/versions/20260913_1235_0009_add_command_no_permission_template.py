"""Add the template a permission-checked slash command answers with when refused.

`has_permissions` raises `MissingPermissions` whenever a server admin has
reconfigured who may run an admin-only command away from the decorator's
default - `default_permissions` is a UI hint Discord lets an admin override, not
an enforced check. The floor needs something to say that is not "reported".

Revision ID: 0009
Revises: 0008
"""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects.postgresql import insert

revision: str = "0009"
down_revision: str | None = "0008"
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
        "key": "command_no_permission",
        "content": "You do not have permission to use this command.",
        "description": "A slash command whose has_permissions check failed",
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
