"""Drop the /fix-birthdays templates.

The command went with the birthday task that no longer lets a birthday lapse, and
these three rows are what it used to say. 0002 was written after the removal, so a
database seeded from it never had them; this is for the ones seeded before.

Revision ID: 0003
Revises: 0002
"""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects.postgresql import insert

revision: str = "0003"
down_revision: str | None = "0002"
branch_labels: str | None = None
depends_on: str | None = None

_message_template = sa.table(
    "message_template",
    sa.column("key", sa.String),
    sa.column("content", sa.Text),
    sa.column("description", sa.String),
)

# Named one at a time rather than matched on a prefix, which would also take a
# key somebody added later. The content is here so the downgrade can restore it.
_ROWS = [
    {
        "key": "admin_birthday_sweep_none",
        "content": "No birthdays needed moving. Every stored birthday is still ahead.",
        "description": "/fix-birthdays when nothing had lapsed",
    },
    {
        "key": "admin_birthday_sweep_done",
        "content": "Moved {count} lapsed birthday(s) forward (dates in UTC):\n{details}",
        "description": "/fix-birthdays summary of what it rescheduled",
    },
    {
        "key": "admin_birthday_sweep_failed",
        "content": "The birthday sweep failed: {error}",
        "description": "/fix-birthdays when the sweep raised",
    },
]


def upgrade() -> None:
    op.execute(
        sa.delete(_message_template).where(
            _message_template.c.key.in_([row["key"] for row in _ROWS])
        )
    )


def downgrade() -> None:
    op.execute(insert(_message_template).values(_ROWS).on_conflict_do_nothing())
