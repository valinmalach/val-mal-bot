"""Store the timezone a birthday was set in.

The roll-forward had nothing to rebuild a local date in, so it bumped the year
on a UTC instant and greeted 201 of 598 zones on the wrong local day - issue #12.
Nullable, and null on every existing row: there is no timezone to backfill from,
and null keeps the old behaviour until the person sets their birthday again.

Revision ID: 0005
Revises: 0004
"""

import sqlalchemy as sa
from alembic import op

revision: str = "0005"
down_revision: str | None = "0004"
branch_labels: str | None = None
depends_on: str | None = None


def upgrade() -> None:
    op.add_column(
        "discord_user",
        sa.Column("birthday_timezone", sa.String(length=64), nullable=True),
    )


def downgrade() -> None:
    op.drop_column("discord_user", "birthday_timezone")
