"""Hold the Twitch users due an automatic shoutout.

Keyed by Twitch user id rather than login: a channel rename must not drop
anyone off the list. ``login`` is stored beside it so the table can be read by
a person, and is allowed to go stale — every trigger uses the login off the
event it is handling, never this one.

Revision ID: 0007
Revises: 0006
"""

import sqlalchemy as sa
from alembic import op

revision: str = "0007"
down_revision: str | None = "0006"
branch_labels: str | None = None
depends_on: str | None = None


def upgrade() -> None:
    op.create_table(
        "twitch_autoshoutout",
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column(
            "twitch_user_id", sa.BigInteger(), autoincrement=False, nullable=False
        ),
        sa.Column("login", sa.String(length=25), nullable=False),
        sa.PrimaryKeyConstraint("twitch_user_id", name=op.f("pk_twitch_autoshoutout")),
    )


def downgrade() -> None:
    op.drop_table("twitch_autoshoutout")
