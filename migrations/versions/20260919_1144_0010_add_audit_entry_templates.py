"""Move the audit entries' hardcoded sentences into message_template.

Half the audit log's wording lived in message_template and half in f-strings in
services/audit.py, so whether an entry could be edited without a redeploy
depended on which half it fell in. The rule is now that an entry's sentences are
rows; see AGENTS.md. Every text below is the f-string it replaces, word for word,
except the two ban captions: the old code built them as `action.capitalize() +
"ed"`, which dropped a letter ("User Baned"), and these say "User Banned".

A sentence that flips on a condition is a row per variant, rather than one row
with a fragment passed in, so no English is left behind in code.

Revision ID: 0010
Revises: 0009
"""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects.postgresql import insert

revision: str = "0010"
down_revision: str | None = "0009"
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
        "key": "audit_user_banned",
        "content": "User Banned",
        "description": "Audit embed author",
    },
    {
        "key": "audit_user_unbanned",
        "content": "User Unbanned",
        "description": "Audit embed author",
    },
    {
        "key": "audit_invite_created",
        "content": "**Invite [{code}]({url}) to {channel} created**\nExpires: {expiry}",
        "description": "Audit log entry",
    },
    {
        "key": "audit_invite_created_by",
        "content": (
            "**Invite [{code}]({url}) to {channel} created by {inviter}**"
            "\nExpires: {expiry}"
        ),
        "description": "Audit log entry",
    },
    {
        "key": "audit_invite_deleted",
        "content": "**Invite [{code}]({url}) deleted**",
        "description": "Audit log entry",
    },
    {
        "key": "audit_role_added",
        "content": "**{mention} was given the role {roles}**",
        "description": "Audit log entry",
    },
    {
        "key": "audit_roles_added",
        "content": "**{mention} was given the roles {roles}**",
        "description": "Audit log entry",
    },
    {
        "key": "audit_role_removed",
        "content": "**{mention} was removed from the role {roles}**",
        "description": "Audit log entry",
    },
    {
        "key": "audit_roles_removed",
        "content": "**{mention} was removed from the roles {roles}**",
        "description": "Audit log entry",
    },
    {
        "key": "audit_message_edited",
        "content": "**Message edited in {channel}** [Jump to Message]({url})",
        "description": "Audit log entry",
    },
    {
        "key": "audit_message_pinned",
        "content": "**Message pinned in {channel}** [Jump to Message]({url})",
        "description": "Audit log entry",
    },
    {
        "key": "audit_message_unpinned",
        "content": "**Message unpinned in {channel}** [Jump to Message]({url})",
        "description": "Audit log entry",
    },
    {
        "key": "audit_message_deleted",
        "content": "**Message sent by {mention} deleted in {channel}**",
        "description": "Audit log entry, deleter unknown",
    },
    {
        "key": "audit_message_deleted_by",
        "content": "**Message sent by {mention} deleted by {deleter} in {channel}**",
        "description": "Audit log entry, deleter known",
    },
    {
        "key": "audit_message_deleted_uncached",
        "content": "**Message deleted by {mention} in {channel}**",
        "description": "Audit log entry, message no longer cached",
    },
    {
        "key": "audit_bulk_deleted",
        "content": "**Bulk Delete in {channel}, {count} messages deleted**",
        "description": "Audit log entry",
    },
    {
        "key": "audit_command_failed",
        "content": "**Command error in {channel}** [Jump to Message]({url})",
        "description": "Audit log entry",
    },
]


def upgrade() -> None:
    # Tolerates a conflict: a database seeded after these rows existed has them.
    op.execute(insert(_message_template).values(_ROWS).on_conflict_do_nothing())


def downgrade() -> None:
    # Only rows still holding the seeded text: one edited since is somebody's.
    op.execute(
        sa.delete(_message_template).where(
            sa.tuple_(_message_template.c.key, _message_template.c.content).in_(
                [(row["key"], row["content"]) for row in _ROWS]
            )
        )
    )
