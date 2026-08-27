"""Initial schema.

Creates all 14 tables. Copies no data: parquet, token files and .env are untouched.

Revision ID: 0001
Revises:
"""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0001"
down_revision: str | None = None
branch_labels: str | None = None
depends_on: str | None = None


def upgrade() -> None:
    op.create_table(
        "app_setting",
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
        sa.Column("key", sa.String(length=64), nullable=False),
        sa.Column("value", sa.Text(), nullable=True),
        sa.Column(
            "value_type",
            sa.Enum(
                "string",
                "integer",
                "boolean",
                "json",
                name="setting_value_type",
                native_enum=False,
                create_constraint=True,
                length=16,
            ),
            nullable=False,
        ),
        sa.Column("description", sa.String(length=255), nullable=True),
        sa.PrimaryKeyConstraint("key", name=op.f("pk_app_setting")),
    )
    op.create_table(
        "discord_auto_response",
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
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("trigger", sa.String(length=255), nullable=False),
        sa.Column("response", sa.Text(), nullable=False),
        sa.Column(
            "match_type",
            sa.Enum(
                "exact",
                "prefix",
                "contains",
                name="auto_response_match",
                native_enum=False,
                create_constraint=True,
                length=16,
            ),
            nullable=False,
        ),
        sa.Column("case_sensitive", sa.Boolean(), nullable=False),
        sa.Column("enabled", sa.Boolean(), nullable=False),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_discord_auto_response")),
        sa.UniqueConstraint("trigger", name=op.f("uq_discord_auto_response_trigger")),
    )
    op.create_table(
        "discord_channel",
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
        sa.Column("key", sa.String(length=64), nullable=False),
        sa.Column("channel_id", sa.BigInteger(), nullable=False),
        sa.Column("description", sa.String(length=255), nullable=True),
        sa.PrimaryKeyConstraint("key", name=op.f("pk_discord_channel")),
        sa.UniqueConstraint("channel_id", name=op.f("uq_discord_channel_channel_id")),
    )
    op.create_table(
        "discord_message",
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column("id", sa.BigInteger(), autoincrement=False, nullable=False),
        sa.Column("contents", sa.Text(), nullable=True),
        sa.Column("guild_id", sa.BigInteger(), nullable=False),
        sa.Column("author_id", sa.BigInteger(), nullable=False),
        sa.Column("channel_id", sa.BigInteger(), nullable=False),
        sa.Column(
            "attachment_urls",
            postgresql.JSONB(astext_type=sa.Text()),
            server_default=sa.text("'[]'::jsonb"),
            nullable=False,
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_discord_message")),
    )
    op.create_index(
        op.f("ix_discord_message_author_id"),
        "discord_message",
        ["author_id"],
        unique=False,
    )
    op.create_index(
        op.f("ix_discord_message_channel_id"),
        "discord_message",
        ["channel_id"],
        unique=False,
    )
    op.create_table(
        "discord_user",
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
        sa.Column("id", sa.BigInteger(), autoincrement=False, nullable=False),
        sa.Column("username", sa.String(length=255), nullable=False),
        sa.Column("birthday", sa.DateTime(timezone=True), nullable=True),
        sa.Column("is_birthday_leap", sa.Boolean(), nullable=True),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_discord_user")),
    )
    op.create_index(
        op.f("ix_discord_user_birthday"), "discord_user", ["birthday"], unique=False
    )
    op.create_table(
        "live_alert",
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
            "broadcaster_id", sa.BigInteger(), autoincrement=False, nullable=False
        ),
        sa.Column("channel_id", sa.BigInteger(), nullable=False),
        sa.Column("message_id", sa.BigInteger(), nullable=False),
        sa.Column("stream_id", sa.BigInteger(), nullable=False),
        sa.Column("stream_started_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("broadcaster_id", name=op.f("pk_live_alert")),
    )
    op.create_table(
        "message_template",
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
        sa.Column("key", sa.String(length=64), nullable=False),
        sa.Column("content", sa.Text(), nullable=False),
        sa.Column("description", sa.String(length=255), nullable=True),
        sa.PrimaryKeyConstraint("key", name=op.f("pk_message_template")),
    )
    op.create_table(
        "oauth_token",
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
            "key",
            sa.Enum(
                "app",
                "user",
                "broadcaster",
                name="oauth_token_key",
                native_enum=False,
                create_constraint=True,
                length=16,
            ),
            nullable=False,
        ),
        sa.Column("access_token", sa.Text(), nullable=False),
        sa.Column("refresh_token", sa.Text(), nullable=True),
        sa.Column("expires_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column(
            "scopes",
            postgresql.JSONB(astext_type=sa.Text()),
            server_default=sa.text("'[]'::jsonb"),
            nullable=False,
        ),
        sa.PrimaryKeyConstraint("key", name=op.f("pk_oauth_token")),
    )
    op.create_table(
        "twitch_command",
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
        sa.Column("name", sa.String(length=32), nullable=False),
        sa.Column("handler", sa.String(length=64), nullable=False),
        sa.Column("description", sa.String(length=255), nullable=True),
        sa.Column("mod_only", sa.Boolean(), nullable=False),
        sa.Column("enabled", sa.Boolean(), nullable=False),
        sa.Column("cooldown_seconds", sa.Integer(), nullable=False),
        sa.Column("position", sa.Integer(), nullable=False),
        sa.PrimaryKeyConstraint("name", name=op.f("pk_twitch_command")),
    )
    op.create_table(
        "discord_embed",
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
        sa.Column("key", sa.String(length=64), nullable=False),
        sa.Column("title", sa.String(length=256), nullable=True),
        sa.Column("description", sa.Text(), nullable=True),
        sa.Column("color", sa.Integer(), nullable=True),
        sa.Column("channel_key", sa.String(length=64), nullable=True),
        sa.Column("position", sa.Integer(), nullable=False),
        sa.ForeignKeyConstraint(
            ["channel_key"],
            ["discord_channel.key"],
            name=op.f("fk_discord_embed_channel_key_discord_channel"),
            ondelete="SET NULL",
        ),
        sa.PrimaryKeyConstraint("key", name=op.f("pk_discord_embed")),
    )
    op.create_table(
        "twitch_command_component",
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
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("parent_name", sa.String(length=32), nullable=False),
        sa.Column("child_name", sa.String(length=32), nullable=False),
        sa.Column("position", sa.Integer(), nullable=False),
        sa.CheckConstraint(
            "parent_name <> child_name",
            name=op.f("ck_twitch_command_component_component_not_self_referential"),
        ),
        sa.ForeignKeyConstraint(
            ["child_name"],
            ["twitch_command.name"],
            name=op.f("fk_twitch_command_component_child_name_twitch_command"),
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(
            ["parent_name"],
            ["twitch_command.name"],
            name=op.f("fk_twitch_command_component_parent_name_twitch_command"),
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_twitch_command_component")),
        sa.UniqueConstraint(
            "parent_name",
            "child_name",
            name=op.f("uq_twitch_command_component_parent_name_child_name"),
        ),
        sa.UniqueConstraint(
            "parent_name",
            "position",
            name=op.f("uq_twitch_command_component_parent_name_position"),
        ),
    )
    op.create_index(
        op.f("ix_twitch_command_component_parent_name"),
        "twitch_command_component",
        ["parent_name"],
        unique=False,
    )
    op.create_table(
        "twitch_command_response",
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
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("command_name", sa.String(length=32), nullable=False),
        sa.Column("position", sa.Integer(), nullable=False),
        sa.Column("message", sa.Text(), nullable=False),
        sa.ForeignKeyConstraint(
            ["command_name"],
            ["twitch_command.name"],
            name=op.f("fk_twitch_command_response_command_name_twitch_command"),
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_twitch_command_response")),
        sa.UniqueConstraint(
            "command_name",
            "position",
            name=op.f("uq_twitch_command_response_command_name_position"),
        ),
    )
    op.create_index(
        op.f("ix_twitch_command_response_command_name"),
        "twitch_command_response",
        ["command_name"],
        unique=False,
    )
    op.create_table(
        "discord_embed_field",
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
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("embed_key", sa.String(length=64), nullable=False),
        sa.Column("position", sa.Integer(), nullable=False),
        sa.Column("name", sa.String(length=256), nullable=False),
        sa.Column("value", sa.Text(), nullable=False),
        sa.Column("inline", sa.Boolean(), nullable=False),
        sa.ForeignKeyConstraint(
            ["embed_key"],
            ["discord_embed.key"],
            name=op.f("fk_discord_embed_field_embed_key_discord_embed"),
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_discord_embed_field")),
        sa.UniqueConstraint(
            "embed_key",
            "position",
            name=op.f("uq_discord_embed_field_embed_key_position"),
        ),
    )
    op.create_index(
        op.f("ix_discord_embed_field_embed_key"),
        "discord_embed_field",
        ["embed_key"],
        unique=False,
    )
    op.create_table(
        "discord_role",
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
        sa.Column("key", sa.String(length=64), nullable=False),
        sa.Column("role_id", sa.BigInteger(), nullable=False),
        sa.Column("name", sa.String(length=100), nullable=False),
        sa.Column("emoji", sa.String(length=32), nullable=True),
        sa.Column("custom_id", sa.String(length=100), nullable=True),
        sa.Column("embed_key", sa.String(length=64), nullable=True),
        sa.Column("position", sa.Integer(), nullable=False),
        sa.Column("assignable", sa.Boolean(), nullable=False),
        sa.Column("description", sa.String(length=255), nullable=True),
        sa.ForeignKeyConstraint(
            ["embed_key"],
            ["discord_embed.key"],
            name=op.f("fk_discord_role_embed_key_discord_embed"),
            ondelete="SET NULL",
        ),
        sa.PrimaryKeyConstraint("key", name=op.f("pk_discord_role")),
        sa.UniqueConstraint("custom_id", name=op.f("uq_discord_role_custom_id")),
        sa.UniqueConstraint("emoji", name=op.f("uq_discord_role_emoji")),
        sa.UniqueConstraint("role_id", name=op.f("uq_discord_role_role_id")),
    )


def downgrade() -> None:
    op.drop_table("discord_role")
    op.drop_index(
        op.f("ix_discord_embed_field_embed_key"), table_name="discord_embed_field"
    )
    op.drop_table("discord_embed_field")
    op.drop_index(
        op.f("ix_twitch_command_response_command_name"),
        table_name="twitch_command_response",
    )
    op.drop_table("twitch_command_response")
    op.drop_index(
        op.f("ix_twitch_command_component_parent_name"),
        table_name="twitch_command_component",
    )
    op.drop_table("twitch_command_component")
    op.drop_table("discord_embed")
    op.drop_table("twitch_command")
    op.drop_table("oauth_token")
    op.drop_table("message_template")
    op.drop_table("live_alert")
    op.drop_index(op.f("ix_discord_user_birthday"), table_name="discord_user")
    op.drop_table("discord_user")
    op.drop_index(op.f("ix_discord_message_channel_id"), table_name="discord_message")
    op.drop_index(op.f("ix_discord_message_author_id"), table_name="discord_message")
    op.drop_table("discord_message")
    op.drop_table("discord_channel")
    op.drop_table("discord_auto_response")
    op.drop_table("app_setting")
