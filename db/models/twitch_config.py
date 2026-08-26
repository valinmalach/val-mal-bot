"""Twitch chat commands and their canned responses.

``handler`` names a callable in the registry; ``static`` just sends the responses.
"""

from sqlalchemy import CheckConstraint, Text, UniqueConstraint
from sqlmodel import Field

from db.base import TimestampMixin

__all__ = ["TwitchCommand", "TwitchCommandComponent", "TwitchCommandResponse"]

STATIC_HANDLER = "static"


class TwitchCommand(TimestampMixin, table=True):
    """A ``!command`` recognised in Twitch chat, stored without the ``!``."""

    __tablename__ = "twitch_command"

    name: str = Field(primary_key=True, max_length=32)
    handler: str = Field(
        default=STATIC_HANDLER,
        max_length=64,
        description="Registry key of the handler; 'static' sends the responses",
    )
    description: str | None = Field(default=None, max_length=255)
    mod_only: bool = Field(default=False)
    enabled: bool = Field(default=True)
    cooldown_seconds: int = Field(default=0)
    position: int = Field(default=0)


class TwitchCommandResponse(TimestampMixin, table=True):
    """One chat message sent by a command, ordered by ``position``."""

    __tablename__ = "twitch_command_response"
    __table_args__ = (UniqueConstraint("command_name", "position"),)

    id: int | None = Field(default=None, primary_key=True)
    command_name: str = Field(
        max_length=32,
        foreign_key="twitch_command.name",
        ondelete="CASCADE",
        index=True,
    )
    position: int = Field(default=0)
    message: str = Field(sa_type=Text)


class TwitchCommandComponent(TimestampMixin, table=True):
    """Lets one command run others, the way ``!everything`` does."""

    __tablename__ = "twitch_command_component"
    __table_args__ = (
        UniqueConstraint("parent_name", "position"),
        UniqueConstraint("parent_name", "child_name"),
        CheckConstraint(
            "parent_name <> child_name",
            name="component_not_self_referential",
        ),
    )

    id: int | None = Field(default=None, primary_key=True)
    parent_name: str = Field(
        max_length=32,
        foreign_key="twitch_command.name",
        ondelete="CASCADE",
        index=True,
    )
    child_name: str = Field(
        max_length=32,
        foreign_key="twitch_command.name",
        ondelete="CASCADE",
    )
    position: int = Field(default=0)
