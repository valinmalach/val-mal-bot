"""Metadata conventions, column types and mixins shared by every table.

Import before any table class: this installs the naming convention.
"""

from datetime import UTC, datetime
from enum import Enum
from typing import Any, ClassVar

from sqlalchemy import DateTime, func
from sqlalchemy import Enum as SAEnum
from sqlmodel import Field, SQLModel

# Deterministic constraint names keep Alembic autogenerate stable: without them
# Postgres invents names and every migration wants to drop and recreate things.
NAMING_CONVENTION = {
    "ix": "ix_%(table_name)s_%(column_0_N_name)s",
    "uq": "uq_%(table_name)s_%(column_0_N_name)s",
    "ck": "ck_%(table_name)s_%(constraint_name)s",
    "fk": "fk_%(table_name)s_%(column_0_N_name)s_%(referred_table_name)s",
    "pk": "pk_%(table_name)s",
}

SQLModel.metadata.naming_convention = NAMING_CONVENTION

# Any because SQLModel types sa_type as type[Any], yet an instance is the only
# way to say "with time zone". Sharing type objects across columns is safe.
UTC_TIMESTAMP: Any = DateTime(timezone=True)


def enum_column(enum_type: type[Enum], name: str, length: int = 16) -> Any:
    """A checked ``VARCHAR`` column type for a string enum.

    Not a native Postgres enum: adding a value to one of those needs ALTER TYPE.
    """
    return SAEnum(
        enum_type,
        name=name,
        native_enum=False,
        create_constraint=True,
        length=length,
        values_callable=lambda enum: [member.value for member in enum],
    )


def utc_now() -> datetime:
    return datetime.now(UTC)


class TableBase(SQLModel):
    """Base for every table class, restoring a usable ``__tablename__``.

    ``Any`` not ``str``: SQLModel declares the name as both. See db/README.md.
    """

    __tablename__: ClassVar[Any]


class TimestampMixin(TableBase):
    """Adds ``created_at``/``updated_at`` columns maintained by the database."""

    created_at: datetime = Field(
        default_factory=utc_now,
        sa_type=UTC_TIMESTAMP,
        sa_column_kwargs={"nullable": False, "server_default": func.now()},
    )
    updated_at: datetime = Field(
        default_factory=utc_now,
        sa_type=UTC_TIMESTAMP,
        sa_column_kwargs={
            "nullable": False,
            "server_default": func.now(),
            "onupdate": func.now(),
        },
    )


class CreatedAtMixin(TableBase):
    """Adds a ``created_at`` column for append-mostly tables."""

    created_at: datetime = Field(
        default_factory=utc_now,
        sa_type=UTC_TIMESTAMP,
        sa_column_kwargs={"nullable": False, "server_default": func.now()},
    )
