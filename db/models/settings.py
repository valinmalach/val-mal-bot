"""Non-secret configuration that used to live in ``.env`` and ``constants.py``."""

from sqlalchemy import Text
from sqlmodel import Field

from db.base import TimestampMixin, enum_column
from db.models.enums import SettingValueType

__all__ = ["AppSetting"]


class AppSetting(TimestampMixin, table=True):
    """A single scalar setting, keyed by a stable slug.

    Secrets are deliberately absent; those stay in ``.env``.
    """

    __tablename__ = "app_setting"

    key: str = Field(primary_key=True, max_length=64)
    value: str | None = Field(default=None, sa_type=Text)
    value_type: SettingValueType = Field(
        default=SettingValueType.STRING,
        sa_type=enum_column(SettingValueType, "setting_value_type"),
        sa_column_kwargs={"nullable": False},
    )
    description: str | None = Field(default=None, max_length=255)
