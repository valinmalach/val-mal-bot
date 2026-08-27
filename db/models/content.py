"""Free-standing message text that is not tied to a command or an embed."""

from sqlalchemy import Text
from sqlmodel import Field

from db.base import TimestampMixin

__all__ = ["MessageTemplate"]


class MessageTemplate(TimestampMixin, table=True):
    """A named message, e.g. the follow thank-you or the ad-break warning.

    ``content`` uses ``str.format`` placeholders documented per template.
    """

    __tablename__ = "message_template"

    key: str = Field(primary_key=True, max_length=64)
    content: str = Field(sa_type=Text)
    description: str | None = Field(default=None, max_length=255)
