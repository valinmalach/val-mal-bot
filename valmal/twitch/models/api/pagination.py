from pydantic import BaseModel


class Pagination(BaseModel):
    """Helix's cursor, absent on the last page and on responses that need none."""

    cursor: str | None = None
