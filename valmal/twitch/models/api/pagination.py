import msgspec


class Pagination(msgspec.Struct):
    """Helix's cursor, absent on the last page and on responses that need none."""

    cursor: str | None = None
