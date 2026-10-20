from enum import Enum

import msgspec

from valmal.twitch.models.api.pagination import Pagination


class StreamType(str, Enum):
    live = "live"
    error = ""


class Stream(msgspec.Struct, kw_only=True):
    id: str
    user_id: str
    user_login: str
    user_name: str
    game_id: str
    game_name: str
    type: StreamType
    title: str
    tags: list[str] | None = None
    viewer_count: int
    started_at: str
    language: str
    thumbnail_url: str
    is_mature: bool


class StreamResponse(msgspec.Struct):
    data: list[Stream]
    pagination: Pagination
