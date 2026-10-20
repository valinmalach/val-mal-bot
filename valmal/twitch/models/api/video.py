from enum import Enum

import msgspec

from valmal.twitch.models.api.pagination import Pagination


class VideoType(str, Enum):
    archive = "archive"
    highlight = "highlight"
    upload = "upload"


class MutedSegment(msgspec.Struct):
    duration: int
    offset: int


class Video(msgspec.Struct, kw_only=True):
    id: str
    stream_id: str | None = None
    user_id: str
    user_login: str
    user_name: str
    title: str
    description: str
    created_at: str
    published_at: str
    url: str
    thumbnail_url: str
    viewable: str
    view_count: int
    language: str
    type: VideoType
    duration: str
    muted_segments: list[MutedSegment] | None = None


class VideoResponse(msgspec.Struct):
    data: list[Video]
    pagination: Pagination
