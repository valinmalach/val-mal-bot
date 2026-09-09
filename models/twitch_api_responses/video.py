from enum import Enum

from pydantic import BaseModel

from .pagination import Pagination


class VideoType(str, Enum):
    archive = "archive"
    highlight = "highlight"
    upload = "upload"


class MutedSegment(BaseModel):
    duration: int
    offset: int


class Video(BaseModel):
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


class VideoResponse(BaseModel):
    data: list[Video]
    pagination: Pagination
