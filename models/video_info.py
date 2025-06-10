from enum import Enum
from typing import List, Optional

from pydantic import BaseModel


class VideoType(str, Enum):
    archive = "archive"
    highlight = "highlight"
    upload = "upload"


class MutedSegment(BaseModel):
    duration: int
    offset: int


class VideoInfo(BaseModel):
    id: str
    stream_id: Optional[str] = None
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
    viewer_count: int
    language: str
    type: VideoType
    duration: str
    muted_segments: Optional[List[MutedSegment]] = None


class Pagination(BaseModel):
    cursor: Optional[str] = None


class VideoInfoResponse(BaseModel):
    data: List[VideoInfo]
    pagination: Pagination
