from enum import Enum
from typing import List

from pydantic import BaseModel


class StreamType(str, Enum):
    live = "live"
    error = ""


class StreamInfo(BaseModel):
    id: str
    user_id: str
    user_login: str
    user_name: str
    game_id: str
    game_name: str
    type: StreamType
    title: str
    tags: List[str]
    viewer_count: int
    started_at: str
    language: str
    thumbnail_url: str
    is_mature: bool


class Pagination(BaseModel):
    cursor: str


class StreamInfoResponse(BaseModel):
    data: List[StreamInfo]
    pagination: Pagination
