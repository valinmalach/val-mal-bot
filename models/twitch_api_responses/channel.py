from typing import List

from pydantic import BaseModel


class Channel(BaseModel):
    broadcaster_id: str
    broadcaster_login: str
    broadcaster_name: str
    broadcaster_language: str
    game_name: str
    game_id: str
    title: str
    delay: int
    tags: List[str]
    content_classification_labels: List[str]
    is_branded_content: bool


class ChannelResponse(BaseModel):
    data: List[Channel]
