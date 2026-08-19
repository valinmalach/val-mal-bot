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
    tags: list[str]
    content_classification_labels: list[str]
    is_branded_content: bool


class ChannelResponse(BaseModel):
    data: list[Channel]
