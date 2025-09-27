from typing import Literal, Optional

from pydantic import BaseModel


class Cheermote(BaseModel):
    prefix: str
    bits: int
    tier: int


class Emote(BaseModel):
    id: str
    emote_set_id: str
    owner_id: str
    format: list[Literal["static", "animated"]]


class Mention(BaseModel):
    user_id: str
    user_name: str
    user_login: str


class Fragment(BaseModel):
    type: Literal["text", "cheermote", "emote", "mention"]
    text: str
    cheermote: Optional[Cheermote]
    emote: Optional[Emote]
    mention: Optional[Mention]


class Message(BaseModel):
    text: str
    fragments: list[Fragment]
