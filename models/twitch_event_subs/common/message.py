from typing import Literal

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
    cheermote: Cheermote | None
    emote: Emote | None
    mention: Mention | None


class Message(BaseModel):
    text: str
    fragments: list[Fragment]
