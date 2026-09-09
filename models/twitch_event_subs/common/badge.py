from pydantic import BaseModel


class Badge(BaseModel):
    set_id: str
