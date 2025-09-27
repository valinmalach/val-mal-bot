from pydantic import BaseModel


class Badge(BaseModel):
    set_id: str
    id: str
    info: str
