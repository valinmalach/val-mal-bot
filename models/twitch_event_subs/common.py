from pydantic import BaseModel


class Subscription(BaseModel):
    id: str
    type: str
    version: str
    status: str
    cost: int
    created_at: str