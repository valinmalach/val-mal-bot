from pydantic import BaseModel


class Subscription(BaseModel):
    """Everything an EventSub subscription carries except its type.

    Each subclass declares that as a Literal, so a payload for the wrong event
    fails to parse. A str here could not be narrowed to one soundly.
    """

    id: str
    version: str
    status: str
    cost: int
    created_at: str
