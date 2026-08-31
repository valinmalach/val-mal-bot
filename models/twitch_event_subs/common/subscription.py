from pydantic import BaseModel


class Subscription(BaseModel):
    """The fields every EventSub subscription carries that mean the same thing.

    Not type, which each subclass declares as a Literal so a payload for the
    wrong event fails to parse; a str here could not be narrowed to one soundly.
    Not transport either, which is in the payload but nothing here reads.
    """

    id: str
    version: str
    status: str
    cost: int
    created_at: str
