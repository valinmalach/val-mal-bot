from pydantic import BaseModel


class AuthResponse(BaseModel):
    access_token: str
    expires_in: int
    token_type: str


class RefreshResponse(BaseModel):
    access_token: str
    expires_in: int | None
    refresh_token: str
    scope: list[str] | str
    token_type: str
