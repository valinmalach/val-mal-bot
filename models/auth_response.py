from pydantic import BaseModel


class AuthResponse(BaseModel):
    access_token: str
    expires_in: int
    token_type: str
