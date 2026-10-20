import msgspec


class AuthResponse(msgspec.Struct):
    access_token: str
    expires_in: int
    token_type: str


class RefreshResponse(msgspec.Struct):
    access_token: str
    expires_in: int | None
    refresh_token: str
    scope: list[str] | str
    token_type: str


class TokenValidationResponse(msgspec.Struct):
    client_id: str
    login: str
    scopes: list[str]
    user_id: str
    expires_in: int
