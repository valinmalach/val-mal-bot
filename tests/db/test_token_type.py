from valmal.db.models.enums import TokenType


def test_token_types_are_the_strings_stored_in_the_database() -> None:
    assert [t.value for t in TokenType] == ["app", "user", "broadcaster"]
    assert TokenType("app") is TokenType.App
    # A (str, Enum): str() and equality with the bare string both matter to callers.
    assert TokenType.User == "user"
