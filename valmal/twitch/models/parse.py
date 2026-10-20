"""The one way a Twitch body becomes a model, so every caller parses alike."""

from typing import Any, TypeVar

import msgspec

# Not a PEP 695 parameter list, hence the UP047s below: Sourcery silently skips a
# file that holds one.
_M = TypeVar("_M", bound=msgspec.Struct)


def decode(text: str, model: type[_M]) -> _M:  # noqa: UP047
    """A JSON reply, parsed into `model`."""
    # strict=False takes "60" for an int and "false" for a bool, as pydantic did:
    # Twitch's docs show some numbers and booleans as strings.
    return msgspec.json.decode(text, type=model, strict=False)


def convert(body: Any, model: type[_M]) -> _M:  # noqa: UP047
    """A body already parsed from JSON, held to `model` as `decode` would."""
    return msgspec.convert(body, model, strict=False)
