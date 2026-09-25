"""``str.format`` over text nobody validated: a database row, not source."""

import re
from typing import Any

from valmal.core.errors import notify_soon

PLACEHOLDER = re.compile(r"\{(channel|role):([a-z0-9_]+)\}")
_FORMAT_FIELD = re.compile(r"\{([^{}]*)\}")


def _field_name(field: str) -> str:
    """The value a replacement field reads, without conversion or format spec."""
    name = re.split(r"[!:]", field, maxsplit=1)[0]
    return re.split(r"[.\[]", name, maxsplit=1)[0].strip()


def safe_format(text: str, values: dict[str, Any]) -> str:
    """``str.format`` over text nobody validated: a database row, not source.

    A brace naming nothing that was passed is left as written, so a template
    holding literal braces still renders, and text that cannot be formatted at
    all is sent as-is rather than not at all.

    A ``{channel:x}``/``{role:x}`` left behind by render() is always one of
    those literal braces, never a real field: render() runs first and reserves
    that shape, so it must not be read as a field named "channel"/"role" just
    because the caller happens to pass a value under that name too. But a row
    that doubles its own braces around that shape (``{{role:x}}``) already
    means it literally, and doubling it again breaks str.format's own escape.
    """

    def protect(match: re.Match[str]) -> str:
        field = match.group(1)
        already_escaped = (
            match.start() > 0
            and text[match.start() - 1] == "{"
            and match.end() < len(text)
            and text[match.end()] == "}"
        )
        if already_escaped:
            return match.group(0)
        if _field_name(field) not in values or PLACEHOLDER.fullmatch(match.group(0)):
            return "{{" + field + "}}"
        return match.group(0)

    protected = _FORMAT_FIELD.sub(protect, text)
    try:
        return protected.format(**values)
    except (IndexError, KeyError, ValueError, AttributeError, TypeError) as e:
        notify_soon(
            f"Could not format template text, so it went out with its braces"
            f" as written: {e}. Text: {text[:200]}",
            # The whole text, not a prefix: two rows sharing an opening line
            # would otherwise be held back as each other. It is a database row,
            # never user input, so the number of distinct keys is bounded by the
            # number of rows.
            key=f"template-unformattable:{text}",
        )
        return text
