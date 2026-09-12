"""Structured JSON logging so Railway reads the level instead of the stream.

Railway colors a line by which stream it landed on unless the line itself
parses as a single-line JSON object carrying a ``level`` key -- see
docs.railway.com/guides/logs. This formatter is that object. ``main.py``
points the root logger's handler at it and at stdout; ``alembic.ini`` does the
same for the migration process, which never sees ``main.py``'s setup.
"""

import json
import logging

# Railway's own level words; WARNING and CRITICAL have no exact match there.
_LEVELS = {"WARNING": "warn", "CRITICAL": "error"}

# Every attribute a LogRecord carries regardless of call site. Read off a real
# instance rather than listed by hand, so a stdlib version that adds one (as
# 3.12 did with taskName) does not silently turn into a spurious extra field.
_RESERVED = frozenset(logging.LogRecord("", 0, "", 0, "", (), None).__dict__) | {
    "message",
    "asctime",
}


class JsonFormatter(logging.Formatter):
    """One JSON object per line: level, message, logger, and any extra=."""

    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, object] = {
            "level": _LEVELS.get(record.levelname, record.levelname.lower()),
            "message": record.getMessage(),
            "logger": record.name,
        }
        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)
        # not in payload: an extra= key named level/message/logger/exception
        # must not overwrite the field it collides with -- getMessage() and
        # record.name are always plain strings, so this can only drop an
        # extra, never the record's own level, text or logger name.
        payload |= (
            (k, v)
            for k, v in record.__dict__.items()
            if k not in _RESERVED and k not in payload
        )
        try:
            # default=str: a value this can't serialise must not lose the
            # line to "--- Logging error ---" on stderr.
            return json.dumps(payload, default=str)
        except Exception as exc:  # noqa: BLE001
            # default=str only covers a type json.dumps doesn't recognise.
            # A circular extra= value raises ValueError, and a non-primitive
            # dict key (an Enum member, a tuple) raises TypeError -- neither
            # reaches default, since it's not the value being rejected. Naming
            # only those two stopped being enough the moment default=str calls
            # an extra= value's own __str__: that call can raise anything, so
            # this has to be Exception, not an enumerable list of types. The
            # three fields above are always plain strings, so this retry can't
            # fail the same way.
            return json.dumps(
                {
                    "level": payload["level"],
                    "message": payload["message"],
                    "logger": payload["logger"],
                    # type(exc).__name__, not str(exc): a class attribute
                    # lookup can't call a broken __str__ the way interpolating
                    # exc itself could -- including on exc's own class, if
                    # that's what a hostile extra= value chose to raise.
                    "formatter_error": f"an extra field was not JSON-serialisable ({type(exc).__name__})",
                }
            )


def _demo() -> None:
    logger = logging.getLogger("logging_json.demo")
    fmt = JsonFormatter()

    def rendered(record: logging.LogRecord) -> dict[str, object]:
        text = fmt.format(record)
        assert "\n" not in text, "a formatted line must never split in two"
        return json.loads(text)

    payload = rendered(
        logger.makeRecord("x", logging.WARNING, __file__, 1, "one\ntwo", (), None)
    )
    assert payload["level"] == "warn"
    assert payload["message"] == "one\ntwo"

    payload = rendered(
        logger.makeRecord("x", logging.CRITICAL, __file__, 1, "boom", (), None)
    )
    assert payload["level"] == "error"

    try:
        raise ValueError("bad")
    except ValueError as exc:
        record = logger.makeRecord(
            "x",
            logging.ERROR,
            __file__,
            1,
            "failed",
            (),
            (type(exc), exc, exc.__traceback__),
        )
    payload = rendered(record)
    assert payload["level"] == "error"
    exception = payload["exception"]
    assert isinstance(exception, str) and "ValueError" in exception

    class _Unserialisable:
        def __str__(self) -> str:
            return "<thing>"

    payload = rendered(
        logger.makeRecord(
            "x",
            logging.INFO,
            __file__,
            1,
            "arrived",
            (),
            None,
            extra={"payload": _Unserialisable(), "broadcaster_id": 123},
        )
    )
    assert payload["payload"] == "<thing>"
    assert payload["broadcaster_id"] == 123

    # An extra= key must not be able to spoof the level or logger Railway reads.
    payload = rendered(
        logger.makeRecord(
            "x",
            logging.ERROR,
            __file__,
            1,
            "bad thing happened",
            (),
            None,
            extra={"level": "debug", "logger": "spoofed"},
        )
    )
    assert payload["level"] == "error"
    assert payload["logger"] == "x"

    # A circular extra= value must not lose the line to a raised ValueError.
    circular: dict[str, object] = {}
    circular["self"] = circular
    payload = rendered(
        logger.makeRecord(
            "x",
            logging.INFO,
            __file__,
            1,
            "arrived",
            (),
            None,
            extra={"loop": circular},
        )
    )
    assert payload["level"] == "info"
    assert payload["message"] == "arrived"
    assert "formatter_error" in payload

    # A non-primitive dict key in an extra= value raises TypeError, not
    # ValueError -- must not lose the line either.
    payload = rendered(
        logger.makeRecord(
            "x",
            logging.INFO,
            __file__,
            1,
            "arrived",
            (),
            None,
            extra={"bad": {("a", "b"): 1}},
        )
    )
    assert payload["level"] == "info"
    assert payload["message"] == "arrived"
    assert "formatter_error" in payload

    # default=str calling a broken __str__ can raise anything -- not just
    # ValueError/TypeError -- and must not lose the line either.
    class _BrokenStr:
        def __str__(self) -> str:
            raise RuntimeError("str is broken too")

    payload = rendered(
        logger.makeRecord(
            "x",
            logging.INFO,
            __file__,
            1,
            "arrived",
            (),
            None,
            extra={"bad": _BrokenStr()},
        )
    )
    assert payload["level"] == "info"
    assert payload["message"] == "arrived"
    assert "formatter_error" in payload


if __name__ == "__main__":
    _demo()
