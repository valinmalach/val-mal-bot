import io
import json
import logging
from types import TracebackType

import pytest

from valmal.core import logging_json
from valmal.core.logging_json import JsonFormatter

ExcInfo = tuple[type[BaseException], BaseException, TracebackType | None]


def _format(
    level: int = logging.INFO,
    msg: str = "hello",
    args: tuple[object, ...] = (),
    exc_info: ExcInfo | None = None,
    extra: dict[str, object] | None = None,
    name: str = "test.logger",
) -> dict[str, object]:
    record = logging.getLogger(name).makeRecord(
        name,
        level,
        __file__,
        1,
        msg,
        args,
        exc_info,
        extra=extra,  # pyright: ignore[reportArgumentType]
    )
    text = JsonFormatter().format(record)
    assert "\n" not in text, "a formatted line must never split in two"
    return json.loads(text)


def test_the_modules_own_self_check_still_passes() -> None:
    """logging_json._demo is a runnable check that used to be run by hand."""
    logging_json._demo()


@pytest.mark.parametrize(
    ("level", "word"),
    [
        (logging.DEBUG, "debug"),
        (logging.INFO, "info"),
        (logging.WARNING, "warn"),
        (logging.ERROR, "error"),
        (logging.CRITICAL, "error"),
    ],
)
def test_levels_are_railways_words(level: int, word: str) -> None:
    assert _format(level)["level"] == word


def test_every_line_carries_level_message_and_logger() -> None:
    payload = _format(logging.WARNING, "careful", name="a.b")

    assert payload == {"level": "warn", "message": "careful", "logger": "a.b"}


def test_percent_arguments_are_interpolated() -> None:
    assert (
        _format(msg="%s took %d ms", args=("call", 42))["message"] == "call took 42 ms"
    )


def test_a_multiline_message_stays_on_one_line_as_an_escaped_string() -> None:
    assert _format(msg="one\ntwo\r\nthree")["message"] == "one\ntwo\r\nthree"


def test_non_ascii_text_survives() -> None:
    assert _format(msg="héllo 🎮 世界")["message"] == "héllo 🎮 世界"


def test_extra_fields_become_top_level_keys() -> None:
    payload = _format(extra={"broadcaster_id": 123, "ok": True, "tags": ["a", "b"]})

    assert payload["broadcaster_id"] == 123
    assert payload["ok"] is True
    assert payload["tags"] == ["a", "b"]


def test_an_unserialisable_extra_is_stringified_rather_than_losing_the_line() -> None:
    class Thing:
        def __str__(self) -> str:
            return "<thing>"

    assert _format(extra={"thing": Thing()})["thing"] == "<thing>"


@pytest.mark.parametrize("reserved", ["level", "logger", "exception"])
def test_an_extra_cannot_overwrite_a_field_the_formatter_owns(reserved: str) -> None:
    """Railway colours the line by `level`; an extra named like it must not win.

    `message` is not here: the stdlib refuses that name before a record exists.
    """
    try:
        raise ValueError("bad")
    except ValueError as exc:
        info = (type(exc), exc, exc.__traceback__)
        payload = _format(
            logging.ERROR,
            "real",
            exc_info=info,
            extra={reserved: "spoofed"},
            name="real.logger",
        )

        assert payload["level"] == "error"
        assert payload["message"] == "real"
        assert payload["logger"] == "real.logger"
        assert "ValueError" in str(payload["exception"])


def test_an_exception_is_added_with_its_traceback() -> None:
    try:
        raise KeyError("missing")
    except KeyError as exc:
        payload = _format(
            logging.ERROR, "failed", exc_info=(type(exc), exc, exc.__traceback__)
        )

        assert isinstance(payload["exception"], str)
        assert "Traceback" in payload["exception"]
        assert "KeyError" in payload["exception"]


def test_no_exception_key_without_an_exception() -> None:
    assert "exception" not in _format()


def test_the_standard_record_attributes_never_leak_as_extras() -> None:
    payload = _format()

    for standard in ("args", "levelno", "pathname", "lineno", "funcName", "process"):
        assert standard not in payload
    assert set(payload) == {"level", "message", "logger"}


def test_a_circular_extra_degrades_to_a_notice_instead_of_raising() -> None:
    circular: dict[str, object] = {}
    circular["self"] = circular

    payload = _format(extra={"loop": circular})

    assert payload["message"] == "hello"
    assert payload["level"] == "info"
    assert "ValueError" in str(payload["formatter_error"])


def test_a_non_string_dict_key_degrades_to_a_notice() -> None:
    payload = _format(extra={"bad": {("a", "b"): 1}})

    assert payload["message"] == "hello"
    assert "TypeError" in str(payload["formatter_error"])


def test_an_extra_whose_str_raises_degrades_without_calling_it_again() -> None:
    class Broken:
        def __str__(self) -> str:
            raise RuntimeError("hostile-detail-7431")

    payload = _format(extra={"bad": Broken()})

    assert payload["message"] == "hello"
    assert "RuntimeError" in str(payload["formatter_error"])
    assert "hostile-detail-7431" not in str(payload["formatter_error"])


def test_end_to_end_through_a_real_handler_one_json_object_per_line() -> None:
    stream = io.StringIO()
    handler = logging.StreamHandler(stream)
    handler.setFormatter(JsonFormatter())
    logger = logging.getLogger("valmal.core.logging_json.e2e")
    logger.handlers = [handler]
    logger.propagate = False
    logger.setLevel(logging.DEBUG)

    logger.info("first", extra={"n": 1})
    logger.warning("second\nline")

    lines = stream.getvalue().splitlines()
    assert [json.loads(line)["message"] for line in lines] == ["first", "second\nline"]
    assert json.loads(lines[0])["n"] == 1
    assert json.loads(lines[1])["level"] == "warn"
