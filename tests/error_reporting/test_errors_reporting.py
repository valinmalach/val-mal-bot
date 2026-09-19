import asyncio
import logging
from types import SimpleNamespace
from typing import Any

import discord
import pytest

import errors
from background import _tasks
from services.config import config
from tests.error_reporting.support import Channel, Clock

pytestmark = pytest.mark.anyio


# --- report ---------------------------------------------------------------------


class TestReport:
    async def test_delivers_the_summary_and_a_traceback_attachment(
        self, clock: Clock, delivered: list[tuple[str, Any]]
    ) -> None:
        try:
            raise ValueError("bad value", 7)
        except ValueError as exc:
            await errors.report(exc, "Doing the thing")

        text, attachment = delivered[0]
        assert text == (
            "Doing the thing - Type: ValueError, Message: ('bad value', 7),"
            " Args: ('bad value', 7)"
        )
        assert attachment is not None
        assert attachment[0] == "traceback.txt"
        assert "ValueError: ('bad value', 7)" in attachment[1]
        assert "Traceback (most recent call last)" in attachment[1]

    async def test_an_exception_that_was_never_raised_here_still_has_a_traceback_body(
        self, clock: Clock, delivered: list[tuple[str, Any]]
    ) -> None:
        """format_exception, not format_exc: an exception collected from
        gather(return_exceptions=True) is not the one being handled."""
        await errors.report(RuntimeError("collected"), "gathered")

        assert delivered[0][1] is not None
        assert "RuntimeError: collected" in delivered[0][1][1]

    async def test_a_repeat_of_the_same_failure_is_held_back(
        self, clock: Clock, delivered: list[tuple[str, Any]]
    ) -> None:
        for _ in range(3):
            await errors.report(RuntimeError("x"), "same place")

        assert len(delivered) == 1

    async def test_two_kinds_of_failure_from_one_place_are_both_delivered(
        self, clock: Clock, delivered: list[tuple[str, Any]]
    ) -> None:
        await errors.report(RuntimeError("x"), "same place")
        await errors.report(ValueError("x"), "same place")

        assert len(delivered) == 2

    async def test_the_same_type_from_two_places_is_delivered_twice(
        self, clock: Clock, delivered: list[tuple[str, Any]]
    ) -> None:
        await errors.report(RuntimeError("x"), "place a")
        await errors.report(RuntimeError("x"), "place b")

        assert len(delivered) == 2

    async def test_an_explicit_key_groups_failures_whose_text_differs(
        self, clock: Clock, delivered: list[tuple[str, Any]]
    ) -> None:
        await errors.report(RuntimeError("id 1"), "ctx", key="shared")
        await errors.report(RuntimeError("id 2"), "ctx", key="shared")

        assert len(delivered) == 1

    async def test_logs_the_failure_with_its_exception(
        self,
        clock: Clock,
        delivered: list[tuple[str, Any]],
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        with caplog.at_level(logging.ERROR, logger="errors"):
            await errors.report(RuntimeError("logged"), "ctx")

        assert any(r.exc_info for r in caplog.records)

    async def test_never_raises_even_when_describing_the_exception_fails(
        self,
        clock: Clock,
        delivered: list[tuple[str, Any]],
        caplog: pytest.LogCaptureFixture,
    ) -> None:

        class Hostile(Exception):
            def __str__(self) -> str:
                raise RuntimeError("no string for you")

        with caplog.at_level(logging.ERROR, logger="errors"):
            await errors.report(Hostile(), "ctx")

        assert not delivered
        assert any("Reporting failed" in r.getMessage() for r in caplog.records)

    async def test_never_raises_when_delivery_raises(
        self, clock: Clock, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        async def deliver(text: str, attachment: object) -> bool:
            raise RuntimeError("send failed")

        monkeypatch.setattr(errors, "_deliver", deliver)

        await errors.report(RuntimeError("x"), "ctx")


# --- notify / notify_file / notify_soon -----------------------------------------


class TestNotify:
    async def test_delivers_and_says_so(
        self, clock: Clock, delivered: list[tuple[str, Any]]
    ) -> None:
        assert await errors.notify("hello") is True

        assert delivered == [("hello", None)]

    async def test_a_repeat_is_held_back_and_still_reads_as_delivered(
        self, clock: Clock, delivered: list[tuple[str, Any]]
    ) -> None:
        await errors.notify("hello")

        assert await errors.notify("hello") is True
        assert len(delivered) == 1

    async def test_the_text_is_the_default_key_so_different_texts_both_go(
        self, clock: Clock, delivered: list[tuple[str, Any]]
    ) -> None:
        await errors.notify("one")
        await errors.notify("two")

        assert len(delivered) == 2

    async def test_a_shared_key_holds_back_texts_that_vary_in_a_detail(
        self, clock: Clock, delivered: list[tuple[str, Any]]
    ) -> None:
        await errors.notify("broadcaster 1 failed", key="failed")
        await errors.notify("broadcaster 2 failed", key="failed")

        assert len(delivered) == 1

    async def test_false_when_nothing_could_be_delivered(
        self, clock: Clock, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        async def deliver(text: str, attachment: object) -> bool:
            return False

        monkeypatch.setattr(errors, "_deliver", deliver)

        assert await errors.notify("hello") is False

    async def test_false_and_no_exception_when_delivery_raises(
        self, clock: Clock, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        async def deliver(text: str, attachment: object) -> bool:
            raise RuntimeError("send failed")

        monkeypatch.setattr(errors, "_deliver", deliver)

        assert await errors.notify("hello") is False

    async def test_a_failed_notice_is_retried_rather_than_suppressed(
        self, clock: Clock, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        outcomes = iter([False, True])

        async def deliver(text: str, attachment: object) -> bool:
            return next(outcomes)

        monkeypatch.setattr(errors, "_deliver", deliver)

        assert await errors.notify("hello") is False
        clock.now += 1
        assert await errors.notify("hello") is True


class TestNotifyFile:
    async def test_delivers_with_the_file(
        self, delivered: list[tuple[str, Any]]
    ) -> None:
        assert await errors.notify_file("dump", "dump.json", "{}") is True

        assert delivered == [("dump", ("dump.json", "{}"))]

    async def test_two_identical_notices_are_both_delivered(
        self, clock: Clock, delivered: list[tuple[str, Any]]
    ) -> None:
        """Two records inside one window are two different records: suppressing
        the second would leave a destruction with nothing to undo it."""
        await errors.notify_file("dump", "dump.json", "{}")
        await errors.notify_file("dump", "dump.json", "{}")

        assert len(delivered) == 2

    async def test_false_without_raising_when_delivery_raises(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        async def deliver(text: str, attachment: object) -> bool:
            raise RuntimeError("send failed")

        monkeypatch.setattr(errors, "_deliver", deliver)

        assert await errors.notify_file("dump", "dump.json", "{}") is False

    async def test_false_when_undelivered(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        async def deliver(text: str, attachment: object) -> bool:
            return False

        monkeypatch.setattr(errors, "_deliver", deliver)

        assert await errors.notify_file("dump", "f", "c") is False


class TestNotifySoon:
    def test_without_a_running_loop_it_only_logs(
        self, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
    ) -> None:
        called: list[str] = []

        async def notify(text: str, *, key: str | None = None) -> bool:
            called.append(text)
            return True

        monkeypatch.setattr(errors, "notify", notify)

        with caplog.at_level(logging.WARNING, logger="errors"):
            errors.notify_soon("no loop here")

        assert not called
        assert any("no loop here" in r.getMessage() for r in caplog.records)

    async def test_with_a_running_loop_it_notifies_in_the_background(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        called: list[tuple[str, str | None]] = []

        async def notify(text: str, *, key: str | None = None) -> bool:
            called.append((text, key))
            return True

        monkeypatch.setattr(errors, "notify", notify)

        errors.notify_soon("from sync code", key="k")
        for _ in range(5):
            await asyncio.sleep(0)

        assert called == [("from sync code", "k")]
        assert not _tasks


# --- _deliver -------------------------------------------------------------------


class TestDeliver:
    async def test_without_a_loaded_configuration_nothing_is_sent_and_it_is_counted(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(config, "_loaded", False)

        assert await errors._deliver("early", None) is False

        assert errors._undelivered == 1

    async def test_sends_to_the_admin_channel_quietly_and_with_no_mentions(
        self, admin: Channel
    ) -> None:
        assert await errors._deliver("hello", None) is True

        (call,) = admin.calls
        assert call["content"] == "hello"
        assert call["channel_id"] == 4242
        assert call["quiet"] is True
        assert call["file"] is None
        mentions = call["allowed_mentions"]
        assert mentions.everyone is False
        assert mentions.users is False
        assert mentions.roles is False
        assert mentions.replied_user is False

    async def test_an_attachment_becomes_a_discord_file(self, admin: Channel) -> None:
        await errors._deliver("hello", ("trace.txt", "the traceback ✓"))

        file = admin.calls[0]["file"]
        assert isinstance(file, discord.File)
        assert file.filename == "trace.txt"
        assert file.fp.read() == "the traceback ✓".encode()

    async def test_success_clears_the_undelivered_count(self, admin: Channel) -> None:
        errors._undelivered = 5

        await errors._deliver("hello", None)

        assert errors._undelivered == 0

    async def test_the_count_leads_the_next_message_that_arrives(
        self, admin: Channel
    ) -> None:
        errors._undelivered = 3

        await errors._deliver("hello", None)

        assert admin.calls[0]["content"] == (
            "[3 message(s) reached nobody while this channel was unreachable]\nhello"
        )

    async def test_an_unresolvable_channel_is_false_and_counted(
        self, admin: Channel
    ) -> None:
        admin.result = None

        assert await errors._deliver("hello", None) is False

        assert errors._undelivered == 1

    async def test_a_channel_that_resolves_but_cannot_be_posted_to_is_counted_too(
        self, admin: Channel
    ) -> None:
        admin.error = discord.Forbidden(SimpleNamespace(status=403, reason="x"), "no")  # pyright: ignore[reportArgumentType]

        with pytest.raises(discord.Forbidden):
            await errors._deliver("hello", None)

        assert errors._undelivered == 1

    async def test_failures_accumulate_until_one_arrives(self, admin: Channel) -> None:
        admin.result = None
        await errors._deliver("a", None)
        await errors._deliver("b", None)
        admin.result = 1

        await errors._deliver("c", None)

        assert admin.calls[-1]["content"].startswith("[2 message(s) reached nobody")
        assert errors._undelivered == 0

    async def test_a_short_message_is_sent_whole(self, admin: Channel) -> None:
        text = "x" * errors._MAX_CONTENT

        await errors._deliver(text, None)

        assert admin.calls[0]["content"] == text
        assert admin.calls[0]["file"] is None

    async def test_an_over_long_notice_is_shortened_and_attached_whole(
        self, admin: Channel
    ) -> None:
        text = "\n".join(f"subscription {i}" for i in range(400))

        await errors._deliver(text, None)

        (call,) = admin.calls
        assert len(call["content"]) <= errors._MAX_CONTENT
        assert call["content"].endswith(errors._OVERFLOW_NOTE)
        assert call["file"].filename == "notice.txt"
        assert call["file"].fp.read().decode() == text

    async def test_an_over_long_report_keeps_its_traceback_as_the_attachment(
        self, admin: Channel
    ) -> None:
        summary = "\n".join(f"line {i}" for i in range(400))

        await errors._deliver(summary, ("traceback.txt", "the trace"))

        (call,) = admin.calls
        assert call["file"].filename == "traceback.txt"
        assert call["file"].fp.read() == b"the trace"
        assert len(call["content"]) <= errors._MAX_CONTENT

    async def test_the_undelivered_prefix_cannot_push_a_borderline_message_over(
        self, admin: Channel
    ) -> None:
        errors._undelivered = 12
        text = "x" * (errors._MAX_CONTENT - 5)

        await errors._deliver(text, None)

        (call,) = admin.calls
        assert len(call["content"]) <= errors._MAX_CONTENT
        assert call["file"] is not None, "the whole notice must still be attached"

    async def test_it_never_pings_anyone_whatever_the_text_says(
        self, admin: Channel
    ) -> None:
        await errors._deliver("@everyone <@1> <@&2>", None)

        mentions = admin.calls[0]["allowed_mentions"]
        assert (mentions.everyone, mentions.users, mentions.roles) == (False,) * 3
        assert mentions.replied_user is False


# --- end to end -----------------------------------------------------------------


class TestEndToEnd:
    async def test_an_outage_says_how_many_reports_it_swallowed_once_it_ends(
        self, clock: Clock, admin: Channel
    ) -> None:
        admin.result = None
        for i in range(3):
            clock.now += 1
            await errors.notify(f"outage {i}")
        admin.result = 1

        clock.now += 1
        await errors.notify("back")

        assert admin.calls[-1]["content"] == (
            "[3 message(s) reached nobody while this channel was unreachable]\nback"
        )

    async def test_a_report_reaches_the_channel_with_its_traceback(
        self, clock: Clock, admin: Channel
    ) -> None:
        await errors.report(KeyError("k"), "reading the thing")

        (call,) = admin.calls
        assert call["content"].startswith("reading the thing - Type: KeyError")
        assert call["file"].filename == "traceback.txt"


class TestAnEnormousSingleLine:
    """A report's summary is one line, and the attachment is the traceback, which
    does not carry the context. If that one line is over the limit and the
    truncation keeps nothing of it, the admin channel gets a note and no idea what
    was being attempted."""

    async def test_a_report_still_says_what_failed(self, admin: Channel) -> None:
        await errors.report(RuntimeError("x" * 5000), "reading the thing")

        (call,) = admin.calls
        assert call["content"].startswith("reading the thing - Type: RuntimeError")
        assert call["content"].endswith(errors._OVERFLOW_NOTE)
        assert len(call["content"]) <= errors._MAX_CONTENT
        assert call["file"].filename == "traceback.txt"

    async def test_a_notice_keeps_its_opening_and_attaches_the_whole(
        self, admin: Channel
    ) -> None:
        text = "Subscription outage: " + "y" * 5000

        await errors.notify(text)

        (call,) = admin.calls
        assert call["content"].startswith("Subscription outage: yyy")
        assert len(call["content"]) <= errors._MAX_CONTENT
        assert call["file"].fp.read().decode() == text
