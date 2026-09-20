from types import SimpleNamespace

import discord
import pytest

import errors
from services.config import config
from tests.error_reporting.support import Channel, Clock

pytestmark = pytest.mark.anyio

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

    async def test_a_summary_that_fits_alone_survives_the_outage_prefix(
        self, admin: Channel, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A summary of about 1,850 characters fits by itself and not behind the
        one-line 'reached nobody' prefix, and was dropped whole rather than cut."""
        monkeypatch.setattr(errors, "_undelivered", 1)
        prefix = "[1 message(s) reached nobody while this channel was unreachable]\n"
        for width in range(600, 1000):
            exc = RuntimeError("m" * width)
            summary = (
                f"reading the thing - Type: RuntimeError, Message: {exc},"
                f" Args: {exc.args}"
            )
            if len(summary) <= errors._MAX_CONTENT < len(prefix) + len(summary):
                break
        else:
            pytest.fail("no width put the summary in the window")

        await errors.report(exc, "reading the thing")

        (call,) = admin.calls
        assert call["content"].startswith(prefix + "reading the thing - Type: Runtime")
        assert call["content"].endswith(errors._OVERFLOW_NOTE)
        assert len(call["content"]) <= errors._MAX_CONTENT
        assert call["file"].filename == "traceback.txt"

    async def test_the_prefix_and_a_notice_both_lead_the_content_and_the_file_has_both(
        self, admin: Channel, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(errors, "_undelivered", 3)
        text = "Outage: " + "y" * 5000

        await errors.notify(text)

        (call,) = admin.calls
        assert call["content"].startswith("[3 message(s) reached nobody")
        assert "Outage: yyy" in call["content"]
        assert len(call["content"]) <= errors._MAX_CONTENT
        whole = call["file"].fp.read().decode()
        assert whole.startswith("[3 message(s) reached nobody") and whole.endswith(text)

    def test_the_cap_stays_under_what_discord_accepts(self) -> None:
        """Every other test states the cap as errors._MAX_CONTENT, so any value
        would satisfy them; Discord rejects a message over 2000."""
        assert errors._MAX_CONTENT <= 2000
