from types import SimpleNamespace

import discord
import pytest
from discord.ext.commands import CommandNotFound

from tests.bot.audit.support import (
    AVATAR,
    COLORS,
    Audit,
    attachment,
    channel,
    message,
    person,
)
from valmal.bot import audit
from valmal.bot.audit import DEFAULT_MISSING_CONTENT, EMPTY_CONTENT, UNKNOWN_USER

pytestmark = pytest.mark.anyio

BACKSLASH = chr(92)


async def deleted(**overrides: object) -> None:
    await audit.message_deleted(
        **{
            "content": "gone",
            "attachments": [],
            "message_id": 9,
            "author": person(id=7, name="val"),
            "deleted_by": None,
            "channel": channel(),
        }
        | overrides  # pyright: ignore[reportArgumentType]
    )


class TestMessageEdited:
    async def test_shows_the_text_before_and_after(self, log: Audit) -> None:
        await audit.message_edited(message(content="new"), "old")

        assert log.description == ("edited <#55> https://discord.com/channels/1/55/9")
        assert [(f.name, f.value) for f in log.embed.fields] == [
            ("**Before**", "old"),
            ("**After**", "new"),
        ]

    async def test_names_the_author_and_carries_their_id(self, log: Audit) -> None:
        await audit.message_edited(message(author=person(id=7, name="val")), "old")

        assert log.embed.author.name == "val"
        assert log.embed.author.icon_url == AVATAR
        assert log.embed.footer.text == "ID: 7"

    async def test_a_message_the_cache_dropped_says_so_instead_of_guessing(
        self, log: Audit
    ) -> None:
        await audit.message_edited(message(content="new"), None)

        assert log.field("**Before**") == DEFAULT_MISSING_CONTENT

    async def test_an_edit_that_emptied_the_message_is_no_text(
        self, log: Audit
    ) -> None:
        await audit.message_edited(message(content=""), "old")

        assert log.field("**After**") == EMPTY_CONTENT

    async def test_both_sides_are_escaped_and_neither_can_overflow(
        self, log: Audit
    ) -> None:
        await audit.message_edited(
            message(content="[x](http://evil.example)"), "*" * 3000
        )

        assert (log.field("**After**") or "").startswith(f"{BACKSLASH}[")
        assert len(log.field("**Before**") or "") <= 1024

    async def test_is_info_coloured(self, log: Audit) -> None:
        await audit.message_edited(message(), "old")

        assert log.embed.colour is not None
        assert log.embed.colour.value == COLORS["embed_color_info"]


class TestPinChanged:
    async def test_a_pin_uses_the_pinned_sentence(self, log: Audit) -> None:
        await audit.pin_changed(message(pinned=True))

        assert log.description.startswith("pinned ")

    async def test_an_unpin_uses_the_unpinned_sentence(self, log: Audit) -> None:
        await audit.pin_changed(message(pinned=False))

        assert log.description.startswith("unpinned ")

    async def test_names_the_author_of_the_message_not_the_pinner(
        self, log: Audit
    ) -> None:
        await audit.pin_changed(message(author=person(id=7, name="val")))

        assert log.embed.author.name == "val"
        assert log.embed.footer.text == "ID: 7"


class TestMessageDeleted:
    async def test_one_embed_with_the_text_and_who_wrote_it(self, log: Audit) -> None:
        await deleted()

        assert log.description == "deleted <@7> <#55>"
        assert log.field("**Message**") == "gone"
        assert log.embed.author.name == "val"
        assert log.embed.footer.text == "Author: 7 | Message ID: 9"
        assert log.embed.colour is not None
        assert log.embed.colour.value == COLORS["embed_color_danger"]

    async def test_a_named_deleter_uses_the_sentence_that_names_them(
        self, log: Audit
    ) -> None:
        await deleted(deleted_by=person(id=3, name="mod"))

        assert log.description == "deleted_by <@7> <@3> <#55>"

    async def test_an_unnamed_deleter_uses_the_plain_sentence(self, log: Audit) -> None:
        await deleted(deleted_by=None)

        assert log.description.startswith("deleted ")
        assert "deleted_by" not in log.description

    async def test_a_message_that_was_only_an_attachment_has_no_text_field(
        self, log: Audit
    ) -> None:
        await deleted(content="", attachments=[attachment()])

        assert log.embeds[0].fields == []

    async def test_each_attachment_gets_its_own_embed_after_the_message(
        self, log: Audit
    ) -> None:
        await deleted(attachments=[attachment("https://a"), attachment("https://b")])

        first, one, two = log.embeds
        assert (first.description or "").startswith("deleted ")
        assert [one.image.url, two.image.url] == ["https://a", "https://b"]
        assert one.description == "attachment <@7> <#55>"
        assert one.footer.text == first.footer.text == "Author: 7 | Message ID: 9"
        assert one.author.name == "val"

    async def test_no_attachments_means_exactly_one_embed(self, log: Audit) -> None:
        await deleted()

        assert len(log.embeds) == 1

    async def test_the_text_is_escaped_and_capped(self, log: Audit) -> None:
        await deleted(content="_" * 2000)

        value = log.field("**Message**") or ""
        assert len(value) <= 1024 and value.endswith("...")
        assert value.startswith(f"{BACKSLASH}_")

    async def test_a_deletion_in_a_dm_or_unknown_channel_still_logs(
        self, log: Audit
    ) -> None:
        await deleted(channel=None)

        assert log.description == "deleted <@7> Unknown Channel"


class TestMessageDeletedUncached:
    async def test_names_only_what_is_known(self, log: Audit) -> None:
        await audit.message_deleted_uncached(
            content="stored",
            message_id=9,
            deleted_by=person(id=3, name="mod"),
            channel=channel(),
        )

        assert log.description == "uncached <@3> <#55>"
        assert log.embed.author.name == "mod"
        assert log.field("**Message**") == "stored"
        assert log.embed.footer.text == "Deleter: 3 | Message ID: 9"

    async def test_an_unnamed_deleter_is_unknown_everywhere_it_appears(
        self, log: Audit
    ) -> None:
        await audit.message_deleted_uncached(
            content="stored", message_id=9, deleted_by=None, channel=channel()
        )

        assert log.description == f"uncached {UNKNOWN_USER} <#55>"
        assert log.embed.author.name == UNKNOWN_USER
        assert log.embed.footer.text == "Deleter: Unknown ID | Message ID: 9"

    async def test_no_stored_text_says_it_was_not_found(self, log: Audit) -> None:
        await audit.message_deleted_uncached(
            content=None, message_id=9, deleted_by=None, channel=channel()
        )

        assert log.field("**Message**") == DEFAULT_MISSING_CONTENT

    async def test_stored_text_is_escaped(self, log: Audit) -> None:
        await audit.message_deleted_uncached(
            content="*x*", message_id=9, deleted_by=None, channel=channel()
        )

        assert log.field("**Message**") == f"{BACKSLASH}*x{BACKSLASH}*"


class TestBulkDeleted:
    async def test_says_how_many_and_where(self, log: Audit) -> None:
        await audit.bulk_deleted(count=12, deleted_by=None, channel=channel())

        assert log.description == "bulk 12 <#55>"
        assert log.embed.author.name == UNKNOWN_USER
        assert log.embed.fields == []

    async def test_names_who_did_it_when_known(self, log: Audit) -> None:
        await audit.bulk_deleted(
            count=2, deleted_by=person(name="mod"), channel=channel()
        )

        assert log.embed.author.name == "mod"


def command_context(content: str = "!nope") -> SimpleNamespace:
    return SimpleNamespace(
        channel=channel(),
        author=person(id=7, name="val"),
        message=SimpleNamespace(jump_url="https://jump", content=content),
    )


class TestCommandFailed:
    async def test_shows_what_was_typed_and_why_it_failed(self, log: Audit) -> None:
        await audit.command_failed(
            command_context("!nope"),  # pyright: ignore[reportArgumentType]
            CommandNotFound('Command "nope" is not found'),
        )

        assert log.description == "failed <#55> https://jump"
        assert log.field("**Command**") == "!nope"
        assert log.field("**Error**") == 'Command "nope" is not found'
        assert log.embed.footer.text == "ID: 7"
        assert log.embed.author.name == "val"

    async def test_both_values_are_escaped_because_the_user_typed_them(
        self, log: Audit
    ) -> None:
        await audit.command_failed(
            command_context("!_x_"),  # pyright: ignore[reportArgumentType]
            CommandNotFound('Command "_x_" is not found'),
        )

        assert log.field("**Command**") == f"!{BACKSLASH}_x{BACKSLASH}_"
        assert f"{BACKSLASH}_x{BACKSLASH}_" in (log.field("**Error**") or "")

    async def test_an_error_with_no_message_is_no_text(self, log: Audit) -> None:
        await audit.command_failed(
            command_context(),  # pyright: ignore[reportArgumentType]
            CommandNotFound(""),
        )

        assert log.field("**Error**") is not None


class TestDiscordDeliveryFailuresPropagate:
    async def test_a_failed_send_is_the_callers_to_handle(
        self, log: Audit, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """valmal/bot/cogs/events.py relies on the raise reaching on_error rather than losing the entry."""

        async def refuse(*_: object, **__: object) -> int:
            raise discord.HTTPException(
                SimpleNamespace(status=500, reason="x"),  # pyright: ignore[reportArgumentType]
                "down",
            )

        monkeypatch.setattr(audit, "send_embed", refuse)

        with pytest.raises(discord.HTTPException):
            await audit.pin_changed(message())
