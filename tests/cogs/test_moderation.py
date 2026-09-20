import logging
from types import SimpleNamespace
from typing import Any
from unittest.mock import MagicMock

import discord
import pytest

from cogs import moderation
from cogs.moderation import Moderation
from valmal.core.config import config

pytestmark = pytest.mark.anyio


class Channel:
    """A channel that records the purge it was asked for."""

    def __init__(self) -> None:
        self.purged: list[int | None] = []
        self.deleted: list[object] = [object(), object(), object()]
        self.error: Exception | None = None

    async def purge(self, *, limit: int | None) -> list[object]:
        self.purged.append(limit)
        if self.error is not None:
            raise self.error
        return self.deleted


class Said:
    def __init__(self, channel: Any) -> None:
        self.channel = channel
        self.sent: list[tuple[str, str, dict[str, Any]]] = []
        self.deferred: list[bool] = []

    def interaction(self) -> Any:
        async def send_message(text: str, **kwargs: Any) -> None:
            self.sent.append(("response", text, kwargs))

        async def defer(*, ephemeral: bool) -> None:
            self.deferred.append(ephemeral)

        async def followup(text: str, **kwargs: Any) -> None:
            self.sent.append(("followup", text, kwargs))

        return SimpleNamespace(
            channel=self.channel,
            response=SimpleNamespace(send_message=send_message, defer=defer),
            followup=SimpleNamespace(send=followup),
        )


@pytest.fixture(autouse=True)
def _templates(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        config,
        "_templates",
        {
            "admin_wrong_channel": "wrong channel",
            "admin_no_bulk_delete": "cannot bulk delete",
            "admin_nuking": "nuking",
            "admin_purge_done": "purged {count}",
            "admin_purge_forbidden": "forbidden",
            "admin_purge_failed": "failed: {error}",
        },
    )


def forbidden() -> discord.Forbidden:
    return discord.Forbidden(MagicMock(status=403, reason="x"), "no")


def failure() -> discord.HTTPException:
    return discord.HTTPException(MagicMock(status=500, reason="x"), "server said no")


class TestAChannelThatCannotBePurged:
    async def test_no_channel_at_all_is_refused_privately(self) -> None:
        said = Said(None)

        await moderation._purge(said.interaction(), 5)

        assert said.sent == [("response", "wrong channel", {"ephemeral": True})]

    @pytest.mark.parametrize(
        "kind",
        [
            discord.ForumChannel,
            discord.CategoryChannel,
            discord.DMChannel,
            discord.GroupChannel,
        ],
    )
    async def test_a_channel_that_holds_no_messages_is_refused_privately(
        self, kind: type
    ) -> None:
        said = Said(MagicMock(spec=kind))

        await moderation._purge(said.interaction(), 5)

        assert said.sent == [("response", "wrong channel", {"ephemeral": True})]

    async def test_a_channel_with_no_purge_is_told_so_privately(self) -> None:
        said = Said(SimpleNamespace())

        await moderation._purge(said.interaction(), 5)

        assert said.sent == [("response", "cannot bulk delete", {"ephemeral": True})]
        assert said.deferred == []

    @pytest.mark.parametrize(("limit", "name"), [(None, "Nuke"), (5, "Purge")])
    async def test_the_log_line_names_which_command_gave_up(
        self, limit: int | None, name: str, caplog: pytest.LogCaptureFixture
    ) -> None:
        """An operator grepping for one command's abort should still find it."""
        with caplog.at_level(logging.WARNING, logger=moderation.logger.name):
            await moderation._purge(Said(None).interaction(), limit)

        assert f"{name} aborted: invalid channel type" in caplog.text


class TestNuke:
    async def test_announces_publicly_then_deletes_everything(self) -> None:
        channel = Channel()
        said = Said(channel)

        await moderation._purge(said.interaction(), None)

        assert said.sent == [("response", "nuking", {})]
        assert channel.purged == [None]

    async def test_an_announcement_that_is_not_ephemeral_is_the_point(self) -> None:
        """Everyone in the channel is about to lose it, so everyone is told."""
        said = Said(Channel())

        await moderation._purge(said.interaction(), None)

        assert "ephemeral" not in said.sent[0][2]

    async def test_defers_nothing_because_it_answers_first(self) -> None:
        said = Said(Channel())

        await moderation._purge(said.interaction(), None)

        assert said.deferred == []

    async def test_a_refusal_from_discord_reaches_the_command_error_handler(
        self,
    ) -> None:
        channel = Channel()
        channel.error = forbidden()

        with pytest.raises(discord.Forbidden):
            await moderation._purge(Said(channel).interaction(), None)


class TestPurge:
    async def test_defers_privately_then_reports_how_many_went(self) -> None:
        channel = Channel()
        said = Said(channel)

        await moderation._purge(said.interaction(), 3)

        assert said.deferred == [True]
        assert channel.purged == [3]
        assert said.sent == [("followup", "purged 3", {"ephemeral": True})]

    async def test_the_count_reported_is_what_was_deleted_not_what_was_asked(
        self,
    ) -> None:
        """Messages older than 14 days are skipped by bulk delete."""
        channel = Channel()
        channel.deleted = [object()]
        said = Said(channel)

        await moderation._purge(said.interaction(), 50)

        assert said.sent[0][1] == "purged 1"

    async def test_nothing_deleted_is_still_reported(self) -> None:
        channel = Channel()
        channel.deleted = []
        said = Said(channel)

        await moderation._purge(said.interaction(), 5)

        assert said.sent[0][1] == "purged 0"

    async def test_missing_permission_is_said_privately(self) -> None:
        channel = Channel()
        channel.error = forbidden()
        said = Said(channel)

        await moderation._purge(said.interaction(), 5)

        assert said.sent == [("followup", "forbidden", {"ephemeral": True})]

    async def test_another_failure_is_logged_and_its_text_shown_privately(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        channel = Channel()
        channel.error = failure()
        said = Said(channel)

        with caplog.at_level(logging.ERROR, logger=moderation.logger.name):
            await moderation._purge(said.interaction(), 5)

        assert said.sent[0][1].startswith("failed: ")
        assert "server said no" in said.sent[0][1]
        assert said.sent[0][2] == {"ephemeral": True}
        assert "Purge failed" in caplog.text

    async def test_forbidden_is_not_mistaken_for_a_general_failure(self) -> None:
        """Forbidden is an HTTPException, so the order of the handlers matters."""
        channel = Channel()
        channel.error = forbidden()
        said = Said(channel)

        await moderation._purge(said.interaction(), 5)

        assert "failed" not in said.sent[0][1]

    async def test_an_unexpected_error_is_not_swallowed(self) -> None:
        channel = Channel()
        channel.error = RuntimeError("bug")

        with pytest.raises(RuntimeError):
            await moderation._purge(Said(channel).interaction(), 5)


class TestTheCommands:
    async def test_nuke_purges_without_a_limit(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        calls: list[int | None] = []

        async def fake(interaction: object, limit: int | None) -> None:
            calls.append(limit)

        monkeypatch.setattr(moderation, "_purge", fake)

        await Moderation.nuke.callback(Moderation(MagicMock()), MagicMock())  # pyright: ignore[reportCallIssue]

        assert calls == [None]

    async def test_purge_passes_the_count_on(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        calls: list[int | None] = []

        async def fake(interaction: object, limit: int | None) -> None:
            calls.append(limit)

        monkeypatch.setattr(moderation, "_purge", fake)

        await Moderation.purge.callback(Moderation(MagicMock()), MagicMock(), 42)  # pyright: ignore[reportCallIssue]

        assert calls == [42]

    def test_the_count_is_bounded_to_what_bulk_delete_can_take(self) -> None:
        (count,) = Moderation.purge.parameters

        assert (count.min_value, count.max_value) == (1, 500)

    def test_the_limit_the_description_promises_is_the_one_enforced(self) -> None:
        (count,) = Moderation.purge.parameters

        assert f"max {count.max_value}" in count.description
