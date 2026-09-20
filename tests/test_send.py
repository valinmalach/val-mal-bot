from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import discord
import pytest

from services import send
from valmal.core import errors

pytestmark = pytest.mark.anyio


class Discord:
    """The channels the bot can resolve, and what was said about the ones it cannot."""

    def __init__(self) -> None:
        self.channels: dict[int, Any] = {}
        self.notified: list[tuple[str, str | None]] = []

    def text_channel(self, channel_id: int = 10) -> MagicMock:
        channel = MagicMock(spec=discord.TextChannel)
        channel.send = AsyncMock(return_value=SimpleNamespace(id=555))
        self.message = SimpleNamespace(edit=AsyncMock())
        channel.fetch_message = AsyncMock(return_value=self.message)
        self.channels[channel_id] = channel
        return channel


@pytest.fixture
def world(monkeypatch: pytest.MonkeyPatch) -> Discord:
    world = Discord()

    async def notify(text: str, *, key: str | None = None) -> bool:
        world.notified.append((text, key))
        return True

    monkeypatch.setattr(
        send.bot, "get_channel", lambda channel_id: world.channels.get(channel_id)
    )
    monkeypatch.setattr(errors, "notify", notify)
    return world


def refused(kind: type[discord.HTTPException], status: int) -> discord.HTTPException:
    return kind(SimpleNamespace(status=status, reason="x"), "text")  # pyright: ignore[reportArgumentType]


class TestSendable:
    async def test_a_resolved_text_channel_is_returned(self, world: Discord) -> None:
        channel = world.text_channel()

        assert await send._sendable(10, quiet=False) is channel
        assert world.notified == []

    async def test_an_unresolvable_channel_is_said_once_by_key(
        self, world: Discord
    ) -> None:
        assert await send._sendable(99, quiet=False) is None

        ((text, key),) = world.notified
        assert "Channel 99 could not be resolved" in text
        assert key == "channel-unresolved:99"

    async def test_quiet_says_nothing_because_announcing_the_admin_channel_through_itself_never_ends(
        self, world: Discord
    ) -> None:
        assert await send._sendable(99, quiet=True) is None

        assert world.notified == []

    @pytest.mark.parametrize(
        "kind",
        [
            discord.ForumChannel,
            discord.CategoryChannel,
            discord.DMChannel,
            discord.GroupChannel,
        ],
    )
    async def test_a_channel_the_bot_cannot_post_a_message_into_is_treated_as_unresolved(
        self, kind: type, world: Discord
    ) -> None:
        world.channels[10] = MagicMock(spec=kind)

        assert await send._sendable(10, quiet=False) is None
        assert len(world.notified) == 1

    @pytest.mark.parametrize(
        "kind", [discord.VoiceChannel, discord.Thread, discord.StageChannel]
    )
    async def test_a_voice_channel_thread_or_stage_can_be_posted_to(
        self, kind: type, world: Discord
    ) -> None:
        channel = MagicMock(spec=kind)
        world.channels[10] = channel

        assert await send._sendable(10, quiet=False) is channel

    def test_the_unsendable_kinds_are_the_ones_moderation_shares(self) -> None:
        assert set(send.UNSENDABLE_CHANNEL_TYPES) == {
            discord.ForumChannel,
            discord.CategoryChannel,
            discord.abc.PrivateChannel,
        }


class TestSendMessage:
    async def test_returns_the_id_of_the_message_it_sent(self, world: Discord) -> None:
        channel = world.text_channel()

        assert await send.send_message("hello", 10) == 555

        channel.send.assert_awaited_once()
        assert channel.send.await_args.args == ("hello",)

    async def test_an_unresolvable_channel_is_none_and_sends_nothing(
        self, world: Discord
    ) -> None:
        assert await send.send_message("hello", 99) is None

    async def test_a_file_goes_with_it(self, world: Discord) -> None:
        channel = world.text_channel()
        attachment = MagicMock(spec=discord.File)

        await send.send_message("hello", 10, file=attachment)

        assert channel.send.await_args.kwargs["file"] is attachment

    async def test_no_file_means_none_is_passed_at_all(self, world: Discord) -> None:
        channel = world.text_channel()

        await send.send_message("hello", 10)

        assert "file" not in channel.send.await_args.kwargs

    async def test_the_mentions_it_was_given_are_the_mentions_sent(
        self, world: Discord
    ) -> None:
        channel = world.text_channel()
        none = discord.AllowedMentions.none()

        await send.send_message("hello", 10, allowed_mentions=none)

        assert channel.send.await_args.kwargs["allowed_mentions"] is none

    async def test_quiet_reaches_the_lookup(self, world: Discord) -> None:
        await send.send_message("hello", 99, quiet=True)

        assert world.notified == []

    async def test_a_failure_posting_to_a_channel_that_resolved_propagates(
        self, world: Discord
    ) -> None:
        """errors._deliver counts on this to know the message reached nobody."""
        channel = world.text_channel()
        channel.send.side_effect = refused(discord.Forbidden, 403)

        with pytest.raises(discord.Forbidden):
            await send.send_message("hello", 10)


class TestSendEmbed:
    async def test_sends_the_embed_and_returns_the_message_id(
        self, world: Discord
    ) -> None:
        channel = world.text_channel()
        embed = discord.Embed(title="t")

        assert await send.send_embed(embed, 10, content="hi") == 555

        assert channel.send.await_args.kwargs == {"content": "hi", "embed": embed}

    async def test_a_view_is_included_only_when_given(self, world: Discord) -> None:
        channel = world.text_channel()
        view = discord.ui.View()

        await send.send_embed(discord.Embed(), 10, view=view)

        assert channel.send.await_args.kwargs["view"] is view

    async def test_an_unresolvable_channel_is_none(self, world: Discord) -> None:
        assert await send.send_embed(discord.Embed(), 99) is None
        assert len(world.notified) == 1


class TestEditEmbed:
    async def test_edits_the_message_and_says_it_did(self, world: Discord) -> None:
        channel = world.text_channel()
        embed = discord.Embed()
        view = discord.ui.View()

        assert await send.edit_embed(900, embed, 10, view=view, content="c") is True

        channel.fetch_message.assert_awaited_once_with(900)
        world.message.edit.assert_awaited_once_with(content="c", embed=embed, view=view)

    async def test_with_no_view_the_edit_clears_it_explicitly(
        self, world: Discord
    ) -> None:
        """An offline alert must lose its Watch button, not keep the live one."""
        world.text_channel()

        await send.edit_embed(900, discord.Embed(), 10)

        assert world.message.edit.await_args.kwargs["view"] is None

    async def test_an_unresolvable_channel_is_false_so_a_caller_can_tell_a_no_op_from_an_edit(
        self, world: Discord
    ) -> None:
        assert await send.edit_embed(900, discord.Embed(), 99) is False

    async def test_a_deleted_message_propagates_because_the_alert_cycle_forgets_its_row_on_it(
        self, world: Discord
    ) -> None:
        channel = world.text_channel()
        channel.fetch_message.side_effect = refused(discord.NotFound, 404)

        with pytest.raises(discord.NotFound):
            await send.edit_embed(900, discord.Embed(), 10)
