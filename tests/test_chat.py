from typing import Any

import pytest

from valmal.core.config import config
from valmal.twitch.client import chat
from valmal.twitch.client.helix import HelixError

pytestmark = pytest.mark.anyio


class Chat:
    def __init__(self) -> None:
        self.sent: list[tuple[str, str]] = []
        self.notified: list[tuple[str, str | None]] = []
        self.error: Exception | None = None


@pytest.fixture
def world(monkeypatch: pytest.MonkeyPatch) -> Chat:
    world = Chat()

    async def send_chat_message(broadcaster_id: str, message: str) -> None:
        if world.error is not None:
            raise world.error
        world.sent.append((broadcaster_id, message))

    async def notify(text: str, *, key: str | None = None) -> bool:
        world.notified.append((text, key))
        return True

    monkeypatch.setattr(chat, "send_chat_message", send_chat_message)
    monkeypatch.setattr(chat, "notify", notify)
    monkeypatch.setattr(config, "_templates", {"greeting": "Hello {name}!"})
    return world


class TestSay:
    async def test_sends_the_line_and_says_it_landed(self, world: Chat) -> None:
        assert await chat.say("111", "hello", "a greeting") is True

        assert world.sent == [("111", "hello")]

    async def test_a_numeric_broadcaster_id_is_sent_as_a_string(
        self, world: Chat
    ) -> None:
        await chat.say(111, "hello", "a greeting")

        assert world.sent == [("111", "hello")]

    @pytest.mark.parametrize("text", ["", " ", "\n", "\t  \n"])
    async def test_a_blank_line_is_not_sent_because_twitch_would_only_answer_400(
        self, text: str, world: Chat
    ) -> None:
        """A missing message_template renders as ""; config.template has already named
        the row."""
        assert await chat.say("111", text, "a greeting") is False

        assert world.sent == []
        assert world.notified == []

    async def test_a_line_twitch_refused_is_reported_and_never_raised(
        self, world: Chat
    ) -> None:
        world.error = HelixError("403 forbidden")

        assert await chat.say("111", "hello", "a greeting") is False

        ((text, key),) = world.notified
        assert text == "Could not send a greeting to broadcaster 111: 403 forbidden"
        assert key == "twitch-chat:a greeting:111"

    async def test_the_key_separates_one_failing_line_from_another_and_one_channel_from_another(
        self, world: Chat
    ) -> None:
        world.error = HelixError("x")

        await chat.say("111", "hi", "line one")
        await chat.say("111", "hi", "line two")
        await chat.say("222", "hi", "line one")

        assert len({key for _, key in world.notified}) == 3

    async def test_a_line_is_sent_verbatim_including_leading_and_trailing_space(
        self, world: Chat
    ) -> None:
        await chat.say("111", "  padded  ", "x")

        assert world.sent == [("111", "  padded  ")]


class TestSayTemplate:
    async def test_renders_the_template_and_says_it_named_by_its_key(
        self, world: Chat
    ) -> None:
        assert await chat.say_template("111", "greeting", name="Bob") is True

        assert world.sent == [("111", "Hello Bob!")]

    async def test_a_failed_send_is_named_by_the_template_key(
        self, world: Chat
    ) -> None:
        world.error = HelixError("down")

        assert await chat.say_template("111", "greeting", name="Bob") is False

        assert world.notified[0][1] == "twitch-chat:greeting:111"
        assert "greeting" in world.notified[0][0]

    async def test_a_missing_template_says_nothing_and_does_not_raise(
        self, world: Chat, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """config.template degrades to "" and reports the row itself; sending "" would
        only add a 400 on top."""
        monkeypatch.setattr(
            "valmal.core.config.notify_soon", lambda text, *, key=None: None
        )

        assert await chat.say_template("111", "no_such_template") is False

        assert world.sent == []

    async def test_a_template_that_cannot_be_rendered_at_all_is_reported_with_its_key(
        self, world: Chat, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        def explode(key: str, **values: Any) -> str:
            raise RuntimeError("no database")

        monkeypatch.setattr(config, "template", explode)

        assert await chat.say_template("111", "greeting") is False

        ((text, key),) = world.notified
        assert "Could not render greeting for broadcaster 111: no database" in text
        assert key == "twitch-chat-render:greeting"
        assert world.sent == []

    async def test_values_the_template_does_not_use_are_ignored(
        self, world: Chat
    ) -> None:
        await chat.say_template("111", "greeting", name="Bob", unused="x")

        assert world.sent == [("111", "Hello Bob!")]
