"""What the chat commands reach out to, recorded."""

from typing import Any

import pytest

from tests.twitch.support import chat_event
from valmal.core.config import config
from valmal.db.models import TwitchCommand
from valmal.twitch.models.api.channel import Channel
from valmal.twitch.models.api.user import User
from valmal.twitch.models.eventsub.channel_chat_message import (
    ChannelChatMessageEventSub,
)


class ChatWorld:
    """Everything the chat commands reach out to, recorded."""

    def __init__(self) -> None:
        self.said: list[tuple[str | int, str, str]] = []
        self.templates: list[tuple[str | int, str, dict[str, Any]]] = []
        self.notified: list[tuple[str, str | None]] = []
        self.users: dict[str, User | Exception | None] = {}
        self.channels: dict[int, Channel | Exception | None] = {}
        self.queued: list[tuple[str, str]] = []
        self.listed: set[int] = set()
        self.spent: list[tuple[str, int]] = []
        self.template_result = True

    def install(
        self,
        monkeypatch: pytest.MonkeyPatch,
        *rows: TwitchCommand,
        responses: dict[str, list[str]] | None = None,
        components: dict[str, list[str]] | None = None,
    ) -> None:
        monkeypatch.setattr(config, "_commands", {row.name: row for row in rows})
        monkeypatch.setattr(config, "_command_responses", responses or {})
        monkeypatch.setattr(config, "_command_components", components or {})


def event(text: str = "!x", **kwargs: Any) -> ChannelChatMessageEventSub:
    return ChannelChatMessageEventSub.model_validate(chat_event(text, **kwargs))


def command(
    name: str, handler: str = "static", mod_only: bool = False
) -> TwitchCommand:
    return TwitchCommand(name=name, handler=handler, mod_only=mod_only)
