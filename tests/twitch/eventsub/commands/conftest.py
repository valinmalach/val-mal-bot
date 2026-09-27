"""Fixtures for the chat command tests."""

from typing import Any

import pytest

from tests.twitch.eventsub.commands.support import ChatWorld
from tests.twitch.support import channel_json, user_json
from valmal.twitch.eventsub import commands
from valmal.twitch.models.api.channel import Channel
from valmal.twitch.models.api.user import User
from valmal.twitch.stream import autoshoutout, stream_session


@pytest.fixture
def chatworld(monkeypatch: pytest.MonkeyPatch) -> ChatWorld:
    world = ChatWorld()

    async def say(broadcaster_id: str | int, text: str, what: str) -> bool:
        world.said.append((broadcaster_id, text, what))
        return True

    async def say_template(broadcaster_id: str | int, key: str, **values: Any) -> bool:
        world.templates.append((broadcaster_id, key, values))
        return world.template_result

    async def notify(text: str, *, key: str | None = None) -> bool:
        world.notified.append((text, key))
        return True

    async def get_user_by_username(username: str) -> User | None:
        answer = world.users.get(
            username, User.model_validate(user_json("1", username))
        )
        if isinstance(answer, Exception):
            raise answer
        return answer

    async def get_channel(broadcaster_id: int) -> Channel | None:
        answer = world.channels.get(
            broadcaster_id,
            Channel.model_validate(channel_json(str(broadcaster_id), "bob", "Bob")),
        )
        if isinstance(answer, Exception):
            raise answer
        return answer

    async def add(twitch_user_id: int, login: str) -> bool:
        added = twitch_user_id not in world.listed
        world.listed.add(twitch_user_id)
        return added

    async def remove(twitch_user_id: int) -> bool:
        present = twitch_user_id in world.listed
        world.listed.discard(twitch_user_id)
        return present

    monkeypatch.setattr(commands, "say", say)
    monkeypatch.setattr(commands, "say_template", say_template)
    monkeypatch.setattr(commands, "notify", notify)
    monkeypatch.setattr(commands, "get_user_by_username", get_user_by_username)
    monkeypatch.setattr(commands, "get_channel", get_channel)
    monkeypatch.setattr(autoshoutout, "add", add)
    monkeypatch.setattr(autoshoutout, "remove", remove)
    monkeypatch.setattr(
        autoshoutout,
        "spend",
        lambda broadcaster, uid: world.spent.append((broadcaster, uid)),
    )
    monkeypatch.setattr(
        commands.shoutout_queue,
        "add_to_queue",
        lambda login, uid: world.queued.append((login, uid)),
    )
    monkeypatch.setattr(stream_session, "_stream", None)
    return world
