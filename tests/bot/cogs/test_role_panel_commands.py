from types import SimpleNamespace
from typing import Any
from unittest.mock import MagicMock

import pytest

from valmal.bot.cogs import role_panels
from valmal.bot.cogs.role_panels import RolePanels
from valmal.core.config import config

pytestmark = pytest.mark.anyio


class Panels:
    def __init__(self) -> None:
        self.posted: list[tuple[object, object, int]] = []
        self.asked: list[str] = []
        self.answers: list[str] = []
        self.order: list[str] = []
        self.panels: dict[str, list[tuple[object, object, int]]] = {}
        self.error: Exception | None = None

    def interaction(self) -> Any:
        async def send_message(text: str) -> None:
            self.order.append("answer")
            self.answers.append(text)

        return SimpleNamespace(response=SimpleNamespace(send_message=send_message))


@pytest.fixture
def panels(monkeypatch: pytest.MonkeyPatch) -> Panels:
    panels = Panels()

    async def send_embed(embed: object, channel_id: int, view: object = None) -> int:
        panels.order.append("post")
        if panels.error is not None:
            raise panels.error
        panels.posted.append((embed, view, channel_id))
        return 1

    def build(channel_key: str) -> list[tuple[object, object, int]]:
        panels.asked.append(channel_key)
        return panels.panels.get(channel_key, [])

    monkeypatch.setattr(role_panels, "send_embed", send_embed)
    monkeypatch.setattr(role_panels, "role_panels", build)
    monkeypatch.setattr(
        config,
        "_templates",
        {"admin_rules_sent": "rules posted", "admin_roles_sent": "roles posted"},
    )
    return panels


def cog() -> RolePanels:
    return RolePanels(MagicMock())


class TestRules:
    async def test_posts_every_rules_panel_then_confirms(self, panels: Panels) -> None:
        panels.panels["rules"] = [("e1", "v1", 10), ("e2", "v2", 11)]

        await RolePanels.rules.callback(cog(), panels.interaction())  # pyright: ignore[reportCallIssue]

        assert panels.posted == [("e1", "v1", 10), ("e2", "v2", 11)]
        assert panels.answers == ["rules posted"]
        assert panels.order == ["post", "post", "answer"]

    async def test_asks_for_the_rules_channels_panels_only(
        self, panels: Panels
    ) -> None:
        await RolePanels.rules.callback(cog(), panels.interaction())  # pyright: ignore[reportCallIssue]

        assert panels.asked == ["rules"]

    async def test_with_nothing_configured_it_still_confirms(
        self, panels: Panels
    ) -> None:
        await RolePanels.rules.callback(cog(), panels.interaction())  # pyright: ignore[reportCallIssue]

        assert panels.posted == [] and panels.answers == ["rules posted"]

    async def test_a_failed_post_is_not_confirmed(self, panels: Panels) -> None:
        panels.panels["rules"] = [("e", "v", 1)]
        panels.error = RuntimeError("discord down")

        with pytest.raises(RuntimeError):
            await RolePanels.rules.callback(cog(), panels.interaction())  # pyright: ignore[reportCallIssue]

        assert panels.answers == []


class TestRoles:
    async def test_posts_every_roles_panel_then_confirms(self, panels: Panels) -> None:
        panels.panels["roles"] = [("e", "v", 20)]

        await RolePanels.roles.callback(cog(), panels.interaction())  # pyright: ignore[reportCallIssue]

        assert panels.posted == [("e", "v", 20)]
        assert panels.answers == ["roles posted"]
        assert panels.order == ["post", "answer"]

    async def test_asks_for_the_roles_channels_panels_only(
        self, panels: Panels
    ) -> None:
        await RolePanels.roles.callback(cog(), panels.interaction())  # pyright: ignore[reportCallIssue]

        assert panels.asked == ["roles"]

    async def test_a_failed_post_is_not_confirmed(self, panels: Panels) -> None:
        panels.panels["roles"] = [("e", "v", 1)]
        panels.error = RuntimeError("discord down")

        with pytest.raises(RuntimeError):
            await RolePanels.roles.callback(cog(), panels.interaction())  # pyright: ignore[reportCallIssue]

        assert panels.answers == []


def test_the_cog_keeps_the_bot_it_was_given() -> None:
    bot = MagicMock()

    assert RolePanels(bot).bot is bot
