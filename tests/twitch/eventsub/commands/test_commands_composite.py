import logging

import pytest

from tests.twitch.eventsub.commands.support import ChatWorld, command, event
from valmal.twitch.eventsub.commands import dispatch

pytestmark = pytest.mark.anyio


class TestComposite:
    async def test_runs_each_member_in_order_with_the_same_arguments(
        self, chatworld: ChatWorld, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        chatworld.install(
            monkeypatch,
            command("everything", handler="composite"),
            command("a"),
            command("b"),
            responses={"a": ["A {target}"], "b": ["B {target}"]},
            components={"everything": ["a", "b"]},
        )

        await dispatch(event(), "everything", "carol")

        assert [text for _, text, _ in chatworld.said] == ["A carol", "B carol"]

    async def test_a_composite_that_is_mod_only_is_refused_once_not_once_per_member(
        self, chatworld: ChatWorld, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        chatworld.install(
            monkeypatch,
            command("everything", handler="composite", mod_only=True),
            command("a"),
            command("b"),
            responses={"a": ["A"], "b": ["B"]},
            components={"everything": ["a", "b"]},
        )

        await dispatch(event(), "everything", "")

        assert chatworld.said == []
        assert chatworld.templates == [("111", "twitch_mod_only", {})]

    async def test_a_mod_only_member_is_checked_itself_and_skipped_in_silence(
        self, chatworld: ChatWorld, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Checking only what was typed made a composite a way around its children."""
        chatworld.install(
            monkeypatch,
            command("everything", handler="composite"),
            command("open"),
            command("secret", mod_only=True),
            command("also_open"),
            responses={"open": ["1"], "secret": ["2"], "also_open": ["3"]},
            components={"everything": ["open", "secret", "also_open"]},
        )

        await dispatch(event(badges=()), "everything", "")

        assert [text for _, text, _ in chatworld.said] == ["1", "3"]
        assert chatworld.templates == [], "one refusal must not become one per member"

    async def test_a_mod_runs_every_member_including_the_mod_only_one(
        self, chatworld: ChatWorld, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        chatworld.install(
            monkeypatch,
            command("everything", handler="composite"),
            command("secret", mod_only=True),
            responses={"secret": ["2"]},
            components={"everything": ["secret"]},
        )

        await dispatch(event(badges=("moderator",)), "everything", "")

        assert [text for _, text, _ in chatworld.said] == ["2"]

    async def test_a_composite_nested_in_a_composite_runs_through(
        self, chatworld: ChatWorld, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        chatworld.install(
            monkeypatch,
            command("outer", handler="composite"),
            command("inner", handler="composite"),
            command("leaf"),
            responses={"leaf": ["leaf"]},
            components={"outer": ["inner"], "inner": ["leaf"]},
        )

        await dispatch(event(), "outer", "")

        assert [text for _, text, _ in chatworld.said] == ["leaf"]

    async def test_a_member_of_its_own_cycle_is_stopped_and_logged(
        self,
        chatworld: ChatWorld,
        monkeypatch: pytest.MonkeyPatch,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        chatworld.install(
            monkeypatch,
            command("a", handler="composite"),
            command("b", handler="composite"),
            command("leaf"),
            responses={"leaf": ["L"]},
            components={"a": ["leaf", "b"], "b": ["leaf", "a"]},
        )

        with caplog.at_level(logging.WARNING, logger="valmal.twitch.eventsub.commands"):
            await dispatch(event(), "a", "")

        assert [text for _, text, _ in chatworld.said] == ["L", "L"]
        assert any("member of its own cycle" in r.getMessage() for r in caplog.records)

    async def test_a_composite_that_contains_itself_terminates(
        self, chatworld: ChatWorld, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        chatworld.install(
            monkeypatch,
            command("loop", handler="composite"),
            components={"loop": ["loop"]},
        )

        await dispatch(event(), "loop", "")

        assert chatworld.said == []

    async def test_a_member_that_is_not_an_enabled_command_is_skipped(
        self, chatworld: ChatWorld, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        chatworld.install(
            monkeypatch,
            command("everything", handler="composite"),
            command("real"),
            responses={"real": ["R"]},
            components={"everything": ["missing", "real"]},
        )

        await dispatch(event(), "everything", "")

        assert [text for _, text, _ in chatworld.said] == ["R"]
