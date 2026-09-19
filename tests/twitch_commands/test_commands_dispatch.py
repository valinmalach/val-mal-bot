import logging
from typing import Any

import pytest

from db.models import TwitchCommand
from models.twitch_event_subs.channel_chat_message import ChannelChatMessageEventSub
from services.twitch import commands
from services.twitch.commands import _is_mod, _render, dispatch, is_twitch_login
from services.twitch.helix import HelixError
from tests.twitch.support import chat_event
from tests.twitch_commands.support import ChatWorld

pytestmark = pytest.mark.anyio

# The look-alike letters, without typing them: an ambiguous character in source is
# one a reader cannot tell from the ASCII one.
FULLWIDTH = {ord(c): chr(ord(c) + 0xFEE0) for c in "abcdefghijklmnopqrstuvwxyz"}


def event(text: str = "!x", **kwargs: Any) -> ChannelChatMessageEventSub:
    return ChannelChatMessageEventSub.model_validate(chat_event(text, **kwargs))


def command(
    name: str, handler: str = "static", mod_only: bool = False
) -> TwitchCommand:
    return TwitchCommand(name=name, handler=handler, mod_only=mod_only)


class TestIsTwitchLogin:
    @pytest.mark.parametrize(
        "login",
        ["a", "bob", "Bob", "bob_1", "_bob", "1234", "x" * 25, "A_b_C_1", "ab"],
    )
    def test_accepts_up_to_twenty_five_ascii_letters_digits_or_underscores(
        self, login: str
    ) -> None:
        """No lower bound: a legacy handle shorter than four is Twitch's to have
        issued, and refusing one would silently drop a raid's shoutout."""
        assert is_twitch_login(login) is True

    @pytest.mark.parametrize(
        "value",
        [
            "",
            "x" * 26,
            "bob smith",
            "bob-smith",
            "bob.smith",
            "@bob",
            "!bob",
            "bob\n",
            "\nbob",
            "bob\x00",
            "böb",
            "ボブ",
            "bob".translate(FULLWIDTH),
            "٢٣",
            "bob`",
            "<@1>",
            " bob",
            "bob ",
        ],
    )
    def test_refuses_anything_that_could_not_name_a_channel(self, value: str) -> None:
        assert is_twitch_login(value) is False

    def test_a_trailing_newline_does_not_sneak_through_the_anchor(self) -> None:
        """`$` would accept it; the value is about to reach a Helix query parameter."""
        assert is_twitch_login("bob\n") is False


class TestIsMod:
    @pytest.mark.parametrize("badge", ["moderator", "broadcaster"])
    def test_these_badges_may_run_a_mod_only_command(self, badge: str) -> None:
        assert _is_mod(event(badges=(badge,))) is True

    @pytest.mark.parametrize(
        "badges", [(), ("vip",), ("subscriber", "premium"), ("Moderator",)]
    )
    def test_nothing_else_does(self, badges: tuple[str, ...]) -> None:
        assert _is_mod(event(badges=badges)) is False

    def test_one_qualifying_badge_among_others_is_enough(self) -> None:
        assert _is_mod(event(badges=("subscriber", "moderator", "vip"))) is True


class TestRender:
    def test_fills_the_chatter_the_broadcaster_and_the_target(self) -> None:
        text = _render(
            "hi {chatter}, from {broadcaster}, to {target}",
            event(chatter_name="Alice", broadcaster_login="bob"),
            "@carol and more",
        )

        assert text == "hi Alice, from Bob, to carol"

    def test_an_unknown_field_is_left_as_written(self) -> None:
        assert _render("{chatter} {nope}", event(chatter_name="A"), "") == "A {nope}"

    def test_a_display_name_that_looks_like_a_field_is_not_reread(self) -> None:
        assert _render("{chatter}", event(chatter_name="{target}"), "x") == "{target}"

    def test_the_target_is_cleaned_before_it_reaches_chat(self) -> None:
        assert _render("{target}", event(), "\u200b!so") == "so"

    def test_a_missing_target_is_empty(self) -> None:
        assert _render("[{target}]", event(), "") == "[]"


class TestUnknownAndStatic:
    async def test_an_unknown_command_does_nothing(
        self, chatworld: ChatWorld, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        chatworld.install(monkeypatch)

        await dispatch(event(), "nope", "")

        assert chatworld.said == [] and chatworld.templates == []

    async def test_a_static_command_says_each_stored_response_in_order(
        self, chatworld: ChatWorld, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        chatworld.install(
            monkeypatch,
            command("discord"),
            responses={"discord": ["first {chatter}", "second", "third {target}"]},
        )

        await dispatch(event(chatter_name="Alice"), "discord", "@carol")

        assert chatworld.said == [
            ("111", "first Alice", "a response for !discord"),
            ("111", "second", "a response for !discord"),
            ("111", "third carol", "a response for !discord"),
        ]

    async def test_a_command_with_no_responses_says_nothing(
        self, chatworld: ChatWorld, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        chatworld.install(monkeypatch, command("empty"))

        await dispatch(event(), "empty", "")

        assert chatworld.said == []

    async def test_a_handler_the_registry_does_not_know_falls_back_to_its_responses(
        self, chatworld: ChatWorld, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        chatworld.install(
            monkeypatch,
            command("odd", handler="retired_handler"),
            responses={"odd": ["still answers"]},
        )

        await dispatch(event(), "odd", "")

        assert [text for _, text, _ in chatworld.said] == ["still answers"]


class TestModOnly:
    async def test_a_non_mod_is_refused_once_out_loud(
        self, chatworld: ChatWorld, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        chatworld.install(
            monkeypatch,
            command("ban", mod_only=True),
            responses={"ban": ["banned"]},
        )

        await dispatch(event(badges=("vip",)), "ban", "")

        assert chatworld.said == []
        assert chatworld.templates == [("111", "twitch_mod_only", {})]

    @pytest.mark.parametrize("badge", ["moderator", "broadcaster"])
    async def test_a_mod_or_the_broadcaster_may_run_it(
        self, badge: str, chatworld: ChatWorld, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        chatworld.install(
            monkeypatch,
            command("ban", mod_only=True),
            responses={"ban": ["banned"]},
        )

        await dispatch(event(badges=(badge,)), "ban", "")

        assert [text for _, text, _ in chatworld.said] == ["banned"]
        assert chatworld.templates == []

    async def test_a_command_that_is_not_mod_only_needs_no_badge(
        self, chatworld: ChatWorld, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        chatworld.install(monkeypatch, command("hi"), responses={"hi": ["hello"]})

        await dispatch(event(badges=()), "hi", "")

        assert len(chatworld.said) == 1


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

        with caplog.at_level(logging.WARNING, logger="services.twitch.commands"):
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


class TestHandlers:
    async def test_a_named_handler_is_called_with_the_event_and_arguments(
        self, chatworld: ChatWorld, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        calls: list[tuple[str, str]] = []

        async def custom(event_sub: ChannelChatMessageEventSub, args: str) -> None:
            calls.append((event_sub.event.chatter_user_login, args))

        monkeypatch.setitem(commands.HANDLERS, "custom", custom)
        chatworld.install(
            monkeypatch,
            command("do", handler="custom"),
            responses={"do": ["never sent: a handler replaces the responses"]},
        )

        await dispatch(event(chatter_login="alice"), "do", "the args")

        assert calls == [("alice", "the args")]
        assert chatworld.said == []

    async def test_a_helix_failure_in_a_handler_is_reported_not_raised(
        self, chatworld: ChatWorld, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        async def failing(event_sub: ChannelChatMessageEventSub, args: str) -> None:
            raise HelixError("twitch is down")

        monkeypatch.setitem(commands.HANDLERS, "failing", failing)
        chatworld.install(monkeypatch, command("do", handler="failing"))

        await dispatch(event(), "do", "")

        assert chatworld.notified == [
            ("Could not run !do: twitch is down", "twitch-command:do")
        ]

    async def test_any_other_failure_is_left_for_the_webhooks_own_guard(
        self, chatworld: ChatWorld, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        async def failing(event_sub: ChannelChatMessageEventSub, args: str) -> None:
            raise ValueError("a bug")

        monkeypatch.setitem(commands.HANDLERS, "failing", failing)
        chatworld.install(monkeypatch, command("do", handler="failing"))

        with pytest.raises(ValueError, match="a bug"):
            await dispatch(event(), "do", "")

    def test_the_registry_holds_exactly_the_handlers_the_database_can_name(
        self,
    ) -> None:
        assert set(commands.HANDLERS) == {
            "auto_shoutout",
            "hug",
            "shoutout",
            "un_auto_shoutout",
        }

    def test_composite_is_not_a_registry_handler_because_the_dispatcher_owns_it(
        self,
    ) -> None:
        assert commands.COMPOSITE_HANDLER == "composite"
        assert commands.COMPOSITE_HANDLER not in commands.HANDLERS
