from typing import Any

import pytest

from tests.twitch.eventsub.commands.support import ChatWorld
from tests.twitch.support import channel_json, chat_event, stream_json, user_json
from valmal.twitch.client.helix import HelixError
from valmal.twitch.eventsub import commands
from valmal.twitch.models.api.channel import Channel
from valmal.twitch.models.api.stream import Stream
from valmal.twitch.models.api.user import User
from valmal.twitch.models.eventsub.channel_chat_message import (
    ChannelChatMessageEventSub,
)
from valmal.twitch.stream import stream_session

pytestmark = pytest.mark.anyio


def event(**kwargs: Any) -> ChannelChatMessageEventSub:
    return ChannelChatMessageEventSub.model_validate(chat_event("!x", **kwargs))


def alice(world: ChatWorld, id: str = "42") -> None:
    """Twitch knows `alice`, and her channel, playing Chess."""
    world.users["alice"] = User.model_validate(user_json(id, "alice"))
    world.channels[int(id)] = Channel.model_validate(
        channel_json(id, "alice", "Alice", "Chess")
    )


def go_live() -> None:
    stream_session._stream = Stream.model_validate(stream_json("10"))


NOT_FOUND = ("111", "twitch_shoutout_not_found", {})


class TestHug:
    async def test_a_named_target_gets_the_target_template(
        self, chatworld: ChatWorld
    ) -> None:
        await commands.hug(event(chatter_name="Bea"), "@carol")

        assert chatworld.templates == [
            ("111", "twitch_hug_target", {"chatter": "Bea", "target": "carol"})
        ]

    async def test_nobody_named_hugs_everyone(self, chatworld: ChatWorld) -> None:
        await commands.hug(event(chatter_name="Bea"), "")

        assert chatworld.templates == [
            ("111", "twitch_hug_everyone", {"chatter": "Bea", "target": ""})
        ]

    async def test_a_target_of_only_a_prefix_or_invisible_characters_hugs_everyone(
        self, chatworld: ChatWorld
    ) -> None:
        await commands.hug(event(), "@!")

        assert chatworld.templates[0][1] == "twitch_hug_everyone"

    async def test_the_target_reaches_the_template_cleaned(
        self, chatworld: ChatWorld
    ) -> None:
        await commands.hug(event(), f"{chr(8238)}!so")

        assert chatworld.templates[0][2]["target"] == "so"


class TestShoutout:
    async def test_shouts_a_known_channel_out_and_says_what_they_play(
        self, chatworld: ChatWorld
    ) -> None:
        alice(chatworld)

        assert await commands.shoutout(event(), "alice") is True

        assert chatworld.templates == [
            (
                "111",
                "twitch_shoutout",
                {"name": "Alice", "login": "alice", "game": "Chess"},
            )
        ]

    async def test_a_bare_command_shouts_out_the_channel_it_was_typed_in(
        self, chatworld: ChatWorld
    ) -> None:
        chatworld.users["bob"] = User.model_validate(user_json("1", "bob"))

        await commands.shoutout(event(broadcaster_login="bob"), "")

        assert chatworld.templates[0][1] == "twitch_shoutout"

    async def test_while_live_the_target_is_queued_for_a_helix_shoutout(
        self, chatworld: ChatWorld
    ) -> None:
        alice(chatworld)
        go_live()

        await commands.shoutout(event(), "alice")

        assert chatworld.queued == [("alice", "42")]

    async def test_between_streams_there_is_no_queue_to_feed(
        self, chatworld: ChatWorld
    ) -> None:
        alice(chatworld)

        await commands.shoutout(event(), "alice")

        assert chatworld.queued == []

    async def test_the_answer_is_whether_the_line_landed_not_whether_a_channel_was_found(
        self, chatworld: ChatWorld
    ) -> None:
        """A shoutout nobody saw is not one, so `!aso` must leave them unsettled."""
        alice(chatworld)
        chatworld.template_result = False

        assert await commands.shoutout(event(), "alice") is False

    @pytest.mark.parametrize("target", ["bad$name", "a" * 26, "böb", "<@1>", "bob`"])
    async def test_a_name_that_cannot_be_a_login_is_answered_like_an_unknown_channel(
        self, target: str, chatworld: ChatWorld
    ) -> None:
        assert await commands.shoutout(event(), target) is False

        assert chatworld.templates == [NOT_FOUND]

    async def test_a_name_that_cannot_be_a_login_costs_no_lookup(
        self, chatworld: ChatWorld, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        async def lookup(username: str) -> User | None:
            raise AssertionError("nothing a chatter typed may reach Helix unchecked")

        monkeypatch.setattr(commands, "get_user_by_username", lookup)

        await commands.shoutout(event(), "bad$name")

    async def test_an_unknown_user_is_not_found(self, chatworld: ChatWorld) -> None:
        chatworld.users["ghost"] = None

        assert await commands.shoutout(event(), "ghost") is False
        assert chatworld.templates == [NOT_FOUND]

    async def test_a_user_with_no_channel_is_not_found(
        self, chatworld: ChatWorld
    ) -> None:
        chatworld.users["alice"] = User.model_validate(user_json("42", "alice"))
        chatworld.channels[42] = None

        assert await commands.shoutout(event(), "alice") is False
        assert chatworld.templates == [NOT_FOUND]

    @pytest.mark.parametrize("failing", ["user", "channel"])
    async def test_a_lookup_that_failed_tells_chat_not_found_and_the_admin_channel_why(
        self, failing: str, chatworld: ChatWorld
    ) -> None:
        alice(chatworld)
        error = HelixError("twitch is down")
        if failing == "user":
            chatworld.users["alice"] = error
        else:
            chatworld.channels[42] = error

        assert await commands.shoutout(event(), "alice") is False

        assert chatworld.templates == [NOT_FOUND]
        assert chatworld.notified == [
            (
                "Could not look up alice for !so: twitch is down",
                "twitch-shoutout-lookup",
            )
        ]

    async def test_nothing_is_queued_for_a_lookup_that_failed(
        self, chatworld: ChatWorld
    ) -> None:
        go_live()
        alice(chatworld)
        chatworld.users["alice"] = HelixError("down")

        await commands.shoutout(event(), "alice")

        assert chatworld.queued == []

    async def test_the_target_is_cleaned_before_it_is_looked_up(
        self, chatworld: ChatWorld
    ) -> None:
        alice(chatworld)

        assert await commands.shoutout(event(), "@!alice extra words") is True


class TestAutoShoutout:
    async def test_a_new_entry_is_listed_shouted_out_now_and_spent(
        self, chatworld: ChatWorld
    ) -> None:
        alice(chatworld)

        await commands.auto_shoutout(event(), "alice")

        assert 42 in chatworld.listed
        assert chatworld.templates[0][1] == "twitch_shoutout"
        assert chatworld.spent == [("111", 42)]

    async def test_someone_already_listed_is_left_alone_in_silence(
        self, chatworld: ChatWorld
    ) -> None:
        """`!so` is what a mod types when they want a shoutout now."""
        alice(chatworld)
        chatworld.listed.add(42)

        await commands.auto_shoutout(event(), "alice")

        assert chatworld.templates == []
        assert chatworld.spent == []

    async def test_a_shoutout_that_did_not_land_is_not_spent_but_the_row_stays(
        self, chatworld: ChatWorld
    ) -> None:
        """Spending on it would settle someone who was never shouted out; the row stays
        because wanting them on the list is what `!aso` records."""
        alice(chatworld)
        chatworld.template_result = False

        await commands.auto_shoutout(event(), "alice")

        assert 42 in chatworld.listed
        assert chatworld.spent == []

    @pytest.mark.parametrize("args", ["", "bad$name", "<@1>", "@!"])
    async def test_nobody_named_or_a_name_that_is_no_login_is_not_found_and_adds_nobody(
        self, args: str, chatworld: ChatWorld
    ) -> None:
        """Unlike `!so`, a bare `!aso` must not quietly add the channel to its own list."""
        await commands.auto_shoutout(event(broadcaster_login="bob"), args)

        assert chatworld.templates == [NOT_FOUND]
        assert chatworld.listed == set()

    async def test_an_unknown_user_is_not_found_and_adds_nobody(
        self, chatworld: ChatWorld
    ) -> None:
        chatworld.users["ghost"] = None

        await commands.auto_shoutout(event(), "ghost")

        assert chatworld.templates == [NOT_FOUND]
        assert chatworld.listed == set()


class TestUnAutoShoutout:
    async def test_removing_someone_listed_confirms_with_their_display_name(
        self, chatworld: ChatWorld
    ) -> None:
        alice(chatworld)
        chatworld.listed.add(42)

        await commands.un_auto_shoutout(event(), "alice")

        assert 42 not in chatworld.listed
        assert chatworld.templates == [
            ("111", "twitch_autoshoutout_removed", {"name": "Alice"})
        ]

    async def test_someone_not_listed_is_silence(self, chatworld: ChatWorld) -> None:
        alice(chatworld)

        await commands.un_auto_shoutout(event(), "alice")

        assert chatworld.templates == []

    @pytest.mark.parametrize("args", ["", "bad$name", "@!"])
    async def test_naming_nobody_valid_is_silence(
        self, args: str, chatworld: ChatWorld
    ) -> None:
        await commands.un_auto_shoutout(event(), args)

        assert chatworld.templates == []

    async def test_an_unknown_user_is_silence(self, chatworld: ChatWorld) -> None:
        chatworld.users["ghost"] = None

        await commands.un_auto_shoutout(event(), "ghost")

        assert chatworld.templates == []

    async def test_it_does_not_unspend_anyone(self, chatworld: ChatWorld) -> None:
        """They already had this stream's autoshoutout."""
        alice(chatworld)
        chatworld.listed.add(42)
        stream_session.settle(42)

        await commands.un_auto_shoutout(event(), "alice")

        assert stream_session.is_settled(42) is True
        stream_session._settled.discard(42)
