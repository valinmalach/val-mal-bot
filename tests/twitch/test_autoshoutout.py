import asyncio
from typing import Any

import pytest

from tests.twitch.support import chat_event, redemption_event, stream_json
from valmal.core.config import config
from valmal.db import repository
from valmal.twitch.models.api.stream import Stream
from valmal.twitch.models.eventsub.channel_chat_message import (
    ChannelChatMessageEventSub,
)
from valmal.twitch.models.eventsub.channel_points_custom_reward_redemption_add import (
    ChannelPointsCustomRewardRedemptionAddEventSub,
)
from valmal.twitch.stream import autoshoutout, stream_session

pytestmark = pytest.mark.anyio


def chat(**kwargs: Any) -> ChannelChatMessageEventSub:
    return ChannelChatMessageEventSub.model_validate(chat_event(**kwargs))


def redemption(**kwargs: Any) -> ChannelPointsCustomRewardRedemptionAddEventSub:
    return ChannelPointsCustomRewardRedemptionAddEventSub.model_validate(
        redemption_event(**kwargs)
    )


def going_live(stream_id: str = "10") -> None:
    stream_session._stream = Stream.model_validate(stream_json(stream_id))


class World:
    def __init__(self) -> None:
        self.listed: set[int] = set()
        self.asked: list[int] = []
        self.said: list[tuple[str | int, str, str]] = []
        self.notified: list[tuple[str, str | None]] = []
        self.reported: list[str] = []
        self.on_ask: Any = None
        self.say_result = True


@pytest.fixture
def world(monkeypatch: pytest.MonkeyPatch) -> World:
    world = World()

    async def is_autoshoutout(twitch_user_id: int) -> bool:
        world.asked.append(twitch_user_id)
        if world.on_ask is not None:
            await world.on_ask()
        return twitch_user_id in world.listed

    async def add_autoshoutout(twitch_user_id: int, login: str) -> bool:
        added = twitch_user_id not in world.listed
        world.listed.add(twitch_user_id)
        return added

    async def remove_autoshoutout(twitch_user_id: int) -> bool:
        present = twitch_user_id in world.listed
        world.listed.discard(twitch_user_id)
        return present

    async def say(broadcaster_id: str | int, text: str, what: str) -> bool:
        world.said.append((broadcaster_id, text, what))
        return world.say_result

    async def notify(text: str, *, key: str | None = None) -> bool:
        world.notified.append((text, key))
        return True

    async def report(exc: Exception, context: str, **_: object) -> None:
        world.reported.append(context)

    monkeypatch.setattr(repository, "is_autoshoutout", is_autoshoutout)
    monkeypatch.setattr(repository, "add_autoshoutout", add_autoshoutout)
    monkeypatch.setattr(repository, "remove_autoshoutout", remove_autoshoutout)
    monkeypatch.setattr(autoshoutout, "say", say)
    monkeypatch.setattr(autoshoutout, "notify", notify)
    monkeypatch.setattr(autoshoutout, "report", report)
    return world


@pytest.fixture(autouse=True)
def _session(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(stream_session, "_stream", None)
    monkeypatch.setattr(stream_session, "_settled", set())
    monkeypatch.setattr(
        config,
        "_settings",
        {"twitch_broadcaster_id": "111", "twitch_bot_user_id": "999"},
    )


class TestChatted:
    async def test_a_listed_chatter_is_shouted_out_with_a_bang_so_line(
        self, world: World
    ) -> None:
        going_live()
        world.listed.add(3)

        await autoshoutout.chatted(chat(chatter_id="3", chatter_login="alice"))

        assert world.said == [("111", "!so alice", "an autoshoutout")]

    async def test_an_unlisted_chatter_costs_one_lookup_and_nothing_is_said(
        self, world: World
    ) -> None:
        going_live()

        await autoshoutout.chatted(chat(chatter_id="3"))

        assert world.asked == [3]
        assert world.said == []

    async def test_either_way_they_are_settled_so_the_list_is_asked_once_a_stream(
        self, world: World
    ) -> None:
        going_live()
        world.listed.add(3)

        for _ in range(5):
            await autoshoutout.chatted(chat(chatter_id="3"))
            await autoshoutout.chatted(chat(chatter_id="4"))

        assert sorted(world.asked) == [3, 4]
        assert len(world.said) == 1
        assert stream_session.is_settled(3) and stream_session.is_settled(4)

    async def test_nothing_happens_while_nobody_is_live(self, world: World) -> None:
        world.listed.add(3)

        await autoshoutout.chatted(chat(chatter_id="3"))

        assert world.asked == []
        assert world.said == []
        assert stream_session.is_settled(3) is False

    async def test_a_line_in_some_other_channel_is_not_this_streams_business(
        self, world: World
    ) -> None:
        """is_live answers for the main broadcaster while the line would be posted to
        whoever the event named; without this a second channel would get `!so` every
        time the main broadcaster was live, and the person would be settled out of
        the autoshoutout they were actually owed."""
        going_live()
        world.listed.add(3)

        await autoshoutout.chatted(chat(chatter_id="3", broadcaster_id="222"))

        assert world.asked == []
        assert world.said == []
        assert stream_session.is_settled(3) is False

    async def test_the_bot_is_not_a_viewer_turning_up(self, world: World) -> None:
        """It says `!so <login>` for every raid and autoshoutout, and each line comes
        back through this same webhook."""
        going_live()

        await autoshoutout.chatted(chat(chatter_id="999"))

        assert world.asked == []
        assert stream_session.is_settled(999) is False

    async def test_a_failure_is_reported_and_never_raised(
        self, world: World, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """An autoshoutout that failed must not stop the command on the same line."""
        going_live()

        async def explode(twitch_user_id: int) -> bool:
            raise ConnectionError("database is down")

        monkeypatch.setattr(repository, "is_autoshoutout", explode)

        await autoshoutout.chatted(chat(chatter_id="3"))

        assert world.reported == ["Error considering an autoshoutout for a chatter"]

    async def test_a_non_numeric_chatter_id_is_reported_not_raised(
        self, world: World
    ) -> None:
        going_live()

        await autoshoutout.chatted(chat(chatter_id="not-a-number"))

        assert len(world.reported) == 1

    async def test_two_lines_from_one_person_at_once_shout_once(
        self, world: World
    ) -> None:
        """Chat webhooks are dispatched concurrently, so both pass the check while the
        first is still awaiting the list. Settling with no await since that check is
        what stops the second."""
        going_live()
        world.listed.add(3)
        gate = asyncio.Event()

        async def hold() -> None:
            await gate.wait()

        world.on_ask = hold
        first = asyncio.ensure_future(autoshoutout.chatted(chat(chatter_id="3")))
        await asyncio.sleep(0)
        second = asyncio.ensure_future(autoshoutout.chatted(chat(chatter_id="3")))
        await asyncio.sleep(0)
        gate.set()
        await asyncio.gather(first, second)

        assert world.asked == [3]
        assert len(world.said) == 1

    async def test_a_stream_that_began_while_the_list_was_asked_is_not_shouted_into(
        self, world: World
    ) -> None:
        """Reading is_live() afterwards would say yes for a stream that started after
        the one this chatter spoke in ended."""
        going_live("10")
        world.listed.add(3)

        async def a_new_stream_begins() -> None:
            going_live("11")

        world.on_ask = a_new_stream_begins

        await autoshoutout.chatted(chat(chatter_id="3"))

        assert world.said == []

    async def test_a_stream_that_ended_while_the_list_was_asked_is_not_shouted_into(
        self, world: World
    ) -> None:
        going_live("10")
        world.listed.add(3)

        async def the_stream_ends() -> None:
            stream_session._stream = None

        world.on_ask = the_stream_ends

        await autoshoutout.chatted(chat(chatter_id="3"))

        assert world.said == []

    @pytest.mark.parametrize(
        "login", ["", "bad login", "@everyone", "x" * 26, "a`b", "café"]
    )
    async def test_a_login_that_cannot_name_a_channel_is_refused_not_built_into_a_command(
        self, login: str, world: World
    ) -> None:
        """The line is posted to chat and returns through the webhook to be
        dispatched, so it must never carry something that is not a login."""
        going_live()
        world.listed.add(3)

        await autoshoutout.chatted(chat(chatter_id="3", chatter_login=login))

        assert world.said == []
        ((text, key),) = world.notified
        assert "Refused an autoshoutout" in text
        assert key == "autoshoutout-bad-login"
        assert stream_session.is_settled(3), "settled regardless: nothing can help them"

    async def test_the_refusal_quotes_the_login_inertly(self, world: World) -> None:
        going_live()
        world.listed.add(3)

        await autoshoutout.chatted(chat(chatter_id="3", chatter_login="<@1>`"))

        assert "`<@1>`" in world.notified[0][0]


class TestRedeemed:
    async def test_a_listed_redeemer_is_shouted_out_like_a_chatter(
        self, world: World
    ) -> None:
        going_live()
        world.listed.add(5)

        await autoshoutout.redeemed(redemption(user_id="5", user_login="alice"))

        assert world.said == [("111", "!so alice", "an autoshoutout")]

    async def test_an_unlisted_redeemer_is_settled_and_not_shouted(
        self, world: World
    ) -> None:
        going_live()

        await autoshoutout.redeemed(redemption(user_id="5"))

        assert world.said == []
        assert stream_session.is_settled(5)

    async def test_a_redemption_in_another_channel_is_ignored(
        self, world: World
    ) -> None:
        going_live()
        world.listed.add(5)

        await autoshoutout.redeemed(redemption(user_id="5", broadcaster_id="222"))

        assert world.said == []

    async def test_it_has_no_guard_of_its_own_so_a_failure_reaches_the_handler(
        self, world: World, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Nothing follows this on the notification, so the handler's own report is
        the only one it needs."""
        going_live()

        async def explode(twitch_user_id: int) -> bool:
            raise ConnectionError("down")

        monkeypatch.setattr(repository, "is_autoshoutout", explode)

        with pytest.raises(ConnectionError):
            await autoshoutout.redeemed(redemption())


class TestRaided:
    def test_the_raider_is_settled_because_the_raid_already_earned_them_one(
        self,
    ) -> None:
        autoshoutout.raided("7")

        assert stream_session.is_settled(7)

    def test_settled_even_when_nobody_is_live_yet(self) -> None:
        """A raid can arrive while stream.online is still confirming the stream."""
        autoshoutout.raided("7")

        assert stream_session.is_live() is False
        assert stream_session.is_settled(7)

    def test_a_raider_id_that_is_not_a_number_is_logged_and_skipped(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        autoshoutout.raided("not-a-number")

        assert stream_session._settled == set()
        assert any("not-a-number" in r.getMessage() for r in caplog.records)


class TestSpend:
    def test_spent_while_the_main_broadcaster_is_live(self) -> None:
        going_live()

        autoshoutout.spend("111", 7)

        assert stream_session.is_settled(7)

    def test_not_spent_between_streams_so_the_first_appearance_next_stream_still_earns_one(
        self,
    ) -> None:
        autoshoutout.spend("111", 7)

        assert stream_session.is_settled(7) is False

    def test_not_spent_for_a_command_answered_in_another_channel(self) -> None:
        """A `!so` posted into that channel is a service to it; settling against this
        session is not, because the session is the main broadcaster's."""
        going_live()

        autoshoutout.spend("222", 7)

        assert stream_session.is_settled(7) is False


class TestTheList:
    async def test_add_reports_whether_it_took(self, world: World) -> None:
        assert await autoshoutout.add(7, "alice") is True
        assert await autoshoutout.add(7, "alice") is False
        assert world.listed == {7}

    async def test_remove_reports_whether_they_were_there(self, world: World) -> None:
        world.listed.add(7)

        assert await autoshoutout.remove(7) is True
        assert await autoshoutout.remove(7) is False
