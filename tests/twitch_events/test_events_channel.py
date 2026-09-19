import asyncio
from types import SimpleNamespace
from typing import Any

import pytest

from models.twitch_event_subs.channel_ad_break_begin import ChannelAdBreakBeginEventSub
from models.twitch_event_subs.channel_chat_message import ChannelChatMessageEventSub
from models.twitch_event_subs.channel_follow import ChannelFollowEventSub
from models.twitch_event_subs.channel_moderate import ChannelModerateEventSub
from models.twitch_event_subs.channel_points_custom_reward_redemption_add import (
    ChannelPointsCustomRewardRedemptionAddEventSub,
)
from models.twitch_event_subs.channel_raid import ChannelRaidEventSub
from services.twitch import events
from tests.twitch.support import chat_event, redemption_event
from tests.twitch_events.support import EventWorld

pytestmark = pytest.mark.anyio


def chat(text: str, **kwargs: Any) -> ChannelChatMessageEventSub:
    return ChannelChatMessageEventSub.model_validate(chat_event(text, **kwargs))


def raid(
    from_id: str = "6",
    from_login: str = "raider",
    to_id: str = "111",
    to_login: str = "valinmalach",
) -> ChannelRaidEventSub:
    return ChannelRaidEventSub.model_validate(
        {
            "subscription": {"type": "channel.raid"},
            "event": {
                "from_broadcaster_user_id": from_id,
                "from_broadcaster_user_login": from_login,
                "from_broadcaster_user_name": from_login.title(),
                "to_broadcaster_user_id": to_id,
                "to_broadcaster_user_login": to_login,
                "to_broadcaster_user_name": to_login.title(),
                "viewers": 12,
            },
        }
    )


class TestChatMessage:
    async def test_an_ordinary_line_is_considered_for_an_autoshoutout_and_dispatches_nothing(
        self, world: EventWorld
    ) -> None:
        """Above the `!` check: most people turn up by saying something ordinary."""
        await events.channel_chat_message(chat("hello everyone", chatter_login="alice"))

        assert world.calls == [("chatted", "alice")]
        assert world.dispatched == []

    async def test_a_command_is_dispatched_after_the_autoshoutout_is_considered(
        self, world: EventWorld
    ) -> None:
        await events.channel_chat_message(chat("!hug bob", chatter_login="alice"))

        assert world.calls == [("chatted", "alice")]
        assert world.dispatched == [("hug", "bob")]

    @pytest.mark.parametrize(
        ("text", "command", "args"),
        [
            ("!hug", "hug", ""),
            ("!HUG bob", "hug", "bob"),
            ("!Hug Bob and more", "hug", "Bob and more"),
            ("!so  bob", "so", " bob"),
            ("!", "", ""),
            ("! hug", "", "hug"),
            ("!so bob ", "so", "bob "),
        ],
    )
    async def test_the_command_is_lowercased_and_the_rest_is_its_arguments_verbatim(
        self, text: str, command: str, args: str, world: EventWorld
    ) -> None:
        await events.channel_chat_message(chat(text))

        assert world.dispatched == [(command, args)]

    @pytest.mark.parametrize("text", ["hello !hug", " !hug", "¡hug", "hug!"])
    async def test_only_a_line_beginning_with_a_bang_is_a_command(
        self, text: str, world: EventWorld
    ) -> None:
        await events.channel_chat_message(chat(text))

        assert world.dispatched == []

    async def test_a_line_relayed_from_another_channels_shared_chat_is_dropped_first(
        self, world: EventWorld
    ) -> None:
        """An autoshoutout is owed to someone who turned up in *this* channel."""
        await events.channel_chat_message(
            chat("!hug bob", source_broadcaster_id="222", broadcaster_id="111")
        )

        assert world.calls == []
        assert world.dispatched == []

    async def test_a_line_whose_source_is_this_very_channel_is_processed(
        self, world: EventWorld
    ) -> None:
        await events.channel_chat_message(
            chat("!hug", source_broadcaster_id="111", broadcaster_id="111")
        )

        assert world.dispatched == [("hug", "")]

    async def test_a_failing_command_is_reported_not_raised(
        self, world: EventWorld
    ) -> None:
        world.dispatch_error = RuntimeError("boom")

        await events.channel_chat_message(chat("!hug"))

        assert world.reported == ["Error processing Twitch chat webhook task"]


class TestFollow:
    async def test_thanks_the_follower_by_display_name(self, world: EventWorld) -> None:
        event = ChannelFollowEventSub.model_validate(
            {
                "subscription": {"type": "channel.follow"},
                "event": {
                    "user_id": "4",
                    "user_login": "fan",
                    "user_name": "Fan Person",
                    "broadcaster_user_id": "111",
                    "broadcaster_user_login": "bob",
                    "broadcaster_user_name": "Bob",
                    "followed_at": "2026-06-15T11:59:00Z",
                },
            }
        )

        await events.channel_follow(event)

        assert world.templates == [
            ("111", "twitch_follow_thanks", {"user": "Fan Person"})
        ]

    async def test_a_failure_is_reported_not_raised(self, world: EventWorld) -> None:
        world.template_error = RuntimeError("boom")
        event = ChannelFollowEventSub.model_validate(
            {
                "subscription": {"type": "channel.follow"},
                "event": {
                    "user_id": "4",
                    "user_login": "fan",
                    "user_name": "Fan",
                    "broadcaster_user_id": "111",
                    "broadcaster_user_login": "bob",
                    "broadcaster_user_name": "Bob",
                    "followed_at": "2026-06-15T11:59:00Z",
                },
            }
        )

        await events.channel_follow(event)

        assert world.reported == ["Error processing Twitch follow webhook task"]


class TestAdBreak:
    def event(self, seconds: int) -> ChannelAdBreakBeginEventSub:
        return ChannelAdBreakBeginEventSub.model_validate(
            {
                "subscription": {"type": "channel.ad_break.begin"},
                "event": {
                    "duration_seconds": seconds,
                    "started_at": "2026-06-15T11:59:00Z",
                    "is_automatic": True,
                    "broadcaster_user_id": "111",
                    "broadcaster_user_login": "bob",
                    "broadcaster_user_name": "Bob",
                    "requester_user_id": "2",
                    "requester_user_login": "req",
                    "requester_user_name": "Req",
                },
            }
        )

    async def test_announces_the_start_waits_out_the_break_announces_the_end_and_schedules_the_next_warning(
        self, world: EventWorld
    ) -> None:
        await events.channel_ad_break_begin(self.event(120))

        assert [t[1] for t in world.templates] == [
            "twitch_ad_break_start",
            "twitch_ad_break_end",
        ]
        assert world.slept == [120]
        assert world.calls == [("schedule_warning", "111")]

    async def test_a_failure_is_reported_not_raised(
        self, world: EventWorld, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        async def boom(seconds: float) -> None:
            raise RuntimeError("cancelled sleep")

        monkeypatch.setattr(
            events, "asyncio", SimpleNamespace(**{**vars(asyncio), "sleep": boom})
        )

        await events.channel_ad_break_begin(self.event(60))

        assert world.reported == ["Error processing Twitch ad break webhook task"]
        assert world.calls == []


class TestRedemption:
    def event(self) -> ChannelPointsCustomRewardRedemptionAddEventSub:
        return ChannelPointsCustomRewardRedemptionAddEventSub.model_validate(
            redemption_event(user_login="alice")
        )

    async def test_the_redeemer_is_considered_for_an_autoshoutout(
        self, world: EventWorld
    ) -> None:
        await events.channel_points_custom_reward_redemption_add(self.event())

        assert world.calls == [("redeemed", "alice")]

    async def test_a_failure_is_reported_not_raised(self, world: EventWorld) -> None:
        world.redeem_error = RuntimeError("boom")

        await events.channel_points_custom_reward_redemption_add(self.event())

        assert world.reported == ["Error processing Twitch redemption webhook task"]


class TestRaid:
    async def test_an_outgoing_raid_is_announced_with_a_link_to_the_target(
        self, world: EventWorld
    ) -> None:
        await events.channel_raid(raid(from_id="111", to_id="6", to_login="raider"))

        assert world.templates == [
            (
                "111",
                "twitch_raid_out",
                {"name": "Raider", "url": "https://www.twitch.tv/raider"},
            )
        ]

    async def test_an_outgoing_raid_to_a_name_that_cannot_be_a_channel_is_not_built_into_a_url(
        self, world: EventWorld
    ) -> None:
        await events.channel_raid(raid(from_id="111", to_id="6", to_login="bad login!"))

        assert world.templates == []
        ((text, key),) = world.notified
        assert "Said nothing about an outgoing raid" in text
        assert key == "raid-out-bad-login"

    async def test_an_incoming_raid_gets_a_bang_so_line_and_spends_the_raiders_autoshoutout(
        self, world: EventWorld
    ) -> None:
        await events.channel_raid(raid(from_id="6", from_login="raider"))

        assert world.said == [("111", "!so raider", "the incoming raid shoutout")]
        assert world.calls == [("raided", "6")]

    async def test_a_line_twitch_refused_leaves_the_raider_owed_their_autoshoutout(
        self, world: EventWorld
    ) -> None:
        """It is the `!so` that gives them one, so a line that never landed must not
        spend it."""
        world.say_result = False

        await events.channel_raid(raid(from_login="raider"))

        assert world.calls == []

    async def test_a_raider_whose_name_cannot_be_a_channel_gets_no_command_built_from_it(
        self, world: EventWorld
    ) -> None:
        """The line is posted to chat and returns through the webhook to be dispatched."""
        await events.channel_raid(raid(from_login="x y"))

        assert world.said == []
        assert world.calls == []
        ((text, key),) = world.notified
        assert "Refused to shout out an incoming raid" in text
        assert key == "raid-bad-login"

    async def test_a_raid_between_two_other_channels_is_not_ours_to_answer(
        self, world: EventWorld
    ) -> None:
        await events.channel_raid(raid(from_id="6", to_id="7"))

        assert world.said == [] and world.templates == [] and world.calls == []

    async def test_a_failure_is_reported_not_raised(
        self, world: EventWorld, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        async def boom(*args: Any) -> bool:
            raise RuntimeError("boom")

        monkeypatch.setattr(events, "say", boom)

        await events.channel_raid(raid())

        assert world.reported == ["Error processing Twitch raid webhook task"]


class TestModerate:
    def event(
        self, action: str, broadcaster_id: str = "111"
    ) -> ChannelModerateEventSub:
        return ChannelModerateEventSub.model_validate(
            {
                "subscription": {"type": "channel.moderate"},
                "event": {"broadcaster_user_id": broadcaster_id, "action": action},
            }
        )

    async def test_starting_a_raid_says_the_farewell(self, world: EventWorld) -> None:
        await events.channel_moderate(self.event("raid"))

        assert world.templates == [("111", "twitch_raid_farewell", {})]

    @pytest.mark.parametrize("action", ["ban", "timeout", "unraid", "RAID", ""])
    async def test_no_other_action_says_anything(
        self, action: str, world: EventWorld
    ) -> None:
        await events.channel_moderate(self.event(action))

        assert world.templates == []

    async def test_another_channels_raid_is_ignored(self, world: EventWorld) -> None:
        await events.channel_moderate(self.event("raid", broadcaster_id="222"))

        assert world.templates == []

    async def test_a_failure_is_reported_not_raised(self, world: EventWorld) -> None:
        world.template_error = RuntimeError("boom")

        await events.channel_moderate(self.event("raid"))

        assert world.reported == ["Error processing Twitch moderate webhook task"]
