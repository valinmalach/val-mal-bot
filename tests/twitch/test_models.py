"""The shapes Twitch sends, parsed exactly as the webhook and Helix layers parse them."""

from typing import Any

import pytest
from pydantic import BaseModel, ValidationError

from tests.twitch.support import stream_json, user_json, video_json
from valmal.twitch.models.api.pagination import Pagination
from valmal.twitch.models.api.stream import Stream, StreamType
from valmal.twitch.models.api.subscription import (
    Subscription,
    SubscriptionCondition,
    SubscriptionResponse,
)
from valmal.twitch.models.api.user import BroadcasterType, User, UserType
from valmal.twitch.models.api.video import Video, VideoType
from valmal.twitch.models.auth import (
    AuthResponse,
    RefreshResponse,
    TokenValidationResponse,
)
from valmal.twitch.models.eventsub.channel_ad_break_begin import (
    ChannelAdBreakBeginEventSub,
)
from valmal.twitch.models.eventsub.channel_chat_message import (
    ChannelChatMessageEventSub,
)
from valmal.twitch.models.eventsub.channel_follow import ChannelFollowEventSub
from valmal.twitch.models.eventsub.channel_moderate import ChannelModerateEventSub
from valmal.twitch.models.eventsub.channel_points_custom_reward_redemption_add import (
    ChannelPointsCustomRewardRedemptionAddEventSub,
)
from valmal.twitch.models.eventsub.channel_raid import ChannelRaidEventSub
from valmal.twitch.models.eventsub.stream_offline import StreamOfflineEventSub
from valmal.twitch.models.eventsub.stream_online import StreamOnlineEventSub

BROADCASTER = {
    "broadcaster_user_id": "1",
    "broadcaster_user_login": "bob",
    "broadcaster_user_name": "Bob",
}

# The documented `type` string each subscription model is bound to, and a payload
# for it. The Literal is the whole reason a model exists: a payload for the wrong
# event fails to parse instead of being compared against a string beside it.
EVENTS: dict[str, tuple[type[BaseModel], dict[str, Any]]] = {
    "channel.ad_break.begin": (
        ChannelAdBreakBeginEventSub,
        {
            "duration_seconds": 60,
            "started_at": "2026-01-01T00:00:00Z",
            "is_automatic": True,
            **BROADCASTER,
            "requester_user_id": "2",
            "requester_user_login": "req",
            "requester_user_name": "Req",
        },
    ),
    "channel.chat.message": (
        ChannelChatMessageEventSub,
        {
            **BROADCASTER,
            "chatter_user_id": "3",
            "chatter_user_login": "chatter",
            "chatter_user_name": "Chatter",
            "message": {"text": "hello", "fragments": []},
            "badges": [{"set_id": "moderator", "id": "1", "info": ""}],
        },
    ),
    "channel.follow": (
        ChannelFollowEventSub,
        {
            "user_id": "4",
            "user_login": "fan",
            "user_name": "Fan",
            **BROADCASTER,
            "followed_at": "2026-01-01T00:00:00Z",
        },
    ),
    "channel.moderate": (
        ChannelModerateEventSub,
        {"broadcaster_user_id": "1", "action": "ban"},
    ),
    "channel.channel_points_custom_reward_redemption.add": (
        ChannelPointsCustomRewardRedemptionAddEventSub,
        {"broadcaster_user_id": "1", "user_id": "5", "user_login": "redeemer"},
    ),
    "channel.raid": (
        ChannelRaidEventSub,
        {
            "from_broadcaster_user_id": "6",
            "from_broadcaster_user_login": "raider",
            "from_broadcaster_user_name": "Raider",
            "to_broadcaster_user_id": "1",
            "to_broadcaster_user_login": "bob",
            "to_broadcaster_user_name": "Bob",
            "viewers": 42,
        },
    ),
    "stream.offline": (StreamOfflineEventSub, dict(BROADCASTER)),
    "stream.online": (
        StreamOnlineEventSub,
        {
            "id": "10",
            **BROADCASTER,
            "type": "live",
            "started_at": "2026-01-01T00:00:00Z",
        },
    ),
}


def _payload(sub_type: str) -> dict[str, Any]:
    return {"subscription": {"type": sub_type}, "event": dict(EVENTS[sub_type][1])}


@pytest.mark.parametrize("sub_type", EVENTS)
class TestEventSubs:
    def test_a_documented_payload_parses(self, sub_type: str) -> None:
        model = EVENTS[sub_type][0]

        parsed = model.model_validate(_payload(sub_type))

        assert parsed.subscription.type == sub_type  # pyright: ignore[reportAttributeAccessIssue]

    def test_a_payload_for_another_event_is_refused(self, sub_type: str) -> None:
        payload = _payload(sub_type)
        payload["subscription"]["type"] = "some.other.event"

        with pytest.raises(ValidationError, match="type"):
            EVENTS[sub_type][0].model_validate(payload)

    def test_every_field_the_handler_reads_is_required(self, sub_type: str) -> None:
        model = EVENTS[sub_type][0]
        for field in EVENTS[sub_type][1]:
            payload = _payload(sub_type)
            del payload["event"][field]

            with pytest.raises(ValidationError) as caught:
                model.model_validate(payload)

            assert field in str(caught.value), field

    def test_what_is_not_modelled_is_ignored_not_an_error(self, sub_type: str) -> None:
        """Twitch adds fields; a 400 for one spends the subscription's failure budget."""
        payload = _payload(sub_type)
        payload["event"]["a_field_twitch_added_later"] = {"x": 1}
        payload["subscription"]["id"] = "abc"
        payload["subscription"]["status"] = "enabled"

        EVENTS[sub_type][0].model_validate(payload)

    def test_the_event_and_the_subscription_are_both_required(
        self, sub_type: str
    ) -> None:
        model = EVENTS[sub_type][0]

        for missing in ("event", "subscription"):
            payload = _payload(sub_type)
            del payload[missing]
            with pytest.raises(ValidationError):
                model.model_validate(payload)


class TestChatMessage:
    def test_a_line_relayed_from_another_channel_carries_its_source(self) -> None:
        payload = _payload("channel.chat.message")
        payload["event"]["source_broadcaster_user_id"] = "77"

        parsed = ChannelChatMessageEventSub.model_validate(payload)

        assert parsed.event.source_broadcaster_user_id == "77"

    def test_an_ordinary_line_has_no_source_rather_than_the_broadcaster(self) -> None:
        parsed = ChannelChatMessageEventSub.model_validate(
            _payload("channel.chat.message")
        )

        assert parsed.event.source_broadcaster_user_id is None

    def test_badges_may_be_empty_but_must_be_present(self) -> None:
        payload = _payload("channel.chat.message")
        payload["event"]["badges"] = []
        assert ChannelChatMessageEventSub.model_validate(payload).event.badges == []

        del payload["event"]["badges"]
        with pytest.raises(ValidationError):
            ChannelChatMessageEventSub.model_validate(payload)

    def test_the_login_and_display_name_are_separate_fields(self) -> None:
        """For some locales the display name is a different string entirely."""
        payload = _payload("channel.chat.message")
        payload["event"]["chatter_user_name"] = "チャッター"

        event = ChannelChatMessageEventSub.model_validate(payload).event

        assert event.chatter_user_login == "chatter"
        assert event.chatter_user_name == "チャッター"

    def test_moderate_takes_any_action_string_twitch_adds(self) -> None:
        payload = _payload("channel.moderate")
        payload["event"]["action"] = "an_action_added_in_2027"

        assert ChannelModerateEventSub.model_validate(payload).event.action == (
            "an_action_added_in_2027"
        )

    def test_the_stream_online_event_type_is_free_text_too(self) -> None:
        payload = _payload("stream.online")
        payload["event"]["type"] = "a_new_kind"

        assert StreamOnlineEventSub.model_validate(payload).event.type == "a_new_kind"


class TestHelixResponses:
    def test_a_user(self) -> None:
        user = User.model_validate(
            user_json("1", "bob", type="staff", broadcaster_type="partner")
        )

        assert user.type is UserType.staff
        assert user.broadcaster_type is BroadcasterType.partner

    @pytest.mark.parametrize("type_", ["", "admin", "global_mod", "staff"])
    def test_every_documented_user_type_parses(self, type_: str) -> None:
        assert User.model_validate(user_json(type=type_)).type.value == type_

    @pytest.mark.parametrize("kind", ["", "affiliate", "partner"])
    def test_every_documented_broadcaster_type_parses(self, kind: str) -> None:
        assert (
            User.model_validate(user_json(broadcaster_type=kind)).broadcaster_type.value
            == kind
        )

    def test_a_live_and_an_errored_stream(self) -> None:
        assert Stream.model_validate(stream_json()).type is StreamType.live
        assert Stream.model_validate(stream_json(type="")).type is StreamType.error

    def test_tags_may_be_null(self) -> None:
        assert Stream.model_validate(stream_json(tags=None)).tags is None

    def test_a_stream_of_an_unknown_type_is_refused(self) -> None:
        with pytest.raises(ValidationError):
            Stream.model_validate(stream_json(type="rerun"))

    def test_a_video_without_a_stream_or_muted_segments(self) -> None:
        video = Video.model_validate(video_json(stream_id=None))

        assert video.stream_id is None
        assert video.muted_segments is None
        assert video.type is VideoType.archive

    def test_a_video_with_muted_segments(self) -> None:
        video = Video.model_validate(
            video_json(muted_segments=[{"duration": 30, "offset": 5}])
        )

        assert video.muted_segments is not None and video.muted_segments[0].offset == 5

    def test_pagination_is_absent_on_the_last_page(self) -> None:
        assert Pagination.model_validate({}).cursor is None
        assert Pagination.model_validate({"cursor": "abc"}).cursor == "abc"


class TestSubscriptions:
    def test_a_condition_keeps_a_key_it_does_not_declare(self) -> None:
        """A condition is round-tripped to recreate a subscription: a dropped key
        would recreate one broader than the one it replaced."""
        condition = SubscriptionCondition.model_validate(
            {"broadcaster_user_id": "1", "reward_id": "abc"}
        )

        assert condition.model_dump(exclude_none=True) == {
            "broadcaster_user_id": "1",
            "reward_id": "abc",
        }

    def test_a_condition_may_be_empty(self) -> None:
        assert (
            SubscriptionCondition.model_validate({}).model_dump(exclude_none=True) == {}
        )

    def test_a_subscription_response_carries_its_totals(self) -> None:
        response = SubscriptionResponse.model_validate(
            {
                "data": [],
                "total": 0,
                "total_cost": 0,
                "max_total_cost": 10,
                "pagination": {},
            }
        )

        assert response.max_total_cost == 10
        assert response.pagination.cursor is None

    def test_a_subscription_needs_its_transport(self) -> None:
        with pytest.raises(ValidationError):
            Subscription.model_validate(
                {
                    "id": "1",
                    "status": "enabled",
                    "type": "stream.online",
                    "version": "1",
                    "condition": {},
                    "created_at": "x",
                    "cost": 1,
                }
            )


class TestAuthResponses:
    def test_an_app_token(self) -> None:
        parsed = AuthResponse.model_validate(
            {"access_token": "a", "expires_in": 3600, "token_type": "bearer"}
        )

        assert parsed.expires_in == 3600

    def test_a_refresh_with_a_scope_list_or_a_single_string(self) -> None:
        base = {
            "access_token": "a",
            "expires_in": 1,
            "refresh_token": "r",
            "token_type": "bearer",
        }

        assert RefreshResponse.model_validate({**base, "scope": ["a", "b"]}).scope == [
            "a",
            "b",
        ]
        assert RefreshResponse.model_validate({**base, "scope": "a"}).scope == "a"

    def test_a_refresh_may_answer_with_no_expiry_but_must_say_so(self) -> None:
        base = {
            "access_token": "a",
            "refresh_token": "r",
            "scope": [],
            "token_type": "bearer",
        }

        assert (
            RefreshResponse.model_validate({**base, "expires_in": None}).expires_in
            is None
        )
        with pytest.raises(ValidationError):
            RefreshResponse.model_validate(base)

    def test_a_token_validation_reply(self) -> None:
        parsed = TokenValidationResponse.model_validate(
            {
                "client_id": "c",
                "login": "bob",
                "scopes": ["chat:read"],
                "user_id": "1",
                "expires_in": 10,
            }
        )

        assert parsed.scopes == ["chat:read"]
