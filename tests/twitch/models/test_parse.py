"""Every Twitch body is parsed here, and lax enough for what Twitch's docs show."""

import msgspec
import pytest

from valmal.twitch.models import parse
from valmal.twitch.models.auth import AuthResponse
from valmal.twitch.models.eventsub.channel_ad_break_begin import (
    ChannelAdBreakBeginEventSub,
)
from valmal.twitch.models.eventsub.channel_raid import ChannelRaidEventSub

NAMES = {"user_id": "2", "user_login": "req", "user_name": "Req"}


def ad_break(duration: object, automatic: object) -> dict[str, object]:
    """Shaped like Twitch's documented example, which sends both as strings."""
    return {
        "subscription": {"type": "channel.ad_break.begin"},
        "event": {
            "duration_seconds": duration,
            "started_at": "2026-01-01T00:00:00Z",
            "is_automatic": automatic,
            **{f"broadcaster_{k}": v for k, v in NAMES.items()},
            **{f"requester_{k}": v for k, v in NAMES.items()},
        },
    }


class TestConvert:
    def test_a_number_and_a_bool_sent_as_strings_parse(self) -> None:
        event = parse.convert(ad_break("60", "false"), ChannelAdBreakBeginEventSub)

        assert (event.event.duration_seconds, event.event.is_automatic) == (60, False)

    def test_the_same_payload_with_real_types_parses_alike(self) -> None:
        event = parse.convert(ad_break(60, False), ChannelAdBreakBeginEventSub)

        assert (event.event.duration_seconds, event.event.is_automatic) == (60, False)

    def test_a_string_that_is_not_a_number_is_still_refused(self) -> None:
        with pytest.raises(msgspec.ValidationError, match="duration_seconds"):
            parse.convert(ad_break("sixty", False), ChannelAdBreakBeginEventSub)

    def test_a_raid_with_its_viewers_as_a_string(self) -> None:
        payload = {
            "subscription": {"type": "channel.raid"},
            "event": {
                **{f"from_broadcaster_{k}": v for k, v in NAMES.items()},
                **{f"to_broadcaster_{k}": v for k, v in NAMES.items()},
                "viewers": "42",
            },
        }

        assert parse.convert(payload, ChannelRaidEventSub).event.viewers == 42


class TestDecode:
    def test_a_reply_with_a_number_sent_as_a_string(self) -> None:
        text = '{"access_token": "a", "expires_in": "3600", "token_type": "bearer"}'

        assert parse.decode(text, AuthResponse).expires_in == 3600

    def test_a_body_that_is_not_json_raises(self) -> None:
        with pytest.raises(msgspec.DecodeError):
            parse.decode("not json", AuthResponse)
