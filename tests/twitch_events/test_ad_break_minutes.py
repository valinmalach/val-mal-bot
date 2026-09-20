import pytest

from models.twitch_event_subs.channel_ad_break_begin import ChannelAdBreakBeginEventSub
from services.twitch import events
from tests.twitch_events.support import EventWorld

pytestmark = pytest.mark.anyio


def ad_break(seconds: int) -> ChannelAdBreakBeginEventSub:
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


@pytest.mark.parametrize(
    ("seconds", "minutes"),
    [(30, "0.5"), (60, "1"), (90, "1.5"), (120, "2"), (150, "2.5"), (180, "3")],
)
async def test_the_length_is_said_in_real_minutes_not_truncated_ones(
    seconds: int, minutes: str, world: EventWorld
) -> None:
    """Twitch's ad breaks are multiples of thirty seconds; a truncating `// 60` made
    30s read as "0 minute" and 90s as "1 minute"."""
    await events.channel_ad_break_begin(ad_break(seconds))

    assert world.templates[0][2] == {"minutes": minutes}
