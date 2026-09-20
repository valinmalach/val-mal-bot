import asyncio
from datetime import UTC, datetime

import pendulum
import pytest

from services.twitch import live_alert
from tests.live_alert.support import AlertWorld, alert, stream, user

pytestmark = pytest.mark.anyio


@pytest.fixture(autouse=True)
def _record_starts(alert_world: AlertWorld, monkeypatch: pytest.MonkeyPatch) -> None:
    """Announcing, waking and restoring all start updaters; here that is only recorded."""
    monkeypatch.setattr(live_alert, "_start", alert_world.start)


class TestAnnounce:
    async def test_posts_the_alert_stores_it_and_starts_its_updater(
        self, alert_world: AlertWorld
    ) -> None:
        await live_alert.announce(
            111, stream(id="10", user_login="valinmalach"), user(), 5001
        )

        (sent,) = alert_world.sent
        assert sent["channel_id"] == 5001
        assert sent["content"] == "<@&777>"
        assert sent["embed"].description.endswith("(https://www.twitch.tv/valinmalach)")
        (button,) = sent["view"].children
        assert button.url == "https://www.twitch.tv/valinmalach"
        assert alert_world.stored == [
            (111, 5001, 900, 10, pendulum.datetime(2026, 1, 1))
        ]
        assert alert_world.started == [(111, 5001, 900, 10, "2026-01-01T00:00:00Z")]

    async def test_only_the_alerts_channel_pings_the_role(
        self, alert_world: AlertWorld
    ) -> None:
        await live_alert.announce(111, stream(), None, 5002)

        assert alert_world.sent[0]["content"] is None

    async def test_a_login_that_cannot_be_linked_is_reported_and_nothing_is_posted(
        self, alert_world: AlertWorld
    ) -> None:
        await live_alert.announce(111, stream(user_login="a)b"), None, 5001)

        assert alert_world.sent == []
        assert alert_world.reported == [
            "Failed to build the live alert URL for broadcaster 111"
        ]
        assert alert_world.started == []

    async def test_a_message_that_could_not_be_sent_is_said_and_stores_nothing(
        self, alert_world: AlertWorld
    ) -> None:
        alert_world.message_id = None

        await live_alert.announce(111, stream(), None, 5001)

        ((text, key),) = alert_world.notified
        assert "Failed to send live alert message" in text
        assert "broadcaster_id: 111" in text and "channel_id: 5001" in text
        assert key == "live-alert-send:111"
        assert alert_world.stored == []
        assert alert_world.started == []

    async def test_a_failure_storing_the_row_is_reported_and_no_updater_starts(
        self, alert_world: AlertWorld
    ) -> None:
        alert_world.store_error = ConnectionError("db down")

        await live_alert.announce(111, stream(), None, 5001)

        assert alert_world.reported == [
            "Failed to store live alert for broadcaster 111"
        ]
        assert alert_world.started == []

    async def test_a_stream_id_that_is_not_a_number_is_reported_not_raised(
        self, alert_world: AlertWorld
    ) -> None:
        await live_alert.announce(111, stream(id="not-a-number"), None, 5001)

        assert alert_world.reported == [
            "Failed to store live alert for broadcaster 111"
        ]


class TestStoredStart:
    def test_a_naive_datetime_is_read_as_utc_because_that_is_what_the_column_means(
        self,
    ) -> None:
        """parse_rfc3339 will not accept one without a zone."""
        stored = alert()
        stored.stream_started_at = datetime(2026, 6, 15, 11, 0)

        assert live_alert._stored_start(stored) == "2026-06-15T11:00:00+00:00"

    def test_an_aware_one_keeps_its_instant(self) -> None:
        stored = alert()
        stored.stream_started_at = datetime(2026, 6, 15, 11, 0, tzinfo=UTC)

        assert live_alert._stored_start(stored) == "2026-06-15T11:00:00+00:00"

    def test_what_it_returns_is_something_parse_rfc3339_accepts(self) -> None:
        from services.twitch.timestamps import parse_rfc3339

        assert parse_rfc3339(live_alert._stored_start(alert())) == pendulum.datetime(
            2026, 6, 15, 11
        )


class TestWake:
    async def test_with_no_row_there_is_nothing_to_do(
        self, alert_world: AlertWorld
    ) -> None:
        alert_world.row = None

        await live_alert.wake(111)

        assert alert_world.started == []

    async def test_starts_the_updater_for_the_row_and_signals_it(
        self, alert_world: AlertWorld
    ) -> None:
        """Starting is a no-op when one is running, so this also repairs a row whose
        updater died or gave up."""
        alert_world.row = alert(message_id=900, stream_id=10)
        wakeup = asyncio.Event()
        live_alert._wakeups[900] = wakeup

        await live_alert.wake(111)

        assert alert_world.started == [
            (111, 5001, 900, 10, "2026-06-15T11:00:00+00:00")
        ]
        assert wakeup.is_set()

    async def test_a_row_with_no_wakeup_yet_is_not_an_error(
        self, alert_world: AlertWorld
    ) -> None:
        alert_world.row = alert()

        await live_alert.wake(111)

        assert len(alert_world.started) == 1

    async def test_never_raises_because_the_session_is_woken_on_the_same_webhook(
        self, alert_world: AlertWorld
    ) -> None:
        """An unreachable database must not also stop a session standing down."""
        alert_world.read_error = ConnectionError("db down")

        await live_alert.wake(111)

        assert alert_world.reported == [
            "Could not wake the live alert for broadcaster 111"
        ]


class TestRestoreAll:
    async def test_starts_an_updater_for_every_stored_alert(
        self, alert_world: AlertWorld
    ) -> None:
        alert_world.rows = [
            alert(broadcaster_id=1, message_id=900, stream_id=10),
            alert(broadcaster_id=2, message_id=901, stream_id=11),
        ]

        await live_alert.restore_all()

        assert [(a[0], a[2], a[3]) for a in alert_world.started] == [
            (1, 900, 10),
            (2, 901, 11),
        ]

    async def test_staggers_them_a_second_apart_to_spread_the_helix_calls(
        self, alert_world: AlertWorld
    ) -> None:
        alert_world.rows = [alert(message_id=i) for i in (900, 901, 902)]

        await live_alert.restore_all()

        assert alert_world.slept == [1, 1, 1]

    async def test_with_nothing_stored_nothing_starts(
        self, alert_world: AlertWorld
    ) -> None:
        await live_alert.restore_all()

        assert alert_world.started == []
        assert alert_world.slept == []
