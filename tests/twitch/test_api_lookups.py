from collections.abc import Callable

import pytest

import valmal.twitch.client.api as api
from tests.twitch.support import (
    Script,
    page,
    reply,
    stream_json,
    subscription_json,
    user_json,
    video_json,
)
from valmal.core.settings import settings
from valmal.twitch.models.api.stream import Stream, StreamType
from valmal.twitch.models.api.subscription import Subscription

pytestmark = pytest.mark.anyio

HttpFactory = Callable[..., Script]


def _subscription(**overrides: object) -> Subscription:
    return Subscription.model_validate(subscription_json(**overrides))  # pyright: ignore[reportArgumentType]


class TestCallbackUrl:
    def test_joins_the_app_url_and_the_path(self) -> None:
        assert (
            api.callback_url("/webhook/twitch") == "https://bot.example/webhook/twitch"
        )

    @pytest.mark.parametrize(
        "app_url", ["https://bot.example/", "https://bot.example//"]
    )
    def test_a_trailing_slash_in_app_url_cannot_make_a_double_slash_path(
        self, app_url: str, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Twitch would dutifully call //webhook/twitch, and FastAPI does not route it."""
        monkeypatch.setattr(settings, "app_url", app_url)

        assert (
            api.callback_url("/webhook/twitch") == "https://bot.example/webhook/twitch"
        )

    def test_the_prefix_is_the_webhook_root(self) -> None:
        assert api.callback_prefix() == "https://bot.example/webhook/twitch"

    def test_online_and_offline_callbacks_differ_only_in_the_suffix(self) -> None:
        assert api._callback_url("online") == "https://bot.example/webhook/twitch"
        assert (
            api._callback_url("offline") == "https://bot.example/webhook/twitch/offline"
        )


class TestLiveStream:
    def test_none_stays_none(self) -> None:
        assert api.live_stream(None) is None

    def test_a_live_stream_is_returned(self) -> None:
        stream = Stream.model_validate(stream_json())

        assert api.live_stream(stream) is stream

    def test_an_error_typed_stream_is_refused_once_here(self) -> None:
        stream = Stream.model_validate(stream_json(type=""))

        assert stream.type is StreamType.error
        assert api.live_stream(stream) is None


class TestSubscriptionTarget:
    def test_names_whichever_id_identifies_it(self) -> None:
        assert api.subscription_target(_subscription(broadcaster_user_id="7")) == (
            "broadcaster 7"
        )
        assert api.subscription_target(_subscription(to_broadcaster_user_id="8")) == (
            "to broadcaster 8"
        )
        assert api.subscription_target(_subscription(from_broadcaster_user_id="9")) == (
            "from broadcaster 9"
        )
        assert api.subscription_target(_subscription(user_id="6")) == "user 6"

    def test_the_broadcaster_wins_when_several_are_set(self) -> None:
        sub = _subscription(broadcaster_user_id="7", user_id="6", moderator_user_id="5")

        assert api.subscription_target(sub) == "broadcaster 7"

    def test_an_empty_string_is_not_a_target(self) -> None:
        """Twitch sends "" for the unset half of a channel.raid condition."""
        sub = _subscription(from_broadcaster_user_id="", to_broadcaster_user_id="8")

        assert api.subscription_target(sub) == "to broadcaster 8"

    def test_with_no_recognised_id_it_falls_back_to_the_subscription_id(self) -> None:
        sub = _subscription(id="abc", moderator_user_id="5")

        assert api.subscription_target(sub) == "id abc"


class TestUndeliverable:
    def test_enabled_at_this_deployments_callback_is_deliverable(self) -> None:
        assert api.undeliverable(_subscription()) is None

    @pytest.mark.parametrize(
        "path", ["/webhook/twitch", "/webhook/twitch/offline", "/webhook/twitch/x/y"]
    )
    def test_the_prefix_and_anything_under_it_is_ours(self, path: str) -> None:
        assert (
            api.undeliverable(_subscription(callback=f"https://bot.example{path}"))
            is None
        )

    @pytest.mark.parametrize(
        "status",
        [
            "webhook_callback_verification_failed",
            "notification_failures_exceeded",
            "authorization_revoked",
            "user_removed",
        ],
    )
    def test_a_status_other_than_enabled_is_the_reason(self, status: str) -> None:
        assert api.undeliverable(_subscription(status=status)) == status

    def test_a_status_problem_is_reported_before_a_callback_problem(self) -> None:
        sub = _subscription(status="user_removed", callback="https://elsewhere/x")

        assert api.undeliverable(sub) == "user_removed"

    @pytest.mark.parametrize(
        "callback",
        [
            "https://old.example/webhook/twitch",
            "https://bot.example/webhook/twitching",
            "https://bot.example/webhook/twitch-old",
            "https://bot.example/webhook",
            "https://bot.example.evil/webhook/twitch",
            "http://bot.example/webhook/twitch",
            "webhook/twitch",
        ],
    )
    def test_a_callback_not_beginning_at_our_path_boundary_is_named(
        self, callback: str
    ) -> None:
        """A bare startswith would accept /webhook/twitching, a different path."""
        assert api.undeliverable(_subscription(callback=callback)) == (
            f"calling back on {callback}"
        )

    def test_no_callback_at_all_is_named(self) -> None:
        assert (
            api.undeliverable(_subscription(callback=None)) == "calling back on nothing"
        )

    def test_an_empty_callback_is_named_too(self) -> None:
        assert (
            api.undeliverable(_subscription(callback="")) == "calling back on nothing"
        )


class TestSingleLookups:
    async def test_get_user_by_id(self, helix_http: HttpFactory) -> None:
        script = helix_http(reply(200, {"data": [user_json("42", "alice")]}))

        user = await api.get_user(42)

        assert user is not None and user.login == "alice"
        assert script.requests[0].url.query == b"id=42"

    async def test_get_user_by_username(self, helix_http: HttpFactory) -> None:
        script = helix_http(reply(200, {"data": [user_json("42", "alice")]}))

        user = await api.get_user_by_username("alice")

        assert user is not None and user.id == "42"
        assert script.requests[0].url.query == b"login=alice"

    async def test_an_unknown_user_is_none_not_an_error(
        self, helix_http: HttpFactory
    ) -> None:
        helix_http(reply(200, {"data": []}))

        assert await api.get_user(1) is None

    async def test_a_lookup_that_fails_raises_rather_than_reading_as_unknown(
        self, helix_http: HttpFactory
    ) -> None:
        helix_http(reply(500))

        with pytest.raises(api.HelixError):
            await api.get_user(1)

    async def test_get_stream_is_none_when_offline_and_asks_by_user(
        self, helix_http: HttpFactory
    ) -> None:
        script = helix_http(reply(200, {"data": [], "pagination": {}}))

        assert await api.get_stream(7) is None
        assert script.requests[0].url.query == b"user_id=7"

    async def test_get_stream_returns_the_stream(self, helix_http: HttpFactory) -> None:
        helix_http(reply(200, {"data": [stream_json("99")], "pagination": {}}))

        stream = await api.get_stream(1)

        assert stream is not None and stream.id == "99"

    async def test_get_channel(self, helix_http: HttpFactory) -> None:
        helix_http(
            reply(
                200,
                {
                    "data": [
                        {
                            "broadcaster_id": "1",
                            "broadcaster_login": "bob",
                            "broadcaster_name": "Bob",
                            "broadcaster_language": "en",
                            "game_name": "G",
                            "game_id": "g",
                            "title": "t",
                            "delay": 0,
                            "tags": [],
                            "content_classification_labels": [],
                            "is_branded_content": False,
                        }
                    ]
                },
            )
        )

        channel = await api.get_channel(1)

        assert channel is not None and channel.broadcaster_login == "bob"

    async def test_the_ad_schedule_uses_the_broadcasters_token(
        self, helix_http: HttpFactory
    ) -> None:
        script = helix_http(
            reply(
                200,
                {
                    "data": [
                        {
                            "snooze_count": 1,
                            "snooze_refresh_at": 2,
                            "next_ad_at": 3,
                            "duration": 60,
                            "last_ad_at": 4,
                            "preroll_free_time": 5,
                        }
                    ]
                },
            )
        )

        schedule = await api.get_ad_schedule(1)

        assert schedule is not None and schedule.duration == 60
        assert script.requests[0].headers["Authorization"] == "Bearer broadcaster-token"

    async def test_the_vod_is_the_archive_of_that_stream(
        self, helix_http: HttpFactory
    ) -> None:
        script = helix_http(
            reply(
                200,
                page(
                    [
                        video_json("v1", stream_id="11"),
                        video_json("v2", stream_id="10"),
                        video_json("v3", stream_id=None),
                    ]
                ),
            )
        )

        vod = await api.get_stream_vod(1, 10)

        assert vod is not None and vod.id == "v2"
        assert b"type=archive" in script.requests[0].url.query

    async def test_no_vod_for_that_stream_is_none(
        self, helix_http: HttpFactory
    ) -> None:
        helix_http(reply(200, page([video_json("v1", stream_id="11")])))

        assert await api.get_stream_vod(1, 10) is None
