from collections.abc import AsyncGenerator
from typing import Any, Literal

import httpx
import pytest
from fastapi import APIRouter, FastAPI
from pydantic import BaseModel

import controller.twitch as ctl
import services.twitch.api as api
from services.twitch.api import callback_url, undeliverable
from tests.twitch.support import subscription_json
from tests.webhook.route_data import ROUTES, notification
from tests.webhook.support import Hooks, delivery
from valmal.twitch.models.api.subscription import Subscription
from valmal.twitch.models.eventsub.stream_online import StreamOnlineEventSub

pytestmark = pytest.mark.anyio


@pytest.fixture
async def real(hooks: Hooks, unawaited: None) -> AsyncGenerator[httpx.AsyncClient]:
    """The real router, with dispatch recorded so no real handler runs."""
    app = FastAPI()
    app.include_router(ctl.twitch_router)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as client:
        yield client


class TestWebhookPaths:
    def test_every_eventsub_type_is_routed_to_its_own_path(self) -> None:
        expected = {t: path for t, (path, _) in ROUTES.items()}

        assert expected == ctl.WEBHOOK_PATHS

    def test_there_are_eight_types_and_no_two_share_a_path(self) -> None:
        assert len(ctl.WEBHOOK_PATHS) == 8
        assert len(set(ctl.WEBHOOK_PATHS.values())) == 8

    def test_every_path_is_the_webhook_root_or_below_it_at_a_path_boundary(
        self,
    ) -> None:
        """undeliverable() treats anything not at this boundary as calling back
        somewhere else, so a route outside it would be reported broken forever."""
        for path in ctl.WEBHOOK_PATHS.values():
            assert path == "/webhook/twitch" or path.startswith("/webhook/twitch/"), (
                path
            )

    def test_the_router_serves_exactly_the_mapped_paths(self) -> None:
        served = {route.path for route in ctl.twitch_router.routes}  # pyright: ignore[reportAttributeAccessIssue]

        assert served == set(ctl.WEBHOOK_PATHS.values())

    def test_each_route_is_named_for_its_path_so_operation_ids_stay_distinct(
        self,
    ) -> None:
        names = [route.name for route in ctl.twitch_router.routes]  # pyright: ignore[reportAttributeAccessIssue]

        assert sorted(names) == sorted(ctl.WEBHOOK_PATHS.values())

    def test_the_stream_routes_are_where_api_subscribes_them(self) -> None:
        """/subscribe creates stream.online and stream.offline at these callbacks, so a
        route moved without touching api.py would receive nothing."""
        assert callback_url(ctl.WEBHOOK_PATHS["stream.online"]) == api._callback_url(
            "online"
        )
        assert callback_url(ctl.WEBHOOK_PATHS["stream.offline"]) == api._callback_url(
            "offline"
        )

    @pytest.mark.parametrize("sub_type", list(ROUTES))
    def test_a_subscription_at_any_route_reads_as_deliverable(
        self, sub_type: str
    ) -> None:
        subscription = Subscription.model_validate(
            subscription_json(
                type=sub_type, callback=callback_url(ctl.WEBHOOK_PATHS[sub_type])
            )
        )

        assert undeliverable(subscription) is None


@pytest.mark.parametrize(("sub_type", "route"), list(ROUTES.items()))
class TestEachRoute:
    async def test_a_signed_notification_is_accepted_and_reaches_its_own_handler(
        self,
        sub_type: str,
        route: tuple[str, str],
        real: httpx.AsyncClient,
        hooks: Hooks,
    ) -> None:
        path, handler = route
        raw, headers = delivery(notification(sub_type))

        response = await real.post(path, content=raw, headers=headers)

        assert response.status_code == 202
        ((name, coro),) = hooks.dispatched
        assert name == path
        assert coro.cr_code.co_name == handler  # pyright: ignore[reportAttributeAccessIssue]

    async def test_a_payload_for_another_event_is_refused(
        self,
        sub_type: str,
        route: tuple[str, str],
        real: httpx.AsyncClient,
        hooks: Hooks,
    ) -> None:
        other = "stream.offline" if sub_type != "stream.offline" else "stream.online"
        raw, headers = delivery(notification(other))

        response = await real.post(route[0], content=raw, headers=headers)

        assert response.status_code == 400
        assert hooks.dispatched == []

    async def test_an_unsigned_request_is_refused(
        self,
        sub_type: str,
        route: tuple[str, str],
        real: httpx.AsyncClient,
        hooks: Hooks,
    ) -> None:
        raw, headers = delivery(notification(sub_type), signature="sha256=" + "0" * 64)

        response = await real.post(route[0], content=raw, headers=headers)

        assert response.status_code == 403
        assert hooks.dispatched == []

    async def test_the_handshake_is_answered(
        self, sub_type: str, route: tuple[str, str], real: httpx.AsyncClient
    ) -> None:
        raw, headers = delivery(
            {"challenge": "abc"}, message_type="webhook_callback_verification"
        )

        response = await real.post(route[0], content=raw, headers=headers)

        assert (response.status_code, response.text) == (200, "abc")

    async def test_only_post_is_allowed(
        self, sub_type: str, route: tuple[str, str], real: httpx.AsyncClient
    ) -> None:
        assert (await real.get(route[0])).status_code == 405


async def test_an_unknown_path_is_a_404(real: httpx.AsyncClient) -> None:
    assert (await real.post("/webhook/twitch/nope")).status_code == 404


class TestSubscriptionType:
    @pytest.mark.parametrize(
        ("model", "expected"),
        [(StreamOnlineEventSub, "stream.online")],
    )
    def test_reads_the_one_type_off_the_literal(
        self, model: type[BaseModel], expected: str
    ) -> None:
        assert ctl._subscription_type(model) == expected

    def test_a_model_whose_subscription_is_not_a_model_is_refused_at_import(
        self,
    ) -> None:
        class Bad(BaseModel):
            subscription: str

        with pytest.raises(TypeError, match="subscription is not a model"):
            ctl._subscription_type(Bad)

    def test_a_type_that_is_not_a_single_value_literal_is_refused(self) -> None:
        class Two(BaseModel):
            type: Literal["a", "b"]

        class Plain(BaseModel):
            type: str

        class WithTwo(BaseModel):
            subscription: Two

        class WithPlain(BaseModel):
            subscription: Plain

        with pytest.raises(TypeError, match="single-value Literal"):
            ctl._subscription_type(WithTwo)
        with pytest.raises(TypeError, match="single-value Literal"):
            ctl._subscription_type(WithPlain)


class TestRegistering:
    @pytest.fixture(autouse=True)
    def _scratch(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """A throwaway router and map, so registering here leaves the real ones alone."""
        monkeypatch.setattr(ctl, "WEBHOOK_PATHS", {})
        monkeypatch.setattr(ctl, "twitch_router", APIRouter())

    @staticmethod
    async def handler(event: Any) -> None:
        return None

    def test_a_route_is_recorded_and_registered_once(self) -> None:
        ctl._route("/x", StreamOnlineEventSub, self.handler)

        assert ctl.WEBHOOK_PATHS == {"stream.online": "/x"}
        assert [r.path for r in ctl.twitch_router.routes] == ["/x"]  # pyright: ignore[reportAttributeAccessIssue]

    def test_two_models_declaring_one_type_are_refused_not_silently_overwritten(
        self,
    ) -> None:
        """A plain assignment would leave the migration repointing every subscription
        of that type at whichever route registered last, with the other route missing
        from the map and never migrated at all."""
        ctl._route("/first", StreamOnlineEventSub, self.handler)

        with pytest.raises(
            ValueError, match=r"stream.online is already routed to /first"
        ):
            ctl._route("/second", StreamOnlineEventSub, self.handler)

        assert ctl.WEBHOOK_PATHS == {"stream.online": "/first"}
