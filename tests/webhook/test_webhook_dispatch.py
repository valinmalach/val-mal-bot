from typing import Any

import httpx
import pytest

import controller.twitch as ctl
from models.twitch_event_subs.stream_online import StreamOnlineEventSub
from tests.webhook.support import Hooks, delivery, stream_online_payload

pytestmark = pytest.mark.anyio


async def post(
    client: httpx.AsyncClient, body: Any = None, **kwargs: Any
) -> httpx.Response:
    raw, headers = delivery(stream_online_payload() if body is None else body, **kwargs)
    return await client.post("/t", content=raw, headers=headers)


class TestDispatch:
    async def test_a_valid_notification_is_answered_202_with_no_body_and_dispatched(
        self, client: httpx.AsyncClient, hooks: Hooks
    ) -> None:
        response = await post(client)

        assert (response.status_code, response.content) == (202, b"")
        ((name, _),) = hooks.dispatched
        assert name == "/t"

    async def test_the_handler_receives_the_parsed_model(
        self, client: httpx.AsyncClient, hooks: Hooks
    ) -> None:
        await post(client, stream_online_payload("77"))
        await hooks.run_dispatched()

        (event,) = hooks.events
        assert isinstance(event, StreamOnlineEventSub)
        assert event.event.id == "77"
        assert event.event.broadcaster_user_login == "bob"

    async def test_fields_twitch_adds_later_do_not_fail_the_delivery(
        self, client: httpx.AsyncClient, hooks: Hooks
    ) -> None:
        payload = stream_online_payload()
        payload["event"]["a_field_added_in_2027"] = [1, 2]
        payload["subscription"]["id"] = "sub"

        assert (await post(client, payload)).status_code == 202

    async def test_the_dispatch_is_named_for_the_endpoint_so_a_failure_reports_under_it(
        self, client: httpx.AsyncClient, hooks: Hooks
    ) -> None:
        await post(client)

        assert hooks.dispatched[0][0] == "/t"


class TestDuplicates:
    async def test_a_delivery_twitch_sends_twice_is_dispatched_once_and_answered_202_both_times(
        self, client: httpx.AsyncClient, hooks: Hooks
    ) -> None:
        """202 is what stops the retries: this end does have it."""
        first = await post(client, message_id="same")
        second = await post(client, message_id="same")

        assert (first.status_code, second.status_code) == (202, 202)
        assert len(hooks.dispatched) == 1

    async def test_different_ids_are_different_deliveries(
        self, client: httpx.AsyncClient, hooks: Hooks
    ) -> None:
        await post(client, message_id="a")
        await post(client, message_id="b")

        assert len(hooks.dispatched) == 2

    async def test_a_delivery_with_no_message_id_is_refused_not_collapsed_into_one(
        self, client: httpx.AsyncClient, hooks: Hooks
    ) -> None:
        """Twitch always sends one and it is inside the HMAC. Claiming "" would treat
        every id-less delivery as the same one and throw the rest away silently."""
        response = await post(client, message_id="")

        assert response.status_code == 400
        assert hooks.dispatched == []
        assert hooks.notified == [("400: Bad request on /t. No message id.", None)]

    async def test_a_duplicate_after_the_window_is_handled_again(
        self, client: httpx.AsyncClient, hooks: Hooks
    ) -> None:
        await post(client, message_id="a")

        hooks.clock += ctl._HANDLED_TTL_SECONDS + 1
        await post(client, message_id="a")

        assert len(hooks.dispatched) == 2

    async def test_a_duplicate_just_inside_the_window_is_still_recognised(
        self, client: httpx.AsyncClient, hooks: Hooks
    ) -> None:
        await post(client, message_id="a")

        hooks.clock += ctl._HANDLED_TTL_SECONDS - 1
        await post(client, message_id="a")

        assert len(hooks.dispatched) == 1

    async def test_the_remembered_window_is_twice_the_freshness_window(self) -> None:
        """A host clock behind Twitch's would otherwise leave a gap where a delivery is
        still fresh but no longer remembered; a clock more than one window out refuses
        everything as stale anyway."""
        assert ctl._HANDLED_TTL_SECONDS == 2 * ctl._MESSAGE_WINDOW_SECONDS


class TestClaimsAreGivenBackWhenThisEndFails:
    async def test_a_payload_that_would_not_validate_can_be_retried(
        self, client: httpx.AsyncClient, hooks: Hooks
    ) -> None:
        """Only ids that were dispatched are remembered, so a delivery Twitch retries
        because this end failed still gets through."""
        bad = stream_online_payload()
        del bad["event"]["id"]

        first = await post(client, bad, message_id="retry")
        second = await post(client, stream_online_payload(), message_id="retry")

        assert (first.status_code, second.status_code) == (400, 202)
        assert len(hooks.dispatched) == 1

    async def test_a_dispatch_that_could_not_start_can_be_retried(
        self, client: httpx.AsyncClient, hooks: Hooks
    ) -> None:
        hooks.fail_dispatch = RuntimeError("no loop")

        first = await post(client, message_id="retry")
        hooks.fail_dispatch = None
        second = await post(client, message_id="retry")

        assert (first.status_code, second.status_code) == (500, 202)
        assert len(hooks.dispatched) == 1

    async def test_a_delivery_that_was_dispatched_stays_claimed_even_if_its_handler_later_fails(
        self, client: httpx.AsyncClient, hooks: Hooks
    ) -> None:
        """A retry of one that worked must not run it twice; what the handler does
        afterwards is its own report's business -- _release is only ever called
        when dispatch itself could not start, never for a handler that ran and
        then failed."""
        hooks.handler_error = RuntimeError("handler blew up")
        await post(client, message_id="done")
        assert "done" in ctl._handled

        with pytest.raises(RuntimeError, match="handler blew up"):
            await hooks.run_dispatched()
        assert "done" in ctl._handled

        retry = await post(client, message_id="done")

        assert retry.status_code == 202
        assert len(hooks.dispatched) == 1, "the retry must not be dispatched again"


class TestRefusedPayloads:
    async def test_a_payload_for_another_event_is_a_400_naming_the_model_and_field(
        self, client: httpx.AsyncClient, hooks: Hooks
    ) -> None:
        payload = stream_online_payload()
        payload["subscription"]["type"] = "stream.offline"

        response = await post(client, payload)

        assert response.status_code == 400
        assert hooks.dispatched == []
        assert hooks.notified == [
            (
                "400: Bad request on /t. Payload did not match"
                " StreamOnlineEventSub at: subscription.type",
                None,
            )
        ]

    async def test_a_missing_field_is_named_by_its_path(
        self, client: httpx.AsyncClient, hooks: Hooks
    ) -> None:
        payload = stream_online_payload()
        del payload["event"]["started_at"]

        await post(client, payload)

        assert "at: event.started_at" in hooks.notified[0][0]

    async def test_at_most_three_fields_are_listed(
        self, client: httpx.AsyncClient, hooks: Hooks
    ) -> None:
        await post(client, {"subscription": {"type": "stream.online"}, "event": {}})

        listed = hooks.notified[0][0].split("at: ")[1].split("; ")
        assert len(listed) == 3

    @pytest.mark.parametrize("missing", ["event", "subscription"])
    async def test_a_body_missing_a_top_level_key_is_a_400(
        self, missing: str, client: httpx.AsyncClient, hooks: Hooks
    ) -> None:
        payload = stream_online_payload()
        del payload[missing]

        assert (await post(client, payload)).status_code == 400

    async def test_a_validation_failure_is_not_reported_as_a_server_fault(
        self, client: httpx.AsyncClient, hooks: Hooks
    ) -> None:
        """A payload this end cannot read is not a server fault, and a traceback for
        one is noise."""
        await post(client, {"subscription": {"type": "x"}, "event": {}})

        assert hooks.reported == []


class TestUnexpectedFailures:
    async def test_is_a_500_reported_under_the_endpoint(
        self, client: httpx.AsyncClient, hooks: Hooks
    ) -> None:
        hooks.fail_dispatch = RuntimeError("boom")

        response = await post(client)

        assert response.status_code == 500
        assert hooks.reported == ["500: Internal server error on /t"]

    async def test_a_base_exception_still_gives_the_claim_back(
        self, client: httpx.AsyncClient, hooks: Hooks
    ) -> None:
        class Stop(BaseException):
            pass

        hooks.fail_dispatch = Stop()  # pyright: ignore[reportAttributeAccessIssue]

        with pytest.raises(Stop):
            await post(client, message_id="x")

        assert "x" not in ctl._handled


class TestClaim:
    def test_the_first_claim_wins_and_the_second_is_refused(self, hooks: Hooks) -> None:
        assert ctl._claim("a") is True
        assert ctl._claim("a") is False

    def test_a_released_claim_can_be_taken_again(self, hooks: Hooks) -> None:
        ctl._claim("a")

        ctl._release("a")

        assert ctl._claim("a") is True

    def test_releasing_what_was_never_claimed_is_harmless(self, hooks: Hooks) -> None:
        ctl._release("never")

    def test_an_expired_claim_is_forgotten_on_the_next_claim(
        self, hooks: Hooks
    ) -> None:
        ctl._claim("old")

        hooks.clock += ctl._HANDLED_TTL_SECONDS + 1
        ctl._claim("new")

        assert list(ctl._handled) == ["new"]

    def test_the_cap_drops_the_oldest_and_counts_it(
        self, hooks: Hooks, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(ctl, "_HANDLED_LIMIT", 3)

        for i in range(5):
            hooks.clock += 1
            ctl._claim(f"id{i}")

        assert list(ctl._handled) == ["id2", "id3", "id4"]
        assert ctl._forgotten_early == 2

    def test_the_cap_holds_twenty_thousand_which_a_busy_chat_would_not_reach_in_a_window(
        self,
    ) -> None:
        assert ctl._HANDLED_LIMIT == 20_000


class TestReplayCacheFull:
    async def test_says_so_once_when_the_cap_bit(
        self, client: httpx.AsyncClient, hooks: Hooks, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Past the cap a redelivery can run twice, so it is said out loud rather than
        silently dropping the oldest."""
        monkeypatch.setattr(ctl, "_HANDLED_LIMIT", 1)
        await post(client, message_id="a")
        await post(client, message_id="b")

        await post(client, message_id="c")

        ((text, _),) = [n for n in hooks.notified if n[1] == "replay-cache-full"]
        assert "Replay cache full on /t: 1 delivery id(s) forgotten" in text
        assert ctl._forgotten_early == 1

    async def test_nothing_is_said_while_the_cap_has_not_bitten(
        self, client: httpx.AsyncClient, hooks: Hooks
    ) -> None:
        await post(client, message_id="a")
        await post(client, message_id="b")

        assert hooks.notified == []

    async def test_the_count_resets_once_it_has_been_said(self, hooks: Hooks) -> None:
        ctl._forgotten_early = 4

        await ctl._report_forgotten("/t")

        assert ctl._forgotten_early == 0
        assert len(hooks.notified) == 1
