from typing import Any

import httpx
import pytest

import valmal.twitch.eventsub.router as ctl
from tests.webhook.support import Hooks, delivery, iso, sign, stream_online_payload

pytestmark = pytest.mark.anyio


async def post(client: httpx.AsyncClient, body: Any, **kwargs: Any) -> httpx.Response:
    raw, headers = delivery(body, **kwargs)
    return await client.post("/t", content=raw, headers=headers)


class TestSignature:
    async def test_a_correctly_signed_notification_is_accepted(
        self, client: httpx.AsyncClient, hooks: Hooks
    ) -> None:
        response = await post(client, stream_online_payload())

        assert response.status_code == 202
        assert hooks.notified == []

    async def test_a_wrong_signature_is_refused_and_nothing_is_dispatched(
        self, client: httpx.AsyncClient, hooks: Hooks
    ) -> None:
        response = await post(
            client, stream_online_payload(), signature="sha256=" + "0" * 64
        )

        assert response.status_code == 403
        assert hooks.dispatched == []
        assert hooks.notified == [
            ("403: Forbidden request on /t. Signature does not match.", None)
        ]

    async def test_a_missing_signature_is_refused(
        self, client: httpx.AsyncClient, hooks: Hooks
    ) -> None:
        raw, headers = delivery(stream_online_payload())
        del headers["Twitch-Eventsub-Message-Signature"]

        response = await client.post("/t", content=raw, headers=headers)

        assert response.status_code == 403
        assert hooks.dispatched == []

    @pytest.mark.parametrize("prefix", ["", "sha1=", "SHA256="])
    async def test_the_scheme_prefix_must_be_exactly_sha256_equals(
        self, prefix: str, client: httpx.AsyncClient
    ) -> None:
        raw, headers = delivery(stream_online_payload())
        digest = headers["Twitch-Eventsub-Message-Signature"].removeprefix("sha256=")
        headers["Twitch-Eventsub-Message-Signature"] = prefix + digest

        assert (
            await client.post("/t", content=raw, headers=headers)
        ).status_code == 403

    async def test_a_signature_over_a_different_body_is_refused(
        self, client: httpx.AsyncClient
    ) -> None:
        signed_for = sign("msg-1", iso(), '{"other": true}')

        response = await post(client, stream_online_payload(), signature=signed_for)

        assert response.status_code == 403

    @pytest.mark.parametrize(
        "changed", [{"message_id": "msg-2"}, {"timestamp": iso(5)}]
    )
    async def test_the_id_and_the_timestamp_are_inside_the_signature(
        self, changed: dict[str, str], client: httpx.AsyncClient
    ) -> None:
        """Replaying a captured body under a fresh id or timestamp must fail."""
        original = sign("msg-1", iso(), '{"a": 1}')
        raw, headers = delivery({"a": 1}, signature=original)
        headers["Twitch-Eventsub-Message-Id"] = changed.get("message_id", "msg-1")
        headers["Twitch-Eventsub-Message-Timestamp"] = changed.get("timestamp", iso())

        assert (
            await client.post("/t", content=raw, headers=headers)
        ).status_code == 403

    async def test_a_non_ascii_signature_header_is_a_refusal_not_a_500(
        self, client: httpx.AsyncClient, hooks: Hooks
    ) -> None:
        """Headers arrive as latin-1, and compare_digest raises on non-ASCII rather than
        returning False, which would turn a forged request into a 500."""
        raw, headers = delivery(stream_online_payload())
        forged = (f"sha256={chr(233)}" + "a" * 60).encode("latin-1")

        response = await client.post(
            "/t",
            content=raw,
            headers={**headers, "Twitch-Eventsub-Message-Signature": forged},  # pyright: ignore[reportArgumentType]
        )

        assert response.status_code == 403
        assert hooks.reported == []

    async def test_a_body_that_is_not_utf8_is_forbidden_not_a_server_error(
        self, client: httpx.AsyncClient, hooks: Hooks
    ) -> None:
        """It cannot be a signed request, and answering it as one keeps every
        malformed byte an unauthenticated caller sends out of the 500 handler."""
        raw, headers = delivery(b"\xff\xfe{}", signature="sha256=" + "0" * 64)

        response = await client.post("/t", content=raw, headers=headers)

        assert response.status_code == 403
        assert hooks.reported == []
        assert hooks.notified == [
            ("403: Forbidden request on /t. Body is not UTF-8.", None)
        ]

    async def test_refusals_never_echo_anything_the_caller_sent(
        self, client: httpx.AsyncClient, hooks: Hooks
    ) -> None:
        raw, headers = delivery({"challenge": "<script>evil</script>"}, signature="bad")
        headers["Twitch-Eventsub-Message-Type"] = "webhook_callback_verification"

        response = await client.post("/t", content=raw, headers=headers)

        assert response.status_code == 403
        assert "evil" not in response.text
        assert all("evil" not in text for text, _ in hooks.notified)


class TestBodySize:
    async def test_a_body_over_the_limit_is_refused_with_413(
        self, client: httpx.AsyncClient, hooks: Hooks
    ) -> None:
        response = await client.post("/t", content=b"x" * (ctl._MAX_BODY_BYTES + 1))

        assert response.status_code == 413
        assert hooks.notified == [
            ("413: Oversized request on /t, refused part-read.", None)
        ]

    async def test_a_chunked_body_is_counted_as_it_arrives(
        self, client: httpx.AsyncClient
    ) -> None:
        """A chunked request sends no Content-Length to trust."""

        async def chunks():  # noqa: ANN202
            for _ in range(5):
                yield b"x" * (ctl._MAX_BODY_BYTES // 4)

        response = await client.post("/t", content=chunks())

        assert response.status_code == 413

    async def test_exactly_the_limit_is_still_read(
        self, client: httpx.AsyncClient
    ) -> None:
        padding = "x" * (ctl._MAX_BODY_BYTES - len('{"challenge": ""}') + 0)
        body = '{"challenge": "' + padding[: ctl._MAX_BODY_BYTES - 17] + '"}'
        assert len(body.encode()) == ctl._MAX_BODY_BYTES
        raw, headers = delivery(body, message_type="webhook_callback_verification")

        response = await client.post("/t", content=raw, headers=headers)

        assert response.status_code == 200

    async def test_the_size_is_checked_before_the_signature_can_be(
        self, client: httpx.AsyncClient, hooks: Hooks
    ) -> None:
        """The whole body has to be read to sign it, so it is bounded first."""
        response = await client.post(
            "/t",
            content=b"x" * (ctl._MAX_BODY_BYTES + 1),
            headers={"Twitch-Eventsub-Message-Signature": "sha256=" + "0" * 64},
        )

        assert response.status_code == 413


class TestBodyShape:
    async def test_a_signed_body_that_is_not_json_is_a_400_because_a_retry_would_fail_too(
        self, client: httpx.AsyncClient, hooks: Hooks
    ) -> None:
        response = await post(client, "not json at all")

        assert response.status_code == 400
        assert hooks.notified == [("400: Bad request on /t. Body is not JSON.", None)]

    @pytest.mark.parametrize("body", ["[1, 2]", '"a string"', "42", "null", "true"])
    async def test_a_signed_body_that_is_not_an_object_is_a_400(
        self, body: str, client: httpx.AsyncClient, hooks: Hooks
    ) -> None:
        response = await post(client, body)

        assert response.status_code == 400
        assert hooks.notified == [
            ("400: Bad request on /t. Body is not a JSON object.", None)
        ]


class TestHandshake:
    async def test_echoes_the_challenge_as_plain_text(
        self, client: httpx.AsyncClient
    ) -> None:
        response = await post(
            client,
            {"challenge": "pogchamp-kappa-360noscope-vohiyo"},
            message_type="webhook_callback_verification",
        )

        assert response.status_code == 200
        assert response.text == "pogchamp-kappa-360noscope-vohiyo"
        assert response.headers["content-type"].startswith("text/plain")

    async def test_the_message_type_header_is_case_insensitive(
        self, client: httpx.AsyncClient
    ) -> None:
        response = await post(
            client, {"challenge": "abc"}, message_type="Webhook_Callback_Verification"
        )

        assert response.text == "abc"

    @pytest.mark.parametrize("challenge", [None, 5, ["x"], {"a": 1}])
    async def test_a_challenge_that_is_not_a_string_is_answered_empty(
        self, challenge: object, client: httpx.AsyncClient
    ) -> None:
        response = await post(
            client,
            {"challenge": challenge},
            message_type="webhook_callback_verification",
        )

        assert (response.status_code, response.text) == (200, "")

    async def test_an_unsigned_handshake_echoes_nothing(
        self, client: httpx.AsyncClient
    ) -> None:
        """Branching before the signature check left the handshake echoing attacker text."""
        response = await post(
            client,
            {"challenge": "attacker-text"},
            message_type="webhook_callback_verification",
            signature="sha256=" + "0" * 64,
        )

        assert response.status_code == 403
        assert "attacker-text" not in response.text

    async def test_a_stale_handshake_is_still_answered(
        self, client: httpx.AsyncClient
    ) -> None:
        """A wrong clock refusing events is recoverable; one that also refused the
        handshake would block the resubscribe that repairs it."""
        response = await post(
            client,
            {"challenge": "abc"},
            message_type="webhook_callback_verification",
            timestamp=iso(-86_400),
        )

        assert (response.status_code, response.text) == (200, "abc")

    async def test_a_handshake_with_no_readable_timestamp_is_still_answered(
        self, client: httpx.AsyncClient
    ) -> None:
        response = await post(
            client,
            {"challenge": "abc"},
            message_type="webhook_callback_verification",
            timestamp="not a time",
        )

        assert response.text == "abc"


class TestRevocation:
    async def test_is_acknowledged_and_announced(
        self, client: httpx.AsyncClient, hooks: Hooks
    ) -> None:
        response = await post(
            client,
            {
                "subscription": {
                    "type": "channel.follow",
                    "condition": {"broadcaster_user_id": "1"},
                    "status": "authorization_revoked",
                }
            },
            message_type="revocation",
        )

        assert response.status_code == 204
        assert hooks.notified == [
            (
                "Revoked channel.follow notifications for condition:"
                " {'broadcaster_user_id': '1'} because authorization_revoked",
                None,
            )
        ]

    @pytest.mark.parametrize("subscription", [None, "text", [1], 5])
    async def test_a_body_without_a_usable_subscription_still_answers_204(
        self, subscription: object, client: httpx.AsyncClient, hooks: Hooks
    ) -> None:
        response = await post(
            client, {"subscription": subscription}, message_type="revocation"
        )

        assert response.status_code == 204
        assert "Revoked unknown notifications" in hooks.notified[0][0]
        assert "No reason provided" in hooks.notified[0][0]

    async def test_a_stale_revocation_is_still_acknowledged(
        self, client: httpx.AsyncClient
    ) -> None:
        response = await post(
            client,
            {"subscription": {}},
            message_type="revocation",
            timestamp=iso(-86_400),
        )

        assert response.status_code == 204

    async def test_an_unsigned_revocation_says_nothing_to_the_admin_channel(
        self, client: httpx.AsyncClient, hooks: Hooks
    ) -> None:
        """Otherwise anyone could put text, mentions included, into it."""
        response = await post(
            client,
            {"subscription": {"type": "@everyone"}},
            message_type="revocation",
            signature="sha256=" + "0" * 64,
        )

        assert response.status_code == 403
        assert all("everyone" not in text for text, _ in hooks.notified)


class TestFreshness:
    @pytest.mark.parametrize("delta", [-600, -1, 0, 1, 600])
    async def test_within_ten_minutes_either_way_is_fresh(
        self, delta: int, client: httpx.AsyncClient
    ) -> None:
        response = await post(client, stream_online_payload(), timestamp=iso(delta))

        assert response.status_code == 202

    @pytest.mark.parametrize("delta", [-601, 601, -86_400, 86_400])
    async def test_beyond_ten_minutes_in_either_direction_is_refused(
        self, delta: int, client: httpx.AsyncClient, hooks: Hooks
    ) -> None:
        """A clock ahead is as wrong as a clock behind."""
        response = await post(client, stream_online_payload(), timestamp=iso(delta))

        assert response.status_code == 403
        assert hooks.dispatched == []
        ((text, key),) = hooks.notified
        assert "Timestamp outside the 600s window" in text
        assert key == "stale-delivery:/t"

    @pytest.mark.parametrize(
        "timestamp", ["", "yesterday", "2026-06-15 12:00:00", "12:00"]
    )
    async def test_an_unreadable_timestamp_is_refused(
        self, timestamp: str, client: httpx.AsyncClient, hooks: Hooks
    ) -> None:
        response = await post(client, stream_online_payload(), timestamp=timestamp)

        assert response.status_code == 403
        assert hooks.notified == [
            ("403: Forbidden request on /t. Unreadable timestamp.", None)
        ]

    async def test_the_window_is_ten_minutes(self) -> None:
        assert ctl._MESSAGE_WINDOW_SECONDS == 600
