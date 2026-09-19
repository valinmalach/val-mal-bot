"""A subscription still being verified is on its way to enabled, not broken.

migrate_plan.decide already refuses to touch one for that reason: verification is a
seconds-long transient, so catching a subscription mid-way and destroying it
destroys something that was about to arrive by itself. _subscribe reached the same
subscription by a different door and did exactly that.
"""

from collections.abc import Callable

import pytest

import services.twitch.api as api
from tests.twitch.support import Script, page, reply, subscription_json, user_json

pytestmark = pytest.mark.anyio

ONLINE = "https://bot.example/webhook/twitch"


@pytest.fixture
def notices(monkeypatch: pytest.MonkeyPatch) -> list[str]:
    seen: list[str] = []

    async def notify(text: str, *, key: str | None = None) -> bool:
        seen.append(text)
        return True

    monkeypatch.setattr(api, "notify", notify)
    return seen


async def test_one_still_being_verified_at_our_callback_is_left_alone(
    helix_http: Callable[..., Script], notices: list[str]
) -> None:
    """A second /subscribe seconds after the first would otherwise delete the one
    Twitch is verifying, and say it "was delivering nothing"."""
    script = helix_http(
        reply(200, {"data": [user_json("42")]}),
        reply(409, text="exists"),
        reply(
            200,
            page(
                [
                    subscription_json(
                        "verifying",
                        status="webhook_callback_verification_pending",
                        callback=ONLINE,
                        broadcaster_user_id="42",
                    )
                ]
            ),
        ),
        reply(202, {}),
    )

    assert await api.subscribe_to_user("alice") is True

    assert all(r.method != "DELETE" for r in script.requests)
    assert not notices


async def test_one_still_being_verified_at_another_callback_is_still_replaced(
    helix_http: Callable[..., Script], notices: list[str]
) -> None:
    """Pending does not excuse a callback this deployment does not answer on."""
    script = helix_http(
        reply(200, {"data": [user_json("42")]}),
        reply(409, text="exists"),
        reply(
            200,
            page(
                [
                    subscription_json(
                        "elsewhere",
                        status="webhook_callback_verification_pending",
                        callback="https://old.example/webhook/twitch",
                        broadcaster_user_id="42",
                    )
                ]
            ),
        ),
        reply(204),
        reply(202, {}),
        reply(202, {}),
    )

    await api.subscribe_to_user("alice")

    assert any(r.method == "DELETE" for r in script.requests)
    assert len(notices) == 1
