from types import SimpleNamespace
from typing import Any
from unittest.mock import MagicMock

import pytest

from cogs import twitch_admin
from cogs.twitch_admin import TwitchAdmin
from models.twitch_api_responses.subscription import Subscription
from services.config import config
from services.twitch.helix import HelixError
from services.twitch.migrate_plan import Outcome
from tests.twitch_migrate.support import sub

BACKSLASH = chr(92)
OWNER = 99


class Admin:
    def __init__(self) -> None:
        self.sent: list[tuple[str, dict[str, Any]]] = []
        self.followed: list[tuple[str, dict[str, Any]]] = []
        self.deferred: list[bool] = []
        self.reported: list[str] = []
        self.listing: list[Subscription] | HelixError = []
        self.users: dict[str, list[Any] | HelixError] = {}
        self.asked: list[list[str]] = []
        self.found = True
        self.action_error: HelixError | None = None
        self.acted: list[tuple[str, str]] = []
        self.migrated: list[tuple[Any, bool]] = []
        self.outcome = Outcome()
        self.migrate_error: HelixError | None = None

    def interaction(self, user_id: int = OWNER) -> Any:
        async def send_message(text: str | None = None, **kwargs: Any) -> None:
            self.sent.append((text or "", kwargs))

        async def defer(*, ephemeral: bool) -> None:
            self.deferred.append(ephemeral)

        async def followup(text: str, **kwargs: Any) -> None:
            self.followed.append((text, kwargs))

        return SimpleNamespace(
            user=SimpleNamespace(id=user_id),
            response=SimpleNamespace(send_message=send_message, defer=defer),
            followup=SimpleNamespace(send=followup),
        )


def install(monkeypatch: pytest.MonkeyPatch) -> Admin:
    """Replace everything the Twitch admin cog reaches for, for one test."""
    admin = Admin()

    async def get_subscriptions() -> list[Subscription]:
        answer = admin.listing
        if isinstance(answer, HelixError):
            raise answer
        return answer

    async def get_users(ids: list[str]) -> list[Any]:
        admin.asked.append(list(ids))
        answer = admin.users.get(ids[0], [])
        if isinstance(answer, HelixError):
            raise answer
        return answer

    def acting(verb: str) -> Any:
        async def act(login: str) -> bool:
            admin.acted.append((verb, login))
            if admin.action_error is not None:
                raise admin.action_error
            return admin.found

        return act

    async def migrate(routes: Any, *, confirm: bool) -> Outcome:
        admin.migrated.append((routes, confirm))
        if admin.migrate_error is not None:
            raise admin.migrate_error
        return admin.outcome

    async def report(exc: Exception, context: str, **_: object) -> None:
        admin.reported.append(context)

    monkeypatch.setattr(twitch_admin, "get_subscriptions", get_subscriptions)
    monkeypatch.setattr(twitch_admin, "get_users", get_users)
    monkeypatch.setattr(twitch_admin, "subscribe_to_user", acting("subscribe"))
    monkeypatch.setattr(twitch_admin, "unsubscribe_to_user", acting("unsubscribe"))
    monkeypatch.setattr(twitch_admin, "migrate", migrate)
    monkeypatch.setattr(twitch_admin, "report", report)
    monkeypatch.setattr(
        twitch_admin,
        "create_authorization_start_url",
        lambda token_type: f"https://bot.example/start/{token_type.value}?state=s",
    )
    monkeypatch.setattr(
        config,
        "_settings",
        {
            "owner_id": OWNER,
            "twitch_bot_user_id": "999",
            "twitch_broadcaster_id": "111",
            "broadcaster_username": "valinmalach",
        },
    )
    return admin


def cog() -> TwitchAdmin:
    return TwitchAdmin(MagicMock())


async def run(command: Any, *args: Any) -> None:
    await command.callback(cog(), *args)


def listing(*entries: tuple[str, str, str]) -> list[Subscription]:
    """(type, broadcaster id, status) as subscriptions."""
    return [
        sub(id=f"s{n}", type=t, status=status, broadcaster_user_id=who)
        for n, (t, who, status) in enumerate(entries)
    ]


def user(name: str) -> Any:
    return SimpleNamespace(display_name=name)
