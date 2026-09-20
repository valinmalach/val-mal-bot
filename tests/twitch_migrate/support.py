from types import SimpleNamespace
from typing import Any

from services.twitch.helix import HelixError
from valmal.twitch.models.api.subscription import Subscription

ROUTES = {
    "stream.online": "/webhook/twitch/stream/online",
    "stream.offline": "/webhook/twitch/stream/offline",
    "channel.raid": "/webhook/twitch/raid",
}
CURRENT = "https://bot.example/webhook/twitch/stream/online"
OLD = "https://old.example/webhook/twitch/stream/online"


def sub(
    id: str = "s1",
    type: str = "stream.online",
    callback: str | None = OLD,
    status: str = "enabled",
    version: str = "1",
    **condition: str,
) -> Subscription:
    return Subscription.model_validate(
        {
            "id": id,
            "status": status,
            "type": type,
            "version": version,
            "condition": condition or {"broadcaster_user_id": "111"},
            "created_at": "2026-01-01T00:00:00Z",
            "transport": {"method": "webhook", "callback": callback},
            "cost": 1,
        }
    )


class MigrateWorld:
    """Helix and the admin channel as the migration sees them, with every call recorded."""

    def __init__(self) -> None:
        self.listing: list[Subscription] = []
        # What Helix lists once anything has been deleted or created, which is
        # the answer _exists_at gets after a 409.
        self.after: list[Subscription] | HelixError = []
        self.listing_error: HelixError | None = None
        self.users: dict[str, str] = {}
        self.users_error: HelixError | None = None
        self.asked_for: list[list[str]] = []
        self.delete_errors: dict[str, HelixError] = {}
        self.create_errors: dict[str, HelixError] = {}
        self.calls: list[tuple[Any, ...]] = []
        self.listings = 0
        self.notified: list[tuple[str, str | None]] = []
        self.files: list[tuple[str, str, str]] = []
        self.file_delivered = True

    async def get_subscriptions(self) -> list[Subscription]:
        self.listings += 1
        if self.listing_error is not None and not self.calls:
            raise self.listing_error
        if not self.calls:
            return self.listing
        if isinstance(self.after, HelixError):
            raise self.after
        return self.after

    async def get_users(self, ids: list[str]) -> list[Any]:
        self.asked_for.append(list(ids))
        if self.users_error is not None:
            raise self.users_error
        return [
            SimpleNamespace(id=i, login=self.users[i]) for i in ids if i in self.users
        ]

    async def delete_subscription(self, subscription_id: str) -> None:
        self.calls.append(("delete", subscription_id))
        if subscription_id in self.delete_errors:
            raise self.delete_errors[subscription_id]

    async def create_subscription(
        self, sub_type: str, version: str, condition: dict[str, Any], callback: str
    ) -> None:
        self.calls.append(("create", sub_type, version, condition, callback))
        if sub_type in self.create_errors:
            raise self.create_errors[sub_type]

    async def notify(self, text: str, *, key: str | None = None) -> bool:
        self.notified.append((text, key))
        return True

    async def notify_file(self, text: str, filename: str, content: str) -> bool:
        self.files.append((text, filename, content))
        return self.file_delivered

    @property
    def deletes(self) -> list[str]:
        return [c[1] for c in self.calls if c[0] == "delete"]

    @property
    def creates(self) -> list[tuple[Any, ...]]:
        return [c[1:] for c in self.calls if c[0] == "create"]


def two_online_and_a_raid() -> list[Subscription]:
    return [
        sub(id="a", broadcaster_user_id="1"),
        sub(id="b", broadcaster_user_id="2", callback=CURRENT),
        sub(id="c", type="channel.follow"),
    ]
