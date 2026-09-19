from types import SimpleNamespace
from typing import Any
from unittest.mock import MagicMock

import discord
import pytest

from cogs import twitch_admin
from cogs.twitch_admin import TwitchAdmin, _login, _refuse_login
from constants import TokenType
from controller.twitch import WEBHOOK_PATHS
from models.twitch_api_responses.subscription import Subscription
from services.config import config
from services.twitch.helix import HelixError
from services.twitch.migrate_plan import Outcome
from tests.twitch_migrate.support import sub

pytestmark = pytest.mark.anyio

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


@pytest.fixture
def admin(monkeypatch: pytest.MonkeyPatch) -> Admin:
    admin = Admin()

    async def get_subscriptions() -> list[Subscription]:
        if isinstance(admin.listing, HelixError):
            raise admin.listing
        return admin.listing

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


class TestLoginParsing:
    @pytest.mark.parametrize(
        ("typed", "login"),
        [
            ("valinmalach", "valinmalach"),
            ("@valinmalach", "valinmalach"),
            ("  Val_Mal  ", "Val_Mal"),
            ("@ x", None),
            ("", None),
            ("@", None),
            ("a" * 26, None),
            ("bad name", None),
            ("bad-name", None),
            ("@@name", None),
        ],
    )
    def test_what_people_type_for_a_twitch_login(
        self, typed: str, login: str | None
    ) -> None:
        assert _login(typed) == login

    async def test_a_refusal_quotes_the_value_privately_and_mentions_nobody(
        self, admin: Admin
    ) -> None:
        await _refuse_login(admin.interaction(), "<@&1>`x`")

        ((text, kwargs),) = admin.sent
        assert text.startswith("`<@&1>x`")
        assert "is not a Twitch username" in text
        assert kwargs["ephemeral"] is True
        mentions = kwargs["allowed_mentions"]
        assert (mentions.everyone, mentions.users, mentions.roles) == (
            False,
            False,
            False,
        )


class TestTwitchAuth:
    async def test_only_the_owner_gets_the_links(self, admin: Admin) -> None:
        await run(TwitchAdmin.twitch_auth, admin.interaction(user_id=5))

        ((text, kwargs),) = admin.sent
        assert text == "Only the configured bot owner can replace Twitch OAuth grants."
        assert kwargs == {"ephemeral": True}

    async def test_the_owner_gets_one_link_per_identity_privately(
        self, admin: Admin
    ) -> None:
        await run(TwitchAdmin.twitch_auth, admin.interaction())

        ((_, kwargs),) = admin.sent
        assert kwargs["ephemeral"] is True
        view = kwargs["view"]
        assert [b.url for b in view.children] == [
            "https://bot.example/start/user?state=s",
            "https://bot.example/start/broadcaster?state=s",
        ]
        assert all(b.style is discord.ButtonStyle.link for b in view.children)
        assert view.timeout == 600

    async def test_the_message_names_which_account_belongs_to_which_grant(
        self, admin: Admin
    ) -> None:
        await run(TwitchAdmin.twitch_auth, admin.interaction())

        text = admin.sent[0][0]
        assert "Twitch ID `999`" in text
        assert "`valinmalach` with Twitch ID `111`" in text

    async def test_a_stranger_is_refused_before_any_link_is_made(
        self, admin: Admin, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        made: list[TokenType] = []
        monkeypatch.setattr(
            twitch_admin,
            "create_authorization_start_url",
            lambda token_type: made.append(token_type) or "x",
        )

        await run(TwitchAdmin.twitch_auth, admin.interaction(user_id=5))

        assert not made


def listing(*entries: tuple[str, str, str]) -> list[Subscription]:
    """(type, broadcaster id, status) as subscriptions."""
    return [
        sub(id=f"s{n}", type=t, status=status, broadcaster_user_id=who)
        for n, (t, who, status) in enumerate(entries)
    ]


def user(name: str) -> Any:
    return SimpleNamespace(display_name=name)


class TestSubscriptions:
    async def test_a_twitch_outage_is_reported_and_said(self, admin: Admin) -> None:
        admin.listing = HelixError("down")

        await run(TwitchAdmin.subscriptions, admin.interaction())

        assert admin.reported == ["Failed to list Twitch subscriptions"]
        assert admin.sent[0][0] == "Could not reach Twitch to list subscriptions."

    async def test_none_at_all_says_so_in_red(self, admin: Admin) -> None:
        await run(TwitchAdmin.subscriptions, admin.interaction())

        embed = admin.sent[0][1]["embed"]
        assert embed.title == "No Subscriptions"
        assert embed.colour == discord.Color.red()

    async def test_groups_the_display_names_under_each_type_alphabetically(
        self, admin: Admin
    ) -> None:
        admin.listing = listing(
            ("stream.online", "1", "enabled"), ("stream.online", "2", "enabled")
        )
        admin.users["1"] = [user("Zed"), user("Amy")]

        await run(TwitchAdmin.subscriptions, admin.interaction())

        embed = admin.sent[0][1]["embed"]
        assert embed.title == "Subscriptions"
        ((field,),) = [embed.fields]
        assert (field.name, field.value) == ("stream.online", "* Amy\n* Zed")
        assert field.inline is False
        assert admin.asked == [["1", "2"]]

    async def test_a_subscription_that_is_not_enabled_is_labelled_with_its_status(
        self, admin: Admin
    ) -> None:
        """A disabled one delivers nothing and looks the same as a working one otherwise."""
        admin.listing = listing(
            ("stream.online", "1", "enabled"),
            ("stream.online", "2", "authorization_revoked"),
        )
        admin.users["1"] = [user("One")]
        admin.users["2"] = [user("Two")]

        await run(TwitchAdmin.subscriptions, admin.interaction())

        names = [f.name for f in admin.sent[0][1]["embed"].fields]
        assert names == ["stream.online", "stream.online (authorization_revoked)"]

    async def test_a_lookup_failure_skips_that_type_and_carries_on(
        self, admin: Admin
    ) -> None:
        admin.listing = listing(("a.type", "1", "enabled"), ("b.type", "2", "enabled"))
        admin.users["1"] = HelixError("down")
        admin.users["2"] = [user("Two")]

        await run(TwitchAdmin.subscriptions, admin.interaction())

        assert admin.reported == ["Failed to fetch users for a.type"]
        assert [f.name for f in admin.sent[0][1]["embed"].fields] == ["b.type"]

    async def test_types_with_no_broadcaster_or_no_users_get_no_field(
        self, admin: Admin
    ) -> None:
        admin.listing = [
            sub(id="x", type="a.type", user_id="5"),
            *listing(("b.type", "2", "enabled")),
        ]
        admin.users["2"] = []

        await run(TwitchAdmin.subscriptions, admin.interaction())

        assert admin.sent[0][1]["embed"].fields == []
        assert admin.asked == [["2"]]

    async def test_a_lookup_that_returns_only_empty_entries_adds_no_field(
        self, admin: Admin
    ) -> None:
        admin.listing = listing(("a.type", "1", "enabled"))
        admin.users["1"] = [None]

        await run(TwitchAdmin.subscriptions, admin.interaction())

        assert admin.sent[0][1]["embed"].fields == []


class TestSubscribeAndUnsubscribe:
    @pytest.mark.parametrize(
        ("command", "verb"),
        [
            (TwitchAdmin.subscribe, "subscribe"),
            (TwitchAdmin.unsubscribe, "unsubscribe"),
        ],
    )
    async def test_acts_on_the_login_with_its_at_sign_stripped(
        self, command: Any, verb: str, admin: Admin
    ) -> None:
        await run(command, admin.interaction(), "@Bob_1")

        assert admin.acted == [(verb, "Bob_1")]

    @pytest.mark.parametrize(
        "command", [TwitchAdmin.subscribe, TwitchAdmin.unsubscribe]
    )
    async def test_something_that_is_not_a_login_is_refused_without_calling_twitch(
        self, command: Any, admin: Admin
    ) -> None:
        await run(command, admin.interaction(), "not a login!")

        assert admin.acted == []
        assert "is not a Twitch username" in admin.sent[0][0]

    async def test_says_when_it_subscribed(self, admin: Admin) -> None:
        await run(TwitchAdmin.subscribe, admin.interaction(), "bob")

        assert admin.sent == [("", {"content": "Subscribed to bob"})]

    async def test_says_when_there_is_no_such_user(self, admin: Admin) -> None:
        admin.found = False

        await run(TwitchAdmin.subscribe, admin.interaction(), "bob")

        assert admin.sent == [("", {"content": "No Twitch user called bob"})]

    async def test_a_twitch_failure_is_reported_and_said(self, admin: Admin) -> None:
        admin.action_error = HelixError("down")

        await run(TwitchAdmin.subscribe, admin.interaction(), "bob")

        assert admin.reported == ["Failed to subscribe bob"]
        assert admin.sent == [
            ("", {"content": "Could not reach Twitch to subscribe bob"})
        ]

    async def test_the_login_is_escaped_where_it_is_echoed(self, admin: Admin) -> None:
        await run(TwitchAdmin.subscribe, admin.interaction(), "a_b")

        assert admin.sent[0][1]["content"] == f"Subscribed to a{BACKSLASH}_b"

    async def test_says_when_it_unsubscribed(self, admin: Admin) -> None:
        await run(TwitchAdmin.unsubscribe, admin.interaction(), "bob")

        assert admin.sent == [("", {"content": "Unsubscribed from bob"})]

    async def test_unsubscribing_from_nobody_says_no_such_user(
        self, admin: Admin
    ) -> None:
        admin.found = False

        await run(TwitchAdmin.unsubscribe, admin.interaction(), "bob")

        assert admin.sent == [("", {"content": "No Twitch user called bob"})]

    async def test_an_unsubscribe_failure_is_reported_and_said(
        self, admin: Admin
    ) -> None:
        admin.action_error = HelixError("down")

        await run(TwitchAdmin.unsubscribe, admin.interaction(), "bob")

        assert admin.reported == ["Failed to unsubscribe bob"]
        assert (
            "Could not reach Twitch to unsubscribe bob" in admin.sent[0][1]["content"]
        )


class TestMigrateSubscriptions:
    async def test_only_the_owner_may_run_it(self, admin: Admin) -> None:
        await run(TwitchAdmin.migrate_subscriptions, admin.interaction(user_id=5), True)

        assert admin.migrated == []
        assert admin.deferred == []
        assert admin.sent[0][0] == (
            "Only the configured bot owner can migrate Twitch subscriptions."
        )

    async def test_defers_privately_because_helix_takes_longer_than_three_seconds(
        self, admin: Admin
    ) -> None:
        await run(TwitchAdmin.migrate_subscriptions, admin.interaction(), False)

        assert admin.deferred == [True]

    async def test_is_a_dry_run_unless_asked_to_confirm(self, admin: Admin) -> None:
        await run(TwitchAdmin.migrate_subscriptions, admin.interaction(), False)

        assert admin.migrated[0][1] is False
        assert TwitchAdmin.migrate_subscriptions.parameters[0].default is False  # pyright: ignore[reportAttributeAccessIssue]

    async def test_hands_over_the_routes_the_webhook_controller_registered(
        self, admin: Admin
    ) -> None:
        await run(TwitchAdmin.migrate_subscriptions, admin.interaction(), True)

        assert admin.migrated[0][0] is WEBHOOK_PATHS and admin.migrated[0][1] is True

    async def test_replies_with_the_summary_of_what_it_found(
        self, admin: Admin
    ) -> None:
        await run(TwitchAdmin.migrate_subscriptions, admin.interaction(), False)

        ((text, _),) = admin.followed
        assert text.startswith("Nothing to migrate")

    async def test_a_confirmed_run_is_summarised_as_one(self, admin: Admin) -> None:
        admin.outcome.repoint = [sub()]
        admin.outcome.migrated = [sub()]
        admin.outcome.dumped = True

        await run(TwitchAdmin.migrate_subscriptions, admin.interaction(), True)

        assert admin.followed[0][0].startswith("Repointed 1/1.")

    async def test_a_failed_listing_says_nothing_was_touched(
        self, admin: Admin
    ) -> None:
        admin.migrate_error = HelixError("down")

        await run(TwitchAdmin.migrate_subscriptions, admin.interaction(), True)

        assert admin.reported == ["Failed to migrate Twitch subscriptions"]
        assert "nothing was touched" in admin.followed[0][0]
