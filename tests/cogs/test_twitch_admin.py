import discord
import pytest

from tests.cogs.twitch_admin_world import Admin, run
from tests.twitch_migrate.support import sub
from valmal.bot.cogs import twitch_admin
from valmal.bot.cogs.twitch_admin import TwitchAdmin, _login, _refuse_login
from valmal.db.models.enums import TokenType
from valmal.twitch.client.helix import HelixError
from valmal.twitch.eventsub.router import WEBHOOK_PATHS

pytestmark = pytest.mark.anyio

BACKSLASH = chr(92)
OWNER = 99


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
