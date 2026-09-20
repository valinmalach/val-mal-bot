from typing import Any

import discord
import pytest

from services.twitch.helix import HelixError
from tests.cogs.twitch_admin_world import Admin, listing, run, user
from tests.twitch_migrate.support import sub
from valmal.bot.cogs.twitch_admin import TwitchAdmin

pytestmark = pytest.mark.anyio

BACKSLASH = chr(92)
OWNER = 99


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

    async def test_markup_in_a_display_name_is_escaped_where_it_is_listed(
        self, admin: Admin
    ) -> None:
        """A name is whatever its owner typed: an underscore alone would italicise the list."""
        admin.listing = listing(("stream.online", "1", "enabled"))
        admin.users["1"] = [user("a_b*c"), user("[x](http://evil.example)")]

        await run(TwitchAdmin.subscriptions, admin.interaction())

        ((field,),) = [admin.sent[0][1]["embed"].fields]
        assert field.value == (
            f"* {BACKSLASH}[x](http://evil.example)\n* a{BACKSLASH}_b{BACKSLASH}*c"
        )

    async def test_a_list_too_long_for_one_field_is_split_across_fields(
        self, admin: Admin
    ) -> None:
        """Discord refuses the whole message over 1024 characters in one field's value."""
        names = [f"Streamer{number:03d}" for number in range(200)]
        admin.listing = listing(("stream.online", "1", "enabled"))
        admin.users["1"] = [user(name) for name in reversed(names)]

        await run(TwitchAdmin.subscriptions, admin.interaction())

        fields = admin.sent[0][1]["embed"].fields
        assert len(fields) > 1
        assert all(len(field.value) <= 1024 for field in fields)
        assert [field.name for field in fields] == [
            "stream.online",
            *["stream.online (continued)"] * (len(fields) - 1),
        ]
        listed = "\n".join(field.value for field in fields).split("\n")
        assert listed == [f"* {name}" for name in names]

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
