from datetime import timedelta
from types import SimpleNamespace
from typing import Any

import pytest

from tests.bot.audit.support import AVATAR, person, role
from tests.bot.cogs.events_world import NOW, EventsWorld, cog
from valmal.bot import audit

pytestmark = pytest.mark.anyio


def joiner(count: int | None = 1, **kwargs: Any) -> Any:
    member = person(**kwargs)
    member.guild = SimpleNamespace(member_count=count)
    return member


class TestMemberJoin:
    async def test_welcomes_them_in_the_welcome_channel(self, ev: EventsWorld) -> None:
        member = joiner(3, id=7, name="val")

        await cog(ev).on_member_join(member)

        ((embed, channel),) = ev.sent_embeds
        assert channel == 900
        assert embed.description == "welcome <@7>"
        assert embed.author.name == "val"
        assert embed.author.icon_url == AVATAR
        assert embed.image.url == AVATAR
        assert embed.footer.text == "3rd member"
        assert embed.colour is not None and embed.colour.value == 1
        assert embed.timestamp == NOW

    @pytest.mark.parametrize(
        ("count", "text"),
        [(1, "1st"), (2, "2nd"), (3, "3rd"), (4, "4th"), (11, "11th"), (112, "112th")],
    )
    async def test_the_footer_counts_them_in_ordinals(
        self, count: int, text: str, ev: EventsWorld
    ) -> None:
        await cog(ev).on_member_join(joiner(count))

        assert ev.sent_embeds[0][0].footer.text == f"{text} member"

    async def test_a_guild_sent_without_a_count_just_has_no_footer(
        self, ev: EventsWorld
    ) -> None:
        await cog(ev).on_member_join(joiner(None))

        assert ev.sent_embeds[0][0].footer.text is None

    async def test_a_legacy_discriminator_is_shown_in_the_author_line(
        self, ev: EventsWorld
    ) -> None:
        await cog(ev).on_member_join(joiner(1, name="val", discriminator="1234"))

        assert ev.sent_embeds[0][0].author.name == "val#1234"

    async def test_logs_the_join_then_records_the_user_in_that_order(
        self, ev: EventsWorld
    ) -> None:
        member = joiner(1, id=7, name="val")

        await cog(ev).on_member_join(member)

        assert ev.order == ["send", "member_joined", "username"]
        assert ev.calls("member_joined") == [((member,), {})]
        assert ev.usernames == [(7, "val")]

    async def test_a_failed_database_write_is_reported_by_name_and_id(
        self, ev: EventsWorld
    ) -> None:
        ev.fail.add("username")

        await cog(ev).on_member_join(joiner(1, id=7, name="val"))

        assert ev.reported == ["Failed to insert user val (7)"]

    async def test_a_failed_audit_entry_leaves_the_user_unrecorded_for_the_gateway_floor(
        self, ev: EventsWorld, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """The audit send raises to on_error, which is the floor for a listener."""

        async def refuse(member: object) -> None:
            raise RuntimeError("audit channel gone")

        monkeypatch.setattr(audit, "member_joined", refuse)

        with pytest.raises(RuntimeError):
            await cog(ev).on_member_join(joiner(1))

        assert ev.usernames == []


class TestMemberLeaves:
    def payload(self, **kwargs: Any) -> Any:
        return SimpleNamespace(user=person(**kwargs))

    async def test_says_goodbye_logs_and_removes_the_row_in_that_order(
        self, ev: EventsWorld
    ) -> None:
        payload = self.payload(id=7, name="val")

        await cog(ev).on_raw_member_remove(payload)

        ((embed, channel),) = ev.sent_embeds
        assert channel == 900
        assert embed.description == "bye <@7>"
        assert embed.image.url == AVATAR
        assert embed.colour is not None and embed.colour.value == 2
        assert ev.order == ["send", "member_left", "delete_user"]
        assert ev.calls("member_left") == [((payload.user,), {})]
        assert ev.removed_users == [7]

    async def test_there_is_no_member_count_footer_on_a_goodbye(
        self, ev: EventsWorld
    ) -> None:
        await cog(ev).on_raw_member_remove(self.payload())

        assert ev.sent_embeds[0][0].footer.text is None

    async def test_a_failed_row_removal_is_reported(self, ev: EventsWorld) -> None:
        ev.fail.add("delete_user")

        await cog(ev).on_raw_member_remove(self.payload(id=7, name="val"))

        assert ev.reported == ["Failed to remove user val (7)"]


class TestMemberUpdate:
    def pair(self) -> tuple[Any, Any]:
        before, after = person(id=7, name="val"), person(id=7, name="val")
        after.roles = list(before.roles)
        before.nick = after.nick = None
        before.timed_out_until = after.timed_out_until = None
        return before, after

    async def test_nothing_changed_logs_nothing(self, ev: EventsWorld) -> None:
        before, after = self.pair()

        await cog(ev).on_member_update(before, after)

        assert ev.audit == []

    async def test_a_new_avatar_is_logged(self, ev: EventsWorld) -> None:
        before, after = self.pair()
        after.avatar = SimpleNamespace(url="https://cdn.example/new.png")

        await cog(ev).on_member_update(before, after)

        assert ev.calls("pfp_changed") == [((after,), {})]

    async def test_a_role_gained_is_logged_with_only_the_new_roles(
        self, ev: EventsWorld
    ) -> None:
        before, after = self.pair()
        kept, gained = role(1), role(2)
        before.roles, after.roles = [kept], [kept, gained]

        await cog(ev).on_member_update(before, after)

        assert ev.names == ["role_added"]
        assert ev.calls("role_added") == [((after, [gained]), {})]

    async def test_a_role_lost_is_logged_with_only_the_lost_roles(
        self, ev: EventsWorld
    ) -> None:
        before, after = self.pair()
        kept, lost = role(1), role(2)
        before.roles, after.roles = [kept, lost], [kept]

        await cog(ev).on_member_update(before, after)

        assert ev.calls("role_removed") == [((after, [lost]), {})]

    async def test_a_swap_logs_both_the_gain_and_the_loss(
        self, ev: EventsWorld
    ) -> None:
        before, after = self.pair()
        old, new = role(1), role(2)
        before.roles, after.roles = [old], [new]

        await cog(ev).on_member_update(before, after)

        assert ev.names == ["role_added", "role_removed"]

    async def test_a_nickname_change_shows_both_sides(self, ev: EventsWorld) -> None:
        before, after = self.pair()
        before.nick, after.nick = "old", "new"

        await cog(ev).on_member_update(before, after)

        assert ev.calls("nickname_changed") == [((after, "old", "new"), {})]

    async def test_no_nickname_falls_back_to_the_account_name_on_each_side(
        self, ev: EventsWorld
    ) -> None:
        before, after = self.pair()
        before.name, after.name = "before_name", "after_name"
        before.nick, after.nick = None, "new"

        await cog(ev).on_member_update(before, after)
        before.nick, after.nick = "old", None
        await cog(ev).on_member_update(before, after)

        assert [a[1:] for a, _ in ev.calls("nickname_changed")] == [
            ("before_name", "new"),
            ("old", "after_name"),
        ]

    async def test_a_new_timeout_is_logged_with_when_it_ends(
        self, ev: EventsWorld
    ) -> None:
        before, after = self.pair()
        until = NOW + timedelta(hours=1)
        after.timed_out_until = until

        await cog(ev).on_member_update(before, after)

        ((args, _),) = ev.calls("timed_out")
        assert args[0] is after and args[1] == until

    async def test_a_lifted_timeout_is_logged(self, ev: EventsWorld) -> None:
        before, after = self.pair()
        before.timed_out_until = NOW + timedelta(hours=1)

        await cog(ev).on_member_update(before, after)

        assert ev.calls("timeout_lifted") == [((after,), {})]

    async def test_a_timeout_that_ran_out_by_itself_is_not_a_lift_by_anyone(
        self, ev: EventsWorld
    ) -> None:
        """Both ends are compared with now: an expired timeout is not one."""
        before, after = self.pair()
        before.timed_out_until = NOW - timedelta(hours=1)

        await cog(ev).on_member_update(before, after)

        assert ev.audit == []

    async def test_a_timeout_that_expired_between_the_two_snapshots_counts_as_lifted(
        self, ev: EventsWorld
    ) -> None:
        before, after = self.pair()
        before.timed_out_until = NOW + timedelta(seconds=1)
        after.timed_out_until = NOW - timedelta(seconds=1)

        await cog(ev).on_member_update(before, after)

        assert ev.names == ["timeout_lifted"]

    async def test_an_expired_timeout_replaced_by_a_live_one_is_new(
        self, ev: EventsWorld
    ) -> None:
        before, after = self.pair()
        before.timed_out_until = NOW - timedelta(days=1)
        after.timed_out_until = NOW + timedelta(hours=1)

        await cog(ev).on_member_update(before, after)

        assert ev.names == ["timed_out"]

    async def test_a_timeout_that_is_only_extended_says_nothing_more(
        self, ev: EventsWorld
    ) -> None:
        before, after = self.pair()
        before.timed_out_until = NOW + timedelta(hours=1)
        after.timed_out_until = NOW + timedelta(hours=2)

        await cog(ev).on_member_update(before, after)

        assert ev.audit == []

    async def test_a_timeout_ending_exactly_now_is_over(self, ev: EventsWorld) -> None:
        assert cog(ev)._is_currently_timed_out(NOW) is False
        assert cog(ev)._is_currently_timed_out(NOW.add(microseconds=1)) is True
        assert cog(ev)._is_currently_timed_out(None) is False

    async def test_everything_changing_at_once_is_logged_in_a_fixed_order(
        self, ev: EventsWorld
    ) -> None:
        before, after = self.pair()
        after.avatar = SimpleNamespace(url="https://cdn.example/new.png")
        before.roles, after.roles = [role(1)], [role(2)]
        after.nick = "new"
        after.timed_out_until = NOW + timedelta(hours=1)

        await cog(ev).on_member_update(before, after)

        assert ev.names == [
            "pfp_changed",
            "role_added",
            "role_removed",
            "nickname_changed",
            "timed_out",
        ]


class TestPassThroughs:
    async def test_a_ban_is_logged_for_the_user_not_the_guild(
        self, ev: EventsWorld
    ) -> None:
        user = person()

        await cog(ev).on_member_ban(object(), user)  # pyright: ignore[reportArgumentType]

        assert ev.calls("banned") == [((user,), {})]

    async def test_an_unban_is_logged(self, ev: EventsWorld) -> None:
        user = person()

        await cog(ev).on_member_unban(object(), user)  # pyright: ignore[reportArgumentType]

        assert ev.calls("unbanned") == [((user,), {})]

    async def test_invites_are_logged_when_made_and_when_removed(
        self, ev: EventsWorld
    ) -> None:
        invite: Any = object()

        await cog(ev).on_invite_create(invite)
        await cog(ev).on_invite_delete(invite)

        assert ev.names == ["invite_created", "invite_deleted"]

    async def test_a_failed_command_is_logged_with_its_context_and_error(
        self, ev: EventsWorld
    ) -> None:
        ctx, error = object(), ValueError("nope")

        await cog(ev).on_command_error(ctx, error)  # pyright: ignore[reportArgumentType]

        assert ev.calls("command_failed") == [((ctx, error), {})]
