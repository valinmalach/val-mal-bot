from datetime import UTC, datetime

import discord
import pendulum
import pytest

from tests.bot.audit.support import (
    AUDIT_CHANNEL,
    AVATAR,
    COLORS,
    Audit,
    channel,
    invite,
    message,
    person,
    role,
)
from valmal.bot import audit
from valmal.bot.audit import EMPTY_CONTENT

pytestmark = pytest.mark.anyio

BACKSLASH = chr(92)


class TestMemberJoined:
    async def test_names_the_member_with_their_age_and_id(self, log: Audit) -> None:
        await audit.member_joined(person(id=7, name="val"))

        embed = log.embed
        assert embed.description == "<@7> val"
        assert embed.author.name == "Member Joined"
        assert embed.author.icon_url == AVATAR
        assert embed.thumbnail.url == AVATAR
        assert log.field("**Account Age**") == "2 days"
        assert embed.footer.text == "ID: 7"
        assert embed.colour is not None
        assert embed.colour.value == COLORS["embed_color_join"]

    async def test_the_age_field_is_not_inline(self, log: Audit) -> None:
        await audit.member_joined(person())

        assert log.embed.fields[0].inline is False

    async def test_a_name_that_is_markdown_is_escaped_in_the_description(
        self, log: Audit
    ) -> None:
        await audit.member_joined(person(name="*bold*"))

        assert log.description == f"<@7> {BACKSLASH}*bold{BACKSLASH}*"


class TestMemberLeft:
    async def test_lists_the_roles_they_held_without_everyone(self, log: Audit) -> None:
        leaver = person(name="val")
        leaver.roles = [role(1, "everyone"), role(2), role(3)]

        await audit.member_left(leaver)

        assert log.field("**Roles**") == "<@&2> <@&3>"
        assert log.description == "<@7> val"
        assert log.embed.author.name == "Member Left"
        assert log.embed.footer.text == "ID: 7"

    async def test_a_member_with_no_roles_keeps_the_embeds_height_with_padding(
        self, log: Audit
    ) -> None:
        await audit.member_left(person(name="val"))

        assert log.description == "<@7> val\n\n\n"
        assert log.embed.fields == []

    async def test_a_user_who_is_no_longer_a_member_has_no_roles_to_list(
        self, log: Audit
    ) -> None:
        await audit.member_left(person(discord.User, name="val"))

        assert log.embed.fields == []
        assert log.description.endswith("\n\n\n")

    async def test_a_long_role_list_is_capped_to_what_a_field_accepts(
        self, log: Audit
    ) -> None:
        """Forty-odd roles overflowed a field once and lost the entry and the row delete."""
        leaver = person()
        leaver.roles = [role(1)] + [role(1000 + n) for n in range(200)]

        await audit.member_left(leaver)

        value = log.field("**Roles**") or ""
        assert len(value) == 1024 and value.endswith("...")

    async def test_is_the_danger_colour(self, log: Audit) -> None:
        await audit.member_left(person())

        assert log.embed.colour is not None
        assert log.embed.colour.value == COLORS["embed_color_danger"]


class TestBans:
    async def test_a_ban_is_danger_coloured(self, log: Audit) -> None:
        await audit.banned(person(id=9, name="bad"))

        embed = log.embed
        assert embed.author.name == "User Banned"
        assert embed.description == "<@9> bad"
        assert embed.thumbnail.url == AVATAR
        assert embed.footer.text == "ID: 9"
        assert embed.colour is not None
        assert embed.colour.value == COLORS["embed_color_danger"]

    async def test_an_unban_is_info_coloured(self, log: Audit) -> None:
        await audit.unbanned(person(id=9, name="bad"))

        assert log.embed.author.name == "User Unbanned"
        assert log.embed.colour is not None
        assert log.embed.colour.value == COLORS["embed_color_info"]

    async def test_a_banned_user_who_is_not_a_member_is_named_all_the_same(
        self, log: Audit
    ) -> None:
        await audit.banned(person(discord.User, name="old", discriminator="0042"))

        assert log.description == "<@7> old#0042"


class TestInvites:
    async def test_created_by_someone_names_them(self, log: Audit) -> None:
        inviter = person(id=3)

        await audit.invite_created(invite(inviter=inviter))

        assert log.description == (
            "created_by abc https://discord.gg/abc <#55> <@3> Never"
        )

    async def test_created_by_nobody_uses_the_sentence_without_an_inviter(
        self, log: Audit
    ) -> None:
        await audit.invite_created(invite(inviter=None))

        assert log.description == "created abc https://discord.gg/abc <#55> Never"

    async def test_an_expiry_is_a_relative_discord_timestamp(self, log: Audit) -> None:
        when = datetime(2026, 6, 15, 11, 0, tzinfo=UTC)

        await audit.invite_created(invite(expires_at=when))

        assert log.description.endswith(f"<t:{int(when.timestamp())}:R>")

    async def test_the_author_line_is_the_guild_and_its_icon(self, log: Audit) -> None:
        await audit.invite_created(invite())

        assert log.embed.author.name == "Home"
        assert log.embed.author.icon_url == "https://cdn.example/g.png"

    async def test_an_invite_from_an_unknown_guild_still_logs(self, log: Audit) -> None:
        await audit.invite_created(invite(where=None))

        assert log.embed.author.name == "Unknown Guild"
        assert log.embed.author.icon_url is None

    async def test_deleted_names_the_code_and_is_danger_coloured(
        self, log: Audit
    ) -> None:
        await audit.invite_deleted(invite())

        assert log.description == "deleted abc https://discord.gg/abc"
        assert log.embed.colour is not None
        assert log.embed.colour.value == COLORS["embed_color_danger"]
        assert log.embed.author.name == "Home"

    async def test_an_invite_created_is_info_coloured(self, log: Audit) -> None:
        await audit.invite_created(invite())

        assert log.embed.colour is not None
        assert log.embed.colour.value == COLORS["embed_color_info"]


class TestRoleChanges:
    @pytest.mark.parametrize(
        ("roles", "sentence"),
        [
            ([role(2)], "role_added <@7> <@&2>"),
            ([role(2), role(3)], "roles_added <@7> <@&2> <@&3>"),
        ],
    )
    async def test_one_role_and_several_use_their_own_sentence(
        self, roles: list, sentence: str, log: Audit
    ) -> None:
        await audit.role_added(person(), roles)

        assert log.description == sentence

    @pytest.mark.parametrize(
        ("roles", "sentence"),
        [
            ([role(2)], "role_removed <@7> <@&2>"),
            ([role(2), role(3), role(4)], "roles_removed <@7> <@&2> <@&3> <@&4>"),
        ],
    )
    async def test_removal_has_its_own_pair_of_sentences(
        self, roles: list, sentence: str, log: Audit
    ) -> None:
        await audit.role_removed(person(), roles)

        assert log.description == sentence

    async def test_the_author_is_the_member_and_the_footer_their_id(
        self, log: Audit
    ) -> None:
        await audit.role_added(person(id=7, name="val"), [role(2)])

        assert log.embed.author.name == "val"
        assert log.embed.footer.text == "ID: 7"

    async def test_the_sentence_is_chosen_by_how_many_not_by_which(
        self, log: Audit
    ) -> None:
        await audit.role_added(person(), [])

        assert log.description.startswith("roles_added")


class TestProfileChanges:
    async def test_a_nickname_change_shows_both_sides(self, log: Audit) -> None:
        await audit.nickname_changed(person(), "old", "new")

        assert log.description == "nickname <@7>"
        assert [(f.name, f.value) for f in log.embed.fields] == [
            ("**Before**", "old"),
            ("**After**", "new"),
        ]
        assert log.embed.footer.text == "ID: 7"

    async def test_a_removed_nickname_is_no_text_rather_than_an_empty_field(
        self, log: Audit
    ) -> None:
        await audit.nickname_changed(person(), "", "new")

        assert log.field("**Before**") == EMPTY_CONTENT

    async def test_nicknames_are_escaped_and_capped(self, log: Audit) -> None:
        await audit.nickname_changed(person(), "_a_", "x" * 2000)

        assert log.field("**Before**") == f"{BACKSLASH}_a{BACKSLASH}_"
        assert len(log.field("**After**") or "") == 1024

    async def test_an_avatar_change_shows_the_new_avatar(self, log: Audit) -> None:
        await audit.pfp_changed(person())

        assert log.description == "pfp <@7>"
        assert log.embed.thumbnail.url == AVATAR
        assert log.embed.footer.text == "ID: 7"

    async def test_a_timeout_says_when_it_ends(self, log: Audit) -> None:
        until = pendulum.datetime(2026, 6, 15, 11, 0)

        await audit.timed_out(person(), until)

        assert log.description == f"timed_out <@7> <t:{int(until.timestamp())}:R>"
        assert log.embed.footer.text == "ID: 7"

    async def test_a_lifted_timeout_says_so(self, log: Audit) -> None:
        await audit.timeout_lifted(person())

        assert log.description == "timeout_removed <@7>"
        assert log.embed.footer.text == "ID: 7"


class TestEveryEntryLandsInTheAuditChannel:
    async def test_and_only_there(self, log: Audit) -> None:
        member = person()
        entries = [
            audit.member_joined(member),
            audit.member_left(member),
            audit.banned(member),
            audit.unbanned(member),
            audit.invite_created(invite()),
            audit.invite_deleted(invite()),
            audit.role_added(member, [role(2)]),
            audit.role_removed(member, [role(2)]),
            audit.nickname_changed(member, "a", "b"),
            audit.pfp_changed(member),
            audit.timed_out(member, pendulum.now()),
            audit.timeout_lifted(member),
            audit.message_edited(message(), "before"),
            audit.pin_changed(message()),
            audit.message_deleted(
                content="c",
                attachments=[],
                message_id=1,
                author=member,
                deleted_by=None,
                channel=channel(),
            ),
            audit.message_deleted_uncached(
                content=None, message_id=1, deleted_by=None, channel=channel()
            ),
            audit.bulk_deleted(count=2, deleted_by=None, channel=channel()),
        ]

        for entry in entries:
            await entry

        assert len(log.sent) == len(entries)
        assert {where for _, where in log.sent} == {AUDIT_CHANNEL}
