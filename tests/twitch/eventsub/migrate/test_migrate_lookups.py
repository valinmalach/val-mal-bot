import json

import pytest

from tests.twitch.eventsub.migrate.support import CURRENT, MigrateWorld, sub
from valmal.twitch.client.helix import HelixError
from valmal.twitch.eventsub import migrate

pytestmark = pytest.mark.anyio


class TestUsable:
    @pytest.mark.parametrize("value", ["1", "0", "141981764", "9" * 30])
    def test_a_run_of_ascii_digits_could_be_a_user_id(self, value: str) -> None:
        assert migrate._usable(value)

    @pytest.mark.parametrize(
        "value",
        ["", " ", "12a", "a12", "-1", "+1", "1.5", "1 2", "1,2", "\n", "0x10"],
    )
    def test_anything_else_could_not(self, value: str) -> None:
        assert not migrate._usable(value)

    @pytest.mark.parametrize(
        "value", [chr(0x0661) + chr(0x0662), chr(0x00B2), chr(0xFF11) + chr(0xFF12)]
    )
    def test_digits_that_are_not_ascii_are_refused_though_isdigit_accepts_some(
        self, value: str
    ) -> None:
        """Arabic-Indic, superscript and full-width digits: Helix parses none of them."""
        assert not migrate._usable(value)


class TestSome:
    def test_quotes_each_value(self) -> None:
        assert migrate._some(["a", "b"]) == "'a', 'b'"

    def test_names_up_to_the_limit_without_a_remainder(self) -> None:
        values = [str(n) for n in range(migrate._NAMED_LIMIT)]

        assert "more" not in migrate._some(values)

    def test_says_how_many_it_did_not_name(self) -> None:
        values = [str(n) for n in range(migrate._NAMED_LIMIT + 3)]

        text = migrate._some(values)

        assert text.endswith(" and 3 more")
        assert "'9'" in text and "'10'" not in text

    def test_nothing_is_an_empty_string(self) -> None:
        assert migrate._some([]) == ""


class TestLogins:
    async def test_maps_each_id_to_its_login(self, world: MigrateWorld) -> None:
        world.users = {"1": "one", "2": "two"}

        result = await migrate._logins(
            [sub(broadcaster_user_id="1"), sub(broadcaster_user_id="2")]
        )

        assert result == {"1": "one", "2": "two"}
        assert world.notified == []

    async def test_asks_once_for_every_distinct_id(self, world: MigrateWorld) -> None:
        await migrate._logins(
            [
                sub(id="a", broadcaster_user_id="2"),
                sub(id="b", broadcaster_user_id="2"),
                sub(id="c", broadcaster_user_id="1"),
            ]
        )

        assert world.asked_for == [["1", "2"]]

    async def test_an_id_helix_does_not_know_is_simply_absent(
        self, world: MigrateWorld
    ) -> None:
        world.users = {"1": "one"}

        result = await migrate._logins(
            [sub(broadcaster_user_id="1"), sub(broadcaster_user_id="2")]
        )

        assert result == {"1": "one"}

    async def test_nothing_to_look_up_asks_helix_nothing(
        self, world: MigrateWorld
    ) -> None:
        assert await migrate._logins([]) == {}

        assert world.asked_for == []

    async def test_a_value_that_cannot_be_an_id_is_kept_out_of_the_request_and_named(
        self, world: MigrateWorld
    ) -> None:
        """One malformed id is a 400 for the whole batch, which cost every login."""
        world.users = {"1": "one"}

        result = await migrate._logins(
            [sub(broadcaster_user_id="1"), sub(id="b", broadcaster_user_id="not-an-id")]
        )

        assert world.asked_for == [["1"]]
        assert result == {"1": "one"}
        ((text, _),) = world.notified
        assert "1 subscription condition value(s) cannot be a Twitch user id" in text
        assert "'not-an-id'" in text
        assert "still carries every condition in full" in text

    async def test_when_nothing_is_usable_there_is_no_request_at_all(
        self, world: MigrateWorld
    ) -> None:
        result = await migrate._logins([sub(broadcaster_user_id="abc")])

        assert result == {}
        assert world.asked_for == []
        assert len(world.notified) == 1

    async def test_the_unusable_notice_is_keyed_on_the_values_it_is_about(
        self, world: MigrateWorld
    ) -> None:
        """Keyed on the text alone, a different set inside the window would be held back."""
        await migrate._logins([sub(broadcaster_user_id="aaa")])
        await migrate._logins([sub(broadcaster_user_id="bbb")])

        first, second = (key for _, key in world.notified)
        assert first != second

    async def test_a_value_holding_a_comma_does_not_collide_with_two_values(
        self, world: MigrateWorld
    ) -> None:
        """['a,b'] and ['a', 'b'] joined on a comma are the same key."""
        await migrate._logins([sub(broadcaster_user_id="a,b")])
        await migrate._logins(
            [sub(id="1", broadcaster_user_id="a"), sub(id="2", broadcaster_user_id="b")]
        )

        first, second = (key or "" for _, key in world.notified)
        assert first != second
        assert json.loads(first.removeprefix("migrate-unusable-ids:")) == ["a,b"]

    async def test_a_failed_lookup_costs_the_names_and_not_the_dump(
        self, world: MigrateWorld
    ) -> None:
        world.users_error = HelixError("400 bad", status=400)

        result = await migrate._logins([sub(broadcaster_user_id="1")])

        assert result == {}
        ((text, key),) = world.notified
        assert "Could not resolve 1 Twitch id(s) to logins" in text
        assert "400 bad" in text and "The ids sent were: '1'" in text
        assert key == 'migrate-dump-logins:["1"]'

    async def test_the_failure_notice_names_no_more_than_the_limit(
        self, world: MigrateWorld
    ) -> None:
        world.users_error = HelixError("down")
        many = [sub(id=f"s{n}", broadcaster_user_id=str(n)) for n in range(25)]

        await migrate._logins(many)

        ((text, _),) = world.notified
        assert "Could not resolve 25 Twitch id(s)" in text
        assert text.count("'") == 2 * migrate._NAMED_LIMIT
        assert "and 15 more" in text

    async def test_an_unset_raid_half_is_never_looked_up(
        self, world: MigrateWorld
    ) -> None:
        raid = sub(
            type="channel.raid",
            from_broadcaster_user_id="",
            to_broadcaster_user_id="222",
        )

        await migrate._logins([raid])

        assert world.asked_for == [["222"]]
        assert world.notified == []


class TestExistsAt:
    async def test_true_when_the_same_subscription_is_on_that_callback(
        self, world: MigrateWorld
    ) -> None:
        world.calls.append(("delete", "x"))
        world.after = [sub(id="new", callback=CURRENT)]

        assert await migrate._exists_at(sub(), CURRENT) is True

    async def test_false_when_it_is_on_another_callback(
        self, world: MigrateWorld
    ) -> None:
        world.calls.append(("delete", "x"))
        world.after = [sub(id="new", callback="https://elsewhere.example/x")]

        assert await migrate._exists_at(sub(), CURRENT) is False

    @pytest.mark.parametrize(
        "other",
        [
            {"type": "stream.offline"},
            {"version": "2"},
            {"broadcaster_user_id": "999"},
        ],
    )
    async def test_false_when_anything_that_makes_it_the_same_one_differs(
        self, other: dict[str, str], world: MigrateWorld
    ) -> None:
        world.calls.append(("delete", "x"))
        world.after = [sub(id="new", callback=CURRENT, **other)]

        assert await migrate._exists_at(sub(), CURRENT) is False

    async def test_a_condition_with_an_extra_key_is_a_different_subscription(
        self, world: MigrateWorld
    ) -> None:
        world.calls.append(("delete", "x"))
        world.after = [sub(callback=CURRENT, broadcaster_user_id="111", reward_id="r")]

        assert await migrate._exists_at(sub(), CURRENT) is False

    async def test_finds_it_among_others(self, world: MigrateWorld) -> None:
        world.calls.append(("delete", "x"))
        world.after = [
            sub(id="a", type="stream.offline", callback=CURRENT),
            sub(id="b", callback=CURRENT),
        ]

        assert await migrate._exists_at(sub(), CURRENT) is True

    async def test_nothing_listed_is_not_present(self, world: MigrateWorld) -> None:
        world.calls.append(("delete", "x"))

        assert await migrate._exists_at(sub(), CURRENT) is False

    async def test_a_failing_lookup_is_false_because_unproven_is_not_present(
        self, world: MigrateWorld
    ) -> None:
        """Over-reporting a loss costs a check; under-reporting one costs a subscription."""
        world.calls.append(("delete", "x"))
        world.after = HelixError("down")

        assert await migrate._exists_at(sub(), CURRENT) is False
