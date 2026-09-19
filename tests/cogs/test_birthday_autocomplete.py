from typing import Any

import pendulum
import pytest

from cogs.birthday import Birthday

pytestmark = pytest.mark.anyio


async def suggest(current: str) -> list[str]:
    autocomplete: Any = Birthday.__new__(Birthday).timezone_autocomplete
    choices = await autocomplete(None, current)
    assert all(choice.name == choice.value for choice in choices)
    return [choice.value for choice in choices]


class TestTimezoneAutocomplete:
    async def test_a_narrow_match_finds_the_zone(self) -> None:
        assert "Europe/London" in await suggest("london")

    async def test_matching_ignores_case(self) -> None:
        assert await suggest("EUROPE/LONDON") == await suggest("europe/london")

    async def test_matches_anywhere_in_the_name_not_only_the_start(self) -> None:
        found = await suggest("york")

        assert "America/New_York" in found

    async def test_never_offers_more_than_discord_will_show(self) -> None:
        assert len(await suggest("")) == 25
        assert len(await suggest("a")) == 25

    async def test_nothing_matching_is_an_empty_list(self) -> None:
        assert await suggest("no-such-zone-anywhere") == []

    async def test_every_suggestion_is_a_zone_the_command_will_accept(self) -> None:
        known = pendulum.timezones()

        assert all(zone in known for zone in await suggest("america"))

    async def test_a_broad_match_is_the_alphabetical_start_not_an_arbitrary_25(
        self,
    ) -> None:
        """pendulum.timezones() is a set, so its order differs between processes."""
        found = await suggest("america")

        expected = sorted(z for z in pendulum.timezones() if "america" in z.lower())
        assert found == expected[:25]

    async def test_the_same_input_gives_the_same_answer_every_time(self) -> None:
        first = await suggest("a")

        for _ in range(5):
            assert await suggest("a") == first

    async def test_an_empty_input_starts_at_the_top_of_the_alphabet(self) -> None:
        assert await suggest("") == sorted(pendulum.timezones())[:25]

    async def test_a_hostile_input_is_only_ever_compared_never_echoed(self) -> None:
        assert await suggest("@everyone" * 20) == []
