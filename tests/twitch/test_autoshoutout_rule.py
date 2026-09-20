import pytest

from valmal.twitch.stream.autoshoutout import _Action, _decide

pytestmark = pytest.mark.anyio


class TestDecide:
    """The whole rule for one appearance, with every input in hand."""

    @pytest.mark.parametrize("settled", [True, False])
    @pytest.mark.parametrize("listed", [None, True, False])
    def test_nobody_live_means_ignore_whatever_else_is_true(
        self, settled: bool, listed: bool | None
    ) -> None:
        assert _decide(False, settled, listed) is _Action.IGNORE

    @pytest.mark.parametrize("listed", [None, True, False])
    def test_a_settled_chatter_is_ignored_even_while_live(
        self, listed: bool | None
    ) -> None:
        assert _decide(True, True, listed) is _Action.IGNORE

    def test_live_and_unsettled_and_not_yet_asked_means_look_them_up(self) -> None:
        assert _decide(True, False, None) is _Action.LOOK_UP

    def test_listed_means_shout(self) -> None:
        assert _decide(True, False, True) is _Action.SHOUT

    def test_asked_and_not_listed_means_settle_without_shouting(self) -> None:
        assert _decide(True, False, False) is _Action.SETTLE

    def test_every_combination_has_exactly_one_answer(self) -> None:
        answers = {
            (live, settled, listed): _decide(live, settled, listed)
            for live in (True, False)
            for settled in (True, False)
            for listed in (None, True, False)
        }

        assert len(answers) == 12
        assert set(answers.values()) == set(_Action)
