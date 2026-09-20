"""_target is what a chatter typed, on its way into a line the bot says.

The bot's own lines come back in through the chat webhook and are dispatched, and it
holds a moderator badge. A target that begins with `!` therefore lets any chatter
post a command in the bot's voice: a template beginning with {target} would run a
mod-only `!so` for someone who is not a mod.
"""

import itertools

import pytest

from valmal.twitch.eventsub.commands import _MAX_TARGET, _target

# Characters that render as nothing (Cc, Cf) mixed with the ones that start a command.
ZERO_WIDTH = "\u200b"  # ZERO WIDTH SPACE
JOINER = "\u200d"  # ZERO WIDTH JOINER
RLO = "\u202e"  # RIGHT-TO-LEFT OVERRIDE
NUL = "\x00"


class TestPlainInput:
    @pytest.mark.parametrize(
        ("args", "expected"),
        [
            ("bob", "bob"),
            ("bob and some more words", "bob"),
            ("  bob", "bob"),
            ("   ", ""),
            (" \t  bob  extra", "bob"),
            ("bob\tevil", "bob"),
            ("bob\nevil", "bob"),
            ("", ""),
            ("@bob", "bob"),
            ("!bob", "bob"),
            ("@!bob", "bob"),
            ("!@!@bob", "bob"),
            ("@@@bob", "bob"),
            ("bo!b", "bo!b"),
            ("bo@b", "bo@b"),
            ("bob!", "bob!"),
            ("!", ""),
            ("@", ""),
            ("!!!", ""),
        ],
    )
    def test_takes_the_first_word_without_a_leading_at_or_bang(
        self, args: str, expected: str
    ) -> None:
        assert _target(args) == expected

    def test_a_display_name_in_another_script_is_kept_whole(self) -> None:
        """A target is a person as the chatter wrote them, and a display name can be
        Japanese or Korean; keeping only [A-Za-z0-9_] would erase one."""
        assert _target("チャッター") == "チャッター"
        assert _target("한글") == "한글"

    def test_bounded_because_a_chat_line_is_five_hundred_characters(self) -> None:
        assert _target("x" * 500) == "x" * _MAX_TARGET
        assert _MAX_TARGET == 50

    def test_exactly_at_the_bound_is_kept_whole(self) -> None:
        assert _target("y" * _MAX_TARGET) == "y" * _MAX_TARGET


class TestInvisibleCharacters:
    @pytest.mark.parametrize(
        ("args", "expected"),
        [
            (f"bo{ZERO_WIDTH}b", "bob"),
            (f"bo{JOINER}b", "bob"),
            (f"{RLO}bob", "bob"),
            (f"bo{NUL}b", "bob"),
            ("bo\tb", "bo"),
            ("bo\nb", "bo"),
            (f"{ZERO_WIDTH}{ZERO_WIDTH}", ""),
        ],
    )
    def test_control_and_format_characters_are_removed(
        self, args: str, expected: str
    ) -> None:
        """They are invisible, so they can reorder or hide what the rest of the line
        says once it reaches chat, and no name needs one."""
        assert _target(args) == expected

    @pytest.mark.parametrize(
        "args",
        [
            f"{ZERO_WIDTH}!so",
            f"@{ZERO_WIDTH}!so",
            f"{ZERO_WIDTH}@!so",
            f"!{ZERO_WIDTH}!{ZERO_WIDTH}!so",
            f"{RLO}!so",
            f"{NUL}!so",
            f"{JOINER}@evil",
        ],
    )
    def test_an_invisible_character_cannot_hide_a_leading_bang_from_the_strip(
        self, args: str
    ) -> None:
        """Stripping `!` before removing what is invisible left `!so` at the front of
        the target once the invisible character went."""
        target = _target(args)

        assert not target.startswith(("!", "@")), repr(target)

    def test_the_command_a_chatter_tried_to_smuggle_is_left_as_a_plain_word(
        self,
    ) -> None:
        assert _target(f"{ZERO_WIDTH}!so") == "so"

    def test_no_mix_of_command_prefixes_and_invisible_characters_can_start_a_command(
        self,
    ) -> None:
        alphabet = ["!", "@", ZERO_WIDTH, JOINER, RLO, NUL, "a"]

        for length in range(1, 6):
            for combo in itertools.product(alphabet, repeat=length):
                target = _target("".join(combo))

                assert not target.startswith(("!", "@")), (combo, target)
