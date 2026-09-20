import pytest

from services.config import _field_name, safe_format


class TestFieldName:
    @pytest.mark.parametrize(
        ("field", "name"),
        [
            ("name", "name"),
            ("name!r", "name"),
            ("name:>5", "name"),
            ("name!s:>5", "name"),
            ("obj.attr", "obj"),
            ("items[0]", "items"),
            ("obj.attr.deeper", "obj"),
            ("items[0].x", "items"),
            ("  name  ", "name"),
            ("", ""),
            (":x", ""),
            ("0", "0"),
        ],
    )
    def test_the_value_a_field_reads_without_conversion_spec_or_access(
        self, field: str, name: str
    ) -> None:
        assert _field_name(field) == name


class TestSafeFormat:
    def test_fills_a_field_that_was_passed(self, notices: list) -> None:
        assert safe_format("hello {name}", {"name": "bob"}) == "hello bob"
        assert not notices

    def test_a_field_nobody_passed_is_left_as_written(self, notices: list) -> None:
        assert safe_format("hi {name} {other}", {"name": "bob"}) == "hi bob {other}"
        assert not notices

    def test_no_values_means_every_field_is_literal(self, notices: list) -> None:
        assert safe_format("{a} and {b}", {}) == "{a} and {b}"
        assert not notices

    @pytest.mark.parametrize("text", ["literal {} braces", "literal {0} braces"])
    def test_positional_fields_are_never_filled(self, text: str, notices: list) -> None:
        assert safe_format(text, {"name": "x"}) == text
        assert not notices

    def test_a_doubled_brace_is_str_formats_own_escape(self, notices: list) -> None:
        assert safe_format("{{name}}", {"name": "x"}) == "{name}"
        assert safe_format("{{{name}}}", {"name": "x"}) == "{x}"
        assert not notices

    def test_format_spec_and_conversion_still_work(self, notices: list) -> None:
        assert safe_format("{name:>5}|{n:05d}", {"name": "ab", "n": 42}) == (
            "   ab|00042"
        )
        assert safe_format("{name!r}", {"name": "ab"}) == "'ab'"
        assert safe_format("{a:{b}}", {"a": 1, "b": 5}) == "    1"

    def test_indexing_and_attribute_access_on_a_value(self, notices: list) -> None:
        assert safe_format("{items[0]} {a[0]}", {"items": [1], "a": "xyz"}) == "1 x"
        assert not notices

    def test_an_empty_string_stays_empty(self, notices: list) -> None:
        assert safe_format("", {"a": 1}) == ""

    def test_text_without_braces_is_untouched(self, notices: list) -> None:
        assert safe_format("plain text, 100% sure", {"a": 1}) == "plain text, 100% sure"

    def test_a_value_containing_braces_is_not_reformatted(self, notices: list) -> None:
        """The value is inserted, not re-read as a template."""
        assert safe_format("{name}", {"name": "{other}"}) == "{other}"

    def test_a_value_is_stringified(self, notices: list) -> None:
        assert safe_format(
            "{n} {f} {b} {x}", {"n": 3, "f": 1.5, "b": True, "x": None}
        ) == ("3 1.5 True None")

    @pytest.mark.parametrize("reserved", ["channel", "role"])
    def test_a_channel_or_role_placeholder_left_by_render_is_never_a_field(
        self, reserved: str, notices: list
    ) -> None:
        """render() reserves that shape, so it must not be read as a field named
        `channel`/`role` because the caller also passed a value called that."""
        text = f"{{{reserved}:promo}}"

        assert safe_format(text, {reserved: "x"}) == text
        assert not notices

    def test_a_doubled_channel_placeholder_is_already_a_literal(
        self, notices: list
    ) -> None:
        assert safe_format("{{role:promo}}", {"role": "x"}) == "{role:promo}"

    @pytest.mark.parametrize(
        "text",
        [
            "{name",
            "name}",
            "{name} }",
            "{ name }",
            "{n:d}",
            "{a!x}",
            "{ a}",
            "{a }",
        ],
    )
    def test_text_that_cannot_be_formatted_goes_out_as_written_with_a_notice(
        self, text: str, notices: list
    ) -> None:
        values = {"name": "x", "n": "notanint", "a": 1}

        assert safe_format(text, values) == text
        assert len(notices) == 1

    def test_an_attribute_that_does_not_exist_degrades_rather_than_raising(
        self, notices: list
    ) -> None:
        assert safe_format("{obj.attr}", {"obj": "str"}) == "{obj.attr}"
        assert len(notices) == 1

    def test_an_index_out_of_range_degrades_rather_than_raising(
        self, notices: list
    ) -> None:
        assert safe_format("{items[9]}", {"items": [1]}) == "{items[9]}"
        assert len(notices) == 1

    def test_the_notice_is_keyed_on_the_whole_text_not_a_prefix(
        self, notices: list
    ) -> None:
        """Two rows sharing an opening line must not be held back as each other."""
        safe_format("same opening {a", {"a": 1})
        safe_format("same opening {b", {"b": 1})

        assert [key for _, key in notices] == [
            "template-unformattable:same opening {a",
            "template-unformattable:same opening {b",
        ]

    def test_the_notice_quotes_at_most_two_hundred_characters_of_the_text(
        self, notices: list
    ) -> None:
        safe_format("{" + "x" * 500, {"x": 1})

        text, _ = notices[0]
        # The text is "{" and 500 x's, so two hundred characters is "{" and 199.
        assert "{" + "x" * 199 in text
        assert "x" * 200 not in text

    def test_indexing_a_value_that_cannot_be_indexed_sends_the_text_as_written(
        self, notices: list
    ) -> None:
        """A TypeError, unlike the KeyError and IndexError a missing field raises."""
        assert safe_format("count is {n[0]}", {"n": 5}) == "count is {n[0]}"

        assert len(notices) == 1
        assert "subscriptable" in notices[0][0]

    def test_a_format_spec_the_value_does_not_support_sends_the_text_as_written(
        self, notices: list
    ) -> None:
        assert safe_format("{n:>+d}", {"n": "text"}) == "{n:>+d}"

        assert len(notices) == 1
