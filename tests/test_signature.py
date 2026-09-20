import pytest

from services.twitch.signature import get_hmac, get_hmac_message, verify_message


def test_message_is_id_then_timestamp_then_body_with_nothing_between() -> None:
    """Twitch's signed string is exactly that concatenation; a separator here
    would fail every real delivery."""
    assert get_hmac_message("id1", "2026-01-01T00:00:00Z", '{"a":1}') == (
        'id12026-01-01T00:00:00Z{"a":1}'
    )


def test_hmac_matches_the_published_sha256_vector() -> None:
    """The HMAC_SHA256 example that is widely published for this exact key and text."""
    assert get_hmac("key", "The quick brown fox jumps over the lazy dog") == (
        "f7bc83f430538424b13298e6aa6fb143ef4d59a14946175997479dbc2d1a3cd8"
    )


def test_hmac_depends_on_the_secret_and_on_every_byte_of_the_message() -> None:
    base = get_hmac("secret", "message")

    assert get_hmac("secret", "message") == base
    assert get_hmac("other", "message") != base
    assert get_hmac("secret", "messagf") != base
    assert get_hmac("secret", "message ") != base


def test_hmac_encodes_non_ascii_as_utf8() -> None:
    digest = get_hmac("séecret", "mëssage 🎮")

    assert len(digest) == 64
    assert digest == get_hmac("séecret", "mëssage 🎮")
    assert digest != get_hmac("secret", "message")


def test_hmac_of_empty_inputs_is_still_a_digest() -> None:
    assert len(get_hmac("", "")) == 64


def test_matching_signature_verifies() -> None:
    digest = get_hmac("secret", "message")

    assert verify_message(digest, digest)


@pytest.mark.parametrize("presented", ["", "sha256=", "0" * 64, "x", "abc" * 30])
def test_wrong_signature_does_not_verify(presented: str) -> None:
    assert not verify_message(get_hmac("secret", "message"), presented)


def test_a_signature_differing_in_only_the_last_character_does_not_verify() -> None:
    digest = get_hmac("secret", "message")
    flipped = digest[:-1] + ("0" if digest[-1] != "0" else "1")

    assert not verify_message(digest, flipped)


@pytest.mark.parametrize("presented", ["é", "sha256=é" + "a" * 60, "ÿ", "🎮"])
def test_a_non_ascii_signature_is_false_rather_than_an_exception(
    presented: str,
) -> None:
    """A header decodes as latin-1 and compare_digest raises on non-ASCII, which
    would turn a forged request into a 500 instead of a refusal."""
    assert verify_message(get_hmac("secret", "message"), presented) is False
