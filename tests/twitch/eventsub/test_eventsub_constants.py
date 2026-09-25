from valmal.twitch.eventsub import constants


def test_the_twitch_header_names_are_the_ones_twitch_sends() -> None:
    assert constants.TWITCH_MESSAGE_ID == "Twitch-Eventsub-Message-Id"
    assert constants.TWITCH_MESSAGE_TYPE == "Twitch-Eventsub-Message-Type"
    assert constants.TWITCH_MESSAGE_TIMESTAMP == "Twitch-Eventsub-Message-Timestamp"
    assert constants.TWITCH_MESSAGE_SIGNATURE == "Twitch-Eventsub-Message-Signature"
    assert constants.HMAC_PREFIX == "sha256="
