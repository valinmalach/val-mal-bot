from enum import Enum

TWITCH_MESSAGE_ID = "Twitch-Eventsub-Message-Id"
TWITCH_MESSAGE_TYPE = "Twitch-Eventsub-Message-Type"
TWITCH_MESSAGE_TIMESTAMP = "Twitch-Eventsub-Message-Timestamp"
TWITCH_MESSAGE_SIGNATURE = "Twitch-Eventsub-Message-Signature"
HMAC_PREFIX = "sha256="


COGS = ["cogs.admin", "cogs.birthday", "cogs.events", "cogs.tasks"]

UNKNOWN_USER = "Unknown User"
DEFAULT_MISSING_CONTENT = "`Message content not found in cache`"
EMPTY_CONTENT = "`No text`"


class Months(Enum):
    January = 1
    February = 2
    March = 3
    April = 4
    May = 5
    June = 6
    July = 7
    August = 8
    September = 9
    October = 10
    November = 11
    December = 12


MAX_DAYS = {
    Months.January: 31,
    Months.February: 29,
    Months.March: 31,
    Months.April: 30,
    Months.May: 31,
    Months.June: 30,
    Months.July: 31,
    Months.August: 31,
    Months.September: 30,
    Months.October: 31,
    Months.November: 30,
    Months.December: 31,
}


class TokenType(str, Enum):
    App = "app"
    User = "user"
    Broadcaster = "broadcaster"
