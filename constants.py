from calendar import monthrange
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
UNNAMED_EVENT = "Audit entry"


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


# The longest that month can ever be, so 29 February is accepted and the year
# it lands in is settled later, by services/birthday. A leap year is passed in
# for exactly that reason.
MAX_DAYS = {month: monthrange(2024, month.value)[1] for month in Months}


class TokenType(str, Enum):
    App = "app"
    User = "user"
    Broadcaster = "broadcaster"
