from enum import Enum
from typing import Optional, TypedDict

TWITCH_MESSAGE_ID = "Twitch-Eventsub-Message-Id"
TWITCH_MESSAGE_TYPE = "Twitch-Eventsub-Message-Type"
TWITCH_MESSAGE_TIMESTAMP = "Twitch-Eventsub-Message-Timestamp"
TWITCH_MESSAGE_SIGNATURE = "Twitch-Eventsub-Message-Signature"
HMAC_PREFIX = "sha256="

GUILD_ID = 813237030385090580

AUDIT_LOGS_CHANNEL = 1291775826655707166
BLUESKY_CHANNEL = 1345582916050354369
BOT_ADMIN_CHANNEL = 1346408909442781237
DM_REQUESTS_CHANNEL = 1292413187270115328
FOOD_CHANNEL = 1291026325045248101
PETS_CHANNEL = 1291027524947546164
PROMO_CHANNEL = 1378917167336001606
RANTING_CHANNEL = 1291026750947590266
ROLES_CHANNEL = 1285277373167570946
RULES_CHANNEL = 1285275553611517963
SHOUTOUTS_CHANNEL = 1291026077287710751
STREAM_ALERTS_CHANNEL = 1285276760044474461
WELCOME_CHANNEL = 1285276874645438544

FOLLOWER_ROLE = 1291769015190032435

ANNOUNCEMENTS_ROLE = 1292347932904915007
LIVE_ALERTS_ROLE = 1292348044888768605
PING_ROLE = 1292348084998897737
BLUESKY_ROLE = 1345584502805626973
FREE_STUFF_ROLE = 1359500454941298709

NSFW_ACCESS_ROLE = 1292348175553794050

HE_HIM_ROLE = 1292386380038672404
SHE_HER_ROLE = 1292386514726289542
THEY_THEM_ROLE = 1292386617348194346
OTHER_ASK_ROLE = 1292386717449453599

STREAMER_ROLE = 1292386827008999486
GAMER_ROLE = 1292386929299689472
ARTIST_ROLE = 1292386998438596710

DMS_OPEN_ROLE = 1292387067568853012
ASK_TO_DM_ROLE = 1292387187576274995
DMS_CLOSED_ROLE = 1292387243964764193

OWNER_ID = 389318636201967628

COGS = ["cogs.admin", "cogs.birthday", "cogs.events", "cogs.tasks"]

UNKNOWN_USER = "Unknown User"
DEFAULT_MISSING_CONTENT = "`Message content not found in cache`"

YOUTUBE_CHANNEL_IDS = ["UC0mQZHMy8vp-r7KD3-r6S3A"]

BROADCASTER_USERNAME = "valinmalach"
TWITCH_DIR = "data/twitch"
LIVE_ALERTS = "data/live_alerts.parquet"
MESSAGES = "data/messages.parquet"
VIDEOS = "data/youtube/videos.parquet"


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


EMOJI_ROLE_MAP = {
    # Rules
    "✅": "🙇Followers",
    # Ping Roles
    "📢": "📢Announcements",
    "🔴": "🔴Live Alerts",
    "❗": "❗Ping Role",
    "🦋": "🦋Bluesky",
    "🎁": "🎁Free Stuff",
    # NSFW Access
    "🔞": "🔞NSFW Access",
    # Pronouns
    "🙋‍♂️": "🙋‍♂️He/Him",
    "🙋‍♀️": "🙋‍♀️She/Her",
    "🙋": "🙋They/Them",
    "❓": "❓Other/Ask",
    # Other Roles
    "📽️": "📽️Streamer",
    "🎮": "🎮Gamer",
    "🎨": "🎨Artist",
    # DMs Open?
    "🟩": "🟩DMs Open",
    "🟨": "🟨Ask to DM",
    "🟥": "🟥DMs Closed",
}


class TokenType(str, Enum):
    App = "app"
    User = "user"
    Broadcaster = "broadcaster"


class UserRecord(TypedDict):
    id: int
    username: str
    birthday: Optional[str]
    isBirthdayLeap: Optional[bool]


class ErrorDetails(TypedDict):
    type: str
    message: str
    args: tuple
    traceback: str


class LiveAlert(TypedDict):
    id: int
    channel_id: int
    message_id: int
    stream_id: int
    stream_started_at: str
