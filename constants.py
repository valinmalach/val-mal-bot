from enum import Enum

GUILD_ID = 813237030385090580

AUDIT_LOGS_CHANNEL = 1291775826655707166
BLUESKY_CHANNEL = 1345582916050354369
BOT_ADMIN_CHANNEL = 1346408909442781237
SHOUTOUTS_CHANNEL = 1291026077287710751
STREAM_ALERTS_CHANNEL = 1285276760044474461
WELCOME_CHANNEL = 1285276874645438544

BLUESKY_ROLE = 1345584502805626973
FOLLOWER_ROLE = 1291769015190032435
LIVE_ALERTS_ROLE = 1292348044888768605

WEISS_ID = 1131782416260935810

COGS = ["cogs.admin", "cogs.birthday", "cogs.events", "cogs.tasks"]


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


MESSAGE_REACTION_ROLE_MAP = {
    1291772906841571500: {"✅": "🙇Followers"},  # Rules message
    1292348915257053367: {  # Ping roles message
        "📢": "📢Announcements",
        "🔴": "🔴Live Alerts",
        "❗": "❗Ping Role",
        "🦋": "🦋Bluesky",
        "🎁": "🎁Free Stuff",
    },
    1292349441763971123: {"🔞": "🔞NSFW Access"},  # NSFW Access role message
    1292349494666465282: {  # Pronouns roles message
        "🙋‍♂️": "🙋‍♂️He/Him",
        "🙋‍♀️": "🙋‍♀️She/Her",
        "🙋": "🙋They/Them",
        "❓": "❓Other/Ask",
    },
    1292350341521739837: {  # Streamer, Gamer, Artist roles message
        "📽️": "📽️Streamer",
        "🎮": "🎮Gamer",
        "🎨": "🎨Artist",
    },
    1292357707365351445: {  # DMs Open, Ask to DM, DMs Closed roles message
        "🟩": "🟩DMs Open",
        "🟨": "🟨Ask to DM",
        "🟥": "🟥DMs Closed",
    },
}
