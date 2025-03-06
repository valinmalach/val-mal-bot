from enum import Enum

AUDIT_LOGS_CHANNEL = 1291775826655707166
BLUESKY_CHANNEL = 1345582916050354369
BOT_ADMIN_CHANNEL = 1346408909442781237
SHOUTOUTS_CHANNEL = 1291026077287710751
STREAM_ALERTS_CHANNEL = 1285276760044474461
WELCOME_CHANNEL = 1285276874645438544


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
