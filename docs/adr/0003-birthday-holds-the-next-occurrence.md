# The birthday column holds the next occurrence, not a date of birth

`discord_user.birthday` stores the next time we will greet someone, as an instant
in UTC, and moves each year — it is not the day they were born, and it carries no
birth year. Storing the parts instead (month, day, timezone) would make a date in
the past unrepresentable rather than merely avoided, which is the stronger design;
it was rejected because the birthday task finds due birthdays every fifteen
minutes with an indexed range scan over that column (`birthday <= now`), and
parts would replace that with projecting every user's next occurrence in Python
on every tick.

## Consequences

Anything writing a birthday has to **ask for** the next occurrence rather than
build a date. `/birthday set` built one, and for ten months of every leap year it
built one that had already passed; that is what `next_birthday_on` exists to stop.

The timezone is used to place the instant and then discarded, so the roll-forward
in `next_birthday` can only bump the year on a UTC instant, which preserves
neither the local date nor the local wall clock. How much that costs depends
entirely on which year's transitions you measure, so no single figure describes
it — rolling every zone and date forward once gives 201 of 598 zones affected
across 2026, the same 201 across 2025, and 411 across 2027. `Africa/Casablanca`
and `Africa/El_Aaiun` are joint-worst in 2026 at 228 dates each and unaffected in
2027. Setting a birthday is exact; rolling one forward is not. Closing that gap
means storing the timezone alongside the instant, which is a schema change and
has not been made — issue #12 carries the measurements.
