# The birthday column holds the next occurrence, not a date of birth

`discord_user.birthday` stores the next time we will greet someone, as an instant
in UTC alongside the `birthday_timezone` it was set in, and moves each year — it
is not the day they were born, and it carries no birth year. Storing the parts instead (month, day, timezone) would make a date in
the past unrepresentable rather than merely avoided, which is the stronger design;
it was rejected because the birthday task finds due birthdays every fifteen
minutes with an indexed range scan over that column (`birthday <= now`), and
parts would replace that with projecting every user's next occurrence in Python
on every tick.

## Consequences

Anything writing a birthday has to **ask for** the next occurrence rather than
build a date. `/birthday set` built one, and for ten months of every leap year it
built one that had already passed; that is what `next_birthday_on` exists to stop.

The timezone was originally used to place the instant and then discarded, so the
roll-forward in `next_birthday` could only bump the year on a UTC instant, which
preserves neither the local date nor the local wall clock. How much that cost
depended entirely on which year's transitions you measured, so no single figure
described it — rolling every zone and date forward once gave 201 of 598 zones
affected across 2026, the same 201 across 2025, and 411 across 2027.

`birthday_timezone` closes that gap, and is the whole reason the column exists:
`next_birthday` reads the local date back off the instant and asks
`next_birthday_on` for the parts, so both directions now run the same
construction and the same sweep reports zero wrong local days and zero wrong
local times in 2025, 2026 and 2027 alike. Issue #12 carries the measurements.

The column is nullable and null on every row written before it, because there is
no timezone to backfill from; those rows keep bumping the year until the person
sets their birthday again. Storing the parts is still the stronger design and
still rejected, for the range-scan reason above — the column only **adds** to
the row, so the indexed `birthday <= now` scan is untouched.
