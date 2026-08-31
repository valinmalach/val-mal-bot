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
in `next_birthday` can only bump the year on a UTC instant. That does not preserve
the local date or the local wall clock: measured across every zone and date, 0.5%
land on the wrong local day and a further 0.7% at the wrong local time, affecting
201 of 598 zones — worst in `Africa/Casablanca`, where 228 of 365 dates move.
Setting a birthday is exact; rolling one forward is not. Closing that gap means
storing the timezone alongside the instant, which is a schema change and has not
been made.
