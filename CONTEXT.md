# Valin Malach Bot

One process serving two faces: a Discord bot and a FastAPI app that receives
Twitch EventSub webhooks. This file fixes the words used for the concepts that
span both, so a term means one thing wherever it appears. It also fixes any term
two modules answer differently, whichever face it belongs to.

## Language

### Admin surface

How the bot talks about itself, as opposed to what it says to viewers.

**Admin channel**:
The one Discord channel the bot uses to report on its own health, resolved by
the slug `bot_admin`. Not a place viewers see.
_Avoid_: error channel, log channel, bot_admin (in prose)

**Report**:
An exception delivered to the **admin channel**, with its traceback attached as
a file. Reporting never raises and never blocks the path that failed.
_Avoid_: error log, handled error, error report

**Notice**:
A message to the **admin channel** that is not an exception — a rejected webhook
signature, a revoked EventSub subscription, an unavailable token.
_Avoid_: warning, alert (see flagged ambiguities), admin message

**Held back**:
Said of a **report** or **notice** identical to one delivered within the last
fifteen minutes. It is counted rather than sent, and the next one through says
how many it stood in for. Only a delivery holds anything back, so the first of
anything is always sent.
_Avoid_: rate limited, throttled, deduplicated, suppressed

**Key**:
What two occurrences must share to be **held back** as the same one. Named
explicitly wherever the text carries a detail that varies between repeats — a
broadcaster id belongs in the key, the error string does not.
_Avoid_: id, fingerprint, hash

### Twitch API

**Helix call**:
A request to Twitch's API, made only through `services/twitch/helix.py`. Its
answer is the value or `None` when Twitch has none; a call that did not complete
raises instead, so "absent" and "unknown" are never the same answer.
_Avoid_: Twitch API call, twitch request

**Repeatable**:
Said of a **Helix call** that may be sent twice without a second effect. Reads
and deletes are; posting a chat message or a shoutout is not, which is why those
are never retried.
_Avoid_: idempotent, safe, retryable

**Chat line**:
One message the bot says in Twitch chat, sent only through
`services/twitch/chat.py`. Saying it never raises and never fails a caller: a
line Twitch refused is a **notice**, not the end of whatever was saying it.
_Avoid_: chat message, chat post

**Undeliverable**:
Said of an EventSub subscription that exists but will never call back — one
Twitch disabled after too many failed deliveries, or one still pointing at a
callback this deployment no longer answers on. Indistinguishable from a working
one in a 409, which is why a 409 is looked into rather than believed.
_Avoid_: broken, dead, stale

### Twitch stream lifecycle

**Live alert**:
The Discord message announcing that a stream is live, kept up to date while it
runs and rewritten once when it ends. One row in `live_alert` per broadcaster, so
a promo alert and the main broadcaster's alert are the same kind of thing.
_Avoid_: stream notification, go-live post, announcement

**Alert updater**:
The task refreshing one **live alert**, and the only thing that closes it. A
`stream.offline` webhook wakes it rather than closing the alert itself.
_Avoid_: poller, alert loop, watcher

**Superseded**:
Said of an **alert updater** whose row no longer names the message and stream it
was started for. It closes its own message — otherwise that message shows a live
stream forever — and leaves the newer alert's row alone.
_Avoid_: stale, replaced, orphaned

**Stream session**:
The main broadcaster being live. At most one at a time, and it owns the
**shoutout queue** and the ad-break notification. Distinct from a **live alert**,
which exists per broadcaster.
_Avoid_: broadcast, live session, stream run

**Shoutout queue**:
The pending `!so` targets, drained at a rate Helix accepts. Active only for the
duration of a **stream session**.
_Avoid_: shoutout list, so queue

### Birthdays

**Stored birthday**:
The next occurrence of someone's birthday, held as an instant in UTC. Never a
date of birth: the year is chosen rather than remembered, and it moves.
_Avoid_: birth date, DOB, birthday date

**Roll forward**:
Moving a **stored birthday** to its next occurrence. The same question asked
when a birthday is set and when one has just been greeted — but not yet the same
answer: setting one knows the timezone and rolling one forward has had it
discarded, so the two disagree wherever a zone's rules move the local day.
_Avoid_: reschedule, bump, advance

**Stale birthday**:
A **stored birthday** that came due more than a day ago, because nothing was
running to greet it. It is rolled forward with a **notice** instead of a
greeting, so nobody is wished a happy birthday on the wrong day.
_Avoid_: missed birthday, overdue, expired

### Audit log

**Audit channel**:
The Discord channel holding the **audit entries**, resolved by the slug
`audit_logs`. Written only by `services/audit.py`. Not the **admin channel**:
this one records what people did, that one records what the bot could not do.
_Avoid_: mod log, staff channel, audit_logs (in prose)

**Audit entry**:
One recorded thing in the **audit channel**, written by one call to
`services/audit.py`. An entry is not an embed — a deleted message that carried
attachments is one entry and several embeds — and callers never learn which.
_Avoid_: log line, audit message, event log

**Person author**:
An **audit entry**'s author line naming who the entry is about: their name,
discriminator and avatar. The usual shape.
_Avoid_: user header, byline

**Caption author**:
An **audit entry**'s author line naming the event instead of a person, used
where no one person is its subject — a member joining, a ban, an invite. The
distinction is internal; nothing outside `services/audit.py` chooses between
them.
_Avoid_: title, header, label

**Their words**:
The part of an **audit entry** a person wrote — a message, a nickname, an
attempted command. Always escaped, because a description and a field value both
render markdown; an **author line** and a footer are plain text and are left
alone. The bot's own text is never escaped, so its formatting survives.
_Avoid_: user input, raw content, unsafe text

**Recovered content**:
The stored copy of a message the gateway cache no longer holds, read back from
Postgres when one is edited or deleted. Unbounded, unlike a live message, so it
is truncated before it can become a field.
_Avoid_: cached content, old message, history

## Flagged ambiguities

**`log_error` names two different behaviours.** Four files define it as
"upload the traceback to the admin channel"; `main.py` defines it as "write to
the local logger and stop". Resolved: **report** is the only name for reaching
the admin channel. Local logging is not a separate concept — every report logs
locally first, and says so when it cannot go further.

**"Audit log" names two different things.** Discord keeps one per guild, which
the bot reads through `guild.audit_logs` to find out who deleted a message or
cleared a channel. The bot also keeps its own, in the **audit channel**.
Resolved: **audit entry** and **audit channel** are always the bot's; Discord's
is always spelled out as "Discord's audit log".

**"Alert" means the live alert, never an admin notice.** An admin-channel
message about a failure is a **notice** or a **report**. Nothing else in this
project is called an alert.

**Age is never a person's age.** It is the time elapsed since an instant — how
long ago an account was created, how long a stream has been live. A **stored
birthday** carries no birth year, so the bot cannot know anyone's age and never
says one.

**Two functions named `_create_offline_embed`** exist, in `controller/twitch.py`
and `services/twitch/api.py`. Resolved in language ahead of the code: closing is
the **alert updater**'s job alone, so there is one closer and one offline embed.
The webhook cannot identify which stream ended — Twitch's `stream.offline`
payload carries no stream id — so it can only wake the updater, which re-checks
Helix and, if it is **superseded**, closes its own message without touching the
newer alert's row.

## Example dialogue

> **Dev**: When a stream ends, who rewrites the message?
>
> **Operator**: Twitch tells you, doesn't it?
>
> **Dev**: It tells us the channel went offline, but not which stream. If you
> ended one and started another, we could not tell them apart.
>
> **Operator**: So you would close the wrong one.
>
> **Dev**: That is why the webhook only wakes the alert updater. The updater
> knows which stream it was started for. If it has been superseded it still
> closes its own message — leaving it would show a live stream forever — but
> it leaves the newer alert's row alone.
>
> **Operator**: And if nothing ever wakes it?
>
> **Dev**: It checks on its own every minute anyway. The webhook just makes it
> quick.
>
> **Operator**: Fine. And when it breaks?
>
> **Dev**: You'd know — that would be a report in the admin channel, with the
> traceback.
>
> **Operator**: Only if the bot is up far enough to post. Otherwise?
>
> **Dev**: It logs locally, then logs that it couldn't reach you. You lose the
> report, but not silently.
>
> **Operator**: And the 403s I see when Twitch signatures don't match?
>
> **Dev**: Those aren't exceptions, so they're notices. Same channel, no
> traceback.
>
> **Operator**: Last time Twitch was down for two hours I got hundreds of them.
>
> **Dev**: Now you get the first one, and then it's held back for fifteen
> minutes at a time. The next one that gets through tells you how many it stood
> in for.
>
> **Operator**: So a thing that breaks once still reaches me.
>
> **Dev**: Always. Only a message that actually landed holds anything back, so
> the first of anything is never the one you lose.
>
> **Operator**: Different thing. Someone tells you their birthday is the 29th of
> February. What do you write down?
>
> **Dev**: The next one. Not the day they were born — we never ask for that, and
> we could not tell you anyone's age if you asked.
>
> **Operator**: So it is a date in the future, and it changes.
>
> **Dev**: Every time we greet them, we roll it forward to the next one. For the
> 29th that is four years, unless the century gets in the way — 2100 is not a
> leap year.
>
> **Operator**: And if the bot is off for a week and misses one?
>
> **Dev**: Then it is stale, and it gets rolled forward without a greeting. You
> get a notice saying nobody wished them a happy birthday, because greeting them
> six days late is worse than not greeting them.
>
> **Operator**: Who works out the next one — the command, or the thing that
> greets?
>
> **Dev**: Same question, so it had better be the same answer. It was not, once:
> setting a 29th of February birthday inside a leap year wrote down a date that
> had already gone.
