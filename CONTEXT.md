# Valin Malach Bot

One process serving two faces: a Discord bot and a FastAPI app that receives
Twitch EventSub webhooks. This file fixes the words used for the concepts that
span both, so a term means one thing wherever it appears.

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

## Flagged ambiguities

**`log_error` names two different behaviours.** Four files define it as
"upload the traceback to the admin channel"; `main.py` defines it as "write to
the local logger and stop". Resolved: **report** is the only name for reaching
the admin channel. Local logging is not a separate concept — every report logs
locally first, and says so when it cannot go further.

**"Alert" means the live alert, never an admin notice.** An admin-channel
message about a failure is a **notice** or a **report**. Nothing else in this
project is called an alert.

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
