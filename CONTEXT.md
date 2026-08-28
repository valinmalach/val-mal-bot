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

### Twitch stream lifecycle

**Live alert**:
The Discord message announcing that a stream is live, kept up to date while it
runs and rewritten once when it ends. One row in `live_alert` per broadcaster.
_Avoid_: stream notification, go-live post, announcement

**Shoutout queue**:
The pending `!so` targets, drained at a rate Helix accepts. Active only while
the broadcaster is live.
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
and `services/twitch/api.py`, and both close a **live alert**. Unresolved — the
duplication is real, not just a naming collision.

## Example dialogue

> **Dev**: When a stream ends, who rewrites the message?
>
> **Operator**: Whoever notices first. The webhook usually, but the poller
> catches it if Twitch never calls.
>
> **Dev**: So the live alert has two closers.
>
> **Operator**: Right, and if they both go it looks the same to me, so I
> wouldn't know.
>
> **Dev**: You'd know if it failed — that would be a report in the admin
> channel, with the traceback.
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
