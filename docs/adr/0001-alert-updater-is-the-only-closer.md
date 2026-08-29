# The alert updater is the only thing that closes a live alert

Twitch's `stream.offline` payload carries `broadcaster_user_id`, `login` and
`name` — and no stream id. A webhook handler therefore cannot tell *which*
stream ended, so it cannot safely close a **live alert**: a late offline for one
stream would close whichever alert the broadcaster currently has. The **alert
updater** can, because it captured the message and stream it was started for and
re-checks both against the row every cycle. So the webhook only wakes the
updater, and closing lives in one place.

## Considered options

- **The webhook closes, with the updater as a fallback** — what the code did.
  Fastest, and wrong in a way that is hard to see: end a stream and start
  another, and the offline for the first closes the second. It also left two
  implementations of the offline embed, one in `controller/twitch.py` and one in
  `services/twitch/api.py`, which could drift apart.
- **Both call one guarded `close()`** — one implementation, two entry points.
  Fixes the drift but not the concurrency: webhook and updater can enter it at
  once, and the webhook still has no stream id to check against.
- **Webhook wakes the updater** — chosen. One closer, one implementation, and
  every close is serialised through the updater task that owns that alert.

## Consequences

Closing is no longer instant on the webhook. The wake makes the updater run its
cycle immediately instead of waiting out its interval, but the close still
happens only once Helix agrees the stream is gone. If Helix lags the webhook,
the alert closes a cycle later than it used to.

In exchange, a wake is safe to issue for any broadcaster at any time: an updater
that has been **superseded** stops instead of editing, and one that finds its
stream still live simply refreshes. That also makes the wake a repair — it
starts an updater when the row has one and the task does not, which previously
needed a restart.
