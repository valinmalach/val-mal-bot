# The stream session ends itself, and a live alert does not end it

A **stream session** owns everything that lasts exactly one stream — the
**shoutout queue**, the ad-break notification, and which **autoshoutout list**
members are **spent** — so the only thing that may *end* one is Helix confirming
the broadcaster is gone. `stream.offline` wakes it; the session re-checks
`get_stream` and ends only on a confirmed answer, and only if it still names the
stream the check was started for. That last clause is not decoration:
`stream_online` can sit in `_wait_for_stream_info` for a minute before `began()`
runs, so a stream that comes straight back can easily begin while the previous
one's offline check is still in flight, and an unguarded check would stand down
the session that had just replaced it.

Ending a session and beginning one are separate transitions, and only the first
is Helix's to decide. `began()` and `resume()` establish a session and discard
whatever a previous one left behind, because that state described a stream that
is over by definition: a reset, not a teardown, and the bot is live on both
sides of it. The two rules therefore do not compete. A reconnect arriving while
an offline check is still in flight supersedes it, by the stream id above; one
arriving with no check outstanding simply starts clean. Nothing but Helix ever
leaves the bot with no session at all, which is the property this ADR is about.

This reads like a contradiction of ADR 0001 and is not. That decision is about
which *alert* a `stream.offline` refers to, and the payload carrying no stream
id makes the question unanswerable from the webhook. The session has no such
ambiguity: there is at most one, for a broadcaster named in configuration, so
"which session ended" has only ever had one answer. Closing an alert and ending
a session are different decisions about different scopes, and only the first of
them needs a stream id.

## Considered options

- **The alert updater ends the session** — what the code did, at
  `live_alert._cycle`, on `_Action.CLOSE` with a confirmed-gone stream. It made
  the session's correctness depend on a `live_alert` row existing, and three
  paths end an updater without ever reaching that branch: `_decide` returns
  `STOP` when the row is missing, `_run` returns after
  `_MAX_INCONCLUSIVE_CYCLES`, and a failed `announce()` starts no updater at
  all. In each, the stream ends and `shoutout_queue.activated` stays true —
  harmless when the only cost was a drainer spinning on an empty queue, and not
  harmless once **spent** hangs off the same flag, because then a list member
  gets one **autoshoutout** ever rather than one per stream.
- **`stream.offline` ends the session directly** — no Helix round trip, and it
  treats a brief disconnect as the end of the stream: the queue is dumped and
  the pending shoutouts with it, moments before `stream.online` starts a new
  session.
- **A periodic Helix heartbeat while active** — ends the session even if the
  `stream.offline` webhook never arrives, which is a real risk given that
  subscription is one of the five provisioned outside this repo. Rejected for
  now as a polling loop that mostly re-asks what the alert updater already asks
  Helix about the main broadcaster; the wake covers the ordinary case.
- **Wake on `stream.offline`, confirm against Helix** — chosen. It reuses the
  pattern ADR 0001 established rather than inventing one, survives a stream
  dropping and coming straight back without losing the queue, and costs no poll.

## Consequences

An undeliverable `stream.offline` subscription now leaves the session up rather
than merely delaying an alert. Two things bound that: the next `stream.online`
starts a session clean whatever the previous one left behind, so the damage is
confined to the gap between two streams rather than compounding across them, and
the hourly `recheck_subscriptions` loop reports the subscription as
**undeliverable** in the **admin channel**. If that proves too thin in practice,
the heartbeat above is the next step and does not require this decision to be
revisited.
