# Helix POSTs are not retried

A retry only helps when repeating the call is free. `GET` and `DELETE` are;
`POST` is not. The bot's two most frequent POSTs send a chat message and a
shoutout, and a request that Twitch accepted before the connection dropped looks
exactly like one it never received — so retrying posts the message twice, in
public. Retry is therefore automatic for `GET` and `DELETE` and off for `POST`,
which a call can override where a repeat is provably harmless.

This is deliberate, not an oversight. The pre-existing code reached the same
place by accident: ten call sites wrapped themselves in `retry_api_call` and the
two chat-sending ones did not, with nothing recording why. An architecture
review of this repo read that as a bug and proposed retrying them.

## Considered options

- **Retry everything** — one rule, nothing to remember, and duplicate chat
  messages whenever the network drops a response.
- **Keep retry opt-in per call site** — what the code did. Correct only for as
  long as everyone adding a call happens to guess right, and silent when they
  do not.
- **Retry by method, overridable** — chosen. The safe default matches HTTP
  semantics, and an override is a visible, local claim that this particular call
  can be repeated.

## Consequences

`subscribe_to_user` opts in. A duplicate subscription is not created: Twitch
rejects the second attempt as a conflict, so the worst case is a call reported
as failed that actually succeeded. That is a false alarm, not a double side
effect, which is the line this decision draws.

A 401 is not covered by any of this. The request was rejected, so it certainly
did not take effect, and re-sending after a token refresh is safe for every
method including `POST`.
