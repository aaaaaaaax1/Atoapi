# Adaptive Prefix Guard and TTFT Design

## Best Route

Keep the proven Responses prefix guard and improve its measurements before changing wait behavior.
The runtime already persists 128-token waterlines, timestamps state updates at stream completion,
and reuses long-lived reqwest clients. A zero-wait experiment failed historical high-hit tests, so
latency changes must not bypass guards without replay evidence that the 99.5% floor still holds.

## Behavior

- Keep the hard foreground wait ceiling at one second, including local preparation time.
- Keep bounded protection for measured avoidable 128-token gaps.
- Preserve the existing protection for stable tails, extreme tool tails, and unstable waterlines.
- Keep session anchors isolated and do not add prewarm, retry, sync, or session-delta requests.

## Metrics and UI

- Display provider gaps at the observed 128-token granularity.
- Hide the normal local response-cache `miss` label from request rows.
- Rename provider cache `rollback` to `waterline drop` in user-facing text.
- Derive `upstream_call_kind` from the body actually sent upstream, including failed responses.
- Record the prefix-state age used by each guard so waits can be measured from real stream completion.
- Record direct/system-proxy path, remote address, and truthful connection-pool capability evidence.
- Do not claim an exact pool hit because reqwest does not expose that signal on a response.
- Keep raw input/cache token counts unchanged and do not synthesize hit data.

## Acceptance

- Measured avoidable gaps retain a non-zero guard capped at one second.
- Existing high-hit guard regression tests remain green before any wait reduction is accepted.
- A failed Codex Responses request sent with `stream=true` is logged as `stream`, not `sync`.
- No extra upstream requests, retries, channel conversion, or payload changes.
- Existing agent isolation, gzip, streaming, and cache accounting tests remain green.
