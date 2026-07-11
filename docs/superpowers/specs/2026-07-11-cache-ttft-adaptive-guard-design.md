# Adaptive Prefix Guard and TTFT Design

## Best Route

Keep the Responses prefix guard only where live evidence proves an avoidable commit gap.
The v0.1.88 live sample showed 66 of 74 requests waiting about one second even though only
1,280 tokens were classified as avoidable. The runtime therefore defaults to zero wait and
uses the foreground budget only for a fresh, exact session-anchor state with a measured
avoidable gap.

## Behavior

- Keep the hard foreground wait ceiling at one second, including local preparation time.
- Keep bounded protection for measured avoidable 128-token gaps.
- Do not wait for real new tails, tool-tail bursts, cold-unstable reads, waterline drops, or
  sibling/family prefix states.
- Use one continuous bounded policy from 128 tokens through hundreds of thousands of tokens.
- Keep session anchors isolated and do not add prewarm, retry, sync, or session-delta requests.

## Metrics and UI

- Display provider gaps at the observed 128-token granularity.
- Hide the normal local response-cache `miss` label from request rows.
- Rename provider cache `rollback` to `waterline drop` in user-facing text.
- Derive `upstream_call_kind` from the body actually sent upstream, including failed responses.
- Record the prefix-state age used by each guard so waits can be measured from real stream completion.
- Record direct/system-proxy path, remote address, and truthful connection-pool capability evidence.
- Record request-body encoding time, gzip encoding time, upstream processing headers, and
  response-provided trace identifiers without injecting a new request header.
- Do not claim an exact pool hit because reqwest does not expose that signal on a response.
- Keep raw input/cache token counts unchanged and do not synthesize hit data.

## Acceptance

- Fresh exact measured avoidable gaps retain a non-zero guard capped at one second.
- Requests without exact avoidable evidence have zero foreground settle wait.
- Different session anchors may use different prefix keys without being reported as a split.
- Existing high-hit guard regression tests remain green before any wait reduction is accepted.
- A failed Codex Responses request sent with `stream=true` is logged as `stream`, not `sync`.
- No extra upstream requests, retries, channel conversion, or payload changes.
- Existing agent isolation, gzip, streaming, and cache accounting tests remain green.
