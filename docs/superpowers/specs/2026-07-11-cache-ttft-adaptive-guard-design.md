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

## v0.1.90 Negative Result and v0.1.91 Recovery

- `ttft_ms` starts at the first real model delta, not at `response.created` or another metadata-only SSE event.
- v0.1.90 incorrectly ended the client stream at `[DONE]` and detached the remaining upstream body into a background drain. The live provider can emit `response.completed` after `[DONE]`, which caused Codex to report `stream closed before response.completed` and allowed old streams to overlap later calls.
- v0.1.91 restores the v0.1.89 single-stream lifecycle: keep reading the same upstream stream through EOF, preserve late terminal events, and never spawn a detached drain from the main Responses path.
- Request gzip uses fast compression below 600 KB, default compression from 600 KB to 1 MB, and best compression at 1 MB or above.
- The explicit direct path uses HTTP/1.1, matching CCSwitch's stable direct transport. The system-proxy path keeps protocol negotiation enabled.
- The transport change does not add probes, retries, duplicate requests, cache waits, or request-body transformations beyond the existing opt-in gzip setting.
- Live gzip A/B showed that disabling gzip increased direct request bodies from roughly 230-410 KB to 1.13-1.23 MB and worsened header waits to 31-108 seconds. Gzip therefore remains user-controlled and enabled for the tested provider.
- A system-proxy transport probe completed a 300,793-byte upload in 71-220 ms over HTTP/2 and 51-96 ms over HTTP/1.1. The 13-64 second live header waits are not explained by local protocol overhead alone.
