# Responses Core Refactor Design

## Objective

Replace the coupled Responses logic in `proxy/mod.rs` with deep modules that preserve request semantics while making stream lifecycle, session identity, provider-prefix control, transport timing, and metrics independently testable.

Success means:

- no `stream closed before response.completed`;
- no second full cold start for the same stable session/model lineage within a short window;
- zero proven avoidable gap, with at most one second of evidence-gated foreground wait;
- no extra upstream request, prewarm, probe, or hidden retry;
- truthful provider usage and latency metrics;
- stable-prefix hit rate at or above 99.5% on eligible live traffic.

## Evidence

### v0.1.90 stream regression

The stream treated `[DONE]` as sufficient to end the client response and moved the remaining body to a detached drain. The live provider can emit `response.completed` after `[DONE]`, causing Codex to reject the stream. Detached drains also allowed old upstream work to overlap later calls.

### Same-session identity split

At 19:06:02 and 19:06:16, two Codex calls used the same real upstream model but produced different session anchors, prompt-cache keys, and prefix fingerprints. The second request was `codex-auto-review` with low reasoning effort. Dynamic invocation role and reasoning were therefore mixed into stable cache identity.

### Freshness conflation

`prefix_guard.rs` uses one second as both the maximum foreground wait and the maximum useful age of exact prefix evidence. Live exact states aged about nine seconds were labelled `avoidable_state_stale`, so proven 256-768 token avoidable gaps received no protection.

### Transport timing

Local preparation and gzip take tens of milliseconds. Most excess time occurs inside upstream send before response headers. A 300 KB route probe completed in under 220 ms, while live API calls waited seconds to tens of seconds, so transport and upstream-edge timing must remain separate from model processing and cache control.

## Modules

### `responses_stream`

Interface:

```rust
StreamState::ingest(&[u8]) -> StreamObservation
StreamState::finish() -> StreamSummary
```

The implementation owns SSE framing, first real model output, usage extraction, terminal ordering, error state, and EOF validation. The caller never decides that `[DONE]` alone is sufficient for a native Responses stream. One inbound request owns one upstream stream task through EOF.

### `session_identity`

Interface:

```rust
SessionIdentity::derive(context: IdentityContext, request: &Value) -> SessionIdentity
```

Identity layers:

1. explicit thread/conversation/session identity or a valid client prompt-cache key, hashed before volatile fields are removed;
2. stable provider/model/workspace scope;
3. normalized stable instructions/tools plus a truncation-resistant input anchor fallback.

Dynamic reasoning effort, output limits, service tier, stream flags, metadata, request ids, and internal aliases such as `codex-auto-review` do not split identity when they resolve to the same real upstream model. Cross-session explicit identities remain isolated.

### `prefix_control`

Interface:

```rust
PrefixController::before_request(identity, tail, budget) -> GuardDecision
PrefixController::observe(identity, usage, tail) -> PrefixObservation
```

The implementation owns monotonic high-waterline state, one cold-start epoch, real-tail versus avoidable decomposition, provider rollback/reset, and evidence age. The one-second request budget limits waiting only; it is not the evidence TTL. Exact evidence remains usable for a bounded 30-second window, while non-exact sibling evidence cannot trigger a wait.

### `transport`

Interface:

```rust
TransportClients::client(NetworkPath) -> &reqwest::Client
TransportTiming::from_send(...)
```

The first migration is behavior-equivalent: pooled direct and system-proxy clients move out of `state.rs`, and timing names become explicit. No network library change is bundled with the cache refactor. A later direct Hyper adapter is allowed only after a same-payload differential test proves a benefit.

### `metrics`

Metrics consume module results. They do not recompute cache classifications independently. Raw input, cached, output, request count, trace id, network path, and timing values remain provider-derived or directly measured.

## Provider Prefix Ordering

Responses JSON is semantically unordered, but provider prefix caching observes serialized byte order. Stable fields and input must precede per-call reasoning and output controls:

1. real model and stable prompt-cache identity;
2. instructions and tools;
3. input/history;
4. reasoning, text/format, token limits, sampling, stream, service tier, metadata, and previous-response controls.

This allows main and auto-review calls with the same stable history to share the longest valid byte prefix without changing either call's meaning.

## Migration

1. Add captured, redacted v0.1.89/v0.1.90 fixtures and failing tests.
2. Extract stream state and delete the old collector.
3. Add session identity and route provider key, session diagnostics, and prefix control through it.
4. Move guard policy and prefix state decisions behind `PrefixController`.
5. Move HTTP client ownership behind `TransportClients` without changing wire behavior.
6. Delete replaced helpers from `proxy/mod.rs`; no dual implementation remains.
7. Run Rust, cache acceptance, metrics regression, build, and live log comparison.

## Rollback

- Stable package: `v0.1.91-stream-lifecycle-recovery-20260711`.
- Source rollback point: Git `f496211` plus the v0.1.91 stream-order regression fix.
- Each module migration is independently revertible before the final old-code deletion.
