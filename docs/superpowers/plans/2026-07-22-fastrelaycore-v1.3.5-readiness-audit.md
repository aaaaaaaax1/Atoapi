# FastRelayCore v1.3.5 Readiness Audit

## Scope And Boundary

This audit covers the user-approved FastRelayCore cache-mainline implementation
against the Stage 0/1 plan in
`2026-07-13-cache-directed-relay-v1-implementation.md` and its design.

It does **not** claim that a provider-side cache can be warmed locally.  The
running desktop process remains the released v1.3.4 executable on port 18883;
v1.3.5 has not been packaged or started as the user's live proxy.

## Static Implementation Evidence

| Planned requirement | Current implementation evidence | Status |
| --- | --- | --- |
| Frozen baseline wire | `proxy/wire_fixture_tests.rs` pins the historical Responses bytes; `scripts/verify-isolated-wire-compat.mjs` compares v1.3.4 and v1.3.5 bodies, headers, one-POST cardinality, and cache/Shadow identity. Its 12-way same-prefix mode also asserts one POST per parallel inbound. | Complete locally |
| One inbound, one Agent POST | Non-cloneable `AttemptToken` in `proxy/attempt_gate.rs`; `OneShotTransport` consumes `OneShotRequestPlan`; Agent terminal metrics are committed through `MetricsTransaction`. | Complete locally |
| Owned dispatch and exactly-once settlement | `proxy/cache_directed_relay.rs`, `proxy/completion_relay.rs`, and `proxy/settlement_pipeline.rs` own the request from handoff through terminal settlement. Cancellation, abort, delayed consumption, and disconnect regressions are in `proxy/mod.rs`. | Complete locally |
| Correct streaming terminal semantics | `SseFrameDecoder` handles chunked UTF-8, CRLF, multi-`data:` frames, overflow, and EOF; `responses_stream::evaluate_terminal` keeps incomplete/error streams out of positive learning. | Complete locally |
| No foreground cache wait or duplicate probe | Agent transport uses an explicit one-shot gate; normal production Shadow routing is permanently disabled unless an isolated test instance or an administrator validation run explicitly enables it. | Complete locally |
| Session/cache identity safety and upgrade continuity | `SessionIdentity` keeps strict continuation scope separate from the legacy-compatible provider cache key; direct, metadata, context, and client-context identity forms have compatibility tests. | Complete locally |
| Persisted Shadow state | `ShadowAffinityStore` has bounded age indexes, safe legacy defaults, a 24-hour TTL, and runtime journal persistence. Expired or legacy evidence fails closed; an inbound request uses a non-blocking shadow receipt and prefix-guard skip when a background snapshot owns either runtime-state lock. | Complete locally |
| Bounded affinity maintenance | New assignments/windows make room before insertion through age indexes; capacity never searches a protected current scope. Full post-burst evidence eviction now updates per-scope queues and expiry indexes incrementally instead of rebuilding the 1,536-record index. | Complete locally |
| Cache observation and truthful metrics | `finalize_agent_generation` writes the real terminal record before shadow observation; metrics distinguish inbound outcomes from upstream attempts and preserve errors outside success history. | Complete locally |
| Explicit local proxy path | `RequestPlan` and `TransportClients` support a configured explicit upstream HTTP proxy without probing, retrying, or changing the direct path. | Complete locally |

## Verified Commands

The v1.3.5 worktree passed:

- `npm run verify:fastrelay:release`
  - `cargo fmt --check`
  - 771 release Rust tests
  - mandatory 4,096-assignment, 1,536-evidence, and full runtime-snapshot
    capacity baselines
  - frontend and Graphite UI regressions
  - request/metrics/provider regressions
  - owner-dispatch acceptance self-test
  - isolated v1.3.4/v1.3.5 wire comparison
  - 12-way isolated same-prefix dispatch stress: 12 inbounds, 12 attempts,
    and 12 upstream POSTs for both binaries
  - 12-way upstream-header gate: every same-prefix inbound reaches upstream
    before any held upstream response header is released
  - 100k deterministic cache replay safety gate
  - `git diff --check`
- `node scripts/tauri-build.mjs --preflight-only`
  - verifies the actual bundle entry point runs the same FastRelay preflight,
    including the mandatory capacity baselines, then exits before packaging.

Additional local performance baselines passed:

- 4,096-entry Shadow Affinity full-capacity p95: 9 microseconds (latest
  release preflight).
- 1,536-entry post-burst readiness refresh p95: 183 microseconds (latest
  release preflight).
- 8,192-prefix / 4,096-assignment / 1,536-evidence runtime snapshot p95:
  6.651ms, below its dedicated 10ms background-writer limit (inbound affinity
  planning and prefix-guard lookup both fail open without waiting for a
  snapshot lock).
- Shadow policy with 300KB / 2MB stable prefix p95: 0.345ms / 2.324ms.
- Native SSE relay delta p95: 2ms.
- Chat/Responses conversion relay delta p95: 0ms in the local fixtures.

These are local overhead bounds, not claims about upstream model prefill or
provider queue time.

## Remaining Live Gates

The following must remain pending until the user explicitly requests a v1.3.5
package and sends normal Codex traffic through it:

1. Stage 1: one selected provider/model profile needs at least 300 successful
   requests, 10 million usage-reported input tokens, at least 95% usage
   coverage, and no identity/performance regression.
2. Stage 2: only after Stage 1 passes, use the separate controlled candidate
   mechanism and require the documented comparable baseline/candidate sample,
   error, TTFT, one-POST, and efficacy gates.
3. Native Delta/`previous_response_id`: remain `FullReplay` unless the exact
   endpoint, resolved model, channel, key realm, and response shape possess a
   valid fidelity certificate. Rejection never retries the current inbound.

The absence of these live samples is intentional: no packaging or restart is
authorized yet, and v1.3.4 must remain untouched.
