# Cache-Directed Relay V1 Implementation Plan

## Baseline And Scope

- Approved design:
  `docs/superpowers/specs/2026-07-13-cache-directed-relay-v1-design.md`
- Baseline: Atoapi v0.2.3, commit `6b22e24`
- Design commit: `aa66f05`
- Implement Stage 0 and Stage 1 only.
- Do not change release versions or package artifacts.
- Do not alter `/responses/compact`, model-list, manual probe, or non-Agent
  retry behavior.
- Preserve all unrelated worktree changes if any appear.

## Continuation Slice: Stage 1 Diagnostics And Stage 2 Canary Seed

- Stage 1 request rows now expose hashed shadow realm/cohort, lane, arm,
  policy epoch, decision, skip reason, and policy compute latency.
- Legacy metrics JSON remains readable through serde defaults.
- A bounded 5% static-cohort candidate arm is seeded only for new trusted
  conversations when the existing smart-hit switch is enabled and the lane is
  steady. Candidate requests change only the provider `prompt_cache_key`; they
  do not add waits, retries, request bodies, headers, or upstream attempts.
- Existing persisted assignments default to the baseline arm. Tool-burst and
  compacted-anchor lanes remain shadow-only until live canary evidence passes
  the Stage 2 gates in the design.
- Do not raise the canary percentage or enable additional lanes without a live
  comparison meeting the documented sample, TTFT, error, attempt, and efficacy
  gates.

## Success Criteria

Stage 0 is complete when:

- every default Agent inbound produces one actual upstream POST;
- the existing reasoning-compatibility exception produces at most two labeled
  attempts within one inbound;
- inbound and upstream-attempt counters are truthful and independent;
- an Agent request canceled before response headers remains owned and reaches
  one final outcome;
- normal, delayed, saturated, and disconnected downstream flows never create
  another upstream attempt;
- incomplete/error streams cannot enter successful history or positive
  session/cache learning;
- v0.2.3 wire behavior remains unchanged outside approved terminal/accounting
  fixes.

Stage 1 is complete when:

- shadow affinity decisions are deterministic and persisted for 24 hours;
- shadow mode never changes request bytes, headers, wait, or attempt count;
- actual selected credential, model, provider, Agent, workspace, and stable
  prefix determine the candidate realm/cohort;
- shadow metrics survive normal, delayed, and disconnected relay completion;
- policy-compute performance stays within the approved limits.

## Task 1: Freeze Baseline Fixtures

Files:

- modify `src-tauri/src/proxy/mod.rs` tests only;
- add test fixtures under `src-tauri/src/proxy/fixtures/` only if inline data
  becomes unreadable.

Steps:

1. Capture secret-free native Responses stream, Responses-to-Chat stream,
   non-stream, gzip, verified delta, explicit reasoning rejection, and opaque
   reasoning 502 cases.
2. Normalize only request IDs, dates, and network-assigned headers.
3. Assert final serialized upstream bytes, relevant headers, downstream event
   order, status, and current attempt behavior.

Verify:

```powershell
cargo test --manifest-path src-tauri/Cargo.toml stream_upstream_ -- --nocapture
cargo test --manifest-path src-tauri/Cargo.toml reasoning_ -- --nocapture
cargo test --manifest-path src-tauri/Cargo.toml verified_third_party_delta -- --nocapture
cargo test --manifest-path src-tauri/Cargo.toml gzip -- --nocapture
```

## Task 2: Split Agent Inbound And Attempt Metrics

Files:

- modify `src-tauri/src/metrics.rs`;
- modify `src-tauri/src/proxy/mod.rs` only for new call sites;
- optionally extend `src/api.ts` with deserialize-only optional fields; do not
  redesign UI.

Steps:

1. Add `AgentGenerationStats` to `MetricsSnapshot` with inbound, successful,
   failed, attempt, multi-attempt, maximum-attempt, and active counts.
2. Add `InboundOutcomeLog` and `UpstreamAttemptLog` using the existing
   `RequestLog` as a compatibility projection rather than duplicating every
   field.
3. Add idempotent Agent APIs:
   `begin_agent_inbound`, `begin_agent_attempt`, `finish_agent_attempt`, and
   `finish_agent_inbound`.
4. Keep legacy `record_request`, `record_upstream_observation`, and
   `record_upstream_call` for non-Agent and compact paths.
5. Ensure provider `total_requests` counts inbound outcomes while provider
   `upstream_requests` counts actual attempts.
6. Keep one successful request-row projection per inbound; intermediate
   reasoning errors remain attempt/error evidence, not extra history rows.

Tests first:

- one inbound / one success attempt;
- one inbound / one transport failure;
- one inbound / two reasoning attempts / success;
- one inbound / two attempts / final failure;
- duplicate finish is a no-op with a diagnostic;
- provider total is one while upstream is two;
- maximum and multi-attempt counters are correct.

Verify:

```powershell
cargo test --manifest-path src-tauri/Cargo.toml metrics::tests -- --nocapture
npm run test:metrics
```

## Task 3: Add A Bounded Attempt Gate

Files:

- add `src-tauri/src/proxy/attempt_gate.rs`;
- register the module in `src-tauri/src/proxy/mod.rs`.

Interface:

```rust
enum AttemptPolicy {
    Single,
    ReasoningCompatibility,
}

enum AttemptReason {
    Primary,
    ReasoningExplicit,
    ReasoningOpaque502,
}
```

Steps:

1. Make `AttemptToken` non-cloneable.
2. Permit one primary token for both policies.
3. Permit a second token only after classified reasoning evidence.
4. Reject generic retries, early second-token requests, and every third token.
5. Give every token a stable inbound ID, unique attempt ID, index, budget, and
   reason.

Verify:

```powershell
cargo test --manifest-path src-tauri/Cargo.toml attempt_gate -- --nocapture
```

## Task 4: Add Agent One-Shot Transport

Files:

- modify `src-tauri/src/proxy/transport.rs`;
- modify `src-tauri/src/proxy/mod.rs`;
- add transport tests beside `transport.rs` or in existing proxy tests.

Steps:

1. Add Agent generation clients with redirect following disabled while keeping
   existing clients for excluded paths.
2. Extract one request serialization/gzip/header/send iteration from
   `send_upstream_request_to_url_with_diagnostics`.
3. Require an `AttemptToken` by value.
4. Call `begin_agent_attempt` immediately before `.send().await` and finish the
   same attempt on response or transport error.
5. Do not include retry loops, gzip fallback, key failover, or protocol
   fallback in this adapter.
6. Preserve direct/system-proxy pooling and existing wire headers.

Tests first:

- direct and system-proxy clients stay distinct and reusable;
- 307/308 responses are returned and not followed;
- network failure records one attempt;
- gzip rejection does not resend uncompressed;
- one token cannot be sent twice.

Verify:

```powershell
cargo test --manifest-path src-tauri/Cargo.toml one_shot_transport -- --nocapture
cargo test --manifest-path src-tauri/Cargo.toml sync_main_single_attempt -- --nocapture
cargo test --manifest-path src-tauri/Cargo.toml gzip -- --nocapture
```

## Task 5: Extract Completion Relay And Terminal State

Files:

- add `src-tauri/src/proxy/completion_relay.rs`;
- modify `src-tauri/src/proxy/responses_stream.rs`;
- reduce `stream_upstream` in `src-tauri/src/proxy/mod.rs` to orchestration and
  final side effects.

Steps:

1. Add a pure terminal state machine covering awaiting, succeeded, failed,
   trailing anomaly, and clean EOF.
2. Require `response.completed` for native Responses unless an explicit
   `DoneAtEof` compatibility profile applies.
3. Preserve `[DONE]` before or after `response.completed` and continue through
   EOF.
4. Treat pre-terminal transport error, SSE error, or incomplete EOF as failed.
5. Treat a post-terminal transport error as a trailing anomaly without
   overturning success.
6. Change the downstream stream item error from `Infallible` to a real relay
   error for pre-terminal failures.
7. Return one `CompletionOutcome`; keep metrics/session/cache writes outside
   the parser.

Tests first:

- normal terminal and EOF;
- `[DONE]` before completed;
- completed before `[DONE]`;
- error event;
- transport error before terminal;
- EOF without terminal;
- transport error after terminal;
- capability-gated `[DONE] + EOF`;
- conversion emits one client terminal sequence.

Verify:

```powershell
cargo test --manifest-path src-tauri/Cargo.toml completion_relay -- --nocapture
cargo test --manifest-path src-tauri/Cargo.toml responses_stream -- --nocapture
cargo test --manifest-path src-tauri/Cargo.toml stream_upstream_ -- --nocapture
```

## Task 6: Start Ownership Before Network I/O

Files:

- add `src-tauri/src/proxy/cache_directed_relay.rs`;
- modify the Agent wrapper and keep the current generation body as a private
  owned runner during the first extraction;
- modify application state only if a minimal task tracker is required.

Steps:

1. Split `handle_generation_for_agent` into a lightweight wrapper and an owned
   runner.
2. Consume the inbound permit, create response-head/body channels, and spawn
   the owner before the first send.
3. Await only the response head in the HTTP handler.
4. If the response-head receiver is dropped, discard the client response but
   continue the owned request and stream finalization.
5. Hand streaming ownership to exactly one `CompletionRelay` task after the
   response head; dropping its body receiver switches it to drain-only mode.
6. Catch owner panic and record one failed outcome. Use existing Tokio/futures
   primitives; do not add a task-tracker dependency unless the native approach
   cannot satisfy shutdown tests.

Tests first:

- cancel wrapper while upstream headers are delayed;
- consume normally;
- delay more than the 32-chunk capacity and later consume;
- drop receiver while sender is capacity-blocked;
- owner panic/abort finalizes once;
- concurrent same/different conversation requests remain isolated.

Verify:

```powershell
cargo test --manifest-path src-tauri/Cargo.toml dispatch_owner -- --nocapture
cargo test --manifest-path src-tauri/Cargo.toml completion_relay -- --nocapture
cargo test --manifest-path src-tauri/Cargo.toml stream_upstream_ -- --nocapture
```

## Task 7: Move The Reasoning Exception Behind The Gate

Files:

- modify `src-tauri/src/proxy/mod.rs`;
- optionally add a focused helper in `cache_directed_relay.rs`.

Steps:

1. Keep the first non-2xx response private until existing reasoning evidence is
   classified.
2. Request the one optional second token only for explicit or strict opaque
   evidence.
3. Label attempts `primary`, `reasoning_explicit`, or
   `reasoning_opaque_502`.
4. Never recurse. A second explicit rejection updates only the next request's
   configured effort.
5. Finalize one inbound outcome after the authorized budget ends.

Tests first:

- explicit rejection succeeds one level lower with one inbound/two attempts;
- second rejection persists the next lower level without a third attempt;
- strict opaque 502 probe succeeds one level lower;
- ordinary Cloudflare/generic 502 never receives a second attempt;
- model/UI configuration changes only for the affected model.

Verify:

```powershell
cargo test --manifest-path src-tauri/Cargo.toml configured_model_reasoning_rejection -- --nocapture
cargo test --manifest-path src-tauri/Cargo.toml opaque_502 -- --nocapture
cargo test --manifest-path src-tauri/Cargo.toml ordinary_cloudflare -- --nocapture
```

## Task 8: Complete Agent Stage 0 Integration

Files:

- modify `src-tauri/src/proxy/mod.rs`;
- modify the new Stage 0 modules;
- update tests and compatibility projections only.

Steps:

1. Construct an owned `PreparedGeneration` only after the actual provider,
   model, credential, channels, final upstream body, response codec, and log
   context are known.
2. Route authenticated Agent generation through
   `CacheDirectedRelay::dispatch_once` and return.
3. Keep excluded paths on their legacy code.
4. Move each Agent side effect to one owner finalizer: usage, session,
   provider-prefix observation, route affinity, cache write, metrics, and
   successful history.
5. Remove only Agent branches made unreachable by this migration.

Verify:

```powershell
cargo fmt --manifest-path src-tauri/Cargo.toml -- --check
cargo test --manifest-path src-tauri/Cargo.toml
npm run test:metrics
npm run build
git diff --check
```

## Task 9: Add Shadow Affinity Identity

Files:

- add `src-tauri/src/proxy/affinity_identity.rs`;
- modify `src-tauri/src/proxy/session_identity.rs` only to extract shared pure
  canonicalization helpers without changing existing output;
- modify `src-tauri/src/proxy/mod.rs` for shadow call sites.

Steps:

1. Derive a separate `AffinityIdentity`; never reuse
   `SessionIdentity.provider_cache_key`.
2. Keep trusted conversation identity separate from cohort identity.
3. Do not treat client `prompt_cache_key` or content anchors as trusted
   continuation identity.
4. Derive `CacheRealmId` after actual credential selection using a non-secret,
   stable key-record ID plus provider deployment, channel, and actual model.
5. Hash stable instructions/tools/schema for cohort identity without mutating
   the request.
6. Record policy-compute latency.

Tests first:

- same Agent/stable prefix across conversations shares cohort;
- different workspace, Agent, provider, model, realm, instructions, tools, or
  schema separates cohort;
- different trusted conversations separate continuation identity;
- unavailable trusted identity disables continuation;
- request value and final wire bytes remain unchanged;
- realm output never contains credentials or key IDs in plaintext.

Verify:

```powershell
cargo test --manifest-path src-tauri/Cargo.toml affinity_identity -- --nocapture
cargo test --manifest-path src-tauri/Cargo.toml session_identity -- --nocapture
```

## Task 10: Add Shadow Assignment Store

Files:

- add `src-tauri/src/proxy/cache_affinity.rs`;
- modify `src-tauri/src/state.rs`;
- extend persisted runtime state with serde-defaulted shadow fields.

Steps:

1. Store only hashed realm/conversation/cohort IDs, policy epoch, lane, shard,
   timestamps, and bounded evidence counters.
2. Preserve old runtime-state compatibility with `#[serde(default)]`.
3. Do not migrate old prefix waterlines into shadow assignments.
4. Keep assignments sticky across restart and expire after 24 inactive hours.
5. Cap records at 4096 and evict by oldest `last_seen_at`.
6. Consume correlated successful compact events to increment anchor epoch;
   never infer compaction from changed content.
7. Compute candidate decisions and observations without changing outbound
   cache metadata.

Tests first:

- old state loads empty shadow state;
- new state round-trips;
- unknown version, expired, and over-limit entries are safely ignored/evicted;
- shard growth affects only new assignments;
- tool burst, failure, and missing usage do not create positive learning;
- restart preserves active assignment;
- compact event changes anchor epoch only for the correlated trusted
  conversation.

Verify:

```powershell
cargo test --manifest-path src-tauri/Cargo.toml cache_affinity -- --nocapture
cargo test --manifest-path src-tauri/Cargo.toml runtime_state -- --nocapture
```

## Task 11: Connect Shadow Observation And Metrics

Files:

- modify Stage 1 modules, `metrics.rs`, and Agent owner finalization;
- extend TypeScript API types only for optional diagnostics.

Steps:

1. Assign a shadow ticket before the one authorized send.
2. Observe it only from the final owner using real usage, terminal state, tail
   class, and provider gap classification.
3. Exclude failed, incomplete, missing-usage, and giant-tail-inconclusive
   samples from positive learning while counting coverage.
4. Add shadow/applied mode, realm, cohort, lane, shard, policy epoch,
   decision/skip reason, and compute latency to diagnostics.
5. Assert outbound bytes and headers are identical with shadow enabled or
   disabled.

Verify:

```powershell
cargo test --manifest-path src-tauri/Cargo.toml shadow_affinity -- --nocapture
cargo test --manifest-path src-tauri/Cargo.toml stream_upstream_ -- --nocapture
npm run test:metrics
npm run build
```

## Task 12: Final Stage 0/1 Verification

Run:

```powershell
cargo fmt --manifest-path src-tauri/Cargo.toml -- --check
cargo test --manifest-path src-tauri/Cargo.toml
npm run test:metrics
$env:CCS_VERIFY_TOTAL='100000'; npm run verify:cache
npm run verify:real-metrics
npm run build
git diff --check
```

Then inspect a live metrics snapshot and confirm:

- default Agent inbound-to-attempt ratio is `1:1`;
- any `2:1` row has only a reasoning-compatibility label;
- foreground cache wait remains zero;
- shadow-on and shadow-off wire hashes match;
- successful history contains one row per successful inbound;
- errors remain visible in totals and failed diagnostics;
- no release bundle or version file changed.

## Stop Conditions

Stop the current slice and fix it before continuing if:

- an Agent inbound exceeds its attempt budget;
- a shadow decision changes outbound bytes or headers;
- a response-head/body cancellation loses final accounting;
- a pre-terminal stream error enters successful history;
- existing non-Agent or compact tests regress;
- local preparation exceeds the approved performance budget.
