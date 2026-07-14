# Cache-Directed Relay V1 Design

## Status

Approved architecture, pending written-spec review.

The current continuation also seeds the Stage 2 static-cohort canary at a
bounded 5% candidate arm for new trusted conversations. Promotion remains
blocked on the live comparison gates below; the canary never adds requests,
waits, or retries and only changes the allow-listed provider cache key.

Baseline: Atoapi v0.2.3 at commit `6b22e24`.

## Scope

V1 covers authenticated Agent generation traffic after local validation and
route preparation. It does not take over:

- `/responses/compact`;
- model-list requests;
- user-triggered capability probes;
- non-Agent compatibility and retry behavior;
- packaging or release-version changes.

Existing Anthropic cache-control behavior is preserved in Stage 0. New cohort
and native-breakpoint experiments initially target only provider/model/channel
profiles that already accept the relevant OpenAI-compatible cache metadata.

## Objective

Increase real provider prompt-cache reuse without adding foreground wait,
changing conversation semantics, hiding extra cache-related requests, or
coupling cache policy back into the HTTP handler.

V1 separates conversation identity from provider cache affinity and places the
complete Agent upstream lifecycle behind one deep module. Later cache
strategies can then change without spreading send, relay, and accounting
decisions across `proxy/mod.rs`.

## Evidence And Targets

The live baseline at design time showed:

- provider request hit rate: `444 / 445 = 99.775%`;
- provider token cache ratio: `95.819%` overall and `94.587%` in the recent
  30-minute window;
- stable warm-prefix token cache ratio: `98.077%`;
- classified gap: `46.75%` real new tail, `2.67%` avoidable, and `50.59%`
  provider-unstable;
- avoidable gap was only `0.107%` of all input tokens;
- eight unstable requests produced `1,209,856` gap tokens, dominated by three
  requests with `780K-840K` tool-output characters;
- natural request spacing showed no monotonic cache benefit from waiting.

The foreground cache wait therefore remains exactly zero. Applied-policy
stages target:

- `provider_unstable_tokens / input_tokens <= 0.5%`;
- provider-unstable tokens are at most `20%` of the classified gap;
- rolling 24-hour provider token cache ratio is at least `97%`;
- stable warm-prefix ratio remains at least `98%`;
- avoidable gap remains at most `0.15%` of input tokens;
- no material TTFT or error-rate regression.

These are rollout gates, not promises that an external provider will always
meet them.

## Attempt Policy

The default Agent generation budget is one actual upstream HTTP POST per
inbound request. Cache placement, session reuse, key pools, gzip, network
errors, redirects, 413 handling, and protocol compatibility can never add an
attempt.

V0.2.3 has one existing, user-authorized exception: when an Agent request
receives evidence classified by the existing reasoning-effort compatibility
logic, it may send one lower-effort attempt in the same inbound request. V1
preserves that behavior during Stage 0, but makes it an explicit
`ReasoningCompatibility` attempt policy with a hard budget of two. It must be
reported as two attempts belonging to one inbound request, never as two inbound
requests.

An explicit reasoning-parameter rejection can update the configured model and
UI immediately. An opaque 502 may use the second attempt only when the existing
strict probe predicate allows it. Generic 5xx responses never receive this
exception. No third attempt is allowed; a second explicit rejection may only
update the next request's configured effort.

This exception is independent of cache policy. Removing it in favor of strict
one-to-one behavior is a separate product change, not part of V1 cache work.

## Hard Invariants

1. A default Agent generation that reaches dispatch creates exactly one actual
   upstream HTTP POST.
2. The only multi-attempt Agent policy is the explicit, bounded reasoning
   compatibility exception described above.
3. No automatic cache prewarm, companion request, key failover, protocol
   fallback, 413 rescue, gzip fallback, full-context retry, or redirect follow
   is allowed for Agent generation traffic.
4. Foreground cache wait is `0ms`; cache learning never sleeps on the request
   path.
5. Full request semantics are preserved. V1 does not summarize, trim, reorder
   arrays, remove history, or change tools, model, reasoning, and output
   settings except for the existing authorized reasoning fallback.
6. `previous_response_id` is used only with trusted conversation identity and
   a model-scoped capability that was verified independently. A rejected delta
   is returned as the original error without a hidden full-context retry.
7. Downstream cancellation after dispatch ownership begins does not cancel the
   owner task. It continues through a protocol terminal event or an error and
   completes usage, cache learning, metrics, and final accounting.
8. Failed or incomplete calls update attempt and error accounting but never
   enter successful request history.
9. Current user configuration and the actual resolved upstream model always
   outrank learned state.
10. Each inbound outcome and each actual upstream POST is accounted exactly
    once, including transport failures before response headers.

## External Seam

The deep module is `CacheDirectedRelay`. A private builder creates an owned,
validated `PreparedGeneration` after authentication, routing, model mapping,
reasoning selection, credential selection, and protocol transformation.

```rust
pub enum ConversationIdentity {
    Trusted {
        conversation_key: ConversationKey,
        anchor_epoch: AnchorEpoch,
    },
    Unavailable,
}

pub struct PreparedGeneration {
    pub request_id: RequestId,
    pub permit: InboundPermit,
    pub route: Arc<ResolvedRoute>,
    pub config: Arc<GenerationConfigSnapshot>,
    pub credential: SelectedCredential,
    pub agent_id: AgentId,
    pub workspace_id: WorkspaceId,
    pub conversation: ConversationIdentity,
    pub client_channel: Channel,
    pub upstream_channel: Channel,
    pub client_request: Arc<Value>,
    pub upstream_body: Value,
    pub upstream_headers: HeaderMap,
    pub response_codec: ResponseCodec,
    pub log_context: GenerationLogContext,
    pub client_stream: bool,
}

pub struct ClientRelay {
    pub status: StatusCode,
    pub headers: HeaderMap,
    pub body: Body,
}

impl CacheDirectedRelay {
    pub async fn dispatch_once(
        &self,
        prepared: PreparedGeneration,
    ) -> Result<ClientRelay, DispatchStartError>;
}
```

The builder preserves the distinction between the original client request,
provider-prefix material, and the final upstream body. `ResponseCodec` owns
cross-protocol status, header, JSON, and SSE transformations. Final usage and
TTFT diagnostics are written by the owner task; they are not falsely exposed
as complete when `ClientRelay` is first returned.

`InboundPermit` is non-cloneable and consumed before network I/O. The external
Interface has no retry, prewarm, wait, probe, or alternate-send entry point.

## Ownership Lifecycle

`dispatch_once` performs these ordered actions:

1. consume `InboundPermit`;
2. create the bounded downstream channel and response-head oneshot;
3. spawn the single tracked owner task before starting network I/O;
4. let the owner record an attempt ID immediately before each authorized HTTP
   POST;
5. wait only for the response head needed to construct `ClientRelay`.

Dropping the handler future or the response-head receiver while headers are
still pending does not cancel the owner. The owner treats the downstream as
disconnected, finishes the authorized send, drains the upstream to a terminal
state or error, and finalizes accounting.

Owner tasks are registered with an application task tracker. Graceful shutdown
waits for tracked tasks within the application's shutdown budget. Forced
cancellation or panic runs an exactly-once finalizer that records
`relay_aborted`; it cannot create successful history or a cache/session write.

## Stage-Specific Internal Modules

Stage 0 introduces only the modules needed for the foundation:

- `CacheDirectedRelay`: owns the Agent generation lifecycle;
- `AttemptGate`: enforces the default and reasoning-compatibility budgets;
- `OneShotTransport`: consumes one attempt token and performs one HTTP POST;
- `CompletionRelay`: owns upstream parsing, forwarding, and finalization;
- `MetricsSink`: records inbound outcomes, attempts, and shadow diagnostics.

Stage 1 adds `ShadowCachePolicy` and a persisted
`ShadowAffinityAssignmentStore` so stickiness, restart behavior, and bounded
state can be verified without changing outbound metadata. Later applied stages
add private `CapabilityLedger` and `ObservationActor` implementations. None of
these policy modules is required to complete Stage 0. This keeps the first
refactor focused while preserving one external Interface.

The provider is a true external dependency behind the private transport port.
Production uses the existing reusable reqwest clients with redirect following
disabled for generation POSTs. Tests use a scripted adapter that counts actual
sends and controls headers, chunks, terminal events, and failures.

## Conversation Identity

Response continuation requires `ConversationIdentity::Trusted`, derived only
from an authenticated, explicit thread, conversation, or session identifier.
Its `anchor_epoch` starts at zero and changes only after a correlated,
successful compact event or an explicit client anchor-reset marker. Client
`prompt_cache_key` and content-derived fallback anchors are not trusted
conversation identity.

When trusted identity is unavailable:

- `previous_response_id` reuse and `SessionLedger` writes are disabled;
- the complete context is forwarded once;
- content anchors may be used only for diagnostics and non-semantic provider
  cache affinity.

This fail-closed rule prevents two independent Agent conversations with an
identical opening message from sharing response-continuation state.

## Provider Cache Realm And Affinity

Credential selection happens before cache planning. Every selected credential
has a non-secret, stable `cache_realm_id`. The conservative default is one
realm per actual key record. A key pool may share a realm only when its
configuration explicitly declares that all enabled keys belong to the same
provider cache domain.

Provider cache affinity includes:

- workspace and agent scope;
- normalized provider deployment and channel;
- actual upstream model;
- selected `cache_realm_id`;
- a canonical digest of stable instructions, tools, and output schema;
- the sticky assignment's policy epoch, lane, and shard.

Different conversations with an identical stable prefix may share this
affinity. This shares only provider cache routing, never conversation state or
model output. The provider still requires an exact prompt-prefix match before
reusing computation.

OpenAI's current prompt-caching guide states that `prompt_cache_key` is combined
with the prefix hash to influence routing, exact prefix matching is still
required, and traffic should stay near 15 requests per minute per key. V1
begins with one shard at current traffic and may add stable shards for new
conversations without remapping active assignments.

Reference:
<https://developers.openai.com/api/docs/guides/prompt-caching>

## Sticky Assignment

An affinity assignment stores the complete derived key, epoch, lane, and shard
for a trusted conversation. A policy-epoch change affects only new assignments;
it does not recompute active keys.

Assignments are persisted without secret material and reloaded after restart.
They expire after 24 hours of inactivity, matching the maximum legacy cache
retention horizon used by this application.

The existing compact handler remains outside this Module, but after a
successful compact it may publish an `AnchorResetEvent` containing the trusted
agent/conversation key and a non-secret compact generation. Stage 1's shadow
assignment store consumes that event and increments `anchor_epoch`. An
explicit, authenticated client anchor-reset marker has the same effect. If a
compact call cannot be correlated to trusted conversation identity, V1 does
not guess from changed content and does not reset the assignment. A new trusted
conversation ID always starts a new assignment.

Without trusted conversation identity, a steady cohort can still be computed
deterministically from the privacy domain and stable-prefix digest, but no
continuation or persistent per-conversation lane state is created.

## Cache Placement Plan

The shadow policy returns an immutable plan:

```rust
struct ProviderCachePlan {
    affinity_key: Option<CacheAffinityKey>,
    capability_epoch: CapabilityEpoch,
    lane: CacheLane,
    metadata: CacheMetadata,
    observation: ObservationReceipt,
}

enum CacheLane {
    Steady,
    ToolBurstQuarantine,
    CompactedAnchor,
    Transparent,
}
```

The plan may change only allow-listed, provider-recognized cache metadata. It
cannot mutate model-visible content or array order.

### Steady Lane

Normal requests with the same stable prefix use the deterministic cohort key.
Trusted conversations remain pinned to their assignment.

### Tool-Burst Quarantine Lane

When existing diagnostics identify a very large tool-output burst, the trusted
conversation branch moves to a deterministic quarantine affinity. The complete
tool output is still sent. The branch stays quarantined until compaction or a
new trusted anchor; it does not oscillate keys on every request.

For unavailable conversation identity, only the current burst request receives
a deterministic burst affinity. No continuation or sticky branch state is
learned.

This lane is enabled only after a provider/model canary shows lower
provider-unstable gap without TTFT or error regression.

### Compacted Anchor Lane

A real client compaction establishes a new full semantic anchor. It receives a
new assignment and may return to steady learning. Atoapi never creates that
compaction itself.

### Transparent Lane

Missing or unsafe capability evidence preserves the complete request and uses
the current baseline metadata behavior. It does not add unverified fields.

## Capability Profiles

Capabilities are scoped to provider deployment, channel, actual model,
`cache_realm_id`, and configuration epoch. Independent evidence states are kept
for:

- `prompt_cache_key`;
- user-authorized legacy `prompt_cache_retention`;
- modern `prompt_cache_options` and explicit breakpoints;
- Responses continuation with `previous_response_id`.

Existing user authorization for third-party retention is preserved. Official
OpenAI model behavior may use documented defaults. Third-party providers
require existing successful evidence, explicit user authorization, or a
user-triggered verification action before Atoapi adds a new, potentially
unsupported field. Normal Agent traffic is never used as an automatic probe.

For GPT-5.6 and later OpenAI model families, current documentation uses
`prompt_cache_options.ttl` and explicit breakpoints; legacy
`prompt_cache_retention` is deprecated for those families. A third-party custom
`gpt-5.6-*` alias does not inherit support merely from its name.

Only an explicit cache-field rejection can quarantine that capability. A
timeout, network error, HTML 502, or ordinary provider 5xx must not be treated
as unsupported cache metadata.

Before Stage 4, one cache-metadata allowlist must be shared by serialization,
field stripping, differential tests, diagnostics, and log redaction.

## Streaming And Terminal Semantics

Success is a protocol outcome, not merely `HTTP 2xx + EOF`.

`CompletionRelay` uses this state machine:

```text
AwaitingTerminal -> TerminalSucceeded
AwaitingTerminal -> TerminalFailed
TerminalSucceeded -> TrailingAnomaly | CleanEof
TerminalFailed -> CleanEof
```

- Native Responses success requires `response.completed`. `[DONE]` may arrive
  before or after it and does not stop upstream reading or forwarding.
- `response.failed`, an SSE error event, a stream transport error, or EOF before
  the required terminal event is a failed outcome.
- Native Chat success requires `[DONE]`, a valid non-stream success object, or
  an explicitly verified compatibility terminal policy.
- Cross-protocol streams use `ResponseCodec` to validate the upstream terminal
  and synthesize exactly one correct client terminal sequence.
- After a successful protocol terminal, the owner continues forwarding while
  connected and drains through EOF. A later socket/read failure is recorded as
  `trailing_transport_anomaly` but does not overturn the already confirmed
  model success or emit a downstream body error.
- A third-party Responses stream that provides only `[DONE] + EOF` is not a
  native success by default. It requires an explicit provider/model
  `DoneAtEof` terminal capability. Stage 0 preserves any existing configured
  compatibility behavior while recording the strict-policy shadow result.
- Incomplete or failed streams never write response-session state, positive
  cache learning, or successful history.

The downstream body carries a real relay error type rather than
`Infallible`. If still connected, an upstream read failure terminates the body
as an error instead of masquerading as normal EOF.

The bounded channel intentionally applies backpressure to a slow but connected
consumer. V1 does not promise to drain an upstream while a client holds the
body forever without polling; doing that would require disk spooling,
unbounded memory, or dropping output. When the receiver is dropped, a blocked
send wakes, the owner switches to drain-without-forward mode, and finalization
continues.

## Metrics Model

Stage 0 separates two records:

- `InboundOutcomeLog`: exactly one final record per inbound request;
- `UpstreamAttemptLog`: exactly one record per actual HTTP POST, including
  failures before headers.

Aggregates include:

- `inbound_requests`;
- `generation_attempts`;
- `multi_attempt_inbounds`;
- `max_attempts_per_inbound`;
- `successful_inbounds` and `failed_inbounds`;
- `policy_compute_ms` and `foreground_cache_wait_ms`;
- policy/capability epoch, shadow/applied mode, cohort, lane, shard,
  `cache_realm_id`, attempt policy, and decision or skip reason.

The success-history UI remains 2xx/protocol-success only. Error totals retain
all failures. Reasoning fallback attempts share the same inbound ID and carry
distinct attempt indices and reason labels.

## Failure And Performance Behavior

- New cache-policy failure fails open to the exact Stage 0 baseline metadata
  behavior and never blocks the request.
- Cache policy adds no disk I/O, sleep, or global lock held across network I/O.
  Existing config snapshots and key-selection locks are migrated separately;
  Stage 0 does not falsely claim they already vanished.
- `OneShotTransport` disables internal retry, gzip fallback, key failover, and
  redirect following. Each invocation consumes one attempt token.
- Upstream transport, HTTP, and stream errors are returned truthfully after the
  authorized attempt budget is exhausted.
- A full learning queue drops the learning sample instead of adding response
  backpressure.
- Generic 4xx/5xx responses do not rotate cache affinity or lower cache
  capability.

Performance instrumentation must prove that `foreground_cache_wait_ms == 0`.
For a 300KB request, shadow policy compute p95 must be at most 5ms; for a 2MB
request it must be at most 20ms. Total local preparation p95 may not regress by
more than 5ms against the v0.2.3 fixture benchmark.

## Delivery Stages

### Stage 0: Foundation Relay

Move Agent send and streaming ownership behind `dispatch_once`, split inbound
and attempt metrics, and preserve current final wire behavior and the explicit
reasoning compatibility exception. No new cache strategy is active.

Exit gate:

- default Agent paths make one actual POST;
- the authorized reasoning path makes at most two labeled attempts;
- response-head cancellation and downstream-body cancellation both finalize;
- protocol terminal and exactly-once accounting tests pass;
- final wire bytes, cache/stream/gzip headers, downstream status/headers, SSE
  event order, and JSON semantics match v0.2.3 outside allow-listed fixes.

### Stage 1: Shadow Policy

Compute candidate realm, cohort, lane, shard, and capability decisions but do
not apply them. Record compact diagnostics only.

Exit gate:

- at least 300 successful requests and 10 million usage-reported input tokens
  for the selected provider/model profile;
- usage coverage is at least 95%; missing usage is excluded from token ratios
  but counted in coverage;
- no trusted-identity collision or unbounded assignment growth;
- shadow policy performance gates pass.

Stage 1 is the V1 foundation/observation build. It cannot improve provider hit
rate because it does not change outbound cache metadata.

### Stage 2: Static Cohort Canary

Deterministically assign only new trusted conversations on one provider/model
profile to candidate or baseline. Existing conversations remain sticky. Begin
with 5% candidate admission, then 25% and 50%; retain at least 20% baseline
holdout until the policy passes its final gate. A single-user installation may
explicitly assign selected new conversations while preserving separate
baseline conversations.

Evaluate the most recent 24-hour window only after each arm has at least 50
comparable successes and 5 million usage-reported input tokens, segmented by
actual model, input-token bucket, tail class, and network path.

Exit gate:

- candidate token cache ratio is not more than one percentage point below
  baseline;
- candidate error rate is not more than 0.5 percentage points above baseline;
- candidate TTFT p95 is not worse by more than the larger of 300ms and 5%;
- no unlabelled multi-attempt inbound or semantic differential occurs.

In addition to those safety gates, promotion requires at least one efficacy
gate:

- candidate token cache ratio exceeds baseline by at least 0.25 percentage
  points; or
- new-session first-two-turn cache shortfall per input token is at least 20%
  lower than baseline; or
- provider-unstable tokens per input token are at least 20% lower than
  baseline.

If no efficacy gate passes, the candidate remains a canary or returns to
shadow; it cannot advance merely because it is non-inferior.

Stage 2 is the first build that can produce a real cache-hit improvement.

### Stage 3: Tool-Burst Quarantine Canary

Enable quarantine only after shadow data contains giant-tail evidence for the
selected profile. Promotion requires at least three comparable giant-tail
events in each candidate and holdout arm. Use the same 24-hour comparison and
segmentation rules as Stage 2.

Exit gate:

- post-burst provider-unstable/input is at least 20% lower than baseline, or the
  candidate post-burst token cache ratio is at least 0.5 percentage points
  higher than baseline;
- rolling 24-hour provider-unstable/input is no higher than baseline; the
  global applied-policy target remains `<=0.5%`;
- post-burst stable-prefix cache ratio is not lower than baseline;
- TTFT, error, semantic, and attempt gates remain green.

### Stage 4: Capability-Gated Native Controls

Add official or verified modern cache options and explicit breakpoints. Keep
legacy user-authorized retention separate. Do not infer third-party support
from a model name.

Exit gate: field-specific compatibility tests pass, unsupported-field evidence
quarantines only that field, and rejection never causes a hidden retry.

## Verification Strategy

### Pure Policy Tests

- identical stable prefixes in one cache realm produce one cohort;
- different workspace, agent, provider, actual model, realm, instructions,
  tools, or schema produce different affinity;
- trusted conversations never share continuation state;
- unavailable identity disables continuation and session writes;
- arrays and all non-allow-listed fields remain unchanged;
- assignment, shard, and lane decisions are deterministic, sticky across
  restart, and reset only on expiration or a real new anchor;
- generic 502/timeout does not disable cache capability;
- explicit unsupported-field evidence disables only that field and profile;
- old policy epochs cannot remap active assignments or update current learning.

### Transport And Attempt Tests

Using the scripted transport adapter:

- normal Agent generation: one inbound and one actual POST;
- redirect response: no automatic POST follow;
- connection failure before headers: one attempt record and no retry;
- gzip rejection, key failure, 413, delta rejection, and protocol compatibility
  failure: one default Agent POST and no fallback;
- explicit reasoning rejection: at most one labeled lower-effort attempt;
- generic 502 outside the strict reasoning predicate: no second attempt;
- duplicate `InboundPermit`: rejected before network I/O;
- policy/state failure: one baseline-compatible send.

### Relay Lifecycle Tests

- normal streaming forwards each event exactly once and finalizes once;
- delayed consumption with more than channel capacity applies bounded
  backpressure without creating another send;
- dropping the receiver while the sender is blocked wakes the owner, drains to
  terminal, and finalizes;
- dropping `dispatch_once` while upstream headers are delayed leaves the owner
  alive through terminal accounting;
- Responses `[DONE]` before `response.completed` preserves the late terminal;
- partial stream plus transport error, SSE error, or EOF without terminal is a
  failed outcome and never enters successful history;
- concurrent requests in the same and different conversations keep correct
  identity, attempt counts, and exactly-once finalization;
- owner panic, graceful shutdown, and forced abort create one failed inbound
  outcome and no positive cache/session write.

### Differential Fixtures And Benchmarks

Captured, secret-free fixtures cover normal conversations, mapped and unmapped
models, all reasoning levels, tool calls, giant tool outputs, compaction,
verified continuation, stream/non-stream, Responses-to-Chat conversion, and
error bodies.

Stage 0 compares:

- final serialized upstream wire bytes and relevant headers;
- downstream status and headers;
- SSE event bytes, order, and count;
- non-stream JSON semantics;
- attempt and final accounting.

Only explicit fixes named in this specification may differ from v0.2.3.
Benchmarks include 300KB and 2MB request bodies with large tool/schema material.

### Live Canary Comparison

Every live sample is segmented by provider, actual model, input-token bucket,
tail class, network path, attempt policy, policy epoch, cohort, lane, shard,
and realm. Compare:

- real `cached_tokens / input_tokens`;
- provider-unstable, avoidable, and real-new-tail gaps;
- TTFT p50/p95 and total-duration p50/p95;
- upstream error rate and field-specific rejection rate;
- inbound-to-upstream attempt cardinality;
- usage coverage and local policy-compute latency.

Freeze a candidate immediately if an inbound exceeds its authorized attempt
budget, any non-allow-listed semantic or wire differential appears, or a cache
field is explicitly rejected. The numeric Stage 2 and Stage 3 gates determine
promotion after the minimum sample size.

## Manual Test Sequence

For one selected provider/model canary:

1. start a baseline conversation and send three ordinary turns;
2. start a candidate conversation with the same Agent profile and send three
   equivalent ordinary turns;
3. verify each default row has one inbound and one upstream attempt, with the
   requested model and reasoning unchanged;
4. trigger one known reasoning-effort compatibility case and verify any second
   attempt is labeled, bounded, and attached to the same inbound row;
5. run one tool call with a deliberately large but non-sensitive output, then
   send two ordinary follow-ups;
6. stop consuming one response before headers and another after receiving part
   of the body; verify both owner tasks finalize without another send;
7. run a real client compaction and verify a new anchor without conversation
   mixing;
8. compare provider-reported input/cached tokens and Atoapi TTFT with the
   separate baseline, not with synthetic local hit labels.

No cache stage requires hidden traffic or duplicate probe calls.

## Non-Goals

- semantic summarization or Atoapi-created compaction;
- automatic third-party compatibility probes;
- fabricated cache-hit metrics;
- provider cache clearing or guaranteed provider retention;
- redesigning model mapping, reasoning policy, or request-row UI;
- changing non-Agent retry behavior;
- guaranteeing lower model generation time after the provider accepts a
  prompt.

## Expected First Implementation

The first implementation contains Stage 0, Stage 1, their test adapters,
differential fixtures, benchmarks, and metrics. It is the safe V1 foundation
and shadow-observation build. It does not change provider cache affinity and is
not expected to improve hit rate by itself.

The first potentially hit-improving build is Stage 2. Stage 2 is enabled only
after the foundation passes automated verification and Stage 1 produces enough
safe shadow evidence for the selected provider/model profile.
