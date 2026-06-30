# Atoapi Current Workflow Checkpoint

Last updated: 2026-06-30

## 2026-06-30 Active Rules

- Active project root is `G:\Atoapi`; do not use old `G:\Flutter\ccs++` as source.
- Current line is v0.1.55+ source. Package only after live-log comparison and build checks.
- Cache-hit optimization hard gate: no active warmup/prewarm, no companion sync request, no extra upstream request, no normal main-path `previous_response_id + delta`.
- Main Responses session-delta is allowed only for 413 self-rescue or compact/compatibility paths with strict same provider/model/scope/tool-context checks.
- Foreground Responses guard budget is capped at about +3s; do not solve hit rate by restoring long waits.
- Do not change, trim, compress, reorder, or summarize tool output content by default.
- For log analysis, classify first: real new tail, true avoidable, cold read, session/context split, upstream error, or statistics-label issue. Do not tune blindly.
- Current live v0.1.53/v0.1.54 analysis showed warm adjusted bucket hit near 99.98% after using logged gap fields; raw hit was mainly lowered by real new tool-output tails, cross-upstream cold starts, and upstream errors, not broad avoidable gaps.
- v0.1.55 keeps outbound `prompt_cache_key` stable, but foreground local prefix waiting must use current-upstream exact/family state only; cross-upstream alias can preserve diagnostics/state, not block a live request.
- v0.1.55 adds multi-key prefix affinity: same provider/model/prefix should prefer the same healthy key to avoid key-rotation cache cold starts; key failure still clears affinity and fails over.
- Responses `prompt_cache_key` should be scoped by stable session anchor: same session appended tail keeps the key; different session anchor splits the key to reduce upstream cache cross-talk.

## Current Flow

Current source/package candidate:

`v0.1.35-atoapi-compact-diagnostics-cache-error-isolation-20260628`

Current release folder:

`G:\Flutter\ccs++\releases\v0.1.35-atoapi-compact-diagnostics-cache-error-isolation-20260628`

Current package is v0.1.35 with two lines. Compression line: it keeps v0.1.32 sync-main gzip/cooldown, v0.1.33 old-chat Chat compatibility, and v0.1.34 Chat stream aggregation for large old Responses compact requests. It adds backend-only compression phase diagnostics (`upstream_headers_ms`, `upstream_first_chunk_ms`, `aggregate_done_ms`, `sse_chunks`) and a conservative Chat non-stream fast path only for small/low-risk compact compatibility bodies. Hit-rate line: it isolates SSE/body errors from prefix learning and cache writes so failed/incomplete upstream responses do not pollute waterlines. Large old conversations still use `stream=true` aggregation and return non-stream Responses JSON to ZCode. This remains one upstream request and does not add active prewarm, companion sync requests, or normal main session-delta.

Response cache/TTFT/tail optimization has a separate baseline set: v0.1.27 / v0.1.28 / v0.1.29. v0.1.27 protects same-prefix cold-read isolation, v0.1.28 adds bounded stale large-tool-output catch-up, and v0.1.29 adds early-anchor/small-context large-tool-output catch-up. Future Response hit-rate changes must compare against these three first, while keeping v0.1.0 forwarding feel and the v0.0.52/v0.0.58/v0.0.64 zero-extra-request cost line. v0.1.30 is a negative live candidate, not a baseline. v0.1.16 is a failed cache-hit experiment and must not be used as a baseline.

Mandatory comparison rule now uses historical cumulative provider token ratio including cold starts as the primary hit-rate number. Recent 5 minute ratio is secondary trend evidence only.

v0.1.24 validation rule: compare live logs against v0.1.13, v0.1.15 early, v0.1.21, v0.1.22, and v0.1.23. First use `local_prepare_ms`, `prefix_guard_wait_ms`, `upstream_ttft_ms`, `upstream_headers_ms`, `upstream_first_chunk_ms`, `upstream_retry_wait_ms`, `upstream_attempts`, `request_body_bytes`, `sent_body_bytes`, `gzip_attempted`, `gzip_fallback_used`, `upstream_header_wait_class`, `request_body_buckets`, and cold_unstable prefix fields to explain TTFT and cold-read gaps before changing cache logic. Do not restore active prewarm, companion sync requests, or normal main session-delta. Request-body gzip is provider opt-in, default off, and must fallback safely; after fallback it should cool down rather than retry every large request.

Historical v0.1.12 live reference, not the active baseline:
- Running exe: `G:\Flutter\ccs++\releases\v0.1.12-atoapi-v010-light-avoidable-guard-20260625\atoapi.exe`
- Snapshot: `G:\Flutter\ccs++\logs\metrics-v012-live-20260625.json`
- Recent 5 minute provider token ratio: about 99.26%.
- Errors/retries: 0 / 0.
- TTFT p95: about 14.2s.
- Total cumulative provider token ratio: about 91.23%, mainly because the first 231k-token cold start is included.
- Current direction: do not treat this as the active baseline anymore. v0.1.13 is the primary baseline because the user's live cumulative observation was better.
- v0.1.14 candidate package: `G:\Flutter\ccs++\releases\v0.1.14-atoapi-stale-small-avoidable-risk-guard-20260625\atoapi.exe`
- v0.1.14 and later packages must be compared against v0.1.13 first, then v0.1.0 for forwarding/TTFT feel, and historical v0.0.89/v0.0.90 for high-hit behavior.

Historical context retained:

v0.0.84 is now marked as a negative optimization for real traffic because missing-state related-prefix long waits produced worse provider cache ratio and very high TTFT. v0.0.86/0.0.87 were also too broad/fragile around tool-output tail classification. v0.0.88 returns to the v0.85-style exact avoidable protection line, keeps v0.67 large-tool-output catch-up waiting, and retains v0.75 Responses compact compatibility. v0.0.89 keeps that recovery line and fixes the remaining priority issue: exact avoidable evidence must not be short-capped by current-tool-output tail caps. v0.0.90 tested repeated 512/1536 wait escalation but is not accepted as baseline because it raised TTFT p95 strongly and did not beat v0.0.89 overall. v0.0.91 reverts v0.0.90's wait escalation. v0.0.92 targeted medium 3072/3584/4608 new-tail gaps only when current/previous tool tails were compact, but live evidence showed the guard did not trigger and metrics were worse. v0.0.93 reverts v0.0.92 and returns to the v0.0.89/v0.0.91 cache line. v0.0.94 adds backend-only prefix guard skip diagnostics for the v0.0.93 161k avoidable cold-read sample without adding prewarm, sync calls, or broader wait escalation. v0.0.95 uses v0.0.94 logs to cap repeated weak waits for unstable dynamic tool-output false-avoidable gaps while preserving v0.0.89 avoidable-first accounting. v0.0.96 is now marked as a negative optimization because dynamic tail evidence in the provider prefix fingerprint split every request into a fresh waterline and caused repeated `no_prefix_state`. v0.0.97 recovers the v0.0.89/v0.0.90 stable waterline-control key. For current live work, v0.1.13 is the primary accepted baseline; v0.0.89/v0.0.90 remain historical high-hit references and lessons.

## What Was Just Done

- Read live v0.0.84 `/admin/metrics`.
- Confirmed bad state:
  - provider cache token ratio around 81%-82%.
  - recent 5 minute ratio around 79%-88%.
  - `cache_avoidable_gap_tokens` looked low only because gaps shifted into `new_tail_gap_tokens`.
  - `prefix_guard_wait_source=missing-state` waited 75s but still got `cache_read_tokens=0`.
  - large current tool-output tails waited 75s-81s but still hit only around 66%-77%.
- Removed missing-state related-prefix wait.
- Capped current tool-output tail waits when `avoidable == 0`.
- Added regression coverage for both failures.
- After v0.0.85 live logs showed `responses_avoidable_gap` waiting 60-125s without recovering, v0.0.86 classified current `1024+` tool-output tails as unreliable for avoidable-gap waiting before the wait starts.
- v0.0.88 correction:
  - Provider gap accounting no longer discards exact avoidable evidence just because the current request has a tool-output tail.
  - Large current tool-output tails can still cap wait time to protect TTFT, but the gap remains classified as avoidable when exact prefix evidence proves it.
  - This preserves v0.85's exact avoidable protection and v0.67's cost-free tool-tail catch-up, while retaining v0.75 non-SSE compact JSON compatibility.
- v0.0.89 correction:
  - If `avoidable > 0`, Responses prefix waiting now follows the avoidable-gap floor instead of current-tool-tail cap.
  - Current-tool-tail cap remains active only when the gap is pure new-tail or there is no exact avoidable evidence.
  - Regression tests now assert both accounting and wait-priority behavior.
- v0.0.90 correction:
  - v0.0.89 live logs showed repeated `512` new-tail gaps on two stable exact prefix keys after 30-45s waits.
  - Repeated 512-aligned small new tails now escalate from 45s to 60s/75s depending on streak.
  - Single small new-tail occurrences keep the previous floor to avoid broad TTFT regression.
- v0.0.91 correction:
  - v0.0.90 live logs showed total hit rate near v0.0.89 but TTFT p95 regressed from about 50s to about 73s.
  - v0.0.90 also increased 513-1024 and 2049-4096 new-tail buckets in the sampled run.
  - Reverted v0.0.90 repeated small-tail wait escalation. Keep the v0.0.89 baseline while looking for non-wait-heavy ways to distinguish true tool-output new tail from catch-up lag.
- v0.0.92 correction:
  - Live logs showed two different medium/large-tail classes: `3584` with only ~134 current tool-output chars, and `7680` with ~27k tool-output chars.
  - Added `responses_compact_tool_tail_recovery_guard` for `3072..4608` gaps with compact current/previous tails.
  - Kept 8k+ real tool-output tails on the short cap path to avoid repeating v0.0.90 TTFT regression.
- v0.0.93 correction:
  - v0.0.92 live/current sample showed current ratio around `98.615%`, recent around `98.626%`, `new_tail=26112`, `avoidable=0`, and TTFT p95 around `63618ms`.
  - The new `responses_compact_tool_tail_recovery_guard` did not appear in the sampled request logs, so it was ineffective for the observed large tool-output and cold-start tails.
  - Removed the compact-tail recovery guard and its regression test. v0.0.93 returns to the v0.0.89/v0.0.91 cache strategy while keeping zero extra upstream requests.
- v0.0.94 correction:
  - v0.0.93 live logs showed one critical large avoidable cold-read: `input_tokens=174456`, `cache_read_tokens=0`, `cache_avoidable_gap_tokens=161280`, with no prefix wait fields recorded.
  - Analysis versus v0.0.89/v0.0.90: v0.0.89's avoidable-first rule is still the right baseline; v0.0.90's broad wait escalation remains too expensive. The issue is that the large avoidable evidence was only visible after the request, not explainable before the send.
  - Added backend-only `prefix_guard_skip_reason` diagnostics and a regression test for large avoidable cold-read protection. Do not claim hit-rate improvement until v0.0.94 live logs show lower avoidable gap or a clear skip reason.
- v0.0.95 correction:
  - v0.0.94 live logs improved total ratio to about `98.646%`, but exposed many weak long waits on `2048/2560/8192` avoidable gaps with current `4k-32k` tool output.
  - These cases waited around `100-155s` and still produced weak ratios, so they are dynamic tool-output false-avoidable patterns rather than normal v0.0.89 stable avoidable gaps. Truly avoidable gaps of every size are still a must-fix target.
  - Added a wait cap only when `cache_instability_score >= 2` and current tool output is `4k+`. This keeps avoidable accounting intact and preserves stable v0.0.89 avoidable-first tests.
- v0.0.96 correction:
  - v0.0.95 3M-token sample reached about `99.012%`, but still did not beat v0.0.89/v0.0.90.
  - The critical failing cluster was fingerprint `7caa...`: 7 successful requests, input `442,309`, cached `425,472`, total avoidable `15,360`, new tail `0`.
  - That cluster mixed roughly `55k-67k` contexts while a different stable line was around `214k-225k`; the previous fingerprint sample only using the first 64k could let same-head/different-tail contexts share a warm-state waterline.
  - Live v0.0.96 logs proved this was the wrong fix for the control key: every request got a different fingerprint and `prefix_guard_skip_reason=no_prefix_state`, so prefix protection could not connect across turns.
- v0.0.97 correction:
  - Provider prefix fingerprint is restored as a stable waterline/control key, not a dynamic-tail identity key.
  - Dynamic Responses/Chat tails are stripped from the fingerprint sample, matching the v0.0.89/v0.0.90 successful line.
  - Regression tests now assert that dynamic tails do not split the provider prefix fingerprint.
- v0.0.98 correction:
  - Live v0.0.97 logs showed no extra requests, no errors, stable fingerprint, and no avoidable gaps, but repeated 512/1024/3072/6144-style new tails still appeared.
  - The safe fix is narrow: full main requests with 512-2048 token shortfall now learn the sent bucket as the next avoidable guard waterline. This targets repeated 512/1024/1536/2048 tails without broad wait escalation.
  - Backend-only `prefix_lag_*` diagnostics were added so the next log pass can distinguish real new tail, avoidable gap, provider catch-up lag, and long-wait weakness.
  - Large 5k/6k tool-output tails are not forcibly relabeled as full buckets. If they are real new tool/output content, zero-extra-request cache logic cannot honestly make them 100%.
- v0.0.99 correction:
  - v0.0.98 live logs showed the provider itself was fast, but local prefix guard waited about 176-178s before sending warm requests.
  - v0.0.99 caps Responses waits with no exact avoidable evidence to normal TTFT range: 30s for 32k+ context, 20s for 16k-32k, and 12s below 16k.
  - Proven avoidable gaps still keep v0.0.89-style stronger protection. This restores speed without turning off exact avoidable protection.
  - Added a regression test for the 149k cold-start follow-up case so non-avoidable local wait cannot drift back to 180s.

## What Must Not Drift

- Do not restore per-stream companion sync calls.
- Do not restore active prewarm unless the user explicitly reverses the cost-first rule.
- Treat "zero extra upstream requests + no active prewarm" as the hard gate for every cache-hit optimization.
- Do not add cheap-model prewarm as an extra request.
- Do not re-enable normal Responses session-delta while the current third-party upstream rejects it.
- Do not trim, compress, or reorder tool outputs by default; it may change agent semantics.
- Do not restore v0.0.78 prompt_cache_key family waterline by default; it is currently a negative optimization for dynamic tool-output traffic.
- Do not restore v0.0.84 missing-state related-prefix long wait. Related prefix state is not proof that the current exact prefix is warm.
- Do not let current large tool-output tails erase or short-cap exact avoidable evidence. If exact state proves avoidable, protect it first; if avoidable is zero, cap pure new-tail waits to control TTFT.

## Next Live Test Checklist

- `upstream_call_kind` should remain `stream` for normal requests.
- `upstream_call_source` should remain `main`.
- `upstream_requests` should not grow because of this guard.
- `background_prewarm` should stay empty.
- `prefix_guard_wait_source=missing-state` should no longer appear.
- Large current tool-output pure new tails should show short capped waits, not 75s/90s waits.
- Large current tool-output with exact avoidable evidence should show `responses_avoidable_gap`, not `responses_current_tool_output_tail_cap`.
- Repeated 512/1024/1536/2048 new tails should not be solved by blindly increasing wait time. v0.0.90 showed this is too expensive without stable overall gains.
- Repeated 512/1024/1536/2048 tails should first be checked for v0.0.98 sent-bucket learning: the next request should become `cache_avoidable_gap_tokens` with `prefix_lag_classification=avoidable_gap` rather than remaining repeated `new_tail`.
- Large 3072+ or 5k/6k new tails must be classified with `prefix_lag_*`, tail diagnostics, and input/cache deltas before any change. Do not expand sent-bucket learning to those tails without evidence that it is not real newly added tool/output content.
- If user-visible TTFT exceeds 60s, first split local `prefix_guard_wait_ms` from upstream TTFT. If local wait dominates and avoidable gap is zero, it is a local guard issue and must be capped rather than blamed on the upstream.
- v0.0.99 target for this user's current upstream: warm TTFT should return to roughly 10-30s local-visible range, while upstream true first byte remains around 3-8s when warm.
- `responses_compact_tool_tail_recovery_guard` must not appear. v0.0.92 was reverted because this guard did not improve live traffic.
- For large avoidable cold-read cases, inspect `prefix_guard_skip_reason` before changing wait durations. `no_prefix_state` means the prefix state was not available before send; `settle_window_elapsed` means the stored state existed but the floor had already expired.
- If avoidable gaps repeatedly show `weak_long_wait` with large current tool output, treat them as false-avoidable dynamic tool-output candidates and cap wait cost only after instability evidence exists. For all other truly avoidable gaps, keep pressing them down regardless of size.
- `responses_avoidable_gap` should not trigger for clearly dynamic large current tool-output tails.
- Stable medium single tool-output tails may still trigger exact small-gap protection.
- If a large avoidable cluster shows one fingerprint covering materially different context lengths or tails, inspect prefix fingerprint cross-talk before adding more wait time.
- Do not let dynamic tail content participate in the provider prefix waterline key. If tail-level diagnostics are needed, add a separate backend-only diagnostic key rather than splitting the control key.
- Check whether provider token ratio recovers from v0.0.84.
- Check both `cache_avoidable_gap_tokens` and `new_tail_gap_tokens`; do not accept improvements that only rename the gap.
- Watch TTFT p95; v0.0.84-style 80s+ waits are not acceptable unless exact avoidable gap evidence justifies them.

## Comparison Rule

For every later cache change, produce:

- Optimization list: what changed and why.
- Positive effects: metrics that improved.
- Negative or neutral effects: metrics that regressed or did not move.
- Cost gate result: confirm upstream request count did not increase and no active prewarm was introduced.
- Three-group log comparison: use two historical reference groups plus the current version. For this phase, compare v0.0.89, v0.0.90, and the current package. v0.0.91 does not need to be a primary comparison group because it is effectively the v0.0.89 recovery line.
- Baseline comparison: still keep v0.0.52, v0.0.81/v0.0.83, v0.0.84 negative result, latest package, and any relevant historical positive version as broader context.

## Root-Cause-First Optimization Rule

Do not solve cache regressions by trial-and-error tuning. Before changing code, classify the live gap into one primary cause and choose the fastest proven path:

1. Waterline / fingerprint problem
   - Symptoms: one fingerprint covers materially different input lengths, same-head/different-tail contexts, avoidable gap is high while new tail is low or zero.
   - First action: inspect provider prefix fingerprint, exact state key, alias key, and request body sampling before changing wait times.
   - Proven fix direction: improve state partitioning or fingerprint evidence, such as v0.0.96 `len + head64k + tail16k`.

2. True avoidable gap
   - Symptoms: exact/fingerprint state proves previous cached bucket should have been reusable.
   - First action: preserve v0.0.89 avoidable-first priority and check why guard skipped or expired.
   - Proven fix direction: fix guard source, state learning, or skip reason. Do not rename the gap to new tail.

3. Real new tail from tool output or user content
   - Symptoms: avoidable is zero, tail diagnostics show new tool output/message content, cache ratio is already near full.
   - First action: do not over-wait. Compare tail size and TTFT.
   - Proven fix direction: short cost-free catch-up guard only when it has evidence; no extra request and no tool-output rewrite.

4. Provider catch-up lag / bucket lag
   - Symptoms: repeated 512-aligned gaps with stable exact prefix and no large semantic tail change.
   - First action: verify it is repeated and stable. v0.0.90 showed broad wait escalation can be a TTFT regression.
   - Proven fix direction: narrow guard, limited to repeated stable cases, and compare against v89/v90/current.

5. Upstream/provider error or session issue
   - Symptoms: 4xx/5xx, previous_response_not_found, 413, or status errors.
   - First action: isolate error handling from cache-hit logic.
   - Proven fix direction: keep session-delta only for 413 rescue; do not restore normal main session-delta.

6. Statistics / label problem
   - Symptoms: UI says one thing but raw usage says another, or gap only moved between buckets.
   - First action: verify raw `input_tokens`, `cache_read_tokens`, `cache_avoidable_gap_tokens`, `cache_new_tail_gap_tokens`, `upstream_requests`, and `upstream_call_source`.
   - Proven fix direction: fix diagnostics only after confirming real provider usage.

Mandatory order:

- First summarize historical lessons that apply.
- Then choose the primary cause.
- Then make the smallest change that attacks that cause.
- Then verify with v0.0.89, v0.0.90, current version, plus any directly relevant historical positive/negative versions.
- Do not keep shipping micro-adjustments without a falsifiable reason and a metric that can prove the change.
