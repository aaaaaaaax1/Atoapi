# Atoapi Final Baseline

Current packaged candidate: `v0.2.14-adaptive-prefix-guard-20260717`

Release folder:

`releases/v0.2.12-cache-accounting-compaction-recovery-20260716`

Current workflow checkpoint:

`CURRENT_WORKFLOW.md`

## Accepted Working Baseline

- v0.2.14 keeps v0.2.13's forwarding and cache accounting line, and changes the Responses agent prefix guard to default 0ms with adaptive repeated-stable evidence capped at +0.5s. It is a packaged candidate pending live TTFT and cache-ratio verification.

- v0.2.13 is the current source candidate over the v0.2.12 accepted cache-hit baseline. It preserves the Responses cached-token accounting and one-inbound/one-upstream relay contract, and reapplies only verified provider-native cache controls after compatibility and rescue body rebuilds. The native Responses route test captures the actual outbound `prompt_cache_options`; unverified capabilities remain stripped.
- v0.2.13 correct-upstream Luna observe evidence passed in an isolated packaged run: `30` inbound = `30` attempts = `30` upstream, `807,603` input tokens, no failures, baseline `65.37%` and candidate shadow `65.74%`; candidate remains shadow-only until the normal applied canary gate is independently satisfied.
- 2026-07-16 isolated sheapi evidence covered `gpt-5.6-sol`, `gpt-5.6-terra`, and `gpt-5.5`. Sol baseline reached `98.93%`, Terra `99.20%`, and the powered gpt-5.5 baseline reached `99.75%`; no tested baseline exposed a material Atoapi-addressable gap.
- The applied gpt-5.5 cohort-key canary reached `99.82%` versus a contemporaneous `99.70%` baseline. This is a positive result under the corrected promotion rule: candidate cache hit must be at least `99.5%` and strictly higher than its contemporaneous baseline; no fixed percentage-point gain is required. The current `9` paired observations are still below the `18`-observation safety gate, so candidate promotion remains pending rather than rejected.
- A fresh isolated forced canary then completed `18` applied candidate observations against `18` contemporaneous baseline observations: candidate `99.72%` versus baseline `99.61%`, with `100%` success and one inbound = one attempt = one upstream. It remains a positive hit-rate result, but promotion is correctly blocked because candidate TTFT p95 was `5367ms` versus baseline `4206ms`; the cache gain does not override a material tail-latency regression.
- A repeat forced canary reproduced the tail issue after only `3` applied observations: candidate hit `99.71%` versus baseline `99.53%`, but candidate TTFT p95 `3583ms` versus `2038ms`, so the canary entered rollback-required state. Treat the hit gain as unpromotable until the p95 regression is removed or disproven with a different candidate design.
- Correct-upstream Luna verification used the Codex-bound `agent-codex-apiaiaiiaiia` route at `api.aiaiai001.com`; a smoke request returned HTTP 200. The 4-shard candidate then failed its real canary at `3` applied observations: candidate hit `99.56%` versus baseline `99.69%`, TTFT p95 `4263ms` versus `2513ms`, and rollback was required. The shard variant is not a promotion candidate for this upstream/model.
- The same correct-upstream Luna 4-shard candidate in the `compacted_anchor` lane completed `20` applied observations: overall candidate hit `75.66%` versus baseline `75.54%`, while stable post-compaction follow-ups were `99.11%` versus `98.93%`. The small positive delta stayed far below the `99.5%` target because the first post-compaction read is a real cold summary prefix; this is not promotable evidence.
- First post-compaction cold read is accepted only when diagnostics show the summary replaced the historical input prefix. Stable later follow-ups must recover without cache-key, instructions, tools, or pre-input-wire drift.
- sheapi Sol currently rejects semantic continuation after a successful seed (`200 -> 400`) and does not expose Responses WebSocket (`404`). These mechanisms are unsupported for this exact provider/model scope and must not be enabled from generic assumptions.
- The older v0.1.x and v0.0.x entries below remain historical comparison evidence, not the current package pointer.

- Historical v0.1.35 two-line package: compression kept v0.1.34 Chat stream aggregation for large old conversations, added backend-only `upstream_headers_ms`, `upstream_first_chunk_ms`, `aggregate_done_ms`, and `sse_chunks` diagnostics on compact/non-stream aggregation paths, and used a conservative Chat non-stream fast path only when the transformed Chat body was small enough and tail/tool-output risk was low. Its hit-rate line isolated upstream/SSE error bodies from prefix learning and local cache writes. It remained one upstream request without active prewarm, companion sync calls, or normal main session-delta.
- v0.1.13 is now the primary accepted comparison baseline because the user's live cumulative hit-rate observation was better than v0.1.12. Future cache work must compare against v0.1.13 first, then v0.1.0 for forwarding/TTFT feel, and v0.0.89/v0.0.90 as historical high-hit references.
- Historical cumulative provider token hit rate is the primary hit-rate number and cold starts must be included. Recent 5 minute ratio is useful for live trend only; it must not hide repeated cold starts caused by multi-chat or routing state drift.
- v0.1.34 is the current corrective package for old-chat ZCode compression failures after v0.1.33. Live v0.1.33 already entered the Chat compatibility path and reduced the old Responses compact body from about 1.23MB to about 172KB, but the upstream still returned 524 after about 126s because the Chat request was non-streaming and the gateway waited for the whole completion. v0.1.34 keeps the same single upstream request but sends the Chat compatibility request as `stream=true`, aggregates Chat SSE locally, and returns non-stream Responses JSON to ZCode. It also applies the same old-chat compatibility to compact endpoint fallback, strips tool fields on this compact compatibility path, detects SSE errors/incomplete streams, and avoids writing `chatcmpl_*` ids into Responses session state.
- Response cache/TTFT/tail optimization baseline is now separate from the compression compatibility line. For future Response hit-rate work, compare against v0.1.27, v0.1.28, and v0.1.29 first. v0.1.27 is the same-prefix cold-read isolation baseline, v0.1.28 is the bounded stale large-tool-output catch-up baseline, and v0.1.29 is the early-anchor/small-context large-tool-output catch-up baseline. Compression fixes must not be used as evidence that Response cache logic improved or regressed unless the live logs show normal Response traffic changed.
- v0.1.33 is the previous corrective package for old-chat ZCode compression failures. Live v0.1.32 showed new chats compress normally, but an old Responses conversation with 823 input items, 237k tool_call chars, and 666k tool_output chars still received upstream service-validation 400 even after gzip. v0.1.33 routes only those 900KB+ mixed/large-tool-output Responses non-stream compact requests through Chat upstream once, then converts the result back to Responses JSON for ZCode. It did not affect new chats or normal streams and did not add extra requests, but live old-chat compression still hit 524 due to non-streaming Chat wait.
- v0.1.32 is the current corrective package for ZCode/Responses compact non-stream failures. Live v0.1.31 logs showed normal stream requests succeeding with gzip while the following `responses-sync-main` compact/non-stream request sent a 1.23MB body without gzip and received upstream service-validation 400 or 502. v0.1.32 allows sync-main to send large bodies with gzip in its single allowed attempt, and records prefix cooldown for sync-main 400/429/5xx so one failed compact request does not repeatedly hit the upstream. It still does not add fallback calls, active prewarm, companion sync requests, or normal main session-delta.
- v0.1.31 is the current corrective package after v0.1.30 live failure. v0.1.30 showed exact warm prefixes shrinking from about 251k to 218k with `cache_read=0` or very low hit; the old calibration treated that as ordinary new_tail and risked learning a broken low waterline. v0.1.31 isolates exact prefix break after warm state, preserves the old warm waterline, and prevents 20w+ broken prefix reads from polluting ordinary new_tail. This does not alter request content, does not add sync calls, does not restore active prewarm, and does not restore normal main session-delta.
- v0.1.30 is now marked as a negative live candidate, not a baseline. It reduced avoidable accounting to zero but pushed the problem into huge new_tail, weak/zero cache_read, and very high TTFT on the user's live log.
- v0.1.30 was the previous packaged candidate and is superseded by v0.1.31. It fixed alias/fingerprint warm-state cold dynamic tail isolation, but live v0.1.30 logs exposed a separate exact-prefix break path that still produced huge new_tail and weak cache_read.
- v0.1.29 is the previous packaged candidate: it keeps v0.1.28's mature large-tool-output new-tail catch-up and adds early-anchor/small-context protection for Responses. Live v0.1.28 logs showed a new session anchor around 7k-13k input tokens with 18k-44k tool output still producing 5k-12k new tails; v0.1.29 allows those early large tool tails a bounded 5s catch-up, without extra requests, active prewarm, or normal main session-delta.
- v0.1.28 is the previous packaged candidate: it keeps v0.1.27 same-prefix cold-read isolation and adds a narrow Responses large-tool-output new-tail catch-up guard. Live v0.1.27 logs showed avoidable gaps at 0 but 12k-17k real new tails after 47k-64k character tool outputs; v0.1.28 gives only those large current tool-output tails a bounded 5s catch-up wait after the normal settle window, without extra requests, active prewarm, or normal main session-delta.
- v0.1.27 is the previous packaged candidate: it keeps v0.1.26 large tool-tail isolation and fixes same-prefix warm-then-cold-read pollution. Live v0.1.26 logs showed two 110k-token `cache_read=0` cold reads on the same session anchor being counted as huge avoidable gaps. v0.1.27 keeps those events as cold starts/instability evidence without overwriting the existing warm waterline or writing avoidable/new-tail gap tokens.
- v0.1.26 is the previous packaged candidate: it keeps v0.1.25 streaming total-time fast path and isolates large mixed/tool-output tails from avoidable-gap learning. Live v0.1.25 logs showed huge dynamic tool outputs being counted as avoidable gaps and polluting waterline decisions; v0.1.26 treats those tails as unreliable for avoidable accounting while preserving full content, no active prewarm, no extra sync request, and no normal main session-delta.
- v0.1.25 is the previous packaged candidate: it keeps v0.1.24 gzip fallback cooldown and adds a streaming total-time fast path. SSE usage / response id are extracted incrementally during forwarding, so long streams no longer require a full SSE text scan at the end. This preserves complete output, local cache body collection, and the zero-extra-request rule.
- v0.1.24 is the previous packaged candidate: it keeps v0.1.23 cache logic and adds request-body gzip fallback cooldown. If an enabled upstream rejects gzip and the request falls back to plain JSON, Atoapi records a short in-memory cooldown per upstream URL/channel so later large requests do not repeatedly spend an extra failed gzip attempt. It does not enable gzip by default and does not restore normal main `previous_response_id + delta`.
- v0.1.23 is the previous packaged candidate: it keeps v0.1.22's 5s max local guard and zero-extra-request line, but fixes small-tail waterline learning. Stable `512/1024/1536/2048` tail granularity no longer advances the full sent bucket or reappears as a repeated avoidable gap, while v0.1.13's high-hit `3072/4096` medium-tail learning stays active. Validate live logs before promoting it as a 99.5% baseline.
- v0.1.22 is the previous packaged candidate: it keeps the v0.1.13 / v0.1.15 / v0.1.21 comparison line, changes Responses local prefix guard to a unified maximum of 5 seconds, strengthens cold_unstable recent-warm handling, preserves previous warm waterline on tool_tail_burst cold reads, isolates upstream 429/502/503 errors from normal prefix cooldown, and adds provider opt-in request-body gzip diagnostics. It still does not enable gzip by default and must be validated on live traffic before being promoted as a 99.5% baseline.
- v0.1.21 is the previous packaged candidate: it keeps v0.1.20 TTFT phase diagnostics, adds backend-only `upstream_header_wait_class`, global/provider `request_body_buckets`, prefix state diagnostics for cold_unstable exact prefixes, and a narrow 6/8/10s Responses recent-warm guard for high-context prefixes that were warm but became unstable with <=2048 shortfall. It does not enable request-body compression by default; compression must be provider opt-in with fallback because third-party gateways may reject `Content-Encoding: gzip`.
- v0.1.20 is the previous packaged candidate: it keeps the v0.1.13 TTFT/new-tail stability line, keeps v0.1.15 early session-anchor isolation, withdraws v0.1.16 tail-lag-as-waterline behavior, keeps the zero-extra-request cost gate, keeps v0.1.18 cleanup, keeps v0.1.19 backend-only session anchor diagnostics/read-only session-sibling wait evidence/tool-tail-burst isolation, and adds backend-only TTFT phase diagnostics.
- v0.1.16 is a failed live experiment for cache-hit behavior. It must not become a baseline: raw cumulative hit fell to about 90.163%, and its tail-lag accounting made diagnostics look better without improving real provider cache behavior.
- v0.1.15 remains a historical high-hit candidate that keeps v0.1.13's zero-extra-request medium-tail waterline learning and changes only the local Responses prefix-state fingerprint to include a stable session anchor. Upstream `prompt_cache_key` stays globally stable, so provider prefix caching is not intentionally split.
- v0.1.14 remains a candidate experiment and is not the primary baseline unless live cumulative metrics prove it beats v0.1.13 without increasing upstream requests or TTFT.
- v0.1.14 keeps v0.1.13's medium-tail waterline learning and adds a zero-extra-request Responses stale small-avoidable risk guard: when a high-context prefix still has a 512/1024 avoidable gap after idle, instability, or a light current tail change, it uses a 6-8s short guard within the existing 10s cap to reduce sudden 3k-6k avoidable drops.
- v0.1.13 adds zero-extra-request learning for high-hit `3072/4096` 512-aligned Responses new tails and fixes cross-provider alias pollution in gap diagnostics.
- Live v0.1.12 snapshot on 2026-06-25: recent 5 minute provider token ratio was about 99.26%, errors 0, retries 0, TTFT p95 about 14.2s. Total cumulative ratio was lower because one initial 231k-token cold start was included.
- UI stays in the current Agent-injection layout.
- Product/package/backend identity is `Atoapi`.
- Config/cache directory is `%APPDATA%/Atoapi`.
- Default local proxy port is `18883`.
- Each packaged version must use its own release folder and not overwrite older folders.
- v0.0.52 cost-first remains the operating baseline.
- v0.0.58 no-active-prewarm guard remains active.
- v0.0.64 disables normal main-request Responses session-delta and keeps delta only for 413 rescue.
- v0.0.69 keeps the no-extra-request Responses wait/cap line.
- v0.0.76 keeps model/channel/fingerprint prefix-state aliasing.
- v0.0.77 keeps cold-read avoidable-gap guarding.
- v0.0.78 prompt_cache_key family waterline is marked as negative for current real tool-output traffic and must not be restored by default.
- v0.0.79 reverts family waterline participation while keeping backend `prefix_guard_wait_source` diagnostics.
- v0.0.80 strengthens exact same-prefix avoidable-gap waiting for cold-read large gaps and 1024/1536 bucket gaps.
- v0.0.81 generalizes Responses avoidable-gap waiting to every size and adds 512-aligned new-tail/current-tail guard without adding upstream requests.
- v0.0.82 is marked as a negative optimization for current real Responses traffic because lowered small-tail waits allowed large avoidable gaps.
- v0.0.83 restores stronger v0.0.81-style Responses guards.
- v0.0.84 is marked as a negative optimization because missing-state related-prefix long waits caused 75s waits with zero cache read and worse real token hit rate.
- v0.0.85 removes the v0.0.84 missing-state long wait and caps large current tool-output new-tail waits while preserving exact-prefix avoidable-gap protection.
- v0.0.86 prevents false avoidable gaps before waiting: current `1024+` tool-output tails are treated as dynamic new-tail evidence, not as proof that exact-prefix long waiting will recover.
- v0.0.87 narrows v0.0.86's filter: medium single tool-output tails no longer disable exact small-gap protection, while clearly large/dynamic tool-output tails still avoid long waits.
- v0.0.88 supersedes the v0.0.86/v0.0.87 classification drift: exact avoidable gap accounting is preserved, large current tool-output tails only cap waiting cost, and v0.67/v0.75 positive optimizations stay in place.
- v0.0.89 keeps the v0.0.88 recovery line but raises exact avoidable evidence above current-tool-tail caps. If exact/fingerprint prefix state proves an avoidable gap, protect it first; tool-tail caps apply only when avoidable is zero.
- v0.0.90 is marked as a negative/neutral experiment: repeated 512-aligned small new-tail wait escalation improved the recent ratio locally but did not beat v0.0.89 overall and pushed TTFT p95 much higher.
- v0.0.91 reverts the v0.0.90 repeated small-tail 60s/75s wait escalation and returns to v0.0.89 wait strength while keeping v0.0.89 avoidable-first behavior.
- v0.0.92 is marked as a negative/ineffective experiment for current live traffic: its compact-tool-tail recovery guard did not trigger in the sampled run, while the observed ratio and TTFT were worse than the v0.0.89/v0.0.90 comparison line.
- v0.0.93 reverts the v0.0.92 compact-tool-tail recovery guard and returns the cache core to the v0.0.89/v0.0.91 line while keeping v0.0.75 compact compatibility and all zero-extra-request constraints.
- v0.0.94 adds backend-only prefix guard skip diagnostics and a large avoidable cold-read regression test. It does not add prewarm, sync calls, or broader wait escalation.
- v0.0.95 caps wait cost for unstable dynamic tool-output false-avoidable patterns exposed by v0.0.94. Truly avoidable gaps of any size must still be protected. It preserves avoidable accounting and v0.0.89 avoidable-first behavior for stable cases, and only caps when `cache_instability_score >= 2` with current `4k+` tool output.
- v0.0.96 is marked as a negative optimization for live traffic: adding dynamic tail evidence into the provider prefix fingerprint split every request into a new waterline and produced repeated `no_prefix_state`.
- v0.0.97 recovers the v0.0.89/v0.0.90-style stable provider prefix waterline key by excluding dynamic Responses/Chat tails from `provider_prefix_fingerprint`. Future hit-rate work should start from the v89/v90 excellent baseline and diagnose remaining gaps there.
- v0.0.98 keeps the v0.0.97 stable waterline key and adds a narrow zero-extra-request fix for repeated small new tails: when a full main request has a 512-2048 token provider bucket gap, the sent bucket is learned as the next avoidable guard waterline. This targets repeated 512/1024/1536/2048 tails without restoring broad v0.0.90 wait escalation.
- v0.0.98 also adds backend-only `prefix_lag_*` diagnostics to classify whether a request is full, avoidable, a small/new tail, or provider tail-lag. These fields must stay diagnostic only and must not be shown as UI clutter by default.
- v0.0.99 restores normal warm TTFT behavior after v0.0.98 exposed 176-178s local prefix-guard waits. Responses waits with no exact avoidable evidence are capped to 10-30s by context size. Proven avoidable gaps keep v0.0.89-style stronger protection.

## Verification For This Package

- `C:\Users\MSJ\.cargo\bin\cargo.exe fmt --manifest-path G:\Flutter\ccs++\src-tauri\Cargo.toml`: passed.
- `C:\Users\MSJ\.cargo\bin\cargo.exe test --manifest-path G:\Flutter\ccs++\src-tauri\Cargo.toml`: 217 passed, 0 failed.
- `npm.cmd run build`: passed.
- `npm.cmd run tauri:build`: passed.

Release artifacts:

- `G:\Flutter\ccs++\releases\v0.1.35-atoapi-compact-diagnostics-cache-error-isolation-20260628\atoapi.exe`
- `G:\Flutter\ccs++\releases\v0.1.35-atoapi-compact-diagnostics-cache-error-isolation-20260628\Atoapi_0.1.35_x64-setup.exe`

## Current Cache Rules

- Normal continuous traffic must not create companion non-streaming prewarm.
- Active foreground/background/bucket prewarm remains disabled.
- Any cache-hit optimization must pass the hard cost gate first: zero extra upstream requests and no active prewarm.
- Normal Responses main requests no longer use `previous_response_id + delta input`.
- Responses session-delta is retained only as a 413 Payload Too Large rescue path.
- Provider prefix optimization still uses stable full outbound body, prompt cache key stabilization, and exact/fingerprint prefix settle waiting.
- Prefix warm state is written to exact scoped keys and model/channel/fingerprint aliases.
- Provider prefix fingerprint is a waterline/control key, not a dynamic-tail identity key. It must ignore dynamic conversation tails so v89/v90-style prefix state can be reused across turns.
- Do not use prompt_cache_key family waterline for waiting, gap accounting, or state learning unless future live logs prove it is safe under dynamic tool-output traffic.
- Do not use related-prefix state as evidence for missing exact-prefix waiting.
- `prefix_guard_wait_source` is backend-only `/admin/metrics` diagnostics.
- Responses avoidable gaps of any size receive dynamic protection only when exact prefix evidence exists.
- Responses current request tail diagnostics can influence waiting, but large current tool-output new tails are capped to short waits only when `avoidable == 0`.
- Exact avoidable evidence wins over current tool-output caps. Do not downgrade a proven avoidable gap into new-tail or cap it short just because the current request contains tool output.
- Medium and large current tool-output tails still receive exact avoidable protection when exact/fingerprint prefix state proves the gap is avoidable.
- Exception: if the prefix has already become unstable (`cache_instability_score >= 2`) and the current request contains `4k+` tool output, cap the avoidable wait cost. This preserves accounting but avoids repeated `weak_long_wait` on dynamic tool-output false-avoidable gaps. This must not be used to weaken truly avoidable gaps; the goal remains to press real avoidable gaps of every size down.
- Do not restore v0.0.90 repeated 512-aligned small new-tail 60s/75s wait escalation by default. Live data showed TTFT regression without a stable overall hit-rate win.
- Do not restore the v0.0.92 compact-tail recovery guard by default. Live data showed it did not trigger and did not improve the v0.0.89/v0.0.90 comparison line.
- Backend `prefix_guard_wait_effect` records wait/gap/ratio classification for later live-log comparison.
- Backend `prefix_guard_skip_reason` records why a prefix guard did not wait: `no_provider_prefix_key`, `no_prefix_state`, `wait_zero`, or `settle_window_elapsed`.
- Backend `prefix_lag_classification`, `prefix_lag_input_delta_tokens`, `prefix_lag_cache_delta_tokens`, and `prefix_lag_previous_gap_tokens` record whether the current gap is avoidable, small/new tail, or tail-lag. Use these fields to avoid guessing in the next comparison.
- Full main requests may learn the sent provider bucket as the next avoidable waterline only when the current 128-token shortfall is at most 2048 and the request is not Responses session-delta. This protects repeated 512/1024/1536/2048 tails while avoiding session-delta and cold-read poisoning.
- Do not broaden v0.0.98 sent-bucket learning to large 5k/6k tool-output tails without live evidence. Large tool-output tails are often real new content and must not be disguised as full buckets.
- Non-avoidable Responses waits must stay in the normal TTFT range: 30s max for 32k+ context, 20s for 16k-32k, and 12s below 16k. Do not reintroduce 90s/150s/180s waits for pure new-tail or cold-start catch-up without exact avoidable evidence.

## Mandatory Release Analysis Workflow

- Before changing cache logic, read live `/admin/metrics`.
- Before implementing any cache optimization, reject anything that adds extra upstream requests or active prewarm by default.
- Identify whether gaps are new-tail, avoidable, cold start, provider error, session/delta issue, or diagnosis-label issue.
- Before tuning wait time or thresholds, classify the root cause first: waterline/fingerprint cross-talk, true avoidable gap, real new tail, provider catch-up lag, upstream error, session issue, or statistics-label issue.
- Prefer the fastest proven fix direction over incremental guessing:
  - waterline/fingerprint issue -> fix state partitioning or sampling evidence;
  - true avoidable gap -> fix guard source, state learning, or skip reason;
  - real new tail -> keep short cost-free catch-up only when evidence proves it;
  - provider bucket lag -> narrow repeated-stable guard, not broad wait escalation;
  - provider/session error -> isolate error handling from cache optimization;
  - statistics issue -> fix diagnostics after checking raw usage fields.
- Preserve positive optimizations while recovering a baseline: v0.85-style avoidable protection, v0.67 tool-tail catch-up, and v0.75 compact compatibility must be kept unless live data proves a specific negative effect.
- In priority conflicts, exact avoidable-gap protection beats current-tool-tail wait caps; caps are only for pure new-tail or missing avoidable evidence.
- Do not optimize 512/1024 new-tail by blindly adding seconds. First distinguish real tool-output new content from provider catch-up lag and compare TTFT.
- For each cache release, always produce a three-group log comparison: two historical reference groups plus the current version. For the current line, use v0.0.89 and v0.0.90 as the two primary comparison groups because v0.0.91 is effectively the v0.0.89 recovery line.
- Also keep the broader historical context in mind: v0.0.52 cost-first, v0.0.81/v0.0.83 exact-prefix protection, v0.0.84 negative result, the latest packaged version, and any relevant historical positive versions.
- Preserve proven zero-extra-request optimizations and exclude known negative experiments.
- Do not claim hit-rate improvement unless live metrics show provider token ratio, avoidable gap total, cold starts/errors, upstream request count, real token cost, or TTFT improved.
- Do not ship a cache micro-adjustment without a falsifiable hypothesis, the historical lesson it follows, and the metric that will prove it.
- Current target: use v0.1.13 as the primary accepted baseline, while keeping v0.0.89/v0.0.90 as historical high-hit references. Do not pursue a new branch that lowers v0.1.13 cumulative-hit behavior unless live data proves a clear win.
