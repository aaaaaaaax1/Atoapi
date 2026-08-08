# Atoapi Current Workflow Checkpoint

## AUTHORITATIVE ACTIVE STATE — 2026-08-08 (supersedes the 2026-07-30 release block below)

### Objective, accepted base, and promotion rule

- The accepted frozen base is **v1.4.33**, packaged at
  `G:\Atoapi\releases\v1.4.33-exact-sent-waterline-maturity-20260807\Atoapi.exe`.
  The source commit is `85b56a6` (`feat: 发布 v1.4.33 命中成熟、测活与 Agent 隔离修复`).
- Current source deliberately **continues from v1.4.33**. Do not revert to an
  earlier cache line, reintroduce a rejected historical candidate, or mix
  unrelated provider/key/model/request-family cohorts.
- A change becomes the next accepted base only when a same Provider, selected
  Key realm, model, channel/request family, and cold-start/compaction filter
  comparison strictly exceeds that scope's verified historical champion in
  raw provider cached-token ratio, while preserving one inbound = one upstream
  POST, no error regression, and no total-TTFT regression. A mixed global
  metric, a fixture pass, a diagnostic reclassification, or a provider-only
  recovery is never a promotion by itself.

### Uncommitted v1.4.33-following candidate (under verification, not promoted)

- Working-tree files: `src-tauri/src/proxy/mod.rs`,
  `src-tauri/src/proxy/prefix_control.rs`, and
  `src-tauri/src/proxy/warm_pending.rs`.
- The candidate expands one **exact, low-noise, already-sent** maturity window
  from `4,096` to `16,384` avoidable tokens, still capped at 500ms.
- An exact material-tool parent may give exactly one direct child containing up
  to `49,152` tool-output characters a bounded maturity window. Larger/noisy
  children remain immediate. This is not a retry, prewarm, route switch, key
  switch, or second upstream request.
- This candidate has no same-scope live positive result yet. Keep it as the
  current test line only; do not call it a cache champion or package it as one.

### Verification completed on 2026-08-08

- Rustup was restored from the official winget source; active toolchain is
  `stable-x86_64-pc-windows-msvc`, `cargo 1.97.1`.
- `cargo fmt --manifest-path src-tauri/Cargo.toml -- --check` passed.
- Focused candidate regressions passed: three direct tool-tail child cases,
  two exact material-tail cases, the 12,288-token exact evidence case, and two
  final-scope sent-waterline cases.
- Full Rust suite passed: `995 passed, 0 failed, 12 ignored`.
- `cargo build --manifest-path src-tauri/Cargo.toml --release --bin atoapi`
  passed, producing a fresh `src-tauri/target/release/atoapi.exe` without
  replacing the live runtime.
- `scripts/verify-missing-response-id-control-prefix.mjs` passed with two
  upstream POSTs and preserved exact control-prefix placement.
- `scripts/verify-giant-cold-warm-pending.mjs` passed with five inbound
  requests = five generation attempts = five upstream POSTs; all foreground
  waits were at or below 500ms.
- First live isolated pilot (2026-08-08) used the packaged v1.4.33 EXE as
  control and the fresh candidate EXE for
  `agent-codex-provider-4 / gpt-5.6-terra / realm 4611…9673`,
  `tool-tail-maturity`, one pair and three turns. Both arms failed at the
  seed with the same `HTTP 500 / atoapi_error`, before any isolated inbound,
  attempt, upstream, usage, or cache metric was recorded. The artifact is
  `output/candidate-v1433-v1433-tool-tail-maturity-pilot-20260808.json`.
- A second minimal `full-replay` seed pilot with only 4,096 stable instruction
  characters and four output tokens failed identically for both arms. Its
  artifact is `output/candidate-v1433-v1433-full-replay-seed-pilot-20260808.json`.
  Therefore the blocker is the shared isolated
  `agent-codex-provider-4` configuration/upstream path, not tool-tail size,
  the v1.4.33-following maturity logic, or the A/B arm selection.
- A single bounded read/write health diagnostic against the currently running
  `18883` Codex route returned HTTP 502 without `response.completed` for the
  same `gpt-5.6-terra` scope. The metrics window advanced from 1,521 to 1,524
  inbounds because other live traffic was concurrent, so no one-request delta
  is attributed to that probe. This confirms the selected upstream path was
  unavailable at the time of comparison; no further real requests were sent.
- A later minimal request using the production Codex wire contract, including
  `store=false`, returned HTTP 200 with `response.completed` and one inbound /
  one attempt / one upstream increment. The running process is therefore
  currently able to reach the configured relay; this does not prove a fresh
  process can decrypt the on-disk Provider credential.
- `scripts/verify-release-champion.mjs` now preserves `store=false` in every
  live comparison request and reports only safe error categories for generic
  `atoapi_error` responses. Its self-test passes. The isolated comparison
  still fails before metrics with HTTP 500; source inspection maps this route
  to the `failed to select provider key` branch, so it remains an invalid
  comparison rather than cache evidence.
- Re-saving the `运河 / agent-codex-provider-4` record updated its on-disk
  timestamp to `2026-08-08T08:50:13.627917700Z`. A credential-safe DPAPI probe
  then established the real boundary: the live `18883` v1.4.33 process can use
  the record, while the Codex verification worker (`desktop-jrsahpg\\codexsandboxoffline`)
  cannot decrypt the same on-disk `dpapi:` Provider credential. Both processes
  are in Windows session 1, but they do not share a DPAPI principal. Copying
  `config.toml` or pointing a child at the live config therefore cannot make a
  worker-launched isolated EXE select that credential. `cache-key.dpapi` is not
  a Provider-key fallback; it protects Atoapi's encrypted response cache.
  This is an invalid fixture/upstream-path result, not a candidate regression
  or a positive optimization. Do not increase its sample size until the two
  isolated arms are launched under the same interactive Windows principal as
  the working v1.4.33 process.
- The verifier was then run under the desktop `msj` principal. It selected the
  same Provider / Key realm in fresh isolated processes, proving that the
  saved configuration is reusable when DPAPI is evaluated in its owning user
  context. The `tool-tail-maturity` seed still had no usable cache evidence:
  v1.4.33 ended after 30.020s with `502 / upstream_transport`, while the
  candidate ended after 64.027s with `504 / system-proxy:header_wait_slow`.
  Both preserved exactly one inbound = one attempt = one upstream POST.
- A second desktop-principal `full-replay` seed reduced the request body to
  4,496 bytes. The v1.4.33 control again ended with `502 / upstream_transport`
  after 30.020s. The candidate's seed completed once (`743` input tokens,
  zero cached tokens), but its first follow-up ended with
  `upstream_sse_error`. Static wire continuity and one-attempt/one-POST held,
  but neither arm completed a comparable cacheable sequence. These artifacts
  are invalid for promotion and indicate fresh isolated upstream/system-proxy
  instability, not a DPAPI/configuration loss or an accepted cache result.
  They are retained at
  `output/candidate-v1433-v1433-tool-tail-maturity-interactive-20260808-171100.json`
  and `output/candidate-v1433-v1433-full-replay-interactive-20260808-171300.json`.
- Same-binary v1.4.33 order controls proved the failure is not candidate code:
  with identical 1,024-character full-replay wires, one fresh arm completed
  `3/3` Responses requests while another stopped at its first follow-up.
  A diagnostic direct-path override on temporary config copies produced the
  same split result, so Windows system-proxy selection is not the root cause.
  The relay's safe `upstream_sse_error:payload_limit` label is not treated as
  a deterministic input-size finding because byte-identical requests both
  completed and failed on different fresh arms. No cache comparison may be
  promoted from this non-deterministic upstream/relay behavior.
- The verifier now supports up to 2,500,000 seed-context characters and an
  explicit `--minimum-seed-input-tokens` acceptance floor. This enables a
  500k-token-class dynamic-tail run only when upstream usage actually proves
  the requested scale; it does not relabel characters as tokens.
- A desktop-principal 1,000,000-character `full-replay` ladder seed then
  completed under the current, explicitly pinned
  `agent-codex-bizd / gpt-5.6-terra / Key 2 / realm 7bf7…9cab7` scope. Both
  v1.4.33 and the candidate completed all `3/3` requests with one inbound =
  one attempt = one upstream POST; the observed realm matched the pinned
  cohort and each seed reported `266,740` or more actual input tokens. This
  is valid scale evidence but not a promotion: candidate raw hit was higher by
  only `0.00139` percentage points, cache-128 was lower by `0.00850` points,
  and its follow-up contained `266,752` provider-instability tokens. Artifact:
  `output/control-v1433-v1433-context-stair-1m-20260808-key2.json`.
- The same 1M ladder was immediately repeated candidate-first to test order
  bias. The candidate completed its three requests, but v1.4.33's seed ended
  as `upstream_sse_error:capacity` before usage/realm evidence. The comparison
  fails closed and is not cache evidence. Together, the two runs show that
  fresh isolated upstream capacity/cache behavior is still not reproducible;
  do not escalate to 1.5M, 2M, 2.35M, or `dynamic-tail-mix`, and do not retry
  in a loop. Artifact:
  `output/control-v1433-v1433-context-stair-1m-candidate-first-20260808-key2.json`.
- During the next return, Codex injection changed the live scope to
   `agent-codex-provider-2` (`https://quiteai.autos/v1`), with no enabled Key
   pool. A same-binary 1M self-control on that new scope completed all requests
   but measured only `183,965` seed input tokens, below the 200k gate. A 1.5M
   same-binary self-control then completed both arms with `274,545` seed input
   tokens and matched realm `7bcb…4ab6`; however one arm had `274,432`
   provider-instability tokens and the aggregate comparison therefore failed
   closed. The apparent cache delta is same-binary arm-order/cache placement,
   not candidate evidence. Artifact:
   `output/control-v1433-v1433-self-1m5-20260808-provider2.json`.
- A read-only metrics snapshot for the current `agent-codex-provider-2` scope
  shows the latest populated buckets all at
  `cache_avoidable_gap_tokens = 0`; observed shortfall is attributed to new
  tails and provider instability. There is no falsifiable physical cache gap
  for the v1.4.33-following wait expansion to recover in this scope.
 - The verifier now records per-arm `seed_cache_read_tokens` and requires exact
  seed-cache symmetry before any strict cache gain can become positive evidence.
  This closes the order-prewarm false-positive seen in the isolated-cache
  artifact; legacy artifacts without the new field are reconstructed from
  their seed request during offline replay. The self-test passes, and the old
  isolated-cache artifact now correctly reports
  `seed_cache_read_symmetry = false` and `positive_cache_evidence = false`.
 - The verifier entrypoint now also writes fail-closed reports to the requested
  `--output` path from its top-level error handler. A local missing-parameter
  probe produced a complete artifact with exit code `2`, and the self-test
  passed; future child/cleanup failures will no longer disappear as
  artifact-less runs.
- A local scan of every retained `candidate-v1433-v1433*.json` artifact found
  no `pass = true` or `positive_cache_evidence = true` result. There is no
  overlooked v1.4.33-following candidate to package or promote.
- One bounded candidate-first repeat at 1.5M confirmed the same-binary order
  effect: both arms reached `274,539` seed tokens and completed `3/3`, but the
  first candidate arm read `0` cached tokens while the later champion arm read
  `274,176`. Provider-instability was `548,864` tokens on the candidate arm
  and `274,432` on the champion arm. The comparison failed closed; this is
  definitive order/cache-placement evidence, not a candidate regression or a
  positive optimization. Artifact:
  `output/control-v1433-v1433-self-1m5-candidate-first-20260808-provider2.json`.
- The follow-up 1.5M candidate comparison with `--isolate-upstream-cache`
  was rejected before launch by the managed approval service because its
  servers were overloaded. No upstream request or artifact was produced; the
  next safe continuation is to rerun that exact isolated-cache scope only
  after the approval service is available.
- Once approval was available, the isolated-cache champion-first comparison
  completed both arms and met the token/one-POST gates, but the candidate seed
  already read `274,176` cached tokens while the champion seed was cold; the
  result also contained `256` provider-instability tokens, so its apparent
  `99.89%` candidate cache-128 rate is not promotable evidence. The matching
  candidate-first same-binary control then failed both arms at the first
  `upstream_sse_error` with zero usage, confirming that the isolated upstream
  path is still non-reproducible. Artifacts:
  `output/candidate-v1433-v1433-isolated-cache-1m5-20260808-provider2.json` and
  `output/control-v1433-v1433-isolated-cache-1m5-candidate-first-20260808-provider2.json`.
 - `npm.cmd run build`, the selected frontend/diagnostic regressions,
  release-champion self-test, acceptance self-test, and `git diff --check`
  have passed in this checkpoint. Existing compiler warnings are dead-code
  warnings only; no test failed.
- A separate Codex injection-stability repair is now present in the uncommitted
  source line (`src-tauri/src/agent_injection.rs`, `src-tauri/src/state.rs`,
  and `src-tauri/src/admin/mod.rs`). The prior writer minted a new random
  `atoapi-model-catalog-<uuid>.json` and rewrote `C:\Users\MSJ\.codex\config.toml`
  on every enabled-injection refresh/startup, even when the effective route,
  model catalog, local key, and endpoint were unchanged. This is a concrete
  configuration-churn mechanism that can make Codex hot-reload while a session
  is active. Catalog artifacts are now content-addressed and reused; an
  unchanged reapply leaves the Codex config untouched, and an unsuccessful
  config replacement cannot delete an already referenced catalog. Startup also
  avoids persisting a no-op injection refresh. The targeted injection suite,
  startup regression, full Rust suite (`995 passed, 0 failed, 12 ignored`),
  release build, formatting, and diff checks pass. The live 18883 v1.4.33
  runtime was not replaced or restarted, so this repair is not yet live.

 - A guarded 1.5M candidate run detected a live scope drift before any
   comparable sequence: the configured provider name remained
   `agent-codex-provider-2`, but both arms observed realm
   `4574f5…5b407` instead of the pinned `7bcb…4ab6`. The verifier failed
   closed; the result contains no promotable evidence. Re-snapshot the current
   realm before any further external comparison.

 - After rebinding the current provider2 Key, the actual scope
   `4574f5…5b407` completed a 1.5M candidate comparison with both arms at
   `3/3` and `274,543` seed tokens. The candidate seed read `274,176` cached
   tokens while the champion seed was cold (`0`), and the candidate also had
   `256` provider-instability tokens. The new seed-symmetry gate rejected the
   apparent `99.86%` candidate hit rate; this is still order/prewarm evidence,
   not a promotion. Artifact:
   `output/candidate-v1433-v1433-isolated-cache-1m5-20260808-provider2-realm4574.json`.
 - A test-only distinct-User-Agent isolation attempt separated the two upstream
  placement lanes, but the champion arm failed its first seed with
  `upstream_sse_error` while the candidate completed. Both arms therefore do
  not form a comparable cohort; no promotion evidence was produced. Artifact:
   `output/candidate-v1433-v1433-isolated-ua-1m5-20260808-provider2-realm4574.json`.
 - A smaller dual-lane stability gate on the same `4574f5…5b407` realm completed
  both arms, but the candidate lane showed `3,584` provider-instability tokens
  and read `2,816` seed-cache tokens while the champion seed was cold. The
  seed-symmetry and provider-stability gates both rejected promotion, so no
  1.5M rerun is justified yet. Artifact:
   `output/control-v1433-v1433-stability-ua-small-20260808-realm4574.json`.
- A 1.5M diagnostic with distinct per-arm prompt-cache keys made seed cache
  symmetry explicit, but the candidate arm still hit provider instability
  (`274,432` tokens) and ended below the champion (`33.29%` vs `66.59%`
  cache-128). It is not promotion evidence; the remaining blocker is upstream
  waterline/capacity behavior, not cache-key contamination. Artifact:
  `output/candidate-v1433-v1433-isolated-prompt-key-1m5-20260808-realm4574.json`.

 - A later keyed small stability gate did not publish an artifact after its
   temporary child exited; its verifier cleanup was allowed to finish without
   touching the live process. It produced no evidence and must not be retried
   concurrently or counted as a cache result.

 - The first valid 1.5M candidate comparison on the current `4574f5…5b407`
   scope passed cohort, seed-symmetry, one-POST, and provider-stability gates.
   Candidate cache-128 was `0.665941` vs champion `0.665114`, but strict
   shortfall/full-bucket improvement was absent and candidate TTFT p95 regressed
   (`34,337ms` vs `13,218ms`). The release verdict remains `pass=false` and
   `positive_cache_evidence=false`; v1.4.33 stays the base. Artifact:
   `output/candidate-v1433-v1433-isolated-prompt-key-1m5-20260808-realm4574-stable.json`.
- A same-binary 1.5M latency control on the identical isolated prompt-key
  setup was itself provider-confounded: the champion arm recorded `281,600`
  provider-instability tokens and the candidate arm `563,200`. Its apparent
  TTFT/cache ordering is therefore not evidence about candidate code. Artifact:
  `output/control-v1433-v1433-latency-1m5-20260808-realm4574.json`.

### Live runtime observation (read-only; not champion evidence)

- Installation verification on 2026-08-08 completed for the separate v1.4.34
  injection-stability package. The live listener is PID `35736` running
  `G:\Atoapi\releases\v1.4.34-injection-stability-20260808\Atoapi.exe` and
  `/health` returned HTTP 200. The live executable is therefore v1.4.34, not
  the prior v1.4.33 process. After installation, the sole content-addressed
  catalog
  `C:\Users\MSJ\.codex\atoapi-model-catalog-fc19c3377eb13062c9689f277bf52f16eb4a862f3fdf7dab312f1a705b8659e8.json`
  and `C:\Users\MSJ\.codex\config.toml` stayed unchanged for a continuous
  30-second observation. This is a passed injection-stability validation, not
  cache-champion evidence; v1.4.33 remains the cache comparison base.
- Workspace cleanup completed on 2026-08-08. `cargo clean` removed the
  generated `G:\Atoapi\src-tauri\target` tree: `47,388` files / `103.7 GiB`.
  The first sandboxed attempt was denied on the Cargo artifact lock; the
  standard clean completed under the user's approved elevated project scope.
  The live v1.4.34 release process, v1.4.33 champion, v1.4.34 rollback package,
  source/configuration, and all raw cache-comparison artifacts were retained.
  Historical release packages total only `1.041 GiB`, while Atoapi runtime data
  is about `0.01 GiB`; neither is a safe replacement for the generated build
  cache and neither was deleted.
- Post-clean live comparison confirms the user's perceived v1.4.34 hit drop is
  cohort mixing, not a cache-policy regression. In the current rolling
  metrics, `agent-codex-provider-2 / realm 4574f5c2… / gpt-5.6-terra` reached
  `3,801,638` input tokens and `3,717,120` cached (`97.7768%`) across 16
  successful rows, with `cache_avoidable_gap_tokens=0`. The mixed
  `agent-codex-bizd / realm 7bf7f91b…` slice had `8,350,127` input tokens and
  `5,123,680` cached (`61.3605%`); its `1,597,548` provider-unstable and
  `1,612,468` new-tail tokens account for the loss, while avoidable remained
  zero. The rolling aggregate was therefore not a valid v1.4.33 comparison.
- Historical v1.4.33 release cohorts show the accepted positive line is
  provider/realm-dependent: the strongest retained Terra cohort was
  `agent-codex-provider-6 / realm bc8fca9d…` at `36,865,244` input and
  `36,451,968` cached (`98.8790%`, 151 requests); another large positive was
  provider-4/realm4611 at `98.3879%`. These cannot be mixed with the current
  bizd/realm7bf7 slice. Current live diagnostic counts are dominated by
  `provider-prefix-break` (84), `tool_tail_burst_real_tail` (28),
  `provider_waterline_rollback` (24), and `tail_lag_previous_not_caught` (23);
  `cache_avoidable_gap_tokens` remains zero. This falsifies another foreground
  wait expansion as a safe fix.
- The source diff from the v1.4.33 commit (`85b56a6`) contains no production
  cache-policy change; the remaining `proxy/mod.rs` delta is test coverage for
  bounded material-tool-tail maturity. The v1.4.34 production delta is the
  injection/config churn repair and verifier hardening. Do not claim v1.4.34
  is a cache champion; compare only same Provider/realm/model/request-family
  cohorts and treat current provider instability as upstream evidence.
- A read-only route audit after the v1.4.34 install confirms the enabled Codex
  injection is bound to `agent-codex-provider-2`; authenticated
  `GET /codex/v1/models` reports that same Provider. The rolling bizd rows are
  therefore not a valid proxy for the currently injected Codex route and must
  not be used to declare a v1.4.34 cache regression. The next A/B must remain
  `provider-2 / realm 4574f5c2… / gpt-5.6-terra / Responses`.
- `http://127.0.0.1:18883/health` returned HTTP 200 on 2026-08-08.
  `GET /codex/v1/models` returned HTTP 401, which confirms that the Codex
  route is mounted and protected by the local key; `GET /codex/v1` itself is
  not an endpoint and returns 404 by design.
- At the read-only snapshot, `agent_generation` reported 1,445 inbound
  requests and 1,445 generation attempts, with zero multi-attempt inbounds.
  The aggregate provider cached-token ratio was 93.6569%, but the recent
  window mixes Provider/model realms and is therefore not comparable with a
  historical champion. The active snapshot also had one request in flight.
- Recent rows showed zero `cache_avoidable_gap_tokens`; misses were attributed
  to new tails or provider instability. Do not broaden the wait domain without
  a falsifiable, nonzero physical cached-token opportunity.
- A fresh read-only snapshot at `2026-08-08T13:57:57Z` confirms the live
  process is still the packaged v1.4.33 executable (PID `43512`), with no
  restart or replacement. Within the current `agent-codex-provider-2 /
  gpt-5.6-terra / realm 4574f5c2…` slice, all 200 retained recent rows
  completed successfully; aggregate `cache_avoidable_gap_tokens` was `0`,
  while `cache_new_tail_gap_tokens` was `1,020,928` and
  `cache_provider_unstable_gap_tokens` was `7,905,792`. This is direct
  evidence against expanding the foreground maturity wait again: the present
  loss is new-tail/provider instability, not a measured avoidable prefix gap.
- The uncommitted candidate's local Rust and synthetic wire regressions still
  pass, but its widened `4,096 -> 16,384` exact-gap and material-tool-child
  waits remain unproven on real traffic. Keep v1.4.33 as the sole accepted
  base and do not package or deploy this candidate until a fresh same-scope
  run shows strict raw-token and strict-shortfall improvement with no TTFT or
  stability regression.
- A follow-up live final-scope audit of the same 200 `provider-2 / terra /
  realm4574…` rows found 159 rows with a positive sent-vs-settled residual
  (`candidate_avoidable_tokens_128`), totaling `4,436,096` tokens. However,
  `cache_avoidable_gap_tokens` stayed zero in every one of those rows; the
  residual was accompanied by `4,394,752` provider-unstable tokens overall.
  Only five rows fell inside the candidate's newly widened `4,096–16,384`
  band, and they likewise did not produce a real avoidable cache gain. This
  falsifies the current wait-expansion hypothesis on live traffic: the missing
  tokens are upstream rollback/instability, not a local maturity window that
  Atoapi can safely recover without another request.
- The uncommitted `4,096 -> 16,384` first-exact window and the widened
  material-tool-child wait were therefore removed from source on 2026-08-08.
  The remaining source cache behavior is again the v1.4.33 policy line; the
  injection-stability repair and fail-closed verifier hardening remain
  untouched. Focused rollback regressions (`17` warm-pending, `27`
  prefix-control, and the immediate small-tool child), formatting, the release
  build, release-champion self-test, and a release-EXE giant-context run all
  passed. That run preserved `5 inbound = 5 attempts = 5 upstream POSTs`, a
  stable prompt-cache key, append-only FullReplay wire, no automatic probes,
  and the 500ms foreground ceiling.
- The logged-in desktop account `desktop-jrsahpg\\msj` is active in Windows
  session 1, but this Codex worker has only the sandbox token and lacks both
  session-token and scheduled-task permissions. A bounded Shell.Application
  launch probe produced no desktop-owned child output, so no real upstream
  request was sent from the wrong DPAPI principal. The current worker cannot
  truthfully perform the same-principal 500k-token comparison by itself;
  forcing it would only recreate the already-known invalid credential result.
- A separate, non-cache-champion reliability package was produced at
  `G:\Atoapi\releases\v1.4.34-injection-stability-20260808` on 2026-08-08.
  It contains the source-built `Atoapi.exe`, an NSIS installer, SHA-256 sums,
  release notes, and the verification report. All version surfaces were
  advanced to `1.4.34`; the package contains only the verified injection
  stability repair and verifier hardening, with the v1.4.33 cache policy.
  Full release preflight passed (`993 passed`, `12 ignored`, three capacity
  baselines, frontend and diagnostic suites), as did the release-EXE giant
  context one-POST regression. NSIS packaging completed successfully. MSI was
  not emitted because WiX `light.exe` failed during bundle creation; this is
  recorded in the package report. The live v1.4.33 process remains untouched.
- A v1.4.35 candidate package was built on 2026-08-08 at
  `G:\Atoapi\releases\v1.4.35-release-champion-runner-20260808`. It contains
  the source-built EXE, NSIS installer, SHA-256 sums, and release/test notes.
  Full preflight passed (`996 passed`, `12 ignored`, three capacity baselines,
  frontend/diagnostic/acceptance checks), and the runner's debug/release unit
  tests passed `3/3`. This package adds only the fixed same-principal runner;
  it does not promote a cache change and has not been installed over live
  v1.4.34.
- Hard baseline check after packaging: live PID `35736` is still
  `v1.4.34-injection-stability-20260808\Atoapi.exe`; the retained champion is
  file-version `1.4.33`; the new candidate is file-version `1.4.35`. The
  production diff from commit `85b56a6` contains only the fixed admin runner
  and runner-support changes; no cache-policy production path was changed.
  The v1.4.33 champion executable remains the runner's fixed control path.
  Therefore baseline preservation is confirmed. A cache-hit improvement is
  **not yet claimed** until v1.4.35 runs the same-principal live A/B; unit,
  synthetic, mixed-realm, or offline evidence cannot promote it.

### Next return point

1. Keep the current 18883 runtime running and untouched. When ready for the
   same-principal comparison, run the v1.4.35 candidate from the workspace
   release layout; use the packaged v1.4.33 EXE as control and the current
   candidate EXE in an isolated, same-scope comparison.
2. Before deploying the source-built injection repair, retain the existing
   Codex configuration as the rollback point and verify one explicit
   enable/apply operation under the interactive desktop principal. The first
   post-repair apply may intentionally replace the legacy UUID catalog with a
   stable content-addressed catalog; subsequent unchanged applies must leave
   both `config.toml` and that catalog's timestamp/path unchanged. Do not
   restart or replace live 18883 merely to perform this source verification.
3. The current Codex injection scope is `agent-codex-bizd / gpt-5.6-terra`
   with explicit `Key 2` and observed realm `7bf7…9cab7`; do not reuse the
   retired `agent-codex-provider-4 / realm 4611…` scope. The verifier wrapper
   now exposes `-KeyId`, `-SeedContextChars`, and
   `-MinimumSeedInputTokens` so the 1M-to-2.35M ladder can be run without
   hand-editing the command. The DPAPI/configuration boundary is resolved, but
   upstream capacity/cache terminals remain non-deterministic. Do not mask
   this with retries, route changes, key rotation, tool-tail changes, or a
   broader sample.
   `scripts/run-release-champion-interactive.ps1` remains the same-profile
   seed entry point.
 3a. Scope override from the latest 20:03 snapshot: Codex now uses
     `agent-codex-provider-2 / gpt-5.6-terra` at
     `https://quiteai.autos/v1`, with no enabled Key pool and observed realm
     `7bcb…4ab6`. The preceding bizd/Key-2 observations remain historical
     artifacts and must not be mixed into this scope.
 4. For the requested long-context work, use `dynamic-tail-mix` with a
   500k-token acceptance floor based on actual upstream `input_tokens`, not
   characters. A full 11-turn, two-arm run is multi-million-token traffic;
   execute it only after the fresh-sequence gate is reproducible and keep
   `v1.4.33` as the comparison base.
 5. After a bounded seed succeeds, run the release-champion comparison only
   against that exact cohort; retain raw result artifacts and reject
   mixed-provider summaries.
 5a. `scripts/run-release-champion-interactive.ps1` now defaults to the
    observed current scope (`agent-codex-provider-2 / realm 4574f5c2…`),
    supports `dynamic-tail-mix`, and for that scenario defaults to the
    requested 2.35M-character / 450k-input-token seed class, 11 turns, and
    131,072-character two-tool tails. It also enables isolated upstream-cache
    lanes and a per-run prompt-cache-key prefix so a future desktop-principal
    run cannot silently fall back to the retired provider4/realm4611 fixture or
    shared placement.
6. Promote this candidate only on a strict positive result under the rule
   above. If it is neutral, unstable, or regresses cache/TTFT/errors, retain
   v1.4.33 as the base and investigate the next measured miss cause instead of
   extending waits again.
7. Each future positive result becomes the sole accepted base for the next
   iteration, with its scope, raw before/after metrics, tests, and return point
   appended here before packaging.
8. After v1.4.35 is running under the owning desktop principal, call the fixed
   local-admin runner with `POST /admin/release-champion/run`, then poll
   `GET /admin/release-champion/status`. It accepts no request body or custom
   arguments; the artifact is written under the Atoapi config
   `release/release-champion` directory. Promote only a strict positive result.

### Task-card and Super Brain status

- `docs/task-card-auto-channel-compact-20260629.md` is complete: automatic
  channel selection, compression compatibility boundaries, and Provider-level
  multi-Key management are all checked off. Its stated next task is the cache
  hit-rate comparison described above.
- Super Brain runtime v0.5.98 was verified on 2026-08-08: package entry,
  private memory root, MCP binding, functional probe, and activation receipt
  are all ready. It is configured not to store raw prompts, transcripts, or
  provider secrets.
- The task-card reporter itself is currently unavailable to this Codex worker:
  its private-state lock is denied by the sandbox principal. Treat this file
  and the verified live probes above as the current authoritative checkpoint;
  do not claim that the G1 task card was refreshed until it runs under its
  owning principal.

## AUTHORITATIVE RELEASE STATE - 2026-07-30

- **v1.4.13 is packaged** at `G:\Atoapi\releases\v1.4.13-full-replay-waf-auto-compact-cache-continuity-20260730`; it contains the independent portable EXE and NSIS installer, both carrying file/product version `1.4.13`.
- The running 18883 service remains **v1.4.12** at `G:\Atoapi\releases\v1.4.12-full-replay-waf-cache-continuity-20260729\Atoapi.exe`, PID 21720, started 2026-07-29 20:29:26. It was not stopped, restarted, replaced, or packaged over.
- v1.4.13 includes the shape-aware full-replay WAF safety repair, canonical SSE `Request blocked` failure mapping, per-Provider Codex auto-compaction threshold/restore logic, and the narrowly bounded exact/high-hit prefix commit-maturity wait. It does not reintroduce the historically rejected dual-digest `static_wire_drift` candidate described below.
- Full release verification passed: `cargo fmt --check`; focused auto-compaction, full-replay and WAF tests; the release Rust suite (`862 passed, 11 ignored`); FastRelay capacity baselines; frontend/UI/static regressions; acceptance and release-champion self-tests; and `git diff --check`.
- Return point: install/run the separate v1.4.13 package, then compare live metrics only within the same hand-selected Provider / Key realm / model / request family and the same cold-start/compaction filters. Do not declare a cache champion from mixed cohorts.

The older 2026-07-29 draft below is retained as historical evidence; this release-state block overrides its outdated “un-packaged” wording.

## AUTHORITATIVE ACTIVE LINE — 2026-07-29 (read this before older history)

### Current runtime and acceptance floor

- Running service is **v1.4.12** at `G:\Atoapi\releases\v1.4.12-full-replay-waf-cache-continuity-20260729\Atoapi.exe`, listening on `127.0.0.1:18883`.  Do **not** stop, restart, replace, or package over this live process while source work is in progress.
- The release gate is a strictly comparable cohort only: same build scope dimensions (provider, selected-Key realm, model, client/upstream channel, stream/request family) and the same cold-start/compaction filters.  No route switch, Key-order change, context trimming, extra upstream call, retry, or statistics redefinition is allowed to manufacture an improvement.
- Strict verified historical floor for `agent-codex-jucodex004 / same realm / gpt-5.6-terra / Responses stream`, excluding cold starts and compactions: **v1.4.11 = 97.018269%** (`227` requests, `48,420,904` input tokens) versus **v1.4.12 = 96.026801%** (`253` requests, `31,978,237` input tokens): **-0.991468pp**.  This is a real regression signal, but not a paired same-input proof of a single code cause.  The older 98.567% hourly snapshot lacks build/realm/model provenance, so it is reference-only, not a releasable champion.

### Rejected v1.4.13 cache candidate (not active source)

- The dual-digest `static_wire_drift` candidate was built and isolated-tested, but it did not strictly beat the comparable baseline and one complete pair had a higher total TTFT p95.  It is **rejected** under the user-confirmed release gate.
- On 2026-07-29 the candidate's runtime code and version bump were manually reverted with precise patches (no `git reset` or `checkout`); the active source/version is again v1.4.12.  The verifier-only improvement that separates local preparation, prefix wait, upstream TTFT, and total TTFT is retained because it changes no request behavior.
- Latest read-only current 18883 evidence: all normal requests are `full_replay`, have one upstream attempt, `cache_avoidable_gap_tokens = 0`, and `final_scope.continuity_reset = 0`.  Large misses are either real new tail/tool output or provider cache rollback after an exact frozen predecessor; no automatic route/key change, context rewrite, retry, or second upstream request is permitted.

### Active next action

1. Do not package or replace the live runtime from this rejected candidate.
2. Treat each hand-selected Provider/Key realm as its own championship line.  Before any new code change, require a falsifiable local cause with nonzero physical cached-token savings; `static_wire_drift`, real tail, and upstream rollback alone do not qualify.
3. A new candidate may be retained only after the same-realm real raw-token ratio **strictly exceeds** its verified historical champion, with one upstream POST per inbound and no reliability or total-TTFT regression.

### Unpackaged full-replay safety follow-up — 2026-07-30

- The full-replay WAF memory is being narrowed from a route-wide six-hour boolean to a bounded, aggregate-only blocked-payload shape. A prior 1.76MB WAF event must not locally reject the observed 976KB / 1052-item / 280KB-tool-output request merely because the route matches; an equivalent-or-larger shape remains locally protected. No payload text, tool output, key, or response id is retained.
- The provider editor now owns an optional `auto_compact_token_limit`: a positive token value is written to Codex as `model_auto_compact_token_limit` only while that provider is the active bound Codex route. Blank restores Codex's pre-Atoapi value/default. This is a Codex-side preemptive compact trigger, not an Atoapi hidden compact/retry, so one inbound still has at most one upstream request.
- Existing Codex config restore state is migrated before this new root field is managed, preventing a previously user-set token limit from being erased when the provider setting is cleared or the injection is removed.
- Current 18883 runtime remains untouched; source work is un-packaged. Verified: Rust shape-policy/WAF and auto-compaction tests, frontend static regression, TypeScript/Vite build, and formatting/diff checks. Return point: run the final combined regression set, then wait for user validation before any package/release decision.

### Continuity rule

- Every cache-related change must append here before packaging: **cause, exact code path, raw metric before/after, scope, test evidence, version status, and next return point**.  Do not reopen a completed item merely because it is mentioned in older historical sections below.

Historical checkpoint below last updated: 2026-07-17

## 2026-07-17 Codex Body Session Metadata

- Formal `18883` metrics showed `167/200` recent requests as `scope-sibling` and `first_prefix_state`, while only `27/200` had trusted identity. The current code accepted `x-codex-turn-metadata` only as an HTTP header.
- Current Codex Responses clients also flatten turn metadata into the request body as `client_metadata["x-codex-turn-metadata"]`; this was verified from the installed Codex client contract and reproduced with a failing regression test.
- The proxy now accepts the body form only for an authorized Codex route, supports the documented JSON-string form and a safe object form, preserves header precedence, enforces bounded ID limits, and still falls back safely when metadata is absent or malformed.
- Regression coverage confirms body metadata projection, unauthorized-route rejection, object-form compatibility, and different-thread isolation. Full Rust `472 passed / 3 ignored`, frontend build, metrics, request-record, and acceptance self-tests pass.
- The running packaged `18883` process was not restarted. After the next requested package/restart, verify recent logs shift from `scope-sibling/first_prefix_state` to trusted `exact`/stable session anchors before judging hit-rate improvement.

## 2026-07-17 Provider-Native Candidate Rejected

- Isolated `provider_native` canary on the current `sheapi / Codex` route with `gpt-5.5` completed `90` inbound = `90` generation attempts = `90` upstream requests, with `0` failed experiments and no hidden retry.
- The shadow arms were comparable at `99.85%` baseline and `99.80%` candidate. After forced application, the candidate fell to `11.10%` cache ratio versus `99.96%` contemporaneous baseline, with average avoidable gap `3,413` and provider-unstable ratio `22.20%`; TTFT p50 also rose from `2,082ms` to `3,335ms`. Readiness was `rollback_required:candidate_cache_regression`.
- `provider_native` is now disabled for normal automatic admission. Its isolated force path remains available only for a future explicitly scoped experiment; normal traffic stays on the stable baseline and must not remove `prompt_cache_key`.
- Next mainline step: analyze remaining baseline misses as real new-tail, upstream cold-read, or provider instability, and only propose a new candidate with a falsifiable zero-extra-request hypothesis.

## 2026-07-16 Cache Mainline V3

- Current source and packaged baseline: `v0.2.14-adaptive-prefix-guard-20260717` under `G:\Atoapi\releases`.
- v0.2.14 changes only the foreground Responses prefix guard: first avoidable evidence waits 0ms; repeated stable exact evidence adapts up to +0.5s. No prewarm, extra upstream request, or normal session-delta is enabled.
- v0.2.13 keeps verified `prompt_cache_options` consistent after Responses compatibility, session rescue, payload rescue, and previous-response fallback body rebuilds. The native Responses route regression test captures the actual outbound JSON and confirms `mode=implicit`, `ttl=30m`; unverified providers remain unchanged.
- Packaged v0.2.13 isolated correct-upstream Luna observe run passed: `30` inbound = `30` attempts = `30` upstream, `807,603` input tokens, `0` failures, baseline `65.37%` vs candidate shadow `65.74%`; TTFT p50/p95 were `2317/8144ms` baseline and `2148/2587ms` candidate. This is shadow evidence only, not a promotion decision.
- Corrected Responses cache accounting is now the measurement baseline. Compatible cached-token fields are reconciled before metrics classification; do not compare new results with older runs that lost `input_tokens_details.cached_tokens`.
- Isolated sheapi `gpt-5.6-sol` post-burst evidence: baseline `98.93%`, candidate shadow `98.44%`; both average avoidable gap and provider-unstable gap were `0`.
- Isolated sheapi `gpt-5.6-terra` evidence: baseline `99.20%`, candidate shadow `99.16%`; both addressable gaps were `0`.
- Powered sheapi `gpt-5.5` shadow A/B: `51` observations per arm and more than `6.2M` input tokens per arm. Baseline was `99.75%`; candidate shadow was `99.69%`.
- Applied `gpt-5.5` canary: contemporaneous baseline `99.70%`, candidate `99.82%`, TTFT p50 improved from `2811ms` to `2233ms`, and TTFT p95 improved from `6682ms` to `5096ms`. The corrected efficacy rule accepts any strict improvement once the candidate is at least `99.5%`; it no longer requires a fixed `0.50` percentage-point gain. The current `9` paired observations remain below the `18`-observation promotion safety gate.
- Every acceptance run preserved one inbound request = one generation attempt = one upstream request. No prewarm, companion request, hidden retry, or package replacement was used.
- After correcting the promotion rule, a fresh isolated gpt-5.5 shadow run completed `30` inbound = `30` attempts = `30` upstream calls. Baseline and candidate both measured `99.75%`, with no addressable gap; this run correctly produced no canary application and no false promotion.
- A test-only isolated force-canary path (guarded by `ATOAPI_ISOLATED_TEST_INSTANCE`, never enabled by normal production defaults) completed `18` candidate-applied and `18` contemporaneous baseline observations. Candidate hit `99.72%` exceeded baseline `99.61%` and the `99.5%` target, with `90` inbound = `90` attempts = `90` upstream and no failures; promotion stayed blocked solely by candidate TTFT p95 `5367ms` versus baseline `4206ms`.
- A repeat of the same isolated applied canary reproduced the issue at `3` applied observations: candidate hit `99.71%` exceeded baseline `99.53%`, but TTFT p95 `3583ms` exceeded baseline `2038ms`, triggering rollback-required. This is a repeatable latency safety failure, not a reason to relax the hit-rate gate.
- After the user's upstream correction, Luna was verified through the Codex-bound `agent-codex-apiaiaiiaiia` provider at `api.aiaiai001.com` (HTTP 200 smoke). The 4-shard candidate failed on that real scope at `3` applied observations: hit `99.56%` below baseline `99.69%`, TTFT p95 `4263ms` above `2513ms`, rollback-required. Do not promote or broaden this shard variant.
- The same Luna shard candidate in `compacted_anchor` completed `20` applied observations with overall hit `75.66%` versus baseline `75.54%`; stable follow-ups were `99.11%` versus `98.93%`. The first post-compaction summary prefix remains a genuine cold read, so the candidate is still far below the `99.5%` promotion target.
- Compaction replaces the historical input prefix with a new summary prefix, so the first post-compaction request is a real cold prefix. From the second follow-up onward the prefix remained stable and cache recovered; measured avoidable and provider-unstable gaps stayed `0`.
- sheapi `gpt-5.6-sol` semantic continuation probe returned `200 -> 400`; Responses WebSocket handshake returned `404`. Do not enable either mechanism for this provider/model scope.
- Current decision: treat the applied cohort-key result as a positive optimization, but keep the existing stable session-anchored `prompt_cache_key` strategy until at least `18` clean paired applied observations confirm cache hit `>=99.5%`, strict improvement over baseline, non-inferior errors, non-inferior TTFT, and one inbound request = one upstream request.
- Next trigger: extend the same isolated gpt-5.5 applied canary from `9` to at least `18` paired observations. Promote only for the verified upstream/model scope when all safety gates still pass, then continue iterating toward `100%` instead of imposing an arbitrary minimum gain per iteration.
- Next trigger: abandon the current single-key and 4-shard cohort candidates for this provider/model scope and design a different cache-affinity mechanism; rerun the same 18-pair gate only after a new candidate has a discriminating hypothesis. Do not promote either failed variant.
- Next trigger: keep the stable session-anchored baseline for this provider/model. A new candidate must specifically improve the real post-compaction cold prefix without falsifying cache accounting, then pass the same hit/error/TTFT gate.

## 2026-06-30 Active Rules

- Active project root is `G:\Atoapi`; do not use old `G:\Flutter\ccs++` as source.
- Current line is v0.1.55+ source. Package only after live-log comparison and build checks.
- Cache-hit optimization hard gate: no active warmup/prewarm, no companion sync request, no extra upstream request, no normal main-path `previous_response_id + delta`.
- Main Responses session-delta is allowed only for 413 self-rescue or compact/compatibility paths with strict same provider/model/scope/tool-context checks.
- Foreground Responses guard defaults to 0ms; only repeated stable exact avoidable evidence may adaptively wait, capped at +0.5s.
- Proxy-added first-token latency versus the upstream dashboard must stay within 2s. Diagnose upload, transport, gateway queueing, and provider processing separately before changing behavior.
- Do not change, trim, compress, reorder, or summarize tool output content by default.
- For log analysis, classify first: real new tail, true avoidable, cold read, session/context split, upstream error, or statistics-label issue. Do not tune blindly.
- Current live v0.1.53/v0.1.54 analysis showed warm adjusted bucket hit near 99.98% after using logged gap fields; raw hit was mainly lowered by real new tool-output tails, cross-upstream cold starts, and upstream errors, not broad avoidable gaps.
- v0.1.55 keeps outbound `prompt_cache_key` stable, but foreground local prefix waiting must use current-upstream exact/family state only; cross-upstream alias can preserve diagnostics/state, not block a live request.
- v0.1.55 adds multi-key prefix affinity: same provider/model/prefix should prefer the same healthy key to avoid key-rotation cache cold starts; key failure still clears affinity and fails over.
- Responses `prompt_cache_key` should be scoped by stable session anchor: same session appended tail keeps the key; different session anchor splits the key to reduce upstream cache cross-talk.

## Current Flow

Current source/package candidate:

`v0.2.15-codex-body-session-metadata-20260717`

Current release folder:

`G:\Atoapi\releases\v0.2.15-codex-body-session-metadata-20260717`

Current package is v0.2.12. It keeps the historical compression/error-isolation protections, fixes cached-token accounting, and makes cache affinity, compaction recovery, and upstream capability decisions evidence-gated. The older v0.1.35 description below remains historical implementation context.

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
