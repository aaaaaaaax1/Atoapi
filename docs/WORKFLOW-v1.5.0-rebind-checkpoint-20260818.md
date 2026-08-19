# v1.5.0 Rebind Checkpoint — 2026-08-18

## Status

- Historical cache-hit champion remains `v1.4.33`.
- The source-built `v1.5.0` candidate is not packaged, promoted, or installed.
- Release evidence JSON under `output/` remains local-only and must not be committed.

## Implemented candidate behavior

- On a proven local FullReplay recovery with an unambiguous closed tool graph,
  regenerated Codex `call_id` values are rebound to the predecessor wire.
- A successful local rebind suppresses only a duplicate no-gap exact-prefix
  settle; genuine avoidable gaps, cold starts, external continuations,
  compaction, and ambiguous lineage retain their existing handling.

## Verification

- `cargo fmt --all -- --check` passed.
- `cargo test --locked prefix_control --lib` passed (31 tests).
- `cargo test --locked local_previous_response_id_rebinds_regenerated_tool_ids_on_frozen_full_replay --lib` passed.
- `cargo test --locked local_rebind_success_suppresses_duplicate_exact_settle_without_gap --lib` passed.
- `cargo build --release --locked` passed; candidate executable SHA-256 starts with `5c10bddf`.
- `node scripts/verify-release-champion.mjs --self-test` passed.

## Live diagnostic evidence

- Scope: `agent-codex-bizd / gpt-5.6-sol / max`, pinned realm `fd1834caa41a...`.
- The corrected three-turn tool fixture establishes normal seed -> tool history -> local response-id rebind continuation. It keeps the initial seed free of synthetic tool history, which the selected relay rejects.
- `candidate-v1500-v1433-local-prid-rebind-20260818-r2.json` completed two balanced pairs with symmetric cold starts, exact client/wire evidence, and a passing rebind witness for both target requests. Candidate total cache-128 was `48.4835%` versus `46.1081%` (+`2.3754pp`); warm cache-128 was `49.1032%` versus `44.5115%` (+`4.5917pp`); local overhead was unchanged.
- That positive run remains diagnostic because one scored pair carried provider-instability evidence. The reversed-start rerun stopped before a second complete pair, and the same-binary control was rejected by the relay at the first tool-tail request (`400 invalid_request_error`); they neither confirm nor negate the candidate.
- Read-only live metrics show the recent normal traffic has no local avoidable cache gap: remaining shortfalls are real tool/message tails or provider-waterline instability. Do not broaden foreground waits merely to chase those records.

## Next gate

Retain this narrow candidate behavior and seek a provider-clean two-pair reproduction on the same refreshed scope before any package or champion change. If the selected relay remains tool-history unstable, switch back to normal dynamic text-tail analysis rather than relaxing the promotion rules.

## 2026-08-18 local wait correction

- A 450k-token dynamic tool-tail diagnostic showed the candidate could inherit a
  `GiantColdRoot` claim after a cold seed and add a 500ms
  `responses_giant_cold_prefix_warm_pending` foreground wait to a real mixed/tool
  child. The equivalent champion request had no wait only because its seed was
  already warm; the run was not promotion evidence.
- `WarmPendingClaim::followup_is_settle_safe` now keeps the bounded giant-root
  wait only for an empty probe or a small clean message child. Tool-bearing,
  noisy, and large dynamic tails fail open immediately. The three-child chain,
  TTL, and 500ms cap remain unchanged.
- `cargo test --manifest-path src-tauri/Cargo.toml --release warm_pending --lib`
  passed all 25 tests; `cargo fmt --manifest-path src-tauri/Cargo.toml -- --check`
  passed.
- The follow-up 450k-token A/B confirmed the wait disappeared (local pre-upstream
  overhead stayed around 20ms), but the provider cohort did not produce two
  scored pairs, so `v1.5.0` remains unpromoted and un-packaged.
- A reverse-start 450k-token rebind run then completed one scored pair with the
  rebind witness and no local wait regression (candidate 34ms vs champion 31ms),
  but the candidate seed was cold and the provider reported a 434,176-token
  unstable gap. The verifier correctly stopped before a second pair; this is
  diagnostic only and does not lower or promote the candidate.

## 2026-08-18 PCK evidence correction and replay

- The isolated `prompt-cache-key` probe had an evidence bug: a candidate that
  rewrote an already-identical root key was still marked `injected`. The Rust
  receipt now marks PCK as injected only when the frozen root actually changes;
  the verifier self-test also fails closed when no final-wire difference is
  present.
- The verifier now treats a changed, redacted `provider_prefix_key_fingerprint`
  as an attributable PCK-only wire difference. Semantic input, instruction,
  and tool-schema fingerprints must still match; other cache-control fields do
  not receive this exception.
- The disposable candidate can receive a deterministic isolated PCK override
  through `ATOAPI_FORCE_ISOLATED_PROMPT_CACHE_KEY`. It is never read by normal
  desktop traffic and is recorded only as a boolean test setting.
- Rebuilt candidate: `v1.5.0`. `node scripts/verify-release-champion.mjs
  --self-test`, `cargo fmt --manifest-path src-tauri/Cargo.toml -- --check`,
  the targeted cache-capability tests, and release `cargo check` passed.
- The refreshed live scope was `agent-codex-bizd / gpt-5.6-sol / max`, Key realm
  `e4e68e8bb008e00bcd60434d3eb14511d36f58408c03e5cf60c32b685f8a84ba`.
- `development-v1500-vs-champion-v1433-dynamic-tail-mix-pck-bizd-e4e6-20260818-r8.json`
  completed one warm-up plus two scored pairs. Candidate warm cache-128 was
  about `+4.985pp`, but the comparison contained roughly `513k` provider-
  unstable tokens and was not promotion evidence.
- The paced replay
  `development-v1500-vs-champion-v1433-dynamic-tail-mix-pck-bizd-e4e6-20260818-r9.json`
  completed the same schedule with a verifier-only 750ms turn delay and 30s
  pair cooldown. The PCK wire witness passed for all 22 request pairs, but
  candidate warm cache-128 was `-4.482pp` and the run still carried about
  `461k` provider-unstable tokens. It is a negative/unstable result, not a
  promotion.
- No release package was created and the live `18883` process was not touched.

The next meaningful comparison requires a newly refreshed provider waterline
or a different hand-selected upstream; repeating the same PCK replay on the
current unstable waterline would not add evidence. The live metrics snapshot
also shows `cache_avoidable_gap_tokens=0`; remaining gaps are new dynamic tails
or provider instability, so broad foreground waits are not justified.

## 2026-08-18 v1.5.0 promotion and packaging

- The runner contract was corrected so thread-stable PCK bridge plus its required
  `prompt-cache-key` field counts as one candidate treatment. The bridge fixture
  now performs a real `base -> rotated` session/conversation transition while
  keeping the explicit thread stable.
- Provider-clean dynamic A/B report:
  `output/development-v1500-vs-champion-v1433-dynamic-tail-mix-thread-bridge-20260818-r3.json`.
  It completed two scored pairs with balanced crossover, symmetric cold starts,
  no extra candidate cold start, exact outbound wire symmetry, and a passing
  thread-stable bridge witness.
- Warm cache-128: v1.4.33 `49.8146%`; v1.5.0 `94.6848%`; delta `+44.8702pp`.
  Overall cache-128: v1.4.33 `34.3052%`; v1.5.0 `65.2748%`; delta
  `+30.9696pp`. Local pre-upstream p95 remained `3ms` for both arms and local
  proxy p95 remained `2ms` for both arms. End-to-end TTFT was upstream-dominated
  and therefore exempt under the local-only latency gate.
- The exact residual wait correction and current exact-scope certificate test
  expectation were fixed; the complete 1,072-test Rust release suite then
  passed, along with frontend build, release-champion self-test, and NSIS build.
- Packaged release:
  `G:\Atoapi\releases\v1.5.0-thread-stable-pck-bridge-20260818`.
  Both `Atoapi.exe` and `Atoapi_1.5.0_x64-setup.exe` report FileVersion and
  ProductVersion `1.5.0`; hashes and build details are in `BUILD-MANIFEST.json`.
- Build caches (`src-tauri\target`, `src-tauri\target-champion-v1433`, `dist`,
  and `.vite`) were removed after packaging. The historical v1.4.33 release,
  `output/*.json` evidence, user configuration, and live PID 3828 on port 18883
  were not modified.

Next step: treat v1.5.0 as the new champion base for the next positive-only
iteration; do not replace the live 18883 process automatically.
