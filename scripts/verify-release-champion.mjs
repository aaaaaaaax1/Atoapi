import assert from "node:assert/strict";
import { createHash, randomUUID } from "node:crypto";
import { spawn } from "node:child_process";
import {
  copyFile,
  mkdir,
  mkdtemp,
  readFile,
  rm,
  stat,
  writeFile
} from "node:fs/promises";
import { createServer as createNetServer } from "node:net";
import { tmpdir } from "node:os";
import { basename, dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";

// This verifier is deliberately standalone.  A normal package build must not
// silently consume real upstream capacity or credentials; live comparison is
// enabled only by an explicit --live plus both executable and config inputs.
const SCHEMA = "atoapi-release-champion-v1";
const STATIC_WIRE_FIELDS = [
  "cache_metadata",
  "instructions",
  "tools_schema",
  "pre_input_wire"
];
// The ordinary script lives in <workspace>/scripts. The fixed local-admin
// runner embeds the identical file under Atoapi's config directory, so it
// supplies the workspace root explicitly without accepting any user command or
// path parameter over the local API.
const repoRoot = resolve(
  process.env.ATOAPI_RELEASE_CHAMPION_WORKSPACE_ROOT ||
    resolve(dirname(fileURLToPath(import.meta.url)), "..")
);
const args = parseArgs(process.argv.slice(2));

class FailClosedError extends Error {
  constructor(code, message, missing = []) {
    super(message);
    this.name = "FailClosedError";
    this.code = code;
    this.missing = missing;
  }
}

// The live Codex metrics guard is intentionally stricter than the isolated
// arm runner.  When it fails after a pair has already completed, retain only
// a bounded, payload-free projection for diagnosis; never let that evidence
// become a promotion result.
class LiveCodexMetricsGateError extends FailClosedError {
  constructor(code, message, evidence) {
    super(code, message);
    this.name = "LiveCodexMetricsGateError";
    this.liveGateEvidence = evidence;
  }
}

if (booleanArg(args.help) || booleanArg(args.h)) {
  printUsage();
  process.exit(0);
}

if (booleanArg(args["self-test"])) {
  await runSelfTest();
  process.exit(0);
}

try {
  const report = hasOfflineArtifacts(args)
    ? await compareOfflineArtifacts(args)
    : await runLiveComparison(args);
  console.log(JSON.stringify(report, null, 2));
  if (args.output) {
    await writeFile(resolve(String(args.output)), `${JSON.stringify(report, null, 2)}\n`, "utf8");
  }
  if (!report.pass) process.exitCode = 1;
} catch (error) {
  const failure = failClosedReport(error);
  console.error(JSON.stringify(failure, null, 2));
  if (args.output) {
    try {
      await writeFile(resolve(String(args.output)), `${JSON.stringify(failure, null, 2)}\n`, "utf8");
    } catch (writeError) {
      console.error(JSON.stringify({
        schema: SCHEMA,
        kind: "release-champion-output-write-failure",
        message: safeErrorMessage(writeError)
      }));
    }
  }
  process.exitCode = 2;
}

async function runLiveComparison(options) {
  const required = [
    ["--live", booleanArg(options.live)],
    ["--champion-exe", Boolean(options["champion-exe"])],
    ["--candidate-exe", Boolean(options["candidate-exe"])],
    ["--source-config-dir", Boolean(options["source-config-dir"])],
    ["--model", Boolean(options.model)],
    ["--key-realm-hash", Boolean(options["key-realm-hash"])]
  ]
    .filter(([, present]) => !present)
    .map(([name]) => name);
  if (required.length > 0) {
    throw new FailClosedError(
      "missing_live_parameters",
      "live verification is disabled until every required parameter is explicit",
      required
    );
  }

  const championExe = resolve(String(options["champion-exe"]));
  const candidateExe = resolve(String(options["candidate-exe"]));
  const sourceConfigDir = resolve(String(options["source-config-dir"]));
  const model = String(options.model).trim();
  const keyRealmHash = validateOpaqueHash(options["key-realm-hash"], "--key-realm-hash");
  const providerScope = normalizeProviderScope(options["provider-scope"] ?? "codex-agent");
  const scenario = normalizeScenario(options.scenario ?? "full-replay");
  if (!scenario) {
    throw new FailClosedError(
      "invalid_scenario",
      "--scenario must be full-replay, tool-burst, dynamic-tail-mix, compacted-anchor, or compaction-root"
    );
  }
  const expectedFamily = requestFamilyForScenario(scenario);
  const requestFamily = String(options["request-family"] ?? expectedFamily).trim();
  if (requestFamily !== expectedFamily) {
    throw new FailClosedError(
      "request_family_scenario_mismatch",
      `scenario ${scenario} requires request_family ${expectedFamily}`
    );
  }
  const pairs = boundedInteger(options.pairs ?? 2, "--pairs", 1, 8);
  // Pair zero is often the first use of a freshly isolated upstream lane.
  // Execute any requested warm-up pairs so the lane reaches its ordinary
  // placement state, but never let their cold-start accounting influence a
  // release aggregate or promotion decision.
  const warmupPairs = boundedInteger(
    options["warmup-pairs"] ?? 0,
    "--warmup-pairs",
    0,
    pairs - 1
  );
  const pairOffset = boundedInteger(options["pair-offset"] ?? 0, "--pair-offset", 0, 1);
  const firstArm = normalizeFirstArm(options["first-arm"] ?? "champion");
  const turns = boundedInteger(
    options.turns ?? 6,
    "--turns",
    scenario === "compaction-root" ? 2 : 3,
    60
  );
  if (scenario === "compaction-root" && turns !== 2) {
    throw new FailClosedError(
      "compaction_root_turn_count",
      "--scenario compaction-root requires --turns 2 (seed plus compaction root)"
    );
  }
  // Eleven turns remains the normal full-five-tail dynamic mix. A three-turn
  // control is also meaningful: it has one changed tail and its one direct
  // successor, so bounded medium-tail maturity behavior can be compared in
  // the largest request-size envelope both binaries can actually complete.
  if (
    scenario === "dynamic-tail-mix" &&
    (turns < 3 || (
      turns % 2 === 0 &&
      !booleanArg(options["require-candidate-late-shallow-provider-waterline-rollback-wait"])
    ))
  ) {
    throw new FailClosedError(
      "dynamic_tail_mix_turn_count",
      "--scenario dynamic-tail-mix requires an odd turn count of at least 3, except the late shallow provider-waterline rollback probe which requires exactly four turns"
    );
  }
  const requestedPort = boundedInteger(options.port ?? 18_885, "--port", 1_024, 65_500);
  if (requestedPort === 18_883) {
    throw new FailClosedError(
      "protected_live_port",
      "--port 18883 is reserved for the running Atoapi service; use an isolated port"
    );
  }
  const maxOutputTokens = boundedInteger(
    options["max-output-tokens"] ?? 32,
    "--max-output-tokens",
    1,
    4_096
  );
  // A Responses-compatible upstream can require the same reasoning effort
  // that the real Codex client selected. Keep it explicit and symmetric: it
  // is fixture compatibility, never a candidate-only cache treatment.
  const reasoningEffort = optionalReasoningEffort(options["reasoning-effort"]);
  const stableInstructionChars = boundedInteger(
    options["stable-instruction-chars"] ?? 16_384,
    "--stable-instruction-chars",
    1_024,
    200_000
  );
  const seedContextChars = boundedInteger(
    options["seed-context-chars"] ?? 0,
    "--seed-context-chars",
    0,
    2_500_000
  );
  const minimumSeedInputTokens = boundedInteger(
    options["minimum-seed-input-tokens"] ?? 0,
    "--minimum-seed-input-tokens",
    0,
    1_000_000
  );
  const minimumPeakInputTokens = boundedInteger(
    options["minimum-peak-input-tokens"] ?? 0,
    "--minimum-peak-input-tokens",
    0,
    1_000_000
  );
  const maximumPeakInputTokens = boundedInteger(
    options["maximum-peak-input-tokens"] ?? 0,
    "--maximum-peak-input-tokens",
    0,
    1_000_000
  );
  if (maximumPeakInputTokens > 0 && maximumPeakInputTokens < minimumPeakInputTokens) {
    throw new FailClosedError(
      "peak_input_token_range_invalid",
      "--maximum-peak-input-tokens must be zero or at least --minimum-peak-input-tokens"
    );
  }
  const toolChars = boundedInteger(options["tool-chars"] ?? 32_768, "--tool-chars", 1_024, 512_000);
  const toolCalls = boundedInteger(options["tool-calls"] ?? 1, "--tool-calls", 1, 8);
  const toolOutputShape = normalizeToolOutputShape(options["tool-output-shape"] ?? "natural");
  // Function calls remain the historical fixture wire. The explicit custom
  // variant is a narrow verifier fixture for the native custom-tool rebind
  // path; it never changes normal desktop traffic or default comparisons.
  const toolProtocol = normalizeToolProtocol(options["tool-protocol"] ?? "function");
  if (!toolProtocol) {
    throw new FailClosedError(
      "invalid_tool_protocol",
      "--tool-protocol must be function or custom"
    );
  }
  // Historical tool-history validation declares the synthetic fixture schema
  // from the seed onward. Keep that behavior by default: omitting the flag
  // must never silently change a previously valid comparison wire.
  // `--include-tool-schema=false` is available only for an explicit protocol
  // compatibility probe and has a distinct fixture identity.
  const includeToolSchema = resolveIncludeToolSchema(options);
  const requireCandidateExactMediumToolTailMaturityWait = booleanArg(
    options["require-candidate-exact-medium-tool-tail-maturity-wait"]
  );
  // This is deliberately a witness gate, rather than a generic guarded-request
  // count: a 500ms wait from any other prefix policy cannot prove that the
  // exact large-message text-tail candidate actually executed.
  const requireCandidateExactLargeMessageTailLag = booleanArg(
    options["require-candidate-exact-large-message-tail-lag"]
  );
  // A distinct witness for a shallow, final-scope-proven provider rollback
  // whose direct child arrives after the ordinary 500ms window. A generic
  // foreground wait cannot qualify this treatment.
  const requireCandidateLateShallowProviderWaterlineRollbackWait = booleanArg(
    options["require-candidate-late-shallow-provider-waterline-rollback-wait"]
  );
  if (requireCandidateExactMediumToolTailMaturityWait) {
    if (scenario !== "tool-tail-maturity") {
      throw new FailClosedError(
        "exact_medium_tool_tail_scenario_mismatch",
        "--require-candidate-exact-medium-tool-tail-maturity-wait requires --scenario tool-tail-maturity"
      );
    }
    if (pairs !== 2 || turns !== 4) {
      throw new FailClosedError(
        "exact_medium_tool_tail_schedule_invalid",
        "exact medium tool-tail maturity requires exactly two crossover pairs and four turns (seed, stable predecessor, tool tail, direct successor)"
      );
    }
    if (toolChars < 4_096 || toolChars > 8_191 || toolCalls !== 1 || !includeToolSchema) {
      throw new FailClosedError(
        "exact_medium_tool_tail_fixture_invalid",
        "exact medium tool-tail maturity requires one 4096-8191 character tool result with the fixture schema enabled"
      );
    }
    if (minimumPeakInputTokens < 16_384) {
      throw new FailClosedError(
        "exact_medium_tool_tail_peak_gate_missing",
        "exact medium tool-tail maturity requires --minimum-peak-input-tokens of at least 16384"
      );
    }
  }
  const dynamicTailProfile = normalizeDynamicTailProfile(
    options["dynamic-tail-profile"] ?? "mixed"
  );
  if (!dynamicTailProfile) {
    throw new FailClosedError(
      "invalid_dynamic_tail_profile",
      "--dynamic-tail-profile must be mixed or natural-dense"
    );
  }
  const dynamicTailMode = normalizeDynamicTailMode(
    options["dynamic-tail-mode"] ?? "tool"
  );
  if (!dynamicTailMode) {
    throw new FailClosedError(
      "invalid_dynamic_tail_mode",
      "--dynamic-tail-mode must be tool or text"
    );
  }
  const scenarioUsesToolFixture = new Set([
    "dynamic-tail-mix",
    "tool-burst",
    "tool-tail-maturity"
  ]).has(scenario) && !(scenario === "dynamic-tail-mix" && dynamicTailMode === "text");
  if (toolProtocol === "custom" && (!scenarioUsesToolFixture || !includeToolSchema)) {
    throw new FailClosedError(
      "custom_tool_fixture_invalid",
      "--tool-protocol custom requires a tool-history scenario with --include-tool-schema true and dynamic-tail-mode tool"
    );
  }
  // The normal dynamic tool fixture intentionally retains stable call ids so
  // historical release evidence remains comparable. This opt-in probe is the
  // one narrow exception: it exercises the local previous_response_id branch
  // where a client replays the same completed tool pairs with fresh call ids.
  // It is a verifier-only fixture switch and never changes normal Atoapi
  // traffic or any existing default benchmark wire.
  const exerciseLocalPreviousResponseIdRebind = booleanArg(
    options["exercise-local-previous-response-id-rebind"]
  );
  const exerciseLocalPreviousResponseIdFullReplay = booleanArg(
    options["exercise-local-previous-response-id-full-replay"]
  );
  if (exerciseLocalPreviousResponseIdRebind && exerciseLocalPreviousResponseIdFullReplay) {
    throw new FailClosedError(
      "local_previous_response_id_fixture_conflict",
      "the rebind and unchanged FullReplay fixtures are mutually exclusive"
    );
  }
  if ((exerciseLocalPreviousResponseIdRebind || exerciseLocalPreviousResponseIdFullReplay) && (
    scenario !== "dynamic-tail-mix" ||
    dynamicTailMode !== "tool" ||
    !includeToolSchema ||
    turns !== 3 ||
    toolCalls < 1
  )) {
    throw new FailClosedError(
      exerciseLocalPreviousResponseIdFullReplay
        ? "local_previous_response_id_full_replay_fixture_invalid"
        : "local_previous_response_id_rebind_fixture_invalid",
      exerciseLocalPreviousResponseIdFullReplay
        ? "--exercise-local-previous-response-id-full-replay requires dynamic-tail-mix, tool mode, the tool schema, at least one tool call, and exactly three turns"
        : "--exercise-local-previous-response-id-rebind requires dynamic-tail-mix, tool mode, the tool schema, at least one tool call, and exactly three turns"
    );
  }
  if (exerciseLocalPreviousResponseIdFullReplay) {
    if (toolChars < 32_768) {
      throw new FailClosedError(
        "local_previous_response_id_full_replay_tool_tail_too_small",
        "--exercise-local-previous-response-id-full-replay requires --tool-chars of at least 32768"
      );
    }
    if (minimumPeakInputTokens < 16_384) {
      throw new FailClosedError(
        "local_previous_response_id_full_replay_peak_gate_missing",
        "--exercise-local-previous-response-id-full-replay requires --minimum-peak-input-tokens of at least 16384"
      );
    }
  }
  if (scenario !== "dynamic-tail-mix" && dynamicTailMode !== "tool") {
    throw new FailClosedError(
      "dynamic_tail_mode_scenario_mismatch",
      "--dynamic-tail-mode text is available only for dynamic-tail-mix"
    );
  }
  const fixtureProfile = normalizeFixtureProfile(options["fixture-profile"] ?? "natural");
  if (!fixtureProfile) {
    throw new FailClosedError(
      "invalid_fixture_profile",
      "--fixture-profile must be natural, natural-dense, or legacy-repeated"
    );
  }
  if (requireCandidateExactLargeMessageTailLag) {
    if (scenario !== "dynamic-tail-mix" || turns !== 3) {
      throw new FailClosedError(
        "exact_large_message_tail_schedule_invalid",
        "exact large message tail lag requires dynamic-tail-mix with exactly three turns (seed, text tail, direct successor)"
      );
    }
    if (minimumSeedInputTokens < 262_144 || minimumPeakInputTokens < 262_144) {
      throw new FailClosedError(
        "exact_large_message_tail_peak_gate_missing",
        "exact large message tail lag requires seed and peak gates of at least 262144 tokens"
      );
    }
    if (dynamicTailProfile !== "mixed" || dynamicTailMode !== "text" || fixtureProfile !== "natural") {
      throw new FailClosedError(
        "exact_large_message_tail_fixture_invalid",
        "exact large message tail lag requires the capacity-reachable natural/mixed/text fixture"
      );
    }
    if (toolChars !== 131_072 || toolCalls !== 2 || !includeToolSchema) {
      throw new FailClosedError(
        "exact_large_message_tail_shape_invalid",
        "exact large message tail lag requires the 131072-character/two-call schema fixture that emits the 32829-character text tail"
      );
    }
  }
  // A repeated payload gives a later pair an upstream-warmed context and can
  // make a version look better or worse purely because of run order. New live
  // comparisons are therefore diverse by default. Reuse is explicit and only
  // appropriate for a narrowly scoped deterministic repro.
  const freshFixturePerPair = resolveFreshFixturePerPair(options);
  const turnDelayMs = boundedInteger(
    options["turn-delay-ms"] ?? 0,
    "--turn-delay-ms",
    0,
    5_000
  );
  const interArmDelayMs = boundedInteger(
    options["inter-arm-delay-ms"] ?? 0,
    "--inter-arm-delay-ms",
    0,
    5_000
  );
  const pairDelayMs = boundedInteger(
    options["pair-delay-ms"] ?? 0,
    "--pair-delay-ms",
    0,
    60_000
  );
  // Retention is an upstream cache-lifetime treatment. An immediate follow-up
  // cannot exercise its 24h horizon, so the verifier exposes one bounded,
  // test-only seed-to-reuse delay. It never reaches the desktop proxy.
  const seedToReuseDelayMs = boundedInteger(
    options["seed-to-reuse-delay-ms"] ?? 0,
    "--seed-to-reuse-delay-ms",
    0,
    3_600_000
  );
  if ((requireCandidateExactMediumToolTailMaturityWait || requireCandidateExactLargeMessageTailLag) &&
    (turnDelayMs !== 0 || interArmDelayMs !== 0)) {
    throw new FailClosedError(
      requireCandidateExactLargeMessageTailLag
        ? "exact_large_message_tail_pacing_invalid"
        : "exact_medium_tool_tail_pacing_invalid",
      requireCandidateExactLargeMessageTailLag
        ? "exact large message tail lag requires zero turn and inter-arm pacing so its direct-successor window is not artificially changed"
      : "exact medium tool-tail maturity requires zero turn and inter-arm pacing so its 500ms direct-successor window is not artificially changed"
    );
  }
  if (requireCandidateLateShallowProviderWaterlineRollbackWait) {
    if (scenario !== "dynamic-tail-mix" || pairs !== 2 || turns !== 4) {
      throw new FailClosedError(
        "late_shallow_waterline_schedule_invalid",
        "late shallow provider-waterline rollback requires two dynamic-tail crossover pairs with four turns (seed, changed tail, rollback witness, delayed direct child)"
      );
    }
    if (minimumSeedInputTokens < 32_768 || minimumPeakInputTokens < 32_768) {
      throw new FailClosedError(
        "late_shallow_waterline_peak_gate_missing",
        "late shallow provider-waterline rollback requires seed and peak gates of at least 32768 tokens"
      );
    }
    if (dynamicTailProfile !== "mixed" || dynamicTailMode !== "text" || fixtureProfile !== "natural") {
      throw new FailClosedError(
        "late_shallow_waterline_fixture_invalid",
        "late shallow provider-waterline rollback requires the natural/mixed/text dynamic fixture"
      );
    }
    if (turnDelayMs < 750 || interArmDelayMs !== 0) {
      throw new FailClosedError(
        "late_shallow_waterline_pacing_invalid",
        "late shallow provider-waterline rollback requires 750-5000ms turn pacing and zero inter-arm pacing so its late-child window is actually exercised"
      );
    }
  }
  const requireCandidateGuardedRequests = boundedInteger(
    options["require-candidate-guarded-requests"] ?? 0,
    "--require-candidate-guarded-requests",
    0,
    turns
  );
  const maxTtftRegressionMs = boundedInteger(
    options["max-ttft-regression-ms"] ?? 0,
    "--max-ttft-regression-ms",
    0,
    120_000
  );
  const maxInputTokenDelta = boundedInteger(
    options["max-input-token-delta"] ?? 128,
    "--max-input-token-delta",
    0,
    10_000
  );
  // Promotion keeps local pre-upstream work strict by default.  A caller may
  // explicitly choose to treat end-to-end TTFT as a diagnostic because it also
  // includes the hand-selected upstream's scheduling variance; that never
  // relaxes the local latency gate.
  const maxLocalProxyOverheadRegressionMs = strictLocalLatencyRegressionBudget(options);
  const maxFullBucketRegressionRequests = boundedInteger(
    options["max-full-bucket-regression-requests"] ?? 0,
    "--max-full-bucket-regression-requests",
    0,
    pairs * turns
  );
  const requireTtftNoRegression = resolvePromotionTtftPolicy(options);
  const sharedUpstreamUserAgent = optionalUpstreamUserAgent(options["upstream-user-agent"]);
  const championUpstreamUserAgent = optionalUpstreamUserAgent(
    options["champion-upstream-user-agent"]
  ) ?? sharedUpstreamUserAgent;
  const candidateUpstreamUserAgent = optionalUpstreamUserAgent(
    options["candidate-upstream-user-agent"]
  ) ?? sharedUpstreamUserAgent;
  // A header split is useful only as a tightly bounded same-binary diagnosis.
  // It must never flow into the promotion path, where the two arms need an
  // identical upstream cache lane.
  const diagnosticUserAgentSplit = booleanArg(options["diagnostic-user-agent-split"]);
  const keepRunDir = booleanArg(options["keep-run-dir"]);
  const responseTimeoutMs = boundedInteger(
    options["response-timeout-ms"] ?? 180_000,
    "--response-timeout-ms",
    30_000,
    600_000
  );
  const isolateUpstreamCache = booleanArg(options["isolate-upstream-cache"]);
  // A live isolated comparison is only meaningful when the native upstream
  // placement telemetry proves both arms stayed on distinct, stable lanes.
  // Offline artifacts predate this proof in some cases, so their explicit
  // compatibility path keeps the requirement disabled.
  const nativePlacementIsolationRequired = isolateUpstreamCache;
  const sharedCacheCrossover = booleanArg(options["shared-cache-crossover"]);
  const candidateUpstreamAffinity = booleanArg(options["candidate-upstream-affinity"]);
  const candidateCacheControlField = optionalCandidateCacheControlField(
    options["candidate-cache-control-field"]
  );
  const candidateThreadStablePckBridge = booleanArg(
    options["candidate-thread-stable-pck-bridge"]
  );
  const candidateCacheOptions24h = booleanArg(options["candidate-cache-options-24h"]);
  const requireCandidateOptions24hSiblingSettle = booleanArg(
    options["require-candidate-options24h-sibling-settle"]
  );
  const candidateHttp1 = booleanArg(options["candidate-http1"]);
  const candidateProviderWaterlineRecoveryWait = booleanArg(
    options["candidate-provider-waterline-recovery-wait"]
  );
  // This is a verifier-only candidate treatment for the isolated runtime
  // thread-stable prompt_cache_key bridge. It is deliberately narrower than
  // the ordinary cache-control probes.
  if (candidateThreadStablePckBridge) {
    if (scenario !== "dynamic-tail-mix") {
      throw new FailClosedError(
        "candidate_thread_stable_pck_bridge_scenario_invalid",
        "--candidate-thread-stable-pck-bridge requires --scenario dynamic-tail-mix"
      );
    }
    if (!sharedCacheCrossover) {
      throw new FailClosedError(
        "candidate_thread_stable_pck_bridge_requires_shared_crossover",
        "--candidate-thread-stable-pck-bridge requires --shared-cache-crossover"
      );
    }
    // The bridge is layered on top of the native prompt-cache-key field.  The
    // field is therefore required, but it is not a second/conflicting
    // treatment; rejecting it here made the wrapper's explicit bridge mode
    // impossible to run.
    if (candidateCacheControlField !== "prompt-cache-key") {
      throw new FailClosedError(
        "candidate_thread_stable_pck_bridge_requires_pck",
        "--candidate-thread-stable-pck-bridge requires --candidate-cache-control-field prompt-cache-key"
      );
    }
    const conflictingCandidateTreatment =
      candidateUpstreamAffinity ||
      (candidateCacheControlField && candidateCacheControlField !== "prompt-cache-key") ||
      candidateCacheOptions24h ||
      candidateHttp1 ||
      candidateProviderWaterlineRecoveryWait ||
      diagnosticUserAgentSplit ||
      exerciseLocalPreviousResponseIdRebind ||
      exerciseLocalPreviousResponseIdFullReplay ||
      Boolean(options["prompt-cache-key-prefix"]) ||
      requireCandidateExactMediumToolTailMaturityWait ||
      requireCandidateExactLargeMessageTailLag ||
      requireCandidateLateShallowProviderWaterlineRollbackWait;
    if (conflictingCandidateTreatment) {
      throw new FailClosedError(
        "candidate_thread_stable_pck_bridge_confounded",
        "--candidate-thread-stable-pck-bridge cannot be combined with another candidate-only treatment or witness"
      );
    }
  }
  if (exerciseLocalPreviousResponseIdFullReplay && (
    diagnosticUserAgentSplit || candidateUpstreamAffinity || candidateCacheControlField ||
    candidateThreadStablePckBridge || candidateHttp1 || candidateProviderWaterlineRecoveryWait ||
    Boolean(options["prompt-cache-key-prefix"])
  )) {
    throw new FailClosedError(
      "local_previous_response_id_full_replay_confounded",
      "the unchanged local previous_response_id FullReplay fixture cannot be combined with another candidate-only or diagnostic treatment"
    );
  }
  // An opaque prompt-cache placement value is local evidence that the two
  // arms chose different values; it does not prove that a selected upstream
  // honors the field as an isolation boundary. The live v1.5.0 replay
  // demonstrated cross-arm cache transfer despite distinct fingerprints, so
  // promotion must use turn-by-turn shared placement crossover until a future
  // upstream-specific isolation proof exists.
  const promotionRequiresSharedUpstreamPlacementCrossover = !diagnosticUserAgentSplit;
  const requestedReuseRuntimePerArm = booleanArg(options["reuse-runtime-per-arm"]);
  if (sharedCacheCrossover && isolateUpstreamCache) {
    throw new FailClosedError(
      "shared_cache_crossover_isolation_conflict",
      "--shared-cache-crossover cannot be combined with --isolate-upstream-cache"
    );
  }
  if (sharedCacheCrossover && !requestedReuseRuntimePerArm) {
    throw new FailClosedError(
      "shared_cache_crossover_requires_reuse",
      "--shared-cache-crossover requires --reuse-runtime-per-arm for turn-by-turn ordering"
    );
  }
  if (diagnosticUserAgentSplit && !isolateUpstreamCache) {
    throw new FailClosedError(
      "diagnostic_user_agent_split_requires_isolated_cache",
      "--diagnostic-user-agent-split requires --isolate-upstream-cache so the intentional header split cannot cross-warm a shared lane"
    );
  }
  if (diagnosticUserAgentSplit && sharedCacheCrossover) {
    throw new FailClosedError(
      "diagnostic_user_agent_split_shared_crossover_conflict",
      "--diagnostic-user-agent-split cannot be combined with --shared-cache-crossover"
    );
  }
  if (diagnosticUserAgentSplit && (
    candidateUpstreamAffinity || candidateCacheControlField || candidateThreadStablePckBridge || candidateHttp1 ||
    candidateProviderWaterlineRecoveryWait || Boolean(options["prompt-cache-key-prefix"])
  )) {
    throw new FailClosedError(
      "diagnostic_user_agent_split_confounded",
      "--diagnostic-user-agent-split cannot be combined with a cache-control, affinity, transport, recovery-wait, or client-key experiment"
    );
  }
  if (candidateUpstreamAffinity && !sharedCacheCrossover) {
    throw new FailClosedError(
      "candidate_upstream_affinity_requires_shared_crossover",
      "--candidate-upstream-affinity requires --shared-cache-crossover"
    );
  }
  if (candidateCacheControlField && !sharedCacheCrossover) {
    throw new FailClosedError(
      "candidate_cache_control_requires_shared_crossover",
      "--candidate-cache-control-field requires --shared-cache-crossover"
    );
  }
  if (candidateCacheOptions24h && candidateCacheControlField !== "prompt-cache-options") {
    throw new FailClosedError(
      "candidate_cache_options_24h_requires_options_field",
      "--candidate-cache-options-24h requires --candidate-cache-control-field prompt-cache-options"
    );
  }
  if (requireCandidateOptions24hSiblingSettle && !candidateCacheOptions24h) {
    throw new FailClosedError(
      "candidate_options24h_sibling_settle_requires_24h",
      "--require-candidate-options24h-sibling-settle requires --candidate-cache-options-24h"
    );
  }
  if (requireCandidateOptions24hSiblingSettle && (turnDelayMs !== 0 || interArmDelayMs !== 0)) {
    throw new FailClosedError(
      "candidate_options24h_sibling_settle_pacing_invalid",
      "--require-candidate-options24h-sibling-settle requires zero turn and inter-arm pacing so the bounded sibling settle is observable"
    );
  }
  if (candidateCacheControlField && candidateUpstreamAffinity) {
    throw new FailClosedError(
      "candidate_experimental_controls_conflict",
      "--candidate-cache-control-field and --candidate-upstream-affinity cannot be combined"
    );
  }
  if (candidateHttp1 && !sharedCacheCrossover) {
    throw new FailClosedError(
      "candidate_http1_requires_shared_crossover",
      "--candidate-http1 requires --shared-cache-crossover"
    );
  }
  if (candidateHttp1 && (candidateCacheControlField || candidateUpstreamAffinity)) {
    throw new FailClosedError(
      "candidate_experimental_controls_conflict",
      "--candidate-http1 cannot be combined with another candidate-only experiment"
    );
  }
  if (candidateProviderWaterlineRecoveryWait && (
    candidateCacheControlField || candidateUpstreamAffinity || candidateHttp1
  )) {
    throw new FailClosedError(
      "candidate_experimental_controls_conflict",
      "--candidate-provider-waterline-recovery-wait cannot be combined with another candidate-only experiment"
    );
  }
  // An isolated-cache arm must not keep a process-owned upstream connection
  // pool across pairs. Otherwise an upstream placement can remain permanently
  // attached to the champion/candidate role even after the metadata lane is
  // crossed over, making a same-binary control look like a product regression.
  const reuseRuntimePerArm = effectiveReuseRuntimePerArm(
    requestedReuseRuntimePerArm,
    isolateUpstreamCache
  );
  // Shared crossover aligns the placement key and turn order, but its two
  // persistent isolated runtimes still own different connection pools. Keep a
  // bounded diagnostic switch so a same-binary control can prove whether a
  // failure follows the second-created runtime rather than the product arm.
  const persistentRuntimeStartOrder = normalizePersistentRuntimeStartOrder(
    options["persistent-runtime-start-order"] ?? "champion"
  );
  if (persistentRuntimeStartOrder !== "champion" && !reuseRuntimePerArm) {
    throw new FailClosedError(
      "persistent_runtime_start_order_requires_reuse",
      "--persistent-runtime-start-order candidate requires --reuse-runtime-per-arm without upstream-cache isolation"
    );
  }
  const pinnedKeyId = optionalOpaqueIdentifier(options["key-id"], "--key-id");
  const forceUseSystemProxy = optionalBoolean(
    options["force-use-system-proxy"],
    "--force-use-system-proxy"
  );
  const liveCodexMetricsUrl = optionalLiveCodexMetricsUrl(
    options["live-codex-metrics-url"]
  );
  const liveCodexMaxAgeSeconds = liveCodexMetricsUrl
    ? boundedInteger(
      options["live-codex-max-age-seconds"] ?? 600,
      "--live-codex-max-age-seconds",
      30,
      3_600
    )
    : null;
  validateSeedToReuseDelay({
    seedToReuseDelayMs,
    scenario,
    turns,
    scoredPairCount: pairs - warmupPairs,
    sharedCacheCrossover,
    reuseRuntimePerArm,
    candidateCacheControlField,
    turnDelayMs,
    interArmDelayMs,
    liveCodexMetricsConfigured: Boolean(liveCodexMetricsUrl)
  });
  const seedToReuseHorizonEnabled = seedToReuseDelayMs > 0;
  const sourceSnapshot = await snapshotLiveConfig(sourceConfigDir);
  try {
    const configText = await readRequiredText(
      join(sourceSnapshot.configDir, "config.toml"),
      "snapshotted source config.toml"
    );
    const liveCodexMetricsKey = extractTomlString(configText, "local_key");
    if (!liveCodexMetricsKey) {
      throw new FailClosedError(
        "missing_local_key",
        "source config.toml has no local_key; live verification cannot authenticate safely"
      );
    }
    const codexRoute = codexAgentRoute(configText);
    const codexProviderId = codexRoute?.provider_id ?? "";
    if (providerScope === "codex-agent") {
      if (!codexProviderId) {
        throw new FailClosedError(
          "missing_codex_provider",
          "source config has no Codex agent injection provider_id"
        );
      }
      if (codexRoute?.enabled !== true) {
        throw new FailClosedError(
          "codex_agent_not_enabled",
          "source config has no enabled Codex agent injection; live verification must use the hand-selected Codex route"
        );
      }
    }
    const activeProviderId = extractTomlString(configText, "active_provider_id");
    const providerId = String(
      options["provider-id"] ?? (providerScope === "active-provider" ? activeProviderId : codexProviderId)
    ).trim();
    if (!providerId) {
      throw new FailClosedError(
        providerScope === "active-provider" ? "missing_active_provider" : "provider_scope_mismatch",
        providerScope === "active-provider"
          ? "active-provider scope requires an explicit active_provider_id in the source config"
          : "--provider-id must match the Codex provider_id in the snapshotted source config"
      );
    }
    if (providerScope === "codex-agent" && providerId !== codexProviderId) {
      throw new FailClosedError(
        "provider_scope_mismatch",
        "--provider-id must match the Codex provider_id in the snapshotted source config"
      );
    }
    if (providerScope === "active-provider") {
      if (providerId !== activeProviderId) {
        throw new FailClosedError(
          "active_provider_scope_mismatch",
          "active-provider scope requires --provider-id to match active_provider_id"
        );
      }
      const activeProviderBlock = tomlArrayBlocks(configText, "providers")
        .map((item) => item.body)
        .find((item) => extractTomlString(item, "id") === activeProviderId);
      if (!activeProviderBlock || extractTomlBoolean(activeProviderBlock, "enabled") !== true) {
        throw new FailClosedError(
          "active_provider_unavailable",
          "active-provider scope requires an enabled active_provider_id Provider"
        );
      }
    }
    if (!model) {
      throw new FailClosedError("invalid_model", "--model must not be empty");
    }
    // A Codex binding may leave model_id empty, meaning the Codex client
    // chooses its model per request. If it is explicitly bound, never let a
    // release comparison silently test a different model on the same Provider.
    if (providerScope === "codex-agent") assertCodexRouteModelScope(codexRoute, model);
    validatePinnedKeyConfiguration(configText, providerId, pinnedKeyId);
    const liveSelectionScopeFingerprint = await currentLiveSelectionScopeFingerprint(
      sourceConfigDir,
      configText,
      providerScope,
      pinnedKeyId
    );
    await assertLiveSelectionScopeUnchanged(
      sourceConfigDir,
      liveSelectionScopeFingerprint,
      "before_isolated_runtime_start",
      providerScope,
      pinnedKeyId
    );
    await assertFile(championExe, "--champion-exe");
    await assertFile(candidateExe, "--candidate-exe");

    const runId = String(options["run-id"] ?? randomUUID()).trim();
    if (!runId) throw new FailClosedError("invalid_run_id", "--run-id must not be empty");
    // The native PCK probe must be a real treatment.  If the selected scope
    // already carries the same generated key on the baseline path, merely
    // asking the candidate to apply that field is a no-op.  Give the isolated
    // candidate a deterministic, secret-free override so its final wire can
    // be proven different; this is never passed to normal desktop traffic.
    const candidatePromptCacheKeyOverride = candidateCacheControlField === "prompt-cache-key" &&
      !candidateThreadStablePckBridge
      ? generatedPromptCacheKey(`native-pck-${runId}`, "candidate")
      : null;

    const cohort = {
      provider_id: providerId,
      model,
      key_realm_hash: keyRealmHash,
      request_family: requestFamily
    };
    const assertLiveCodexScope = async (checkpoint, {
      requireFresh = true,
      selectionMode = "expected-scope"
    } = {}) => {
      if (!liveCodexMetricsUrl) return;
      await assertLiveCodexMetricsScopeUnchanged({
        metricsUrl: liveCodexMetricsUrl,
        localKey: liveCodexMetricsKey,
        expectedProviderId: cohort.provider_id,
        expectedModel: cohort.model,
        expectedRealm: cohort.key_realm_hash,
        maxAgeSeconds: liveCodexMaxAgeSeconds,
        checkpoint,
        requireFresh,
        selectionMode
      });
    };
    // The launch gate must observe the newest real Codex main request rather
    // than searching backwards for an older matching scope. That prevents a
    // just-selected Provider/model/Key realm from being masked at startup.
    await assertLiveCodexScope("before_isolated_runtime_start", {
      selectionMode: "latest-main"
    });
    const settings = {
      scenario,
      pairs,
      warmup_pairs: warmupPairs,
      pair_offset: pairOffset,
      first_arm: firstArm,
      turns,
      max_output_tokens: maxOutputTokens,
      response_timeout_ms: responseTimeoutMs,
      reasoning_effort: reasoningEffort,
      stable_instruction_chars: stableInstructionChars,
      seed_context_chars: seedContextChars,
      minimum_seed_input_tokens: minimumSeedInputTokens,
      minimum_peak_input_tokens: minimumPeakInputTokens,
      maximum_peak_input_tokens: maximumPeakInputTokens,
      tool_chars: toolChars,
      tool_calls: toolCalls,
      tool_output_shape: toolOutputShape,
      tool_protocol: toolProtocol,
      include_tool_schema: includeToolSchema,
      dynamic_tail_profile: dynamicTailProfile,
      dynamic_tail_mode: dynamicTailMode,
      exercise_local_previous_response_id_rebind:
        exerciseLocalPreviousResponseIdRebind,
      exercise_local_previous_response_id_full_replay:
        exerciseLocalPreviousResponseIdFullReplay,
      local_previous_response_id_fixture_mode:
        exerciseLocalPreviousResponseIdFullReplay
          ? "unchanged_full_replay"
          : exerciseLocalPreviousResponseIdRebind
            ? "rebind"
            : "none",
      fixture_profile: fixtureProfile,
      fresh_fixture_per_pair: freshFixturePerPair,
      turn_delay_ms: turnDelayMs,
      inter_arm_delay_ms: interArmDelayMs,
      pair_delay_ms: pairDelayMs,
      seed_to_reuse_delay_ms: seedToReuseDelayMs,
      seed_to_reuse_delay_stage: seedToReuseHorizonEnabled
        ? "after_both_seed_sse_before_turn_1"
        : "not_requested",
      live_codex_scope_gate_strategy: seedToReuseHorizonEnabled
        ? "fresh-at-launch/latest-main-post-launch-with-config-and-inbound-cohort"
        : "fresh-at-every-checkpoint",
      require_candidate_guarded_requests: requireCandidateGuardedRequests,
      candidate_exact_medium_tool_tail_maturity_wait:
        requireCandidateExactMediumToolTailMaturityWait,
      candidate_exact_large_message_tail_lag:
        requireCandidateExactLargeMessageTailLag,
      candidate_late_shallow_provider_waterline_rollback_wait:
        requireCandidateLateShallowProviderWaterlineRollbackWait,
      client_prompt_cache_key: Boolean(options["prompt-cache-key-prefix"]),
      candidate_upstream_affinity: candidateUpstreamAffinity,
      candidate_cache_control_field: candidateCacheControlField,
      candidate_thread_stable_pck_bridge: candidateThreadStablePckBridge,
      candidate_thread_stable_pck_bridge_env:
        candidateThreadStablePckBridge
          ? "candidate-isolated-runtime-only"
          : "disabled",
      candidate_thread_stable_pck_bridge_wire_policy:
        "prompt_cache_key_only_dynamic_input_symmetric",
      // The bridge probe must exercise a real metadata identity transition:
      // keep the explicit thread stable while rotating session/conversation
      // after the seed.  Scope this fixture-only behavior to the bridge
      // treatment so every historical scenario remains byte-for-byte stable.
      fixture_identity_churn:
        candidateThreadStablePckBridge && scenario === "dynamic-tail-mix",
      candidate_prompt_cache_key_override: Boolean(candidatePromptCacheKeyOverride),
      candidate_cache_options_24h: candidateCacheOptions24h,
      candidate_options24h_sibling_settle: requireCandidateOptions24hSiblingSettle,
      candidate_http1: candidateHttp1,
      candidate_provider_waterline_recovery_wait:
        candidateProviderWaterlineRecoveryWait,
      max_ttft_regression_ms: maxTtftRegressionMs,
      max_input_token_delta: maxInputTokenDelta,
      require_ttft_no_regression: requireTtftNoRegression,
      promotion_latency_policy: requireTtftNoRegression
        ? "end-to-end-no-regression"
        : "local-no-regression-upstream-exempt",
      max_local_proxy_overhead_regression_ms: maxLocalProxyOverheadRegressionMs,
      max_full_bucket_regression_requests: maxFullBucketRegressionRequests,
      champion_upstream_user_agent: championUpstreamUserAgent,
      candidate_upstream_user_agent: candidateUpstreamUserAgent,
      diagnostic_user_agent_split: diagnosticUserAgentSplit,
      promotion_eligible:
        !diagnosticUserAgentSplit && !exerciseLocalPreviousResponseIdFullReplay,
      shared_turn_crossover_promotion_gate: sharedCacheCrossover
        ? "required-live-order-balanced-v1"
        : "not_applicable",
      forced_use_system_proxy: forceUseSystemProxy,
      isolate_upstream_cache: isolateUpstreamCache,
      native_placement_isolation_required: nativePlacementIsolationRequired,
      promotion_requires_shared_upstream_placement_crossover:
        promotionRequiresSharedUpstreamPlacementCrossover,
      shared_cache_crossover: sharedCacheCrossover,
      isolation_lane_strategy: sharedCacheCrossover
        ? "shared-turn-crossover-v1"
        : isolateUpstreamCache
          ? "pair-crossover-v1"
          : "shared-v1",
      reuse_runtime_per_arm: reuseRuntimePerArm,
      reuse_runtime_per_arm_requested: requestedReuseRuntimePerArm,
      runtime_reuse_disabled_for_isolation:
        requestedReuseRuntimePerArm && isolateUpstreamCache,
      persistent_runtime_start_order: reuseRuntimePerArm
        ? persistentRuntimeStartOrder
        : "not_applicable",
      key_id_pinned: Boolean(pinnedKeyId),
      hand_selected_model_bound: providerScope === "codex-agent" && Boolean(codexRoute?.model_id),
      codex_route_model_bound: Boolean(codexRoute?.model_id),
      live_selection_scope_guard: "fail_closed",
      live_codex_metrics_scope_guard: liveCodexMetricsUrl ? "fail_closed" : "not_configured",
      provider_scope: providerScope
    };
    const artifacts = {
      champion: await executableArtifact(championExe),
      candidate: await executableArtifact(candidateExe)
    };
    const providerBlock = tomlArrayBlocks(configText, "providers")
      .map((item) => item.body)
      .find((item) => extractTomlString(item, "id") === providerId);
    if (!providerBlock) {
      throw new FailClosedError(
        "provider_not_found_for_user_agent_parity",
        "the selected Provider is missing from the snapshotted config; User-Agent parity cannot be proven"
      );
    }
    const cacheCapabilityChannel = cacheCapabilityProbeChannel(providerBlock);
    if (candidateCacheControlField && !cacheCapabilityChannel) {
      throw new FailClosedError(
        "candidate_cache_control_channel_unsupported",
        "the selected Provider must use the responses or chat channel before an isolated native cache-control field can be certified"
      );
    }
    settings.candidate_cache_control_certificate_scope = candidateCacheControlField
      ? "same-isolated-runtime-before-scoring"
      : "not_applicable";
    settings.candidate_cache_control_certificate_channel = candidateCacheControlField
      ? cacheCapabilityChannel
      : null;
    const upstreamUserAgentParity = evaluateUpstreamUserAgentParity({
      championUpstreamUserAgent,
      candidateUpstreamUserAgent,
      sourceCustomUserAgent: extractTomlString(providerBlock, "custom_user_agent").trim(),
      championExecutableSha256: artifacts.champion.sha256,
      candidateExecutableSha256: artifacts.candidate.sha256,
      diagnosticUserAgentSplit
    });
    if (!upstreamUserAgentParity.ok) {
      throw new FailClosedError(
        upstreamUserAgentParity.code,
        upstreamUserAgentParity.message
      );
    }
    settings.upstream_user_agent_parity = upstreamUserAgentParity.mode;
    // Preserve every executed arm result for diagnostics, while keeping only
    // non-warm-up pairs in the aggregates below. This makes a cold lane
    // visible without giving it any path to a promotion verdict.
    const rawArmRuns = { champion: [], candidate: [] };
    const orderedPairs = [];
    const interleavedTurnOrders = [];
    const seedToReuseDelayEvidenceByPair = [];
    let abortedAfterPair = null;
    let afterPairLiveGateFailure = null;
    let persistentArmRuntimes = null;
    try {
      if (reuseRuntimePerArm) {
        persistentArmRuntimes = await startPersistentIsolatedArmRuntimes({
          championExe,
          candidateExe,
          sourceConfigDir: sourceSnapshot.configDir,
          configProviderId: providerId,
          modelId: model,
          cacheCapabilityChannel,
          requestedPort,
          championUpstreamUserAgent,
          candidateUpstreamUserAgent,
          pinnedKeyId,
          forceUseSystemProxy,
          candidateUpstreamAffinity,
          candidateCacheControlField,
          candidatePromptCacheKeyOverride,
          candidateCacheOptions24h,
          candidateHttp1,
          candidateProviderWaterlineRecoveryWait,
          candidateThreadStablePckBridge,
          startOrder: persistentRuntimeStartOrder,
          keepRunDir
        });
      }

      for (let pair = 0; pair < pairs; pair += 1) {
        await assertLiveCodexScope(
          "before_pair_" + pair,
          seedToReuseHorizonEnabled && pair > 0
            ? { requireFresh: false, selectionMode: "latest-main" }
            : seedToReuseHorizonEnabled
              ? { selectionMode: "latest-main" }
              : undefined
        );
        await assertLiveSelectionScopeUnchanged(
          sourceConfigDir,
          liveSelectionScopeFingerprint,
          `before_pair_${pair}`,
          providerScope,
          pinnedKeyId
        );
        const fixtureFamily = freshFixturePerPair
          ? `fixture-${sha256Parts([
            "release-champion-fixture-v3",
            runId,
            requestFamily,
            scenario,
            stableInstructionChars,
            seedContextChars,
            toolChars,
            toolOutputShape,
            ...(includeToolSchema ? ["include-tool-schema-v1"] : []),
            dynamicTailProfile,
            dynamicTailMode,
            fixtureProfile,
            pair
          ]).slice(0, 16)}`
          : null;
        const armSpecFor = (arm) => {
          const executable = arm === "champion" ? championExe : candidateExe;
          // Isolated cache lanes must not remain permanently assigned to one
          // executable. A session-scoped upstream placement can have a
          // different cache waterline, so alternate which arm owns lane A/B
          // for every pair while keeping the two arms separate within a pair.
          const isolationLane = isolateUpstreamCache
            ? isolationLaneForPair(pair, arm)
            : null;
          const lane = releaseCachePlacementLane({
            runId,
            keyRealmHash,
            requestFamily,
            pair,
            arm,
            isolationLane,
            isolateUpstreamCache,
            sharedCacheCrossover
          });
          return {
            arm,
            executable,
            sourceConfigDir: sourceSnapshot.configDir,
            configProviderId: providerId,
            modelId: model,
            cacheCapabilityChannel,
            cohort,
            settings,
            requestedPort,
            runId,
            pair,
            lane,
            isolationLane,
            fixtureFamily,
            promptCacheKeyPrefix: options["prompt-cache-key-prefix"],
            upstreamAffinityTestEnabled:
              arm === "candidate" && candidateUpstreamAffinity,
            cacheControlField:
              arm === "candidate" ? candidateCacheControlField : null,
            promptCacheKeyOverride:
              arm === "candidate" ? candidatePromptCacheKeyOverride : null,
            cacheOptions24hTestEnabled:
              arm === "candidate" && candidateCacheOptions24h,
            upstreamHttp1TestEnabled:
              arm === "candidate" && candidateHttp1,
            providerWaterlineRecoveryWaitTestEnabled:
              arm === "candidate" && candidateProviderWaterlineRecoveryWait,
            threadStablePromptCacheKeyBridgeTestEnabled:
              arm === "candidate" && candidateThreadStablePckBridge,
             upstreamUserAgent: arm === "champion"
               ? championUpstreamUserAgent
               : candidateUpstreamUserAgent,
             pinnedKeyId,
             forceUseSystemProxy,
             keepRunDir
          };
        };
        let pairResult;
        if (persistentArmRuntimes) {
          // Alternating whole arms is not enough when a selected upstream has
          // a short capacity window: the second arm otherwise receives a full
          // burst after the first. Interleave each matching turn and rotate the
          // first sender every turn, while retaining isolated local runtimes.
          const result = await runInterleavedDynamicPair({
            champion: {
              ...armSpecFor("champion"),
              runtime: persistentArmRuntimes.champion.runtime,
              capabilityCertificate: persistentArmRuntimes.champion.capabilityCertificate
            },
            candidate: {
              ...armSpecFor("candidate"),
              runtime: persistentArmRuntimes.candidate.runtime,
              capabilityCertificate: persistentArmRuntimes.candidate.capabilityCertificate
            },
            afterSeedToReuseDelay: seedToReuseHorizonEnabled
              ? async (delayPair, delayEvidence) => {
                await assertLiveSelectionScopeUnchanged(
                  sourceConfigDir,
                  liveSelectionScopeFingerprint,
                  `after_seed_to_reuse_delay_pair_${delayPair}`,
                  providerScope,
                  pinnedKeyId
                );
                await assertLiveCodexScope(
                  `after_seed_to_reuse_delay_pair_${delayPair}`,
                  { requireFresh: false, selectionMode: "latest-main" }
                );
                return {
                  ...delayEvidence,
                  post_delay_selection_scope_verified: true,
                  post_delay_live_scope_verified: true
                };
              }
              : null
          });
          orderedPairs.push(
            result.pair_order ?? interleavedTurnOrder(pair, 0, pairOffset, firstArm)
          );
          // The report is indexed by the actual pair id.  Warm-up pairs stay
          // visible here even though they are excluded from scoring, so the
          // later promotion gate can never accidentally associate a scored
          // pair with another pair's turn order.
          interleavedTurnOrders[pair] = result.turn_order;
          seedToReuseDelayEvidenceByPair[pair] = result.seed_to_reuse_delay ?? null;
          rawArmRuns.champion.push(result.champion);
          rawArmRuns.candidate.push(result.candidate);
          pairResult = result;
        } else {
          // The one-runtime-per-arm fallback retains the previous pair-level
          // alternation, because the two isolated processes do not coexist.
          const order = interleavedTurnOrder(pair, 0, pairOffset, firstArm);
          orderedPairs.push(order);
          const results = {};
          for (const arm of order) {
            await assertLiveCodexScope("before_" + arm + "_pair_" + pair);
            await assertLiveSelectionScopeUnchanged(
              sourceConfigDir,
              liveSelectionScopeFingerprint,
              `before_${arm}_pair_${pair}`,
              providerScope,
              pinnedKeyId
            );
            const armSpec = armSpecFor(arm);
            const result = await runIsolatedDynamicArm(armSpec);
            rawArmRuns[arm].push(result);
            results[arm] = result;
          }
          pairResult = results;
        }
        try {
          await assertLiveCodexScope(
            "after_pair_" + pair,
            seedToReuseHorizonEnabled
              ? { requireFresh: false, selectionMode: "latest-main" }
              : undefined
          );
        } catch (error) {
          // The pair itself is already complete and is useful for diagnosis,
          // but the live Codex scope is no longer valid for a release verdict.
          // Retain it in a diagnostic-only report and stop before any more
          // isolated traffic is sent.
          if (!(error instanceof LiveCodexMetricsGateError)) throw error;
          abortedAfterPair = pair;
          afterPairLiveGateFailure = {
            code: error.code,
            evidence: error.liveGateEvidence
          };
          break;
        }
        await assertLiveSelectionScopeUnchanged(
          sourceConfigDir,
          liveSelectionScopeFingerprint,
          `after_pair_${pair}`,
          providerScope,
          pinnedKeyId
        );
        // A release comparison is already invalid after either arm fails. Do
        // not convert a single third-party rejection into a burst of fresh
        // test traffic; future verification starts from a fresh, explicit run.
        if (comparisonPairInvalid(pairResult)) {
          abortedAfterPair = pair;
          break;
        }
        if (pair + 1 < pairs && pairDelayMs > 0) {
          await delay(pairDelayMs);
        }
      }
    } finally {
      if (persistentArmRuntimes) {
        await Promise.all(Object.entries(persistentArmRuntimes).map(([arm, workspace]) =>
          disposeIsolatedRuntimeWorkspace(workspace, `${arm} persistent isolated runtime`, keepRunDir)
        ));
      }
    }

    if (afterPairLiveGateFailure) {
      return buildAfterPairLiveGateFailureReport({
        mode: reuseRuntimePerArm ? "live-isolated-reused-runtime" : "live-isolated",
        runId,
        cohort,
        settings,
        pairOrder: orderedPairs,
        turnOrder: reuseRuntimePerArm ? interleavedTurnOrders : null,
        abortedAfterPair,
        warmupPairs,
        rawArmRuns,
        artifacts,
        liveGateFailure: afterPairLiveGateFailure
      });
    }

    const championRunPartitions = partitionRunsByWarmup(rawArmRuns.champion, warmupPairs);
    const candidateRunPartitions = partitionRunsByWarmup(rawArmRuns.candidate, warmupPairs);
    const warmupRawRuns = {
      champion: championRunPartitions.warmup,
      candidate: candidateRunPartitions.warmup
    };
    const scoredArmRuns = {
      champion: championRunPartitions.scored,
      candidate: candidateRunPartitions.scored
    };
    const scoredPairIds = pairedRunIds(scoredArmRuns.champion, scoredArmRuns.candidate);
    const champion = aggregateArm(
      "champion",
      cohort,
      artifacts.champion,
      scoredArmRuns.champion,
      0,
      minimumPeakInputTokens,
      maximumPeakInputTokens
    );
    const candidate = aggregateArm(
      "candidate",
      cohort,
      artifacts.candidate,
      scoredArmRuns.candidate,
      requireCandidateGuardedRequests,
      minimumPeakInputTokens,
      maximumPeakInputTokens,
      requireCandidateExactMediumToolTailMaturityWait,
      requireCandidateExactLargeMessageTailLag,
      requireCandidateLateShallowProviderWaterlineRollbackWait
    );
    const comparison = compareArmResults(
      champion,
      candidate,
      maxTtftRegressionMs,
      maxLocalProxyOverheadRegressionMs,
      maxFullBucketRegressionRequests,
      requireTtftNoRegression,
      maxInputTokenDelta,
      nativePlacementIsolationRequired,
      {
        require_shared_upstream_placement_crossover:
          promotionRequiresSharedUpstreamPlacementCrossover,
        shared_upstream_placement_crossover_observed: sharedCacheCrossover,
        candidate_cache_control_field: candidateCacheControlField,
        candidate_cache_options_24h: candidateCacheOptions24h,
        candidate_thread_stable_pck_bridge: candidateThreadStablePckBridge,
        fixture_identity_churn:
          candidateThreadStablePckBridge && scenario === "dynamic-tail-mix",
        require_candidate_options24h_sibling_settle:
          requireCandidateOptions24hSiblingSettle,
        require_candidate_provider_waterline_recovery_wait:
          candidateProviderWaterlineRecoveryWait,
        require_candidate_exact_medium_tool_tail_maturity_wait:
          requireCandidateExactMediumToolTailMaturityWait,
        require_candidate_exact_large_message_tail_lag:
          requireCandidateExactLargeMessageTailLag,
        require_candidate_late_shallow_provider_waterline_rollback_wait:
          requireCandidateLateShallowProviderWaterlineRollbackWait,
        exercise_local_previous_response_id_rebind:
          exerciseLocalPreviousResponseIdRebind,
        expected_local_previous_response_id_rebind_requests:
          exerciseLocalPreviousResponseIdRebind
            ? scoredPairIds.length * localPreviousResponseIdRebindTargetRequestCount(turns)
            : 0,
        exercise_local_previous_response_id_full_replay:
          exerciseLocalPreviousResponseIdFullReplay,
        expected_local_previous_response_id_full_replay_requests:
          exerciseLocalPreviousResponseIdFullReplay
            ? scoredPairIds.length * localPreviousResponseIdFullReplayTargetRequestCount(turns)
            : 0,
        tool_protocol: toolProtocol,
        // This gate is deliberately scoped to live shared-turn crossover.
        // Offline artifacts and isolated-lane diagnostics retain their
        // historical comparison semantics.
        live_shared_turn_crossover:
          sharedCacheCrossover === true && reuseRuntimePerArm === true,
        shared_turn_orders: interleavedTurnOrders,
        scored_pair_ids: scoredPairIds,
        scenario,
        shared_turn_crossover: {
          required: promotionRequiresSharedUpstreamPlacementCrossover,
          observed: sharedCacheCrossover,
          scenario,
          turns,
          candidate_late_shallow_provider_waterline_rollback_wait:
            requireCandidateLateShallowProviderWaterlineRollbackWait,
          scored_pair_ids: scoredPairIds,
          turn_orders: interleavedTurnOrders,
          champion_runs: scoredArmRuns.champion,
          candidate_runs: scoredArmRuns.candidate
        }
      }
    );
    if (diagnosticUserAgentSplit) {
      comparison.diagnostic_only = true;
      comparison.promotion_eligible = false;
      comparison.diagnostic_evidence_complete =
        comparison.checks.champion_valid === true &&
        comparison.checks.candidate_valid === true &&
        comparison.checks.cohort_matches === true &&
        comparison.checks.observed_key_realm_matches === true &&
        comparison.checks.actual_outbound_input_symmetry === true &&
        comparison.checks.native_placement_isolation === true &&
        comparison.checks.cold_seed_symmetry === true &&
        comparison.checks.candidate_no_extra_cold_start === true;
      comparison.non_promotion_reason =
        "upstream User-Agent intentionally differs between same-binary arms";
      comparison.promotion_ineligibility_reasons = [{
        code: "diagnostic_user_agent_split",
        message: comparison.non_promotion_reason
      }];
      // `pass` is reserved for a promotable release comparison. Keep the
      // measured deltas and diagnostic evidence, but fail closed on promotion.
      comparison.pass = false;
    }
    if (exerciseLocalPreviousResponseIdFullReplay) {
      comparison.diagnostic_only = true;
      comparison.promotion_eligible = false;
      comparison.diagnostic_evidence_complete =
        comparison.checks.champion_valid === true &&
        comparison.checks.candidate_valid === true &&
        comparison.checks.cohort_matches === true &&
        comparison.checks.observed_key_realm_matches === true &&
        comparison.checks.actual_outbound_input_symmetry === true &&
        comparison.checks.local_previous_response_id_full_replay === true &&
        comparison.checks.cold_seed_symmetry === true &&
        comparison.checks.candidate_no_extra_cold_start === true;
      comparison.non_promotion_reason =
        "unchanged local previous_response_id FullReplay is a maturity-wait correctness fixture, not a candidate treatment";
      comparison.promotion_ineligibility_reasons = [
        ...(comparison.promotion_ineligibility_reasons ?? []),
        {
          code: "local_previous_response_id_full_replay_diagnostic",
          message: comparison.non_promotion_reason
        }
      ];
      // `pass` is reserved for a promotable release comparison. The fixture's
      // correctness result remains available as diagnostic_evidence_complete.
      comparison.pass = false;
    }
    settings.promotion_eligible = comparison.promotion_eligible;
    settings.promotion_ineligibility_reasons = comparison.promotion_ineligibility_reasons;
    return {
      schema: SCHEMA,
      kind: "release-champion-comparison",
      mode: diagnosticUserAgentSplit
        ? "live-isolated-user-agent-split-diagnostic"
        : exerciseLocalPreviousResponseIdFullReplay
          ? "live-isolated-local-prid-full-replay-diagnostic"
        : reuseRuntimePerArm ? "live-isolated-reused-runtime" : "live-isolated",
      pass: comparison.pass,
      promotion_eligible: comparison.promotion_eligible,
      promotion_ineligibility_reasons: comparison.promotion_ineligibility_reasons,
      diagnostic_only: diagnosticUserAgentSplit || exerciseLocalPreviousResponseIdFullReplay,
      run_id: runId,
      cohort,
      settings,
      pair_order: orderedPairs,
      turn_order: reuseRuntimePerArm ? interleavedTurnOrders : null,
      seed_to_reuse_delay_evidence: reuseRuntimePerArm
        ? seedToReuseDelayEvidenceByPair
        : null,
      aborted_after_pair: abortedAfterPair,
      warmup_pair_ids: scheduledPairIds(warmupPairs),
      scored_pair_ids: scoredPairIds,
      warmup_raw_runs: warmupRawRuns,
      champion,
      candidate,
      comparison
    };
  } finally {
    await removeTemporaryDirectory(sourceSnapshot.root);
  }
}

async function runIsolatedDynamicArm(spec) {
  let workspace = null;
  try {
    workspace = await startIsolatedRuntimeWorkspace(spec);
    return await runDynamicArmOnRuntime({
      ...spec,
      runtime: workspace.runtime,
      capabilityCertificate: workspace.capabilityCertificate
    });
  } finally {
    if (workspace) {
      await disposeIsolatedRuntimeWorkspace(
        workspace,
        `${spec.arm} isolated runtime`,
        spec.keepRunDir
      );
    }
  }
}

function diagnosticRunChecks() {
  return [
  "no_runtime_failure",
  "every_sse_completed",
  "every_inbound_one_attempt_one_main_post",
  "cohort_bound_on_every_request",
  "complete_usage_coverage",
  "complete_cache_read_token_evidence",
  "complete_timing_coverage",
  "input_usage_present",
  "required_seed_input_tokens",
  "required_peak_input_tokens",
  "maximum_peak_input_tokens",
  "dynamic_tail_terminal_followup_peak_in_range",
  "cacheable_128_evidence_present",
  "warm_stable_prefix_evidence_present",
  "static_wire_continuity",
  "small_context_cold_read_no_foreground_wait",
  "one_observed_key_realm",
  "avoidable_gap_zero",
  "required_guarded_requests",
  "candidate_exact_medium_tool_tail_maturity_wait_observed",
  "candidate_exact_large_message_tail_lag_observed",
  "candidate_late_shallow_provider_waterline_rollback_wait_observed",
  "candidate_upstream_affinity_injected",
  "candidate_cache_control_field_injected",
  "candidate_cache_options_24h_injected",
  "candidate_options24h_sibling_settle_observed",
  "candidate_http1_observed",
  "compaction_observed",
    "dynamic_tail_followup_coverage"
  ];
}

function diagnosticAggregateChecks() {
  return [
  "every_run_passed",
  "cohort_consistent",
  "one_observed_key_realm",
  "every_sse_completed",
  "every_inbound_one_attempt_one_main_post",
  "avoidable_gap_zero",
  "complete_timing_coverage",
  "input_usage_present",
  "required_peak_input_tokens",
  "maximum_peak_input_tokens",
  "cacheable_128_evidence_present",
  "warm_stable_prefix_evidence_present",
  "static_wire_continuity",
  "full_bucket_denominator_present",
  "required_guarded_requests",
  "candidate_exact_medium_tool_tail_maturity_wait_observed",
  "candidate_exact_large_message_tail_lag_observed",
  "candidate_late_shallow_provider_waterline_rollback_wait_observed",
  "candidate_upstream_affinity_injected",
  "candidate_cache_control_field_injected",
  "candidate_cache_options_24h_injected",
  "candidate_options24h_sibling_settle_observed",
  "candidate_http1_observed",
  "dynamic_tail_followup_coverage",
    "dynamic_tail_terminal_followup_peak_in_range"
  ];
}

function diagnosticRequestChecks() {
  return [
  "terminal_response_completed",
  "terminal_usage_shape_present",
  "exact_counter_delta",
  "per_inbound_one_attempt_one_post",
  "aggregate_no_multi_attempt",
  "metric_present",
  "provider_matches_cohort",
  "model_matches_cohort",
  "observed_key_realm_present",
  "observed_key_realm_matches_cohort",
  "usage_present",
  "cache_read_tokens_present",
    "timing_present"
  ];
}

function diagnosticMetricFields() {
  return [
  "requests",
  "successful_sse_requests",
  "input_tokens",
  "warm_input_tokens",
  "seed_input_tokens",
  "seed_cache_read_tokens",
  "seed_request_count",
  "cold_seed_request_count",
  "peak_input_tokens",
  "dynamic_tail_terminal_followup_input_tokens",
  "cache_read_tokens",
  "raw_token_hit_rate",
  "warm_cache_read_tokens",
  "warm_raw_token_hit_rate",
  "cacheable_tokens_128",
  "cacheable_read_tokens_128",
  "cache_128_hit_rate",
  "warm_cacheable_tokens_128",
  "warm_cacheable_read_tokens_128",
  "warm_cache_128_hit_rate",
  "warm_stable_prefix_tokens_128",
  "warm_stable_prefix_cached_tokens_128",
  "warm_stable_prefix_hit_rate",
  "full_bucket_requests",
  "full_bucket_rate",
  "warm_full_bucket_requests",
  "warm_full_bucket_rate",
  "warm_full_bucket_denominator",
  "cacheable_request_count",
  "full_bucket_denominator",
  "avoidable_gap_tokens",
  "new_tail_gap_tokens",
  "provider_unstable_gap_tokens",
  "shortfall_tokens",
  "guarded_requests",
  "upstream_affinity_test_requests",
  "upstream_affinity_learned_requests",
  "upstream_affinity_injected_requests",
  "candidate_cache_control_field_requests",
  "candidate_cache_options_24h_requests",
  "candidate_options24h_sibling_settle_requests",
  "candidate_http1_requests",
  "candidate_exact_large_message_tail_lag_requests",
  "candidate_late_shallow_provider_waterline_rollback_wait_requests",
  "exact_medium_tool_tail_predecessor_requests",
  "exact_medium_tool_tail_direct_successor_requests",
  "exact_medium_tool_tail_maturity_wait_requests",
  "non_target_prefix_guard_wait_requests",
  "timing_complete_requests",
  "local_pre_upstream_overhead_p95_ms",
  "local_proxy_overhead_p95_ms",
  "upstream_ttft_p95_ms",
  "ttft_p95_ms",
  "compaction_request_count",
  "compaction_local_proxy_overhead_p95_ms",
  "compaction_upstream_ttft_p95_ms",
  "compaction_ttft_p95_ms",
    "usage_coverage"
  ];
}

function safeDiagnosticHash(value) {
  const normalized = String(value ?? "").trim().toLowerCase();
  return /^[a-f0-9]{16,128}$/u.test(normalized) ? normalized : null;
}

// A prompt-cache key is opaque placement material and must never be retained
// in a release artifact. Its truncated one-way fingerprint is sufficient to
// prove whether two live arms actually used separate native placement keys.
function opaquePlacementFingerprint(value) {
  const raw = typeof value === "string" ? value.trim() : "";
  return raw ? sha256Text(raw).slice(0, 32) : null;
}

function safeDiagnosticPair(value) {
  return Number.isInteger(value) && value >= 0 && value <= 8 ? value : null;
}

function projectDiagnosticChecks(source, fields) {
  const projected = {};
  for (const field of fields) {
    if (typeof source?.[field] === "boolean") projected[field] = source[field];
  }
  return projected;
}

function projectDiagnosticMetrics(metrics) {
  const projected = {};
  for (const field of diagnosticMetricFields()) {
    const value = finiteNonNegativeNumber(metrics?.[field]);
    if (value !== null) projected[field] = value;
  }
  const dynamicTail = metrics?.dynamic_tail_mix;
  if (dynamicTail && typeof dynamicTail === "object") {
    const dynamicTailProjection = {};
    for (const field of [
      "injections",
      "injected_characters",
      "followups_observed",
      "followup_new_tail_tokens",
      "followup_provider_unstable_tokens",
      "followup_tail_lag_count"
    ]) {
      const value = finiteNonNegativeNumber(dynamicTail[field]);
      if (value !== null) dynamicTailProjection[field] = value;
    }
    projected.dynamic_tail_mix = dynamicTailProjection;
  }
  return projected;
}

function projectDiagnosticFingerprints(value) {
  if (!value || typeof value !== "object" || Array.isArray(value)) return null;
  const projected = {};
  for (const field of ["input_full", ...STATIC_WIRE_FIELDS]) {
    const hash = safeDiagnosticHash(value[field]);
    if (hash) projected[field] = hash;
  }
  return Object.keys(projected).length > 0 ? projected : null;
}

function projectDiagnosticCounters(counters) {
  const projected = {};
  for (const field of ["inbound_requests", "generation_attempts", "upstream_requests"]) {
    const value = finiteNonNegativeNumber(counters?.[field]);
    if (value !== null) projected[field] = value;
  }
  return projected;
}

function projectDiagnosticTransport(transport) {
  const projected = {};
  const numericFields = {
    request_bytes: "request_body_bytes",
    sent_bytes: "sent_body_bytes",
    request_encode_ms: "request_body_encode_ms",
    gzip_encode_ms: "gzip_encode_ms",
    upstream_headers_ms: "upstream_attempt_headers_ms",
    upstream_wait_ms: "stream_upstream_wait_ms",
    client_backpressure_ms: "stream_client_backpressure_ms",
    sse_chunks: "sse_chunks"
  };
  for (const [target, source] of Object.entries(numericFields)) {
    const value = finiteNonNegativeNumber(transport?.[source]);
    if (value !== null) projected[target] = value;
  }
  for (const field of ["gzip_attempted", "gzip_fallback_used", "downstream_disconnected"]) {
    if (typeof transport?.[field] === "boolean") projected[field] = transport[field];
  }
  return projected;
}

function projectDiagnosticRequest(request) {
  const metrics = {};
  for (const field of [
    "input_tokens",
    "cache_read_tokens",
    "cache_avoidable_gap_tokens",
    "cache_new_tail_gap_tokens",
    "cache_provider_unstable_gap_tokens",
    "cache_shortfall_tokens",
    "tail_input_items",
    "tail_message_chars",
    "tail_tool_call_chars",
    "tail_tool_output_chars",
    "tail_largest_tool_output_chars",
    "tail_tool_output_lines",
    "tail_tool_output_repeated_line_chars",
    "tail_tool_output_timestamp_like_count",
    "tail_tool_output_hash_like_count",
    "tail_tool_output_json_like_chars",
    "prefix_lag_input_delta_tokens",
    "prefix_lag_cache_delta_tokens",
    "prefix_lag_previous_gap_tokens",
    "prefix_cache_instability_score",
    "prefix_state_cache_read_tokens"
  ]) {
    const value = finiteNonNegativeNumber(request?.[field]);
    if (value !== null) metrics[field] = value;
  }
  const timing = {};
  for (const field of [
    "prefix_guard_wait_ms",
    "local_prepare_ms",
    "upstream_headers_ms",
    "upstream_first_chunk_ms",
    "upstream_ttft_ms",
    "ttft_ms"
  ]) {
    const value = finiteNonNegativeNumber(request?.[field]);
    if (value !== null) timing[field] = value;
  }
  const projected = {
    arm: request?.arm === "champion" || request?.arm === "candidate" ? request.arm : undefined,
    pair: safeDiagnosticPair(request?.pair),
    phase: safeLiveCodexLabel(request?.phase),
    request_kind: safeLiveCodexLabel(request?.request_kind),
    pass: request?.pass === true,
    http_status: (() => {
      const status = Number(request?.http_status);
      return Number.isInteger(status) && status >= 100 && status <= 599 ? status : null;
    })(),
    response_failure_code: safeLiveCodexLabel(request?.response_failure_code),
    response_failure_kind: safeLiveCodexLabel(request?.response_failure_kind),
    elapsed_ms: finiteNonNegativeNumber(request?.elapsed_ms),
    sse_completed: request?.sse_completed === true,
    terminal_usage_shape: safeLiveCodexLabel(request?.terminal_usage_shape),
    input_fingerprint: safeDiagnosticHash(request?.input_fingerprint),
    outbound_prefix_fingerprints: projectDiagnosticFingerprints(request?.outbound_prefix_fingerprints),
    provider_prefix_fingerprint: safeDiagnosticHash(request?.provider_prefix_fingerprint),
    provider_prefix_key_present: request?.provider_prefix_key_present === true,
    provider_prefix_key_fingerprint: safeDiagnosticHash(request?.provider_prefix_key_fingerprint),
    cache_read_tokens_observed: request?.cache_read_tokens_observed === true,
    local_previous_response_id_sent: request?.local_previous_response_id_sent === true,
    local_response_id_present: request?.local_response_id_present === true,
    local_rebind_target: request?.local_rebind_target === true,
    local_full_replay_target: request?.local_full_replay_target === true,
    local_previous_response_id_fixture_mode: safeLiveCodexLabel(
      request?.local_previous_response_id_fixture_mode
    ),
    fixture_identity_phase: safeLiveCodexLabel(request?.fixture_identity_phase),
    fixture_identity_thread_stable: request?.fixture_identity_thread_stable === true,
    regenerated_tool_pair_count: finiteNonNegativeNumber(request?.regenerated_tool_pair_count),
    fixture_tool_protocol: safeLiveCodexLabel(request?.fixture_tool_protocol),
    // These are bounded, allow-listed diagnostic labels emitted by Atoapi.
    // Keep them when a live-scope gate fail-closes after an isolated pair: the
    // wait duration alone cannot prove which candidate maturity branch ran.
    // They never include request content, routes, keys, or upstream messages.
    prefix_guard_wait_reason: safeLiveCodexLabel(request?.prefix_guard_wait_reason),
    prefix_guard_wait_source: safeLiveCodexLabel(request?.prefix_guard_wait_source),
    prefix_guard_skip_reason: safeLiveCodexLabel(request?.prefix_guard_skip_reason),
    metrics,
    timing,
    transport: projectDiagnosticTransport(request?.transport),
    counters: projectDiagnosticCounters(request?.counters),
    runtime_error_scopes: array(request?.runtime_error_scopes)
      .map(safeLiveCodexLabel)
      .filter(Boolean)
      .slice(0, 4),
    runtime_error_classes: array(request?.runtime_error_classes)
      .map(safeLiveCodexLabel)
      .filter(Boolean)
      .slice(0, 4),
    checks: projectDiagnosticChecks(request?.checks, diagnosticRequestChecks())
  };
  return Object.fromEntries(Object.entries(projected).filter(([, value]) => value !== undefined));
}

function projectDiagnosticArmRun(run) {
  return {
    arm: run?.arm === "champion" || run?.arm === "candidate" ? run.arm : null,
    pair: safeDiagnosticPair(run?.pair),
    scenario: safeLiveCodexLabel(run?.scenario),
    pass: run?.pass === true,
    prompt_cache_key_used: run?.prompt_cache_key_used === true,
    metrics: projectDiagnosticMetrics(run?.metrics),
    checks: projectDiagnosticChecks(run?.checks, diagnosticRunChecks()),
    requests: array(run?.requests).map(projectDiagnosticRequest)
  };
}

function projectDiagnosticArmAggregate(aggregate) {
  return {
    arm: aggregate?.arm === "champion" || aggregate?.arm === "candidate" ? aggregate.arm : null,
    completed_run_count: array(aggregate?.runs).length,
    pass: aggregate?.pass === true,
    metrics: projectDiagnosticMetrics(aggregate?.metrics),
    checks: projectDiagnosticChecks(aggregate?.checks, diagnosticAggregateChecks())
  };
}

function buildAfterPairLiveGateFailureReport({
  mode,
  cohort,
  settings,
  pairOrder,
  turnOrder,
  abortedAfterPair,
  warmupPairs,
  rawArmRuns,
  artifacts,
  liveGateFailure
}) {
  const championRuns = array(rawArmRuns?.champion);
  const candidateRuns = array(rawArmRuns?.candidate);
  const diagnosticChampion = aggregateArm(
    "champion",
    cohort,
    artifacts.champion,
    championRuns,
    0,
    number(settings?.minimum_peak_input_tokens),
    number(settings?.maximum_peak_input_tokens)
  );
  const diagnosticCandidate = aggregateArm(
    "candidate",
    cohort,
    artifacts.candidate,
    candidateRuns,
    0,
    number(settings?.minimum_peak_input_tokens),
    number(settings?.maximum_peak_input_tokens)
  );
  const checkpoint = `after_pair_${Number.isInteger(abortedAfterPair) ? abortedAfterPair : "unknown"}`;
  const code = safeLiveCodexLabel(liveGateFailure?.code) ?? "live_codex_metrics_gate_failed";
  return {
    schema: SCHEMA,
    kind: "release-champion-diagnostic",
    mode: safeLiveCodexLabel(mode),
    pass: false,
    fail_closed: true,
    diagnostic_only: true,
    promotion_eligible: false,
    pair_order: array(pairOrder).map((order) => array(order)
      .filter((arm) => arm === "champion" || arm === "candidate")),
    turn_order: Array.isArray(turnOrder)
      ? turnOrder.map((order) => array(order).map((turn) => array(turn)
        .filter((arm) => arm === "champion" || arm === "candidate")))
      : null,
    aborted_after_pair: safeDiagnosticPair(abortedAfterPair),
    warmup_pair_ids: scheduledPairIds(warmupPairs),
    scored_pair_ids: [],
    completed_isolated_pair_ids: pairedRunIds(championRuns, candidateRuns),
    completed_isolated_arm_runs: {
      champion: championRuns.map(projectDiagnosticArmRun),
      candidate: candidateRuns.map(projectDiagnosticArmRun)
    },
    diagnostic_arm_aggregates: {
      champion: projectDiagnosticArmAggregate(diagnosticChampion),
      candidate: projectDiagnosticArmAggregate(diagnosticCandidate)
    },
    live_codex_gate: {
      state: "failed",
      checkpoint,
      code,
      evidence: liveGateFailure?.evidence ?? {
        schema: "atoapi-live-codex-gate-evidence-v1",
        checkpoint,
        record_available: false
      }
    },
    error: {
      code,
      message: "the after-pair live Codex scope gate failed; completed isolated arm evidence is diagnostic-only",
      missing_parameters: []
    }
  };
}

async function runDynamicArmOnRuntime(spec) {
  try {
    return await exerciseScenario(await prepareDynamicArmSpec(spec));
  } catch (error) {
    return failedDynamicRun({
      arm: spec.arm,
      pair: spec.pair,
      cohort: spec.cohort,
      executable: await executableArtifact(spec.executable),
      reason: safeErrorMessage(error)
    });
  }
}

async function prepareDynamicArmSpec(spec) {
  const executable = await executableArtifact(spec.executable);
  const promptCacheKey = spec.promptCacheKeyPrefix
    ? generatedPromptCacheKey(spec.promptCacheKeyPrefix, spec.lane)
    : null;
  return {
      runtime: spec.runtime,
      arm: spec.arm,
      pair: spec.pair,
      runId: spec.runId,
      lane: spec.lane,
      isolationLane: spec.isolationLane,
      fixtureFamily: spec.fixtureFamily,
      cohort: spec.cohort,
      settings: spec.settings,
      expectedProviderId: spec.configProviderId,
      promptCacheKey,
      capabilityCertificate: spec.capabilityCertificate ?? null,
      executable
  };
}

async function startPersistentIsolatedArmRuntimes(spec) {
  const workspaces = {};
  try {
    for (const arm of persistentRuntimeStartOrder(spec.startOrder)) {
      workspaces[arm] = await startIsolatedRuntimeWorkspace({
        arm,
        executable: arm === "champion" ? spec.championExe : spec.candidateExe,
        sourceConfigDir: spec.sourceConfigDir,
        configProviderId: spec.configProviderId,
        modelId: spec.modelId,
        cacheCapabilityChannel: spec.cacheCapabilityChannel,
        upstreamUserAgent: arm === "champion"
          ? spec.championUpstreamUserAgent
          : spec.candidateUpstreamUserAgent,
        pinnedKeyId: spec.pinnedKeyId,
        forceUseSystemProxy: spec.forceUseSystemProxy,
        upstreamAffinityTestEnabled:
          arm === "candidate" && spec.candidateUpstreamAffinity,
        cacheControlField:
          arm === "candidate" ? spec.candidateCacheControlField : null,
        promptCacheKeyOverride:
          arm === "candidate" ? spec.candidatePromptCacheKeyOverride : null,
        cacheOptions24hTestEnabled:
          arm === "candidate" && spec.candidateCacheOptions24h,
        upstreamHttp1TestEnabled:
          arm === "candidate" && spec.candidateHttp1,
        providerWaterlineRecoveryWaitTestEnabled:
          arm === "candidate" && spec.candidateProviderWaterlineRecoveryWait,
        threadStablePromptCacheKeyBridgeTestEnabled:
          arm === "candidate" && spec.candidateThreadStablePckBridge,
        requestedPort: spec.requestedPort,
        keepRunDir: spec.keepRunDir
      });
    }
    return workspaces;
  } catch (error) {
    await Promise.all(Object.entries(workspaces).map(([arm, workspace]) =>
      disposeIsolatedRuntimeWorkspace(workspace, `${arm} persistent isolated startup`, spec.keepRunDir)
    ));
    throw error;
  }
}

function normalizePersistentRuntimeStartOrder(value) {
  const normalized = String(value ?? "").trim().toLowerCase();
  if (normalized === "champion" || normalized === "candidate") return normalized;
  throw new FailClosedError(
    "invalid_persistent_runtime_start_order",
    "--persistent-runtime-start-order must be champion or candidate"
  );
}

function persistentRuntimeStartOrder(firstArm) {
  return normalizePersistentRuntimeStartOrder(firstArm) === "champion"
    ? ["champion", "candidate"]
    : ["candidate", "champion"];
}

async function startIsolatedRuntimeWorkspace(spec) {
  const tempRoot = await mkdtemp(join(tmpdir(), `atoapi-release-champion-${safeSegment(spec.arm)}-`));
  const configDir = join(tempRoot, "config");
  let runtime = null;
  try {
    await copyIsolatedConfig(spec.sourceConfigDir, configDir, {
      providerId: spec.configProviderId,
      upstreamUserAgent: spec.upstreamUserAgent,
      pinnedKeyId: spec.pinnedKeyId,
      forceUseSystemProxy: spec.forceUseSystemProxy
    });
    runtime = await startIsolatedRuntime({
      executable: spec.executable,
      configDir,
      requestedPort: spec.requestedPort,
      upstreamAffinityTestEnabled: spec.upstreamAffinityTestEnabled === true,
      cacheControlField: spec.cacheControlField ?? null,
      promptCacheKeyOverride: spec.promptCacheKeyOverride ?? null,
      cacheOptions24hTestEnabled: spec.cacheOptions24hTestEnabled === true,
      upstreamHttp1TestEnabled: spec.upstreamHttp1TestEnabled === true,
      providerWaterlineRecoveryWaitTestEnabled:
        spec.providerWaterlineRecoveryWaitTestEnabled === true,
      threadStablePromptCacheKeyBridgeTestEnabled:
        spec.threadStablePromptCacheKeyBridgeTestEnabled === true
    });
    // The admin probe writes the exact provider/model/channel/selected-Key
    // certificate into this disposable runtime's in-memory config before any
    // scored request is sent. It is intentionally not copied into the source
    // snapshot or the live desktop config.
    const capabilityCertificate = spec.cacheControlField
      ? await certifyIsolatedCacheControlField({
        runtime,
        providerId: spec.configProviderId,
        modelId: spec.modelId,
        channel: spec.cacheCapabilityChannel,
        expectedKeyId: spec.pinnedKeyId,
        field: spec.cacheControlField
      })
      : null;
    return { tempRoot, runtime, capabilityCertificate };
  } catch (error) {
    if (runtime) await stopChild(runtime.child, `${spec.arm} isolated startup`);
    await removeTemporaryDirectory(tempRoot);
    throw error;
  }
}

// A forced isolated candidate remains subject to the same exact capability
// gate as normal traffic. Certify the requested field inside the disposable,
// already key-pinned candidate process instead of copying a certificate from a
// different temporary process or weakening the Rust gate.
async function certifyIsolatedCacheControlField({
  runtime,
  providerId,
  modelId,
  channel,
  expectedKeyId = null,
  field
}) {
  const response = await fetch(`${runtime.baseUrl}/admin/cache-capabilities/probe`, {
    method: "POST",
    headers: {
      authorization: `Bearer ${runtime.localKey}`,
      "content-type": "application/json"
    },
    // Candidate certification needs only the exact field it will place on the
    // frozen wire. Avoid serially probing unrelated controls before scored
    // traffic; slow upstreams otherwise turn a one-field preflight into an
    // avoidable five-request timeout.
    body: JSON.stringify({ provider_id: providerId, model_id: modelId, channel, fields: [field] }),
    signal: AbortSignal.timeout(180_000)
  });
  const payload = await response.json().catch(() => null);
  return validateIsolatedCacheControlCertificatePayload(payload, {
    httpStatus: response.status,
    providerId,
    modelId,
    channel,
    expectedKeyId,
    field
  });
}

function validateIsolatedCacheControlCertificatePayload(payload, expected) {
  const providerId = String(expected?.providerId ?? "").trim();
  const modelId = String(expected?.modelId ?? "").trim();
  const channel = String(expected?.channel ?? "").trim();
  const field = String(expected?.field ?? "").trim();
  const expectedKeyId = expected?.expectedKeyId == null
    ? null
    : String(expected.expectedKeyId).trim();
  const httpStatus = Number(expected?.httpStatus);
  const failed = (code, message) => {
    throw new FailClosedError(code, message);
  };
  if (!Number.isInteger(httpStatus) || httpStatus < 200 || httpStatus >= 300 ||
    !payload || typeof payload !== "object" || Array.isArray(payload)) {
    const statusLabel = Number.isInteger(httpStatus)
      ? `HTTP ${httpStatus}`
      : "an invalid HTTP status";
    const diagnosticCategory = isolatedCacheControlProbeFailureCategory(payload);
    failed(
      "candidate_cache_control_capability_probe_failed",
      "the disposable candidate could not obtain an exact cache-control capability " +
      `certificate before scored traffic (${statusLabel}; ${diagnosticCategory})`
    );
  }
  if (String(payload.provider_id ?? "").trim() !== providerId ||
    String(payload.model_id ?? "").trim() !== modelId ||
    String(payload.channel ?? "").trim() !== channel) {
    failed(
      "candidate_cache_control_capability_scope_mismatch",
      "the disposable candidate capability certificate did not match the selected Provider, model, and channel"
    );
  }
  const selectedKeyId = payload.key_id == null ? null : String(payload.key_id).trim();
  if (expectedKeyId && selectedKeyId !== expectedKeyId) {
    failed(
      "candidate_cache_control_capability_key_mismatch",
      "the disposable candidate capability certificate did not use the currently pinned Key"
    );
  }
  const matchedField = Array.isArray(payload.fields)
    ? payload.fields.find((item) => String(item?.field ?? "").trim() === field)
    : null;
  if (String(matchedField?.status ?? "").trim() !== "verified") {
    failed(
      "candidate_cache_control_capability_unverified",
      `the selected upstream did not verify ${field} for the disposable candidate scope`
    );
  }
  const fieldStatus = Number(matchedField?.http_status);
  if (!Number.isInteger(fieldStatus) || fieldStatus < 200 || fieldStatus >= 300) {
    failed(
      "candidate_cache_control_capability_field_failed",
      `the selected upstream did not accept ${field} during the disposable candidate capability probe`
    );
  }
  // Keep the public report payload-free: Key IDs and upstream messages remain
  // inside the isolated process. The live scope guard separately proves the
  // selected Key realm did not change during the A/B.
  return {
    schema: "atoapi-isolated-cache-control-certificate-v1",
    state: "verified",
    provider_id: providerId,
    model_id: modelId,
    channel,
    field,
    selected_key_scope: expectedKeyId ? "pinned" : selectedKeyId ? "selected" : "direct",
    management_request_count: 2,
    field_http_status: fieldStatus
  };
}

// The isolated admin endpoint may include a detailed local error message.  It
// can contain deployment-specific transport detail, so retain only a bounded
// category in the verifier report.  This is diagnostic-only: it neither
// relaxes certificate validation nor changes a scored request.
function isolatedCacheControlProbeFailureCategory(payload) {
  const message = String(payload?.error?.message ?? "").trim().toLowerCase();
  if (!message) return "unspecified_local_probe_failure";
  if (message.includes("provider settings changed while cache capability verification")) {
    return "provider_configuration_changed";
  }
  if (message.includes("provider api key is not configured") ||
    message.includes("requested provider key is not enabled") ||
    message.includes("did not match any currently usable provider key")) {
    return "selected_key_unavailable";
  }
  if (message.includes("failed to select the requested provider key") ||
    message.includes("selected provider key realm does not match")) {
    return "selected_key_scope_mismatch";
  }
  if (message.includes("provider") && message.includes("was not found")) {
    return "provider_not_found";
  }
  if (message.includes("provider") && message.includes("is disabled")) {
    return "provider_disabled";
  }
  if (/(timeout|timed out|proxy|connect|connection|dns|transport|request failed)/.test(message)) {
    return "upstream_transport_failure";
  }
  return "other_local_probe_failure";
}

async function disposeIsolatedRuntimeWorkspace(workspace, label, keepRunDir) {
  if (!workspace) return;
  if (workspace.runtime) await stopChild(workspace.runtime.child, label);
  if (!keepRunDir) await removeTemporaryDirectory(workspace.tempRoot);
}

async function exerciseScenario(spec) {
  const cursor = createScenarioCursor(spec);
  for (let turn = 0; turn < spec.settings.turns; turn += 1) {
    await advanceScenarioCursor(cursor, turn);
    if (cursor.fatal) break;
    if (turn + 1 < spec.settings.turns && spec.settings.turn_delay_ms > 0) {
      await delay(spec.settings.turn_delay_ms);
    }
  }
  return finalizeScenarioCursor(cursor);
}

function createScenarioCursor(spec) {
  const baseFixtureFamily = spec.fixtureFamily ?? null;
  // Keep custom-tool probes on a distinct local conversation identity. This
  // prevents a same-run function/custom diagnostic from accidentally sharing
  // a session-scoped placement while leaving the historical function fixture
  // identity byte-for-byte unchanged.
  const fixtureFamily = spec.settings.tool_protocol === "custom"
    ? (baseFixtureFamily ? `${baseFixtureFamily}-custom-tool` : "custom-tool")
    : baseFixtureFamily;
  // A standard version comparison replays the same conversation identity.
  // A placement-key A/B can explicitly isolate metadata-only identities so
  // one arm cannot warm the other's session-scoped upstream placement. The
  // identity headers are stripped before the native upstream body is sent.
  const conversationIdentity = releaseFixtureConversationIdentity(
    spec.pair,
    fixtureFamily,
    spec.isolationLane
  );
  const stableInstructions = buildStableInstructions(
    spec.settings.stable_instruction_chars,
    fixtureFamily,
    spec.settings.fixture_profile
  );
  // Keep the first request a normal seed.  The selected live relay rejects a
  // synthetic function_call/function_call_output history on the seed itself;
  // the real path we need to exercise is the later successor that reuses a
  // completed local response and rebinds regenerated tool ids.
  const seedInput = [message(buildSeedContext(
    spec.settings.seed_context_chars,
    fixtureFamily,
    spec.settings.fixture_profile
  ))];
  return {
    spec,
    fixtureFamily,
    state: {
      input: seedInput,
      compactionSeen: false,
      previousResponseId: null
    },
    sessionId: conversationIdentity.session_id,
    conversationId: spec.settings.fixture_identity_churn === true
      ? conversationIdentity.conversation_id
      : null,
    threadId: conversationIdentity.thread_id,
    fixtureIdentityPhase: "base",
    stableInstructions,
    requests: [],
    dynamicTailEvents: [],
    fatal: null
  };
}

function buildLocalPreviousResponseIdRebindSeedItems({
  pair,
  fixtureFamily = null,
  toolProtocol = "function"
}) {
  return buildToolFixtureItems({
    pair,
    fixtureFamily: fixtureFamily ? `${fixtureFamily}-local-rebind-seed` : "local-rebind-seed",
    targetChars: 512,
    shape: "natural",
    calls: 1,
    eventOrdinal: 71,
    toolProtocol
  });
}

function localPreviousResponseIdRebindTargetRequestCount(turns) {
  return Number(turns) >= 3 ? 1 : 0;
}

function localPreviousResponseIdFullReplayTargetRequestCount(turns) {
  return Number(turns) >= 3 ? 1 : 0;
}

function cloneFixtureInput(input) {
  try {
    return JSON.parse(JSON.stringify(input));
  } catch {
    throw new FailClosedError(
      "local_previous_response_id_input_clone_failed",
      "the local rebind fixture input must be JSON serializable"
    );
  }
}

function localRebindFixtureCallId({
  pair,
  fixtureFamily = null,
  turn,
  ordinal,
  toolProtocol = "function"
}) {
  return `call_local_rebind_${sha256Parts([
    "local-previous-response-id-rebind-v1",
    normalizeToolProtocol(toolProtocol) ?? String(toolProtocol),
    String(pair),
    String(fixtureFamily ?? ""),
    String(turn),
    String(ordinal)
  ]).slice(0, 40)}`;
}

function regenerateClosedToolPairCallIds(input, {
  pair,
  fixtureFamily = null,
  turn,
  toolProtocol = "function"
}) {
  if (!Array.isArray(input)) {
    throw new FailClosedError(
      "local_previous_response_id_input_invalid",
      "the local rebind fixture input must be an array"
    );
  }
  const fixtureTool = releaseFixtureToolProtocol(toolProtocol);
  const cloned = cloneFixtureInput(input);
  const pending = new Map();
  const seenOutputIds = new Set();
  let regeneratedToolPairCount = 0;
  for (let index = 0; index < cloned.length; index += 1) {
    const item = cloned[index];
    if (!item || typeof item !== "object") continue;
    if (item.type === fixtureTool.call_type) {
      const callId = typeof item.call_id === "string" ? item.call_id : "";
      if (!callId || pending.has(callId) || seenOutputIds.has(callId)) {
        throw new FailClosedError(
          "local_previous_response_id_tool_call_id_invalid",
          `a local rebind fixture ${fixtureTool.label} call id must be unique and non-empty`
        );
      }
      pending.set(callId, { item, ordinal: regeneratedToolPairCount });
      continue;
    }
    if (item.type !== fixtureTool.output_type) {
      if (isFixtureToolHistoryItemType(item.type)) {
        throw new FailClosedError(
          "local_previous_response_id_tool_protocol_mismatch",
          `a local rebind fixture must use one ${fixtureTool.label} call/output protocol`
        );
      }
      continue;
    }
    const callId = typeof item.call_id === "string" ? item.call_id : "";
    const call = pending.get(callId);
    if (!call || seenOutputIds.has(callId)) {
        throw new FailClosedError(
          "local_previous_response_id_tool_pair_unclosed",
          `a local rebind fixture output must close exactly one earlier ${fixtureTool.label} call`
      );
    }
    const reboundCallId = localRebindFixtureCallId({
      pair,
      fixtureFamily,
      turn,
      ordinal: call.ordinal,
      toolProtocol
    });
    call.item.call_id = reboundCallId;
    item.call_id = reboundCallId;
    pending.delete(callId);
    seenOutputIds.add(callId);
    regeneratedToolPairCount += 1;
  }
  if (pending.size > 0) {
    throw new FailClosedError(
      "local_previous_response_id_tool_pair_unclosed",
      `every local rebind fixture ${fixtureTool.label} call must have a matching output`
    );
  }
  return { input: cloned, regeneratedToolPairCount };
}

function preserveClosedToolPairCallIds(input) {
  if (!Array.isArray(input)) {
    throw new FailClosedError(
      "local_previous_response_id_input_invalid",
      "an unchanged FullReplay fixture input must be an array"
    );
  }
  return {
    input: cloneFixtureInput(input),
    regeneratedToolPairCount: 0
  };
}

async function advanceScenarioCursor(cursor, turn) {
  if (cursor.fatal) return false;
  const { spec, fixtureFamily, state } = cursor;
  try {
    let requestKind = "turn";
    let phase = turn === 0 ? "seed" : `followup-${turn}`;
    const toolTailMaturityTurn = spec.settings.scenario === "tool-tail-maturity" && turn === 2;
    const dynamicTail = spec.settings.scenario === "dynamic-tail-mix"
      ? dynamicTailProfileForTurn(
        turn,
        spec.settings.tool_chars,
        spec.settings.tool_calls,
        spec.settings.dynamic_tail_profile,
        spec.settings.candidate_late_shallow_provider_waterline_rollback_wait === true
      )
      : null;
    if (dynamicTail) {
      const eventOrdinal = dynamicTail.ordinal;
      const scopedFixtureFamily = fixtureFamily
        ? `${fixtureFamily}-tail-${eventOrdinal}`
        : `tail-${eventOrdinal}`;
      state.input.push(
        ...(spec.settings.dynamic_tail_mode === "text"
          ? [message(buildDynamicTextTail({
            targetChars: dynamicTail.targetChars,
            shape: dynamicTail.shape,
            fixtureFamily: scopedFixtureFamily,
            eventOrdinal
          }))]
          : buildToolFixtureItems({
            pair: spec.pair,
            fixtureFamily: scopedFixtureFamily,
            targetChars: dynamicTail.targetChars,
            shape: dynamicTail.shape,
            calls: dynamicTail.calls,
            eventOrdinal,
            toolProtocol: spec.settings.tool_protocol
          })),
        message(`Dynamic tail ${eventOrdinal} completed. Preserve it and reply with OK only.`)
      );
      cursor.dynamicTailEvents.push({
        ordinal: eventOrdinal,
        turn,
        shape: dynamicTail.shape,
        target_chars: dynamicTail.targetChars,
        calls: dynamicTail.calls,
        phase: `dynamic-tail-${eventOrdinal}-${dynamicTail.shape}`
      });
      phase = `dynamic-tail-${eventOrdinal}-${dynamicTail.shape}`;
    } else if (turn > 0 && (spec.settings.scenario === "tool-burst" && turn === 1 || toolTailMaturityTurn)) {
      // The arm-specific lane isolates each runtime's cache placement, but it
      // must never leak into the fixture input. Otherwise champion and
      // candidate replay different function-call histories and the paired
      // hit/TTFT comparison is no longer meaningful.
      state.input.push(
        ...buildToolFixtureItems({
          pair: spec.pair,
          fixtureFamily,
          targetChars: spec.settings.tool_chars,
          shape: spec.settings.tool_output_shape,
          calls: spec.settings.tool_calls,
          toolProtocol: spec.settings.tool_protocol
        }),
        message("Use the completed tool output. Reply with OK only.")
      );
      phase = toolTailMaturityTurn ? "tool-tail-maturity" : "tool-burst";
    } else if (
      turn > 0 &&
      (spec.settings.scenario === "compacted-anchor" ||
        spec.settings.scenario === "compaction-root") &&
      turn === 1
    ) {
      state.input.push({ type: "compaction_trigger" });
      requestKind = "compaction";
      phase = "compaction";
    } else if (turn > 0) {
      state.input.push(message(`Stable follow-up ${turn}. Reply with OK only.`));
    }

    const fixtureTools = releaseFixtureToolsForScenario(
      spec.settings.scenario,
      spec.settings.dynamic_tail_mode,
      spec.settings.include_tool_schema,
      spec.settings.tool_protocol
    );
    const localPreviousResponseIdRebind =
      spec.settings.exercise_local_previous_response_id_rebind === true;
    const localPreviousResponseIdFullReplay =
      spec.settings.exercise_local_previous_response_id_full_replay === true;
    const localPreviousResponseIdFixture =
      localPreviousResponseIdRebind || localPreviousResponseIdFullReplay;
    // Turn 1 establishes a normal full-replay predecessor containing the
    // completed tool pair.  Only turn 2 is the target continuation, so the
    // fixture remains a valid three-turn conversation for providers that do
    // not accept tool history in the initial seed.
    const localRebindTarget = localPreviousResponseIdRebind && turn === 2;
    const localFullReplayTarget = localPreviousResponseIdFullReplay && turn === 2;
    const localPreviousResponseIdTarget = localRebindTarget || localFullReplayTarget;
    let outboundInput = state.input;
    let regeneratedToolPairCount = 0;
    let previousResponseId = null;
    if (localPreviousResponseIdTarget) {
      if (typeof state.previousResponseId !== "string" || !state.previousResponseId) {
        throw new FailClosedError(
          "local_previous_response_id_missing",
          "a local previous response id is required before a rebind target request"
        );
      }
      if (localRebindTarget) {
        const rebound = regenerateClosedToolPairCallIds(state.input, {
          pair: spec.pair,
          fixtureFamily,
          turn,
          toolProtocol: spec.settings.tool_protocol
        });
        outboundInput = rebound.input;
        regeneratedToolPairCount = rebound.regeneratedToolPairCount;
        if (regeneratedToolPairCount < 1) {
          throw new FailClosedError(
            "local_previous_response_id_tool_pair_missing",
            "a rebind target request must contain at least one closed tool pair"
          );
        }
      } else {
        // Keep the unchanged FullReplay fixture explicit and detached from the
        // cursor state. No call_id regeneration is allowed in this mode.
        const preserved = preserveClosedToolPairCallIds(state.input);
        outboundInput = preserved.input;
        regeneratedToolPairCount = preserved.regeneratedToolPairCount;
      }
      previousResponseId = state.previousResponseId;
    }
    const turnIdentity = releaseFixtureTurnIdentity({
      pair: spec.pair,
      fixtureFamily,
      isolationLane: spec.isolationLane,
      turn,
      scenario: spec.settings.scenario,
      identityChurn: spec.settings.fixture_identity_churn === true,
      baseSessionId: cursor.sessionId,
      baseConversationId: cursor.conversationId,
      threadId: cursor.threadId
    });
    const inbound = await sendOneInbound({
      runtime: spec.runtime,
      sessionId: turnIdentity.session_id,
      conversationId: turnIdentity.conversation_id,
      threadId: turnIdentity.thread_id,
      fixtureIdentityPhase: spec.settings.fixture_identity_churn === true
        ? turnIdentity.phase
        : null,
      fixtureIdentityThreadStable: spec.settings.fixture_identity_churn === true
        ? turnIdentity.thread_stable
        : false,
      cohort: spec.cohort,
      input: outboundInput,
      instructions: cursor.stableInstructions,
      maxOutputTokens: spec.settings.max_output_tokens,
      responseTimeoutMs: spec.settings.response_timeout_ms,
      reasoningEffort: spec.settings.reasoning_effort,
      tools: fixtureTools,
      toolChoice: fixtureTools.length > 0 ? "none" : null,
      requestKind,
      phase,
      promptCacheKey: spec.promptCacheKey,
      previousResponseId,
      localPreviousResponseIdRebind,
      localRebindTarget,
      localFullReplayTarget,
      localPreviousResponseIdFixtureMode: localRebindTarget
        ? "rebind"
        : localFullReplayTarget
          ? "unchanged_full_replay"
          : null,
      regeneratedToolPairCount,
      fixtureToolProtocol: spec.settings.tool_protocol,
      requireCompletedResponseId: localPreviousResponseIdFixture
    });
    const record = inbound.record;
    cursor.requests.push(record);
    if (!record.pass) {
      cursor.fatal = record.failure ?? "inbound verification failed";
      return false;
    }
    if (localPreviousResponseIdFixture) {
      if (typeof inbound.completedResponseId !== "string" || !inbound.completedResponseId) {
        cursor.fatal = "local_previous_response_id_response_id_missing";
        return false;
      }
      state.previousResponseId = inbound.completedResponseId;
    }
    if (requestKind === "compaction" && spec.settings.scenario === "compacted-anchor") {
      const compacted = record.compacted_input;
      if (!Array.isArray(compacted) || compacted.length === 0) {
        cursor.fatal = "compaction response did not contain a reusable compaction item";
        return false;
      }
      state.input = compacted;
      state.compactionSeen = true;
    } else if (requestKind === "compaction") {
      // Some third-party HTTP Responses routes perform compaction internally
      // but do not return a reusable compaction item. The root request is
      // still a valid, independently measurable cache-placement boundary.
      state.compactionSeen = true;
    }
    return true;
  } catch (error) {
    cursor.fatal = `scenario_turn_error:${safeErrorMessage(error)}`;
    return false;
  }
}

function finalizeScenarioCursor(cursor) {
  const { spec, state, requests, fatal } = cursor;
  return buildDynamicRun({
    arm: spec.arm,
    pair: spec.pair,
    cohort: spec.cohort,
    executable: spec.executable,
    scenario: spec.settings.scenario,
    promptCacheKeyUsed: Boolean(spec.promptCacheKey),
    capabilityCertificate: spec.capabilityCertificate,
    dynamicTailEvents: cursor.dynamicTailEvents,
    // A required guarded count applies to the candidate aggregate, not to
    // each fresh pair independently. Per-pair enforcement would abort a
    // healthy crossover after its first valid guarded successor and prevent
    // the order-balanced second pair from running.
    minimumGuardedRequests: 0,
    minimumSeedInputTokens: spec.settings.minimum_seed_input_tokens,
    minimumPeakInputTokens: spec.settings.minimum_peak_input_tokens,
    maximumPeakInputTokens: spec.settings.maximum_peak_input_tokens,
    requireCandidateUpstreamAffinity:
      spec.arm === "candidate" && spec.settings.candidate_upstream_affinity === true,
    requireCandidateCacheControlField:
      spec.arm === "candidate" ? spec.settings.candidate_cache_control_field : null,
    requireCandidateCacheOptions24h:
      spec.arm === "candidate" && spec.settings.candidate_cache_options_24h === true,
    requireCandidateOptions24hSiblingSettle:
      spec.arm === "candidate" && spec.settings.candidate_options24h_sibling_settle === true,
    requireCandidateHttp1:
      spec.arm === "candidate" && spec.settings.candidate_http1 === true,
    requireCandidateProviderWaterlineRecoveryWait:
      spec.arm === "candidate" &&
      spec.settings.candidate_provider_waterline_recovery_wait === true,
    requireCandidateExactMediumToolTailMaturityWait:
      spec.arm === "candidate" &&
      spec.settings.candidate_exact_medium_tool_tail_maturity_wait === true,
    requireCandidateLateShallowProviderWaterlineRollbackWait:
      // The late rollback is intentionally sparse: an observed witness in
      // either of the two order-reversed scored pairs is aggregate evidence.
      // Requiring it per individual run would abort after a healthy first
      // pair before the second, complementary placement can be exercised.
      false,
    requests,
    fatal,
    compactionSeen: state.compactionSeen
  });
}

function interleavedTurnOrder(
  pair,
  turn,
  pairOffset = 0,
  firstArm = "champion",
  scenario = null
) {
  const candidateFirst = firstArm === "candidate";
  const championStartsPair =
    (pair + pairOffset + Number(candidateFirst)) % 2 === 0;
  // A dynamic-tail fixture alternates "changed tail" and direct-follow-up
  // turns. Rotating every individual turn makes one arm first on every changed
  // tail and the other first on every follow-up, so the order itself creates a
  // fake new-tail advantage. Rotate in two-turn waves instead: the first seed
  // stays paired with the first changed tail, and subsequent direct-follow-up
  // / changed-tail waves reverse together. The next pair reverses again. That
  // preserves crossover while preventing phase identity from being tied to an
  // arm.
  const championFirst = scenario === "dynamic-tail-mix" && turn > 0
    ? (Math.floor(turn / 2) % 2 === 0
      ? championStartsPair
      : !championStartsPair)
    : (pair + turn + pairOffset + Number(candidateFirst)) % 2 === 0;
  return championFirst
    ? ["champion", "candidate"]
    : ["candidate", "champion"];
}

function isWarmupPair(pair, warmupPairs) {
  return Number.isInteger(pair) && pair >= 0 && pair < warmupPairs;
}

function partitionRunsByWarmup(runs, warmupPairs) {
  const warmup = [];
  const scored = [];
  for (const run of array(runs)) {
    (isWarmupPair(run?.pair, warmupPairs) ? warmup : scored).push(run);
  }
  return { warmup, scored };
}

function scheduledPairIds(pairCount) {
  return Array.from({ length: pairCount }, (_, pair) => pair);
}

function pairedRunIds(championRuns, candidateRuns) {
  const pairIdsFor = (runs) => new Set(array(runs)
    .map((run) => run?.pair)
    .filter((pair) => Number.isInteger(pair) && pair >= 0));
  const championPairIds = pairIdsFor(championRuns);
  const candidatePairIds = pairIdsFor(candidateRuns);
  return [...championPairIds]
    .filter((pair) => candidatePairIds.has(pair))
    .sort((left, right) => left - right);
}

function comparisonPairInvalid(result) {
  return [result?.champion, result?.candidate].some((run) =>
    run?.pass !== true && !providerInstabilityOnlyRun(run)
  );
}

// A completed pair with exact route/realm and one upstream call per inbound
// can be retained as confounded evidence when the selected upstream reports a
// waterline instability. It is not a promotion result, but stopping before
// the crossover pair would make the benchmark permanently order-biased. A
// stream failure, payload rejection, route drift, or multi-attempt result
// still stops immediately.
function providerInstabilityOnlyRun(run) {
  if (!run || run.pass === true || number(run.metrics?.provider_unstable_gap_tokens) <= 0) {
    return false;
  }
  const requests = array(run.requests);
  return requests.length > 0 &&
    requests.every((request) => request.sse_completed === true) &&
    run.checks?.every_inbound_one_attempt_one_main_post === true &&
    run.checks?.cohort_bound_on_every_request === true &&
    run.checks?.one_observed_key_realm === true &&
    run.checks?.static_wire_continuity === true;
}

async function runInterleavedDynamicPair(specs) {
  if (specs?.champion?.settings?.scenario === "tool-tail-maturity") {
    return runInterleavedToolTailMaturityPair(specs);
  }
  const turnOrder = [];
  const requestedSeedToReuseDelayMs = Number(
    specs?.champion?.settings?.seed_to_reuse_delay_ms ?? 0
  );
  let seedToReuseDelay = null;
  let cursors = null;
  try {
    const [champion, candidate] = await Promise.all([
      prepareDynamicArmSpec(specs.champion),
      prepareDynamicArmSpec(specs.candidate)
    ]);
    cursors = {
      champion: createScenarioCursor(champion),
      candidate: createScenarioCursor(candidate)
    };
    const turns = champion.settings.turns;
    for (let turn = 0; turn < turns; turn += 1) {
      const order = interleavedTurnOrder(
        champion.pair,
        turn,
        champion.settings.pair_offset,
        champion.settings.first_arm,
        champion.settings.scenario
      );
      turnOrder.push(order);
      let terminalFailure = false;
      for (let index = 0; index < order.length; index += 1) {
        const arm = order[index];
        const advanced = await advanceScenarioCursor(cursors[arm], turn);
        if (!advanced) {
          terminalFailure = true;
          break;
        }
        if (index + 1 < order.length && champion.settings.inter_arm_delay_ms > 0) {
          await delay(champion.settings.inter_arm_delay_ms);
        }
      }
      // A failed arm makes the pair invalid. Stop this pair rather than adding
      // new upstream load after a known failed fresh inbound; the next pair has
      // the opposite first sender and remains independently observable.
      if (terminalFailure) break;
      if (turn === 0 && requestedSeedToReuseDelayMs > 0) {
        const startedAtMs = Date.now();
        await delay(requestedSeedToReuseDelayMs);
        seedToReuseDelay = seedToReuseDelayEvidence({
          pair: champion.pair,
          requestedMs: requestedSeedToReuseDelayMs,
          observedMs: Date.now() - startedAtMs,
          seedTurnOrder: order
        });
        if (typeof specs.afterSeedToReuseDelay !== "function") {
          throw new FailClosedError(
            "seed_to_reuse_delay_scope_gate_missing",
            "the delayed reuse verifier requires a post-delay scope gate before it can send turn 1"
          );
        }
        const guard = await specs.afterSeedToReuseDelay(champion.pair, seedToReuseDelay);
        seedToReuseDelay = {
          ...seedToReuseDelay,
          post_delay_selection_scope_verified:
            guard?.post_delay_selection_scope_verified === true,
          post_delay_live_scope_verified:
            guard?.post_delay_live_scope_verified === true
        };
      }
      if (turn + 1 < turns && champion.settings.turn_delay_ms > 0) {
        await delay(champion.settings.turn_delay_ms);
      }
    }
  } catch (error) {
    const reason = `interleaved_pair_error:${safeErrorMessage(error)}`;
    if (cursors) {
      for (const cursor of Object.values(cursors)) {
        if (!cursor.fatal) cursor.fatal = reason;
      }
    } else {
      return {
        champion: failedDynamicRun({
          arm: specs.champion.arm,
          pair: specs.champion.pair,
          cohort: specs.champion.cohort,
          executable: await executableArtifact(specs.champion.executable),
          reason
        }),
        candidate: failedDynamicRun({
          arm: specs.candidate.arm,
          pair: specs.candidate.pair,
          cohort: specs.candidate.cohort,
          executable: await executableArtifact(specs.candidate.executable),
          reason
        }),
        turn_order: turnOrder,
        seed_to_reuse_delay: seedToReuseDelay
      };
    }
  }
  return {
    champion: finalizeScenarioCursor(cursors.champion),
    candidate: finalizeScenarioCursor(cursors.candidate),
    turn_order: turnOrder,
    pair_order: turnOrder[0] ?? null,
    seed_to_reuse_delay: seedToReuseDelay
  };
}

// The exact-medium maturity branch belongs to the arm that writes the tool
// tail before the shared upstream has caught up.  A turn-by-turn interleave
// puts the other arm between that tail and its own child, which makes the
// 500ms direct-successor claim untestable.  Run each arm's tail/child pair as
// one contiguous dispatch group, then reverse the leader in the next pair.
function toolTailMaturityDispatchSchedule(pair, pairOffset = 0, firstArm = "champion") {
  const pairStart = interleavedTurnOrder(pair, 0, pairOffset, firstArm);
  const stablePredecessor = interleavedTurnOrder(pair, 1, pairOffset, firstArm);
  const leader = pairStart[0];
  const follower = pairStart[1];
  const sequence = [
    ...pairStart.map((arm) => ({ turn: 0, arm, stage: "seed" })),
    ...stablePredecessor.map((arm) => ({ turn: 1, arm, stage: "stable-predecessor" })),
    { turn: 2, arm: leader, stage: "tool-tail-maturity", role: "leader-tail" },
    { turn: 3, arm: leader, stage: "direct-successor", role: "leader-successor" },
    { turn: 2, arm: follower, stage: "tool-tail-maturity", role: "follower-tail" },
    { turn: 3, arm: follower, stage: "direct-successor", role: "follower-successor" }
  ];
  return {
    schema: "atoapi-tool-tail-maturity-dispatch-v1",
    pair_start: pairStart,
    stable_predecessor: stablePredecessor,
    leader,
    follower,
    sequence
  };
}

async function runInterleavedToolTailMaturityPair(specs) {
  const schedule = toolTailMaturityDispatchSchedule(
    specs.champion.pair,
    specs.champion.settings.pair_offset,
    specs.champion.settings.first_arm
  );
  const actualSequence = [];
  let cursors = null;
  try {
    const [champion, candidate] = await Promise.all([
      prepareDynamicArmSpec(specs.champion),
      prepareDynamicArmSpec(specs.candidate)
    ]);
    cursors = {
      champion: createScenarioCursor(champion),
      candidate: createScenarioCursor(candidate)
    };
    for (const event of schedule.sequence) {
      const advanced = await advanceScenarioCursor(cursors[event.arm], event.turn);
      if (!advanced) break;
      actualSequence.push({ turn: event.turn, arm: event.arm, stage: event.stage, role: event.role ?? null });
    }
  } catch (error) {
    const reason = `interleaved_pair_error:${safeErrorMessage(error)}`;
    if (cursors) {
      for (const cursor of Object.values(cursors)) {
        if (!cursor.fatal) cursor.fatal = reason;
      }
    } else {
      return {
        champion: failedDynamicRun({
          arm: specs.champion.arm,
          pair: specs.champion.pair,
          cohort: specs.champion.cohort,
          executable: await executableArtifact(specs.champion.executable),
          reason
        }),
        candidate: failedDynamicRun({
          arm: specs.candidate.arm,
          pair: specs.candidate.pair,
          cohort: specs.candidate.cohort,
          executable: await executableArtifact(specs.candidate.executable),
          reason
        }),
        pair_order: schedule.pair_start,
        turn_order: { ...schedule, actual_sequence: actualSequence }
      };
    }
  }
  return {
    champion: finalizeScenarioCursor(cursors.champion),
    candidate: finalizeScenarioCursor(cursors.candidate),
    pair_order: schedule.pair_start,
    turn_order: { ...schedule, actual_sequence: actualSequence }
  };
}

async function sendOneInbound(spec) {
  const before = await getJson(
    `${spec.runtime.baseUrl}/admin/metrics`,
    10_000,
    spec.runtime.localKey
  );
  const beforeCounters = requestCounters(before);
  const knownRawInboundIds = new Set(
    requestLogRows(before)
      .map((item) => String(item?.inbound_request_id ?? ""))
      .filter(Boolean)
  );
  const knownErrorFingerprints = new Set(
    array(before?.recent_errors).map(errorFingerprint)
  );
  const body = buildResponsesRequestBody(spec);
  const serializedBody = JSON.stringify(body);
  // Hash only the replayed input array. The candidate's placement field is
  // intentionally excluded, while any arm-specific fixture drift becomes a
  // fail-closed, content-free evidence mismatch.
  const inputFingerprint = sha256Text(JSON.stringify(body.input));
  const requestBodyBytes = Buffer.byteLength(serializedBody, "utf8");
  const startedAt = Date.now();
  let responseStatus = 0;
  let responseText = "";
  let transportError = null;
  let transportErrorClass = null;
  let responseDeadlineExceeded = false;
  const responseTimeoutMs = boundedInteger(
    spec.responseTimeoutMs ?? 180_000,
    "response timeout",
    30_000,
    600_000
  );
  const responseAbortController = new AbortController();
  const responseDeadline = setTimeout(() => {
    responseDeadlineExceeded = true;
    responseAbortController.abort();
  }, responseTimeoutMs);
  try {
    const response = await fetch(`${spec.runtime.baseUrl}/codex/v1/responses`, {
      method: "POST",
      headers: {
        authorization: `Bearer ${spec.runtime.localKey}`,
        "content-type": "application/json",
        accept: "text/event-stream",
        "x-codex-turn-metadata": JSON.stringify({
          session_id: spec.sessionId,
          ...(typeof spec.conversationId === "string" && spec.conversationId
            ? { conversation_id: spec.conversationId }
            : {}),
          thread_id: spec.threadId,
          request_kind: spec.requestKind
        })
      },
      body: serializedBody,
      signal: responseAbortController.signal
    });
    responseStatus = response.status;
    responseText = await response.text();
  } catch (error) {
    transportError = safeErrorMessage(error);
    transportErrorClass = responseDeadlineExceeded
      ? "client_deadline_exceeded"
      : classifyDownstreamTransportError(error);
  } finally {
    clearTimeout(responseDeadline);
  }
  const after = await waitForSettledInbound({
    baseUrl: spec.runtime.baseUrl,
    localKey: spec.runtime.localKey,
    beforeCounters,
    knownRawInboundIds
  });
  const counters = subtractCounters(requestCounters(after), beforeCounters);
  const metric = selectNewRequestLog(after, knownRawInboundIds);
  const inboundId = String(metric?.inbound_request_id ?? "");
  const outcome = array(after.recent_agent_inbound_outcomes).find(
    (item) => String(item?.inbound_request_id ?? "") === inboundId
  );
  const attempts = array(after.recent_agent_upstream_attempts).filter(
    (item) => String(item?.inbound_request_id ?? "") === inboundId
  );
  const runtimeErrorEvidence = freshRuntimeErrorEvidence(after, knownErrorFingerprints);
  const deadlineDiagnostics = responseDeadlineExceeded
    ? await responseDeadlineDiagnostics({
      runtime: spec.runtime,
      metrics: after,
      knownRawInboundIds,
      metric,
      responseTimeoutMs
    })
    : null;
  const responseFailureCode = responseErrorCode(responseText);
  const responseFailed = responseHasNativeFailure(responseText);
  const responseFailureKind = (
    !(responseStatus >= 200 && responseStatus < 300) || responseFailed
  ) ? responseErrorKind(responseText, responseFailureCode) : null;
  const terminal = responseStatus >= 200 && responseStatus < 300 &&
    /\bresponse\.completed\b/u.test(responseText) &&
    !responseFailed;
  const completedResponseId = extractCompletedResponseId(responseText);
  const terminalUsage = terminalUsageShape(responseText);
  const exactCounterDelta = counters.inbound_requests === 1 &&
    counters.generation_attempts === 1 &&
    counters.upstream_requests === 1;
  const perInboundSingleAttempt = metric?.upstream_attempt_index === 1 &&
    metric?.upstream_attempt_total === 1 &&
    outcome?.attempt_count === 1 &&
    outcome?.attempt_budget === 1 &&
    attempts.length === 1 &&
    attempts[0]?.attempt_index === 1 &&
    attempts[0]?.attempt_budget === 1;
  const aggregateSingleAttempt = number(after.agent_generation?.multi_attempt_inbounds) === 0 &&
    number(after.agent_generation?.max_attempts_per_inbound) <= 1;
  const providerMatches = String(metric?.provider_id ?? "") === spec.cohort.provider_id;
  const modelMatches = String(metric?.model ?? "") === spec.cohort.model;
  const observedRealmId = String(metric?.shadow_affinity_realm_id ?? "");
  const observedRealmPresent = observedRealmId.length > 0;
  const observedKeyRealmMatches = observedRealmPresent &&
    observedRealmId === spec.cohort.key_realm_hash;
  const finalScopeWaterline = finalScopeWaterlineEvidence(metric?.final_scope_waterline);
  const rawCacheReadTokens = explicitFiniteNonNegativeNumber(metric?.cache_read_tokens);
  const cacheReadTokensPresent = rawCacheReadTokens !== null;
  const completedInput = spec.requestKind === "compaction"
    ? extractCompactionItems(responseText)
    : [];
  const timing = {
    prefix_guard_wait_ms: finiteNonNegativeNumber(metric?.prefix_guard_wait_ms),
    local_prepare_ms: finiteNonNegativeNumber(metric?.local_prepare_ms),
    request_body_encode_ms: finiteNonNegativeNumber(metric?.request_body_encode_ms),
    gzip_encode_ms: finiteNonNegativeNumber(metric?.gzip_encode_ms),
    upstream_headers_ms: finiteNonNegativeNumber(metric?.upstream_headers_ms),
    upstream_first_chunk_ms: finiteNonNegativeNumber(metric?.upstream_first_chunk_ms),
    upstream_ttft_ms: finiteNonNegativeNumber(metric?.upstream_ttft_ms),
    ttft_ms: finiteNonNegativeNumber(metric?.ttft_ms)
  };
  const localPreUpstreamOverhead = localPreUpstreamOverheadMs(timing);
  const timingPresent = !terminal || [
    timing.prefix_guard_wait_ms,
    timing.local_prepare_ms,
    timing.request_body_encode_ms,
    timing.gzip_encode_ms,
    timing.upstream_ttft_ms,
    timing.ttft_ms,
    localPreUpstreamOverhead
  ].every((value) => value !== null);
  const upstreamAffinity = upstreamAffinityTestEvidence(metric?.upstream_pool_diagnostic);
  const cacheControlFields = cacheControlFieldEvidence(metric);
  const cacheOptions24h = cacheOptions24hEvidence(metric);
  const checks = {
    terminal_response_completed: terminal,
    terminal_usage_shape_present: !terminal || terminalUsage === "present",
    exact_counter_delta: exactCounterDelta,
    per_inbound_one_attempt_one_post: perInboundSingleAttempt,
    aggregate_no_multi_attempt: aggregateSingleAttempt,
    metric_present: Boolean(metric),
    provider_matches_cohort: providerMatches,
    model_matches_cohort: modelMatches,
    observed_key_realm_present: observedRealmPresent,
    observed_key_realm_matches_cohort: observedKeyRealmMatches,
    usage_present: number(metric?.input_tokens) > 0,
    cache_read_tokens_present: cacheReadTokensPresent,
    timing_present: timingPresent,
    local_response_id_present:
      spec.requireCompletedResponseId !== true || typeof completedResponseId === "string"
  };
  const failure = inboundFailureReason({
    transportError,
    responseDeadlineExceeded,
    responseStatus,
    responseFailureCode,
    responseFailed,
    checks
  });
  return {
    record: {
    phase: spec.phase,
    request_kind: spec.requestKind,
    pass: !failure,
    failure,
    http_status: responseStatus || null,
    response_failure_code: responseFailureCode,
    // Never retain an upstream error message: it may echo request material.
    // This bounded classification is enough to separate a payload ceiling from
    // an authentication, model, or request-shape failure in live evidence.
    response_failure_kind: responseFailureKind,
    transport_error_class: transportErrorClass,
    response_deadline_ms: responseTimeoutMs,
    deadline_diagnostics: deadlineDiagnostics,
    terminal_usage_shape: terminalUsage,
    elapsed_ms: Date.now() - startedAt,
    sse_completed: terminal,
    input_fingerprint: inputFingerprint,
    request_body_bytes: requestBodyBytes,
    counters,
    inbound_id_hash: inboundId ? sha256Text(inboundId).slice(0, 24) : null,
    observed_realm_id: observedRealmId || null,
    provider_id: metric?.provider_id ?? null,
    model: metric?.model ?? null,
    sse_end_reason: metric?.sse_end_reason ?? null,
    // Keep only an allow-listed, payload-free transport projection. This is
    // enough to distinguish a JSON/wire regression from an HTTP, gzip, or
    // SSE-path difference without retaining proxy addresses, URLs, headers,
    // request bodies, credentials, or upstream messages.
    transport: transportEvidence(metric),
    upstream_affinity: upstreamAffinity,
    cache_control_fields: cacheControlFields,
    local_previous_response_id_sent: typeof spec.previousResponseId === "string" &&
      spec.previousResponseId.length > 0,
    local_response_id_present: typeof completedResponseId === "string" &&
      completedResponseId.length > 0,
    local_rebind_target: spec.localRebindTarget === true,
    local_full_replay_target: spec.localFullReplayTarget === true,
    local_previous_response_id_fixture_mode: spec.localPreviousResponseIdFixtureMode ?? null,
    // Keep identity churn evidence bounded and content-free. Raw session,
    // conversation, and thread IDs never enter the artifact. Omit the
    // projection entirely for historical non-bridge fixtures.
    ...(spec.fixtureIdentityPhase
      ? {
        fixture_identity_phase: spec.fixtureIdentityPhase,
        fixture_identity_thread_stable: spec.fixtureIdentityThreadStable === true
      }
      : {}),
    regenerated_tool_pair_count: finiteNonNegativeNumber(spec.regeneratedToolPairCount) ?? 0,
    fixture_tool_protocol: normalizeToolProtocol(spec.fixtureToolProtocol) ?? null,
    cache_options_24h: cacheOptions24h,
    // Never retain raw runtime error strings: a transport library can echo an
    // endpoint or request material. These are allow-listed scopes and coarse
    // classes only, so a later self-control can discriminate a proxy/TLS/DNS
    // failure from a candidate wire regression without exposing live details.
    runtime_error_scopes: runtimeErrorEvidence.scopes,
    runtime_error_classes: runtimeErrorEvidence.classes,
    // These fields are bounded diagnostics only: they contain no request
    // body, tool text, credential, or cache-key value.  Retain them per
    // request so a large aggregate new-tail gap can be traced to one phase
    // without rerunning the arm blind.
    provider_prefix_fingerprint: metric?.provider_prefix_fingerprint ?? null,
    // The value itself is never retained in release evidence. This boolean
    // distinguishes an Atoapi-generated placement from an omitted field,
    // including at a compaction root.
    provider_prefix_key_present:
      typeof metric?.provider_prefix_key === "string" && metric.provider_prefix_key.length > 0,
    provider_prefix_key_fingerprint: opaquePlacementFingerprint(metric?.provider_prefix_key),
    outbound_prefix_fingerprints: metric?.outbound_prefix_fingerprints ?? null,
    prefix_lag_classification: metric?.prefix_lag_classification ?? null,
    prefix_lag_input_delta_tokens: number(metric?.prefix_lag_input_delta_tokens),
    prefix_lag_cache_delta_tokens: number(metric?.prefix_lag_cache_delta_tokens),
    prefix_lag_previous_gap_tokens: number(metric?.prefix_lag_previous_gap_tokens),
    prefix_cache_instability_score: number(metric?.prefix_cache_instability_score),
    prefix_state_cache_read_tokens: number(metric?.prefix_state_cache_read_tokens),
    input_tokens: number(metric?.input_tokens),
    cache_read_tokens: rawCacheReadTokens ?? 0,
    // Preserve whether the upstream metric was actually present.  A missing
    // counter must never later masquerade as a legitimate cold-cache zero.
    cache_read_tokens_observed: cacheReadTokensPresent,
    cache_avoidable_gap_tokens: number(metric?.cache_avoidable_gap_tokens),
    cache_new_tail_gap_tokens_observed:
      finiteNonNegativeNumber(metric?.cache_new_tail_gap_tokens) !== null,
    cache_new_tail_gap_tokens: number(metric?.cache_new_tail_gap_tokens),
    cache_provider_unstable_gap_tokens_observed:
      finiteNonNegativeNumber(metric?.cache_provider_unstable_gap_tokens) !== null,
    cache_provider_unstable_gap_tokens: number(metric?.cache_provider_unstable_gap_tokens),
    cache_shortfall_tokens: number(metric?.cache_shortfall_tokens),
    tail_input_items: number(metric?.tail_input_items),
    tail_message_chars: number(metric?.tail_message_chars),
    tail_tool_call_chars: number(metric?.tail_tool_call_chars),
    tail_tool_output_chars: number(metric?.tail_tool_output_chars),
    tail_largest_tool_output_chars: number(metric?.tail_largest_tool_output_chars),
    tail_tool_output_lines: number(metric?.tail_tool_output_lines),
    tail_tool_output_repeated_line_chars: number(metric?.tail_tool_output_repeated_line_chars),
    tail_tool_output_timestamp_like_count: number(metric?.tail_tool_output_timestamp_like_count),
    tail_tool_output_path_like_count: number(metric?.tail_tool_output_path_like_count),
    tail_tool_output_url_like_count: number(metric?.tail_tool_output_url_like_count),
    tail_tool_output_hash_like_count: number(metric?.tail_tool_output_hash_like_count),
    tail_tool_output_json_like_chars: number(metric?.tail_tool_output_json_like_chars),
    tail_tool_output_noise_hint: metric?.tail_tool_output_noise_hint ?? null,
    tail_source: metric?.tail_source ?? null,
    // The full ledger contains an opaque scope digest. Release evidence only
    // retains the bounded booleans and token waterlines needed to prove that
    // a candidate maturity gate actually ran on an exact direct successor.
    final_scope_waterline: finalScopeWaterline,
    // Keep local pre-upstream work separate from end-to-end provider timing.
    // Promotion can explicitly exempt remote TTFT variance, but it never
    // exempts an increase in any local component represented here.
    prefix_guard_wait_ms: timing.prefix_guard_wait_ms,
    prefix_guard_wait_reason: metric?.prefix_guard_wait_reason ?? null,
    prefix_guard_wait_source: metric?.prefix_guard_wait_source ?? null,
    prefix_guard_skip_reason: metric?.prefix_guard_skip_reason ?? null,
    static_wire_drift_late_mutation_categories:
      metric?.static_wire_drift_late_mutation_categories ?? null,
    local_prepare_ms: timing.local_prepare_ms,
    request_body_encode_ms: timing.request_body_encode_ms,
    gzip_encode_ms: timing.gzip_encode_ms,
    local_pre_upstream_overhead_ms: localPreUpstreamOverhead,
    upstream_headers_ms: timing.upstream_headers_ms,
    upstream_first_chunk_ms: timing.upstream_first_chunk_ms,
    upstream_ttft_ms: timing.upstream_ttft_ms,
    ttft_ms: timing.ttft_ms,
    upstream_attempt_index: metric?.upstream_attempt_index ?? null,
    upstream_attempt_total: metric?.upstream_attempt_total ?? null,
    outcome_attempt_count: outcome?.attempt_count ?? null,
    outcome_attempt_budget: outcome?.attempt_budget ?? null,
    matched_attempts: attempts.length,
    compacted_input: completedInput,
    checks
    },
    // The raw id is deliberately returned in an ephemeral sibling slot. The
    // record above is the only value retained in the run/report artifact.
    completedResponseId
  };
}

function buildResponsesRequestBody(spec) {
  const body = {
    model: spec.cohort.model,
    stream: true,
    // Codex emits store=false. Preserve that production wire contract in
    // release validation: several OpenAI-compatible relays reject the
    // implicit/default store mode before a generation is recorded.
    store: false,
    max_output_tokens: spec.maxOutputTokens,
    instructions: spec.instructions,
    input: spec.input
  };
  if (typeof spec.previousResponseId === "string" && spec.previousResponseId.length > 0) {
    body.previous_response_id = spec.previousResponseId;
  }
  if (spec.reasoningEffort) body.reasoning = { effort: spec.reasoningEffort };
  if (Array.isArray(spec.tools) && spec.tools.length > 0) body.tools = spec.tools;
  if (spec.toolChoice !== undefined && spec.toolChoice !== null) {
    body.tool_choice = spec.toolChoice;
  }
  if (spec.promptCacheKey) body.prompt_cache_key = spec.promptCacheKey;
  return body;
}

// Tool-history scenarios replay a completed call and its output as part of the
// input history. An explicit schema probe declares that tool from the seed
// onward as a real Codex request does; some Responses-compatible relays reject
// an otherwise valid historical call when its name is absent from the top-level
// tools schema. Leaving the probe off preserves the prior fixture wire.
// This is verifier-fixture compatibility only, never a production rewrite.
function releaseFixtureToolsForScenario(
  scenario,
  dynamicTailMode = "tool",
  includeToolSchema = true,
  toolProtocol = "function"
) {
  if (!includeToolSchema) return [];
  if (scenario === "dynamic-tail-mix" && dynamicTailMode === "text") return [];
  if (!new Set(["dynamic-tail-mix", "tool-burst", "tool-tail-maturity"]).has(scenario)) {
    return [];
  }
  const fixtureTool = releaseFixtureToolProtocol(toolProtocol);
  if (fixtureTool.protocol === "custom") {
    return [{
      type: "custom",
      name: "read_release_fixture",
      description: "Read a deterministic release-validation fixture."
    }];
  }
  return [{
    type: "function",
    name: "read_release_fixture",
    description: "Read a deterministic release-validation fixture.",
    parameters: {
      type: "object",
      properties: {
        part: { type: "integer", minimum: 1 },
        total_parts: { type: "integer", minimum: 1 }
      },
      additionalProperties: false
    }
  }];
}

async function waitForSettledInbound({ baseUrl, localKey, beforeCounters, knownRawInboundIds }) {
  const deadline = Date.now() + 15_000;
  let latest = null;
  do {
    latest = await getJson(`${baseUrl}/admin/metrics`, 5_000, localKey);
    const counters = requestCounters(latest);
    const hasNewRequest = requestLogRows(latest).some((item) => {
      const id = String(item?.inbound_request_id ?? "");
      return id && !knownRawInboundIds.has(id);
    });
    if (
      counters.inbound_requests >= beforeCounters.inbound_requests + 1 &&
      counters.generation_attempts >= beforeCounters.generation_attempts + 1 &&
      counters.upstream_requests >= beforeCounters.upstream_requests + 1 &&
      hasNewRequest
    ) {
      return latest;
    }
    await delay(50);
  } while (Date.now() < deadline);
  return latest ?? { agent_generation: {}, recent_requests: [] };
}

async function responseDeadlineDiagnostics({
  runtime,
  metrics,
  knownRawInboundIds,
  metric,
  responseTimeoutMs
}) {
  const recentFailedRows = array(metrics?.recent_failed_requests);
  const newFailedRequestLogSeen = recentFailedRows.some((item) => {
    const id = String(item?.inbound_request_id ?? "");
    return id && !knownRawInboundIds.has(id);
  });
  let healthOkAfterAbort = false;
  try {
    const health = await getJson(`${runtime.baseUrl}/health`, 1_000);
    healthOkAfterAbort = health?.ok === true;
  } catch {
    // The health probe is intentionally a boolean-only diagnostic.  Never
    // retain a transport error or endpoint detail in release evidence.
  }
  const agentGeneration = metrics?.agent_generation ?? {};
  const transport = transportEvidence(metric);
  const transportMetricPresent = Object.values(transport).some((value) => value !== null);
  return {
    response_deadline_exceeded: true,
    response_deadline_ms: responseTimeoutMs,
    child_alive_after_abort: processIsAlive(runtime?.child?.pid),
    health_ok_after_abort: healthOkAfterAbort,
    metrics_snapshot_reachable: Boolean(metrics && typeof metrics === "object"),
    active_agent_inbounds_after_abort: finiteNonNegativeNumber(agentGeneration.active_inbounds),
    active_agent_attempts_after_abort: finiteNonNegativeNumber(agentGeneration.active_attempts),
    new_request_log_seen: Boolean(metric),
    new_failed_request_log_seen: newFailedRequestLogSeen,
    transport_metric_present: transportMetricPresent
  };
}

function selectNewRequestLog(metrics, knownRawInboundIds) {
  return requestLogRows(metrics).find((item) => {
    const id = String(item?.inbound_request_id ?? "");
    return id && !knownRawInboundIds.has(id);
  }) ?? null;
}

function transportEvidence(metric) {
  return {
    upstream_http_version: safeTransportLabel(metric?.upstream_http_version),
    upstream_network_path: safeTransportLabel(metric?.upstream_network_path),
    upstream_header_wait_class: safeTransportLabel(metric?.upstream_header_wait_class),
    request_body_bytes: finiteNonNegativeNumber(metric?.request_body_bytes),
    sent_body_bytes: finiteNonNegativeNumber(metric?.sent_body_bytes),
    request_body_encode_ms: finiteNonNegativeNumber(metric?.request_body_encode_ms),
    gzip_encode_ms: finiteNonNegativeNumber(metric?.gzip_encode_ms),
    gzip_attempted: metric?.gzip_attempted === true,
    gzip_fallback_used: metric?.gzip_fallback_used === true,
    upstream_attempt_headers_ms: finiteNonNegativeNumber(metric?.upstream_attempt_headers_ms),
    stream_upstream_wait_ms: finiteNonNegativeNumber(metric?.stream_upstream_wait_ms),
    stream_client_backpressure_ms: finiteNonNegativeNumber(metric?.stream_client_backpressure_ms),
    downstream_disconnected: metric?.downstream_disconnected === true,
    sse_chunks: finiteNonNegativeNumber(metric?.sse_chunks)
  };
}

function upstreamAffinityTestEvidence(value) {
  const diagnostic = String(value ?? "");
  return {
    test_enabled: /:affinity-(?:none|learned|injected|injected-learned)(?:$|:)/u.test(diagnostic),
    learned: /:affinity-(?:learned|injected-learned)(?:$|:)/u.test(diagnostic),
    injected: /:affinity-injected(?:-learned)?(?:$|:)/u.test(diagnostic)
  };
}

function cacheControlFieldEvidence(metric) {
  const allowed = new Set([
    "prompt-cache-key",
    "prompt-cache-retention",
    "prompt-cache-options",
    "prompt-cache-breakpoint"
  ]);
  const fields = array(metric?.sent_cache_capability_fields).map(
    (item) => String(item ?? "")
  ).filter(
    (field) => allowed.has(field)
  );
  const diagnostic = String(metric?.upstream_pool_diagnostic ?? "");
  if (/:cache-options(?:-24h)?-injected(?:$|:)/u.test(diagnostic)) {
    fields.push("prompt-cache-options");
  }
  if (/:cache-key-injected(?:$|:)/u.test(diagnostic)) {
    fields.push("prompt-cache-key");
  }
  if (/:cache-retention-injected(?:$|:)/u.test(diagnostic)) {
    fields.push("prompt-cache-retention");
  }
  return unique(fields);
}

// The final-wire diagnostic is deliberately payload-free. It proves the
// isolated 24h variant survived all compatibility/rebuild paths without
// retaining the options object itself in a release artifact.
function cacheOptions24hEvidence(metric) {
  return /:cache-options-24h-injected(?:$|:)/u.test(
    String(metric?.upstream_pool_diagnostic ?? "")
  );
}

function finalScopeWaterlineEvidence(value) {
  if (!value || typeof value !== "object" || Array.isArray(value)) return null;
  return {
    outcome: safeLiveCodexLabel(value.outcome),
    sent_prediction_eligible: value.sent_prediction_eligible === true,
    predecessor_proof: safeLiveCodexLabel(value.predecessor_proof),
    predecessor_exact: value.predecessor_exact === true,
    predecessor_bound: value.predecessor_bound === true,
    continuity_reset: value.continuity_reset === true,
    raw_input_tokens: number(value.raw_input_tokens),
    raw_cache_read_tokens: number(value.raw_cache_read_tokens),
    sent_prefix_bucket_tokens_128: number(value.sent_prefix_bucket_tokens_128),
    settled_prefix_bucket_tokens_128: number(value.settled_prefix_bucket_tokens_128),
    candidate_avoidable_tokens_128: number(value.candidate_avoidable_tokens_128),
    rollback_tokens_128: number(value.rollback_tokens_128)
  };
}

function errorFingerprint(error) {
  return sha256Parts([
    String(error?.at ?? ""),
    String(error?.scope ?? ""),
    String(error?.message ?? "")
  ]);
}

function freshRuntimeErrorEvidence(metrics, knownFingerprints) {
  const scopes = [];
  const classes = [];
  for (const error of array(metrics?.recent_errors)) {
    if (knownFingerprints.has(errorFingerprint(error))) continue;
    const scope = safeRuntimeErrorScope(error?.scope);
    if (scope) scopes.push(scope);
    const category = runtimeErrorCategory(error?.message);
    if (category) classes.push(category);
  }
  return {
    scopes: unique(scopes).slice(0, 4),
    classes: unique(classes).slice(0, 4)
  };
}

function safeRuntimeErrorScope(value) {
  const normalized = String(value ?? "").trim();
  return /^[A-Za-z0-9_:-]{1,96}$/u.test(normalized) ? normalized : null;
}

function runtimeErrorCategory(value) {
  const normalized = String(value ?? "").toLowerCase();
  if (!normalized) return null;
  if (/(?:dns|name or service|failed to lookup|could not resolve)/u.test(normalized)) {
    return "dns";
  }
  if (/(?:tls|ssl|certificate|handshake)/u.test(normalized)) return "tls";
  if (/(?:proxy|tunnel)/u.test(normalized)) return "proxy";
  if (/(?:timed?\s*out|timeout|deadline)/u.test(normalized)) return "timeout";
  if (/(?:connection|connect|broken pipe|reset by peer|connection refused)/u.test(normalized)) {
    return "connection";
  }
  if (/(?:http|bad gateway|status code)/u.test(normalized)) return "http";
  return "other";
}

function classifyDownstreamTransportError(error) {
  const name = String(error?.name ?? "").toLowerCase();
  const code = String(error?.code ?? "").toLowerCase();
  const message = String(error?.message ?? error ?? "").toLowerCase();
  if (name === "aborterror" || code === "abort_err" || /aborted|abort/u.test(message)) {
    return "aborted";
  }
  if (/(?:timeout|timed out|deadline)/u.test(message)) return "timeout";
  if (/(?:dns|resolve|name or service)/u.test(message)) return "dns";
  if (/(?:tls|ssl|certificate|handshake)/u.test(message)) return "tls";
  if (/(?:proxy|tunnel)/u.test(message)) return "proxy";
  if (/(?:connect|connection|socket|reset|broken pipe)/u.test(message)) return "connection";
  return "other";
}

function safeTransportLabel(value) {
  const normalized = String(value ?? "").trim();
  // Deliberately reject whitespace, URLs, query strings, and anything that
  // might carry an endpoint or user-controlled header value.
  return /^[A-Za-z0-9._:/-]{1,160}$/u.test(normalized) ? normalized : null;
}

function requestLogRows(metrics) {
  return [
    ...array(metrics?.recent_requests),
    ...array(metrics?.recent_failed_requests)
  ];
}

function usageShapeHasNumericToken(value) {
  if (value === null || typeof value !== "object") return false;
  if (Array.isArray(value)) return value.some(usageShapeHasNumericToken);
  return Object.entries(value).some(([key, child]) =>
    (/token/u.test(key) && explicitFiniteNonNegativeNumber(child) !== null) ||
    usageShapeHasNumericToken(child)
  );
}

function parseSseDataEvents(responseText) {
  const events = [];
  let eventName = "";
  let dataLines = [];
  const flush = () => {
    if (dataLines.length === 0) return;
    const candidates = [dataLines.join("\n"), ...dataLines];
    const parsedValues = [];
    for (const candidate of candidates) {
      if (!candidate || candidate === "[DONE]") continue;
      try {
        const parsed = JSON.parse(candidate);
        if (parsed && typeof parsed === "object") parsedValues.push(parsed);
      } catch {
        // A fragmented or non-JSON SSE line is not a usable response event.
      }
    }
    for (const parsed of parsedValues) events.push({ eventName, parsed });
    eventName = "";
    dataLines = [];
  };
  for (const line of String(responseText ?? "").split(/\r?\n/u)) {
    if (!line.trim()) {
      flush();
      continue;
    }
    if (/^event:\s*/u.test(line)) {
      if (dataLines.length > 0) flush();
      eventName = line.replace(/^event:\s*/u, "").trim();
      continue;
    }
    if (/^data:\s*/u.test(line)) {
      dataLines.push(line.replace(/^data:\s*/u, "").trim());
    }
  }
  flush();
  return events;
}

function extractCompletedResponseId(responseText) {
  for (const event of parseSseDataEvents(responseText)) {
    const hasExplicitEventName = Boolean(event.eventName);
    const isCompleted = hasExplicitEventName
      ? event.eventName === "response.completed"
      : event.parsed?.type === "response.completed";
    if (!isCompleted) continue;
    const id = event.parsed?.response?.id ?? event.parsed?.data?.response?.id;
    if (typeof id === "string" && /^[A-Za-z0-9._:-]{1,200}$/u.test(id)) return id;
  }
  return null;
}

function terminalUsageShape(responseText) {
  const blocks = String(responseText ?? "").split(/\r?\n\r?\n+/u);
  let terminalSeen = false;
  for (const block of blocks) {
    const terminalEvent = /(?:^|\n)event:\s*response\.completed\b/u.test(block) ||
      /"type"\s*:\s*"response\.completed"/u.test(block);
    if (!terminalEvent) continue;
    terminalSeen = true;
    const dataText = block
      .split(/\r?\n/u)
      .filter((line) => /^data:\s*/u.test(line))
      .map((line) => line.replace(/^data:\s*/u, "").trim())
      .join("\n");
    let parsed = null;
    for (const candidate of [dataText, block]) {
      if (!candidate) continue;
      try {
        parsed = JSON.parse(candidate);
        break;
      } catch {
        // A malformed/unknown terminal payload is retained only as a shape
        // classification; the payload itself never enters the artifact.
      }
    }
    const usage = parsed && typeof parsed === "object"
      ? (parsed.response?.usage ?? parsed.usage ?? parsed.data?.response?.usage)
      : undefined;
    if (usage !== undefined) {
      return usageShapeHasNumericToken(usage) ? "present" : "unrecognized";
    }
    if (/"usage"\s*:/u.test(block)) return "unrecognized";
  }
  return terminalSeen ? "absent" : "not_seen";
}

function responseErrorCode(responseText) {
  const text = String(responseText ?? "");
  const responseFailed = text.indexOf("response.failed");
  const failedPayload = responseFailed >= 0
    ? text.slice(responseFailed, responseFailed + 1_024)
    : text.match(/"error"\s*:\s*\{[^}]{0,1024}\}/u)?.[0] ?? "";
  const code = failedPayload.match(/"code"\s*:\s*"([A-Za-z0-9._-]{1,96})"/u);
  if (code?.[1]) return code[1];
  const type = failedPayload.match(/"type"\s*:\s*"([A-Za-z0-9._-]{1,96})"/u);
  return type?.[1] ?? null;
}

function responseErrorKind(responseText, responseFailureCode = responseErrorCode(responseText)) {
  const normalized = String(responseText ?? "").toLowerCase();
  if (responseFailureCode === "atoapi_error") {
    // Atoapi deliberately emits one public error type. Preserve the privacy
    // boundary, but retain a bounded cause category so a failed isolated
    // champion run is not mistaken for a cache result.
    if (/failed to select provider key|provider key is not configured/u.test(normalized)) {
      return "provider_key_selection";
    }
    if (/(?:upstream request failed|transport|connect|connection|dns|tls|proxy|timeout)/u.test(normalized)) {
      return "upstream_transport";
    }
    if (/(?:decrypt|dpapi|secret.*(?:unavailable|invalid)|key.*(?:decrypt|unavailable))/u.test(normalized)) {
      return "local_secret_unavailable";
    }
    if (/(?:api key|credential|authorization).*?(?:missing|not configured|invalid)/u.test(normalized)) {
      return "authentication";
    }
  }
  if (responseFailureCode === "upstream_sse_error") {
    // The relay deliberately replaces the raw upstream error with one native
    // terminal code. Keep that privacy boundary, but retain only a small,
    // allow-listed cause category so a failed follow-up is actionable without
    // persisting its message, body, endpoint, or any fixture content.
    if (/(?:\b(?:context|input|payload|body|request)(?:[ _-]*(?:token|size|length))?\b[^\n]{0,80}\b(?:limit|exceed(?:ed|s)?|too large|too many|max(?:imum)?))|(?:\b(?:maximum|max)[ _-]*(?:input|context|request|payload|tokens?)\b)/u.test(normalized)) {
      return "upstream_sse_error:payload_limit";
    }
    if (/(?:unsupported|unknown|invalid)\s+(?:parameter|field)|(?:parameter|field)\s+(?:unsupported|unknown)/u.test(normalized)) {
      return "upstream_sse_error:unsupported_parameter";
    }
    if (/\b(?:authentication|unauthorized|forbidden|api[ _-]?key)\b|\b(?:access|bearer)[ _-]?token\b[^\n]{0,80}\b(?:invalid|expired|missing|unavailable)\b/u.test(normalized)) {
      return "upstream_sse_error:authentication";
    }
    if (/(?:rate.?limit|quota|too many requests)/u.test(normalized)) {
      return "upstream_sse_error:rate_limited";
    }
    if (/(?:overloaded|capacity|temporarily unavailable|server busy)/u.test(normalized)) {
      return "upstream_sse_error:capacity";
    }
    if (/(?:waf|request blocked|blocked by)/u.test(normalized)) {
      return "upstream_sse_error:request_blocked";
    }
  }
  if (responseFailureCode) return `code:${responseFailureCode}`;
  if (/\b(?:context|token|payload|body size|request size|too large|too many|maximum input)\b/u.test(normalized)) {
    return "payload_limit";
  }
  if (/(?:unsupported|unknown|invalid)\s+(?:parameter|field)|(?:parameter|field)\s+(?:unsupported|unknown)/u.test(normalized)) {
    return "unsupported_parameter";
  }
  if (/\b(?:authentication|unauthorized|forbidden|api[ _-]?key)\b/u.test(normalized)) {
    return "authentication";
  }
  if (/\bmodel\b/u.test(normalized)) return "model_rejected";
  return normalized ? "unclassified" : null;
}

function responseHasNativeFailure(responseText) {
  const text = String(responseText ?? "");
  return /(?:^|\n)event:\s*response\.failed\b/u.test(text) ||
    /"type"\s*:\s*"response\.failed"/u.test(text);
}

function inboundFailureReason({
  transportError,
  responseDeadlineExceeded = false,
  responseStatus,
  responseFailureCode,
  responseFailed,
  checks
}) {
  if (responseDeadlineExceeded) return "client_deadline_exceeded";
  if (transportError) return "downstream_transport_failed";
  if (!(responseStatus >= 200 && responseStatus < 300)) {
    const suffix = responseFailureCode ? `:${responseFailureCode}` : "";
    return `http_status_${responseStatus || 0}${suffix}`;
  }
  if (responseFailed) {
    return `response_failed:${responseFailureCode ?? "unknown"}`;
  }
  return Object.entries(checks).find(([, passed]) => !passed)?.[0] ?? null;
}

// Static request members must remain stable while a semantic epoch is being
// replayed. A compaction request starts a new epoch by definition, so it is a
// legal boundary at which the baseline is reset. Only bounded hashes and
// field names are retained; no request body or user content is inspected.
function staticWireContinuity(requests) {
  const drift = new Set();
  let missing = false;
  let baseline = null;
  let boundaries = 0;

  for (const request of requests) {
    const epochBoundary = request.request_kind === "compaction" || request.phase === "compaction";
    if (!epochBoundary && request.prefix_lag_classification === "static_wire_drift") {
      drift.add("server_static_wire_drift");
    }
    if (!epochBoundary && array(request.static_wire_drift_late_mutation_categories).length > 0) {
      drift.add("late_mutation");
    }
    const fingerprints = request?.outbound_prefix_fingerprints;
    if (!fingerprints || STATIC_WIRE_FIELDS.some((field) => !fingerprints[field])) {
      missing = true;
      baseline = null;
      continue;
    }

    if (epochBoundary) {
      baseline = null;
      boundaries += 1;
    }

    const current = Object.fromEntries(
      STATIC_WIRE_FIELDS.map((field) => [field, fingerprints[field]])
    );
    if (baseline) {
      for (const field of STATIC_WIRE_FIELDS) {
        if (current[field] !== baseline[field]) drift.add(field);
      }
    }
    baseline = current;
  }

  return {
    pass: requests.length > 0 && !missing && drift.size === 0,
    drift_categories: [...drift].sort(),
    missing,
    epoch_boundaries: boundaries
  };
}

function dynamicTailRecoverySummary(events, requests) {
  const injected = array(events);
  const shapeCounts = {};
  let followupsObserved = 0;
  let followupNewTailTokens = 0;
  let followupProviderUnstableTokens = 0;
  let followupTailLagCount = 0;
  for (const event of injected) {
    shapeCounts[event.shape] = (shapeCounts[event.shape] ?? 0) + 1;
    const injectionIndex = requests.findIndex((request) => request.phase === event.phase);
    const followup = injectionIndex >= 0 ? requests[injectionIndex + 1] : null;
    if (!followup) continue;
    followupsObserved += 1;
    followupNewTailTokens += number(followup.cache_new_tail_gap_tokens);
    followupProviderUnstableTokens += number(followup.cache_provider_unstable_gap_tokens);
    if (String(followup.prefix_lag_classification ?? "").startsWith("tail_lag_")) {
      followupTailLagCount += 1;
    }
  }
  return {
    injections: injected.length,
    injected_characters: injected.reduce((total, event) => total + number(event.target_chars), 0),
    shape_counts: shapeCounts,
    followups_observed: followupsObserved,
    followup_new_tail_tokens: followupNewTailTokens,
    followup_provider_unstable_tokens: followupProviderUnstableTokens,
    followup_tail_lag_count: followupTailLagCount
  };
}

function counterObservationKind(request, counterName, observedName) {
  const hasFiniteValue = Number.isFinite(Number(request?.[counterName]));
  if (request?.[observedName] === true) return hasFiniteValue ? "explicit" : "missing";
  if (request?.[observedName] === false) return "missing";
  // Older artifacts did not preserve the explicit observed bit.  They remain
  // useful for a historical readback, but never carry the same confidence as
  // a newly captured, explicitly observed counter.
  return Object.prototype.hasOwnProperty.call(request ?? {}, counterName) && hasFiniteValue
    ? "legacy_inferred"
    : "missing";
}

function allCounterObservations(requests, counterName, observedName) {
  const kinds = array(requests).map((request) =>
    counterObservationKind(request, counterName, observedName)
  );
  return {
    complete: kinds.length > 0 && kinds.every((kind) => kind !== "missing"),
    explicit: kinds.length > 0 && kinds.every((kind) => kind === "explicit"),
    legacy_inferred: kinds.some((kind) => kind === "legacy_inferred"),
    kinds
  };
}

// Keep the raw provider result separate from the portion that remains after a
// recorded provider-side cache rollback.  This is an attribution view only:
// it never replaces the raw cache score and can never promote a release by
// itself.  Its purpose is to stop a transient upstream waterline from being
// mistaken for a binary-level hit-rate improvement.
function providerExcludedWarmCache128(warmRequests) {
  // Do not infer how many cached tokens a partially unstable request *would*
  // have read.  Exclude that entire request instead, so this diagnostic is
  // based only on observations untouched by a recorded provider rollback.
  const providerCleanRequests = array(warmRequests).filter(
    (request) =>
      cacheableInputTokens128(number(request?.input_tokens)) > 0 &&
      number(request?.cache_provider_unstable_gap_tokens) === 0
  );
  const providerExcludedCacheableTokens = providerCleanRequests.reduce(
    (total, request) => total + cacheableInputTokens128(number(request?.input_tokens)),
    0
  );
  const providerExcludedCachedTokens = providerCleanRequests.reduce(
    (total, request) => total + Math.min(
      number(request?.cache_read_tokens),
      cacheableInputTokens128(number(request?.input_tokens))
    ),
    0
  );
  return {
    // "provider_clean" is the canonical name.  The older "excluded" aliases
    // remain for already-written evidence consumers.
    provider_clean_warm_cacheable_tokens_128: providerExcludedCacheableTokens,
    provider_clean_warm_cacheable_read_tokens_128: providerExcludedCachedTokens,
    provider_clean_warm_cache_128_hit_rate: ratio(
      providerExcludedCachedTokens,
      providerExcludedCacheableTokens
    ),
    provider_excluded_warm_cacheable_tokens_128: providerExcludedCacheableTokens,
    provider_excluded_warm_cacheable_read_tokens_128: providerExcludedCachedTokens,
    provider_excluded_warm_cache_128_hit_rate: ratio(
      providerExcludedCachedTokens,
      providerExcludedCacheableTokens
    )
  };
}

// Dynamic-tail production work needs a stronger proof than a whole-run ratio:
// inspect the retained, scored warm requests per pair.  A mismatch is useful
// diagnostic evidence, but it is never silently normalized into a winner.
function dynamicRunWarmAttribution(run) {
  const warmRequests = array(run?.requests).filter(
    (request) => request?.phase !== "seed" && number(request?.input_tokens) > 0
  );
  const cacheableRequests = warmRequests.filter(
    (request) => cacheableInputTokens128(number(request?.input_tokens)) > 0
  );
  if (warmRequests.length === 0 || cacheableRequests.length === 0) return null;
  const newTailObservation = allCounterObservations(
    warmRequests,
    "cache_new_tail_gap_tokens",
    "cache_new_tail_gap_tokens_observed"
  );
  const providerUnstableObservation = allCounterObservations(
    warmRequests,
    "cache_provider_unstable_gap_tokens",
    "cache_provider_unstable_gap_tokens_observed"
  );
  const warmCacheableTokens = cacheableRequests.reduce(
    (total, request) => total + cacheableInputTokens128(number(request?.input_tokens)),
    0
  );
  const warmCacheableReadTokens = cacheableRequests.reduce(
    (total, request) => total + Math.min(
      number(request?.cache_read_tokens),
      cacheableInputTokens128(number(request?.input_tokens))
    ),
    0
  );
  const newTailGapTokens = sum(warmRequests, "cache_new_tail_gap_tokens");
  const providerUnstableGapTokens = sum(
    warmRequests,
    "cache_provider_unstable_gap_tokens"
  );
  const providerCleanNewTailGapTokens = sum(
    warmRequests.filter(
      (request) => number(request?.cache_provider_unstable_gap_tokens) === 0
    ),
    "cache_new_tail_gap_tokens"
  );
  return {
    counter_observations_complete:
      newTailObservation.complete && providerUnstableObservation.complete,
    counter_observations_explicit:
      newTailObservation.explicit && providerUnstableObservation.explicit,
    counter_observations_legacy_inferred:
      newTailObservation.legacy_inferred || providerUnstableObservation.legacy_inferred,
    new_tail_gap_observation_kinds: newTailObservation.kinds,
    provider_unstable_gap_observation_kinds: providerUnstableObservation.kinds,
    warm_cacheable_tokens_128: warmCacheableTokens,
    warm_cacheable_read_tokens_128: warmCacheableReadTokens,
    new_tail_gap_tokens: newTailGapTokens,
    provider_clean_new_tail_gap_tokens: providerCleanNewTailGapTokens,
    provider_excluded_new_tail_gap_tokens: providerCleanNewTailGapTokens,
    provider_unstable_gap_tokens: providerUnstableGapTokens,
    ...providerExcludedWarmCache128(warmRequests)
  };
}

function pairedDynamicTailAttribution(champion, candidate) {
  const championRuns = array(champion?.runs).filter(
    (run) => run?.scenario === "dynamic-tail-mix"
  );
  const candidateRuns = array(candidate?.runs).filter(
    (run) => run?.scenario === "dynamic-tail-mix"
  );
  const applicable = championRuns.length > 0 || candidateRuns.length > 0;
  if (!applicable) {
    return {
      applicable: false,
      complete: true,
      counter_observations_explicit: true,
      pair_count: 0,
      scored_pair_ids: [],
      pairs: [],
      candidate_new_tail_delta_tokens: 0,
      candidate_provider_clean_new_tail_delta_tokens: 0,
      // Compatibility alias for reports written before provider-clean naming.
      candidate_provider_excluded_new_tail_delta_tokens: 0,
      candidate_provider_unstable_delta_tokens: 0,
      provider_instability_free: true,
      provider_instability_state: "not_applicable",
      candidate_new_tail_direction: "not_applicable",
      candidate_provider_clean_new_tail_direction: "not_applicable",
      candidate_new_tail_non_regressing_every_pair: true,
      candidate_provider_clean_new_tail_non_regressing_every_pair: true,
      candidate_new_tail_strictly_improves_consistently: false,
      candidate_new_tail_improvement_hypothesis: false,
      candidate_new_tail_confirmed_improvement: false,
      hypothesis_only: false
    };
  }

  const championByPair = new Map(championRuns.map((run) => [run?.pair, run]));
  const candidateByPair = new Map(candidateRuns.map((run) => [run?.pair, run]));
  const pairIds = pairedRunIds(championRuns, candidateRuns);
  const uniqueChampionPairs = new Set(championRuns.map((run) => run?.pair));
  const uniqueCandidatePairs = new Set(candidateRuns.map((run) => run?.pair));
  let complete =
    championRuns.length > 0 &&
    candidateRuns.length > 0 &&
    championRuns.length === candidateRuns.length &&
    uniqueChampionPairs.size === championRuns.length &&
    uniqueCandidatePairs.size === candidateRuns.length &&
    pairIds.length === championRuns.length;
  const pairs = [];
  for (const pair of pairIds) {
    const championAttribution = dynamicRunWarmAttribution(championByPair.get(pair));
    const candidateAttribution = dynamicRunWarmAttribution(candidateByPair.get(pair));
    if (!championAttribution || !candidateAttribution) {
      complete = false;
      pairs.push({ pair, complete: false });
      continue;
    }
    const newTailDelta =
      candidateAttribution.new_tail_gap_tokens - championAttribution.new_tail_gap_tokens;
    const counterObservationsComplete =
      championAttribution.counter_observations_complete &&
      candidateAttribution.counter_observations_complete;
    const counterObservationsExplicit =
      championAttribution.counter_observations_explicit &&
      candidateAttribution.counter_observations_explicit;
    const providerInstabilityObserved = counterObservationsComplete;
    const providerClean =
      providerInstabilityObserved &&
      number(championAttribution.provider_unstable_gap_tokens) === 0 &&
      number(candidateAttribution.provider_unstable_gap_tokens) === 0;
    if (!counterObservationsComplete) complete = false;
    pairs.push({
      pair,
      complete: counterObservationsComplete,
      counter_observations_complete: counterObservationsComplete,
      counter_observations_explicit: counterObservationsExplicit,
      champion: championAttribution,
      candidate: candidateAttribution,
      candidate_new_tail_delta_tokens: newTailDelta,
      candidate_new_tail_direction:
        newTailDelta < 0 ? "candidate_lower" : newTailDelta > 0 ? "candidate_higher" : "tie",
      provider_instability_observed: providerInstabilityObserved,
      provider_clean: providerClean,
      // A provider-clean pair keeps both request sequences intact.  We never
      // invent a normalized score by subtracting some tokens from an unstable
      // request or by comparing a different subset on each arm.
      candidate_provider_clean_new_tail_delta_tokens: providerClean ? newTailDelta : null,
      candidate_provider_excluded_new_tail_delta_tokens: providerClean ? newTailDelta : null,
      candidate_provider_clean_new_tail_direction: !providerInstabilityObserved
        ? "unknown"
        : providerClean
          ? (newTailDelta < 0 ? "candidate_lower" : newTailDelta > 0 ? "candidate_higher" : "tie")
          : "not_comparable",
      candidate_provider_unstable_delta_tokens:
        candidateAttribution.provider_unstable_gap_tokens -
        championAttribution.provider_unstable_gap_tokens
    });
  }
  const completePairs = pairs.filter((pair) => pair.complete === true);
  const rawNewTailDeltas = completePairs.map(
    (pair) => number(pair.candidate_new_tail_delta_tokens)
  );
  const providerCleanPairs = completePairs.filter((pair) => pair.provider_clean === true);
  const providerCleanNewTailDeltas = providerCleanPairs.map(
    (pair) => number(pair.candidate_provider_clean_new_tail_delta_tokens)
  );
  const counterObservationsExplicit =
    complete && completePairs.length > 0 &&
    completePairs.every((pair) => pair.counter_observations_explicit === true);
  const providerInstabilityFree =
    complete && completePairs.length > 0 &&
    completePairs.every((pair) => pair.provider_clean === true);
  const candidateNewTailNonRegressingEveryPair =
    complete && completePairs.length > 0 && rawNewTailDeltas.every((delta) => delta <= 0);
  const candidateProviderCleanNewTailNonRegressingEveryPair =
    providerInstabilityFree && providerCleanNewTailDeltas.every((delta) => delta <= 0);
  const candidateNewTailStrictlyImprovesConsistently =
    providerInstabilityFree && candidateNewTailNonRegressingEveryPair &&
    rawNewTailDeltas.some((delta) => delta < 0);
  const directionFor = (deltas, fallback) => !complete || deltas.length === 0
    ? fallback
    : deltas.every((delta) => delta <= 0) && deltas.some((delta) => delta < 0)
      ? "candidate_lower"
      : deltas.every((delta) => delta === 0)
        ? "tie"
        : deltas.every((delta) => delta >= 0) && deltas.some((delta) => delta > 0)
          ? "candidate_higher"
          : "mixed";
  const candidateNewTailDirection = directionFor(rawNewTailDeltas, "incomplete");
  const candidateProviderCleanNewTailDirection = providerInstabilityFree
    ? directionFor(providerCleanNewTailDeltas, "not_available")
    : complete
      ? "not_comparable"
      : "incomplete";
  const candidateNewTailDeltaTokens = sum(
    completePairs,
    "candidate_new_tail_delta_tokens"
  );
  const candidateProviderCleanNewTailDeltaTokens = providerCleanPairs.length > 0
    ? sum(providerCleanPairs, "candidate_provider_clean_new_tail_delta_tokens")
    : null;
  const candidateProviderUnstableDeltaTokens = sum(
    completePairs,
    "candidate_provider_unstable_delta_tokens"
  );
  const hypothesisOnly =
    !complete ||
    !counterObservationsExplicit ||
    !providerInstabilityFree ||
    candidateNewTailDirection === "mixed";
  const candidateNewTailImprovementHypothesis =
    candidateNewTailDeltaTokens < 0 && hypothesisOnly;
  const candidateNewTailConfirmedImprovement =
    candidateNewTailDeltaTokens < 0 && !hypothesisOnly &&
    candidateNewTailStrictlyImprovesConsistently;
  const providerInstabilityState = !complete
    ? "incomplete"
    : !counterObservationsExplicit
      ? "legacy_or_missing_counter"
      : providerInstabilityFree
        ? "clean"
        : "unstable";
  return {
    applicable: true,
    complete,
    counter_observations_explicit: counterObservationsExplicit,
    pair_count: pairIds.length,
    scored_pair_ids: pairIds,
    pairs,
    candidate_new_tail_delta_tokens: candidateNewTailDeltaTokens,
    candidate_provider_clean_new_tail_delta_tokens:
      candidateProviderCleanNewTailDeltaTokens,
    candidate_provider_excluded_new_tail_delta_tokens:
      candidateProviderCleanNewTailDeltaTokens,
    candidate_provider_unstable_delta_tokens: candidateProviderUnstableDeltaTokens,
    provider_instability_free: providerInstabilityFree,
    provider_instability_state: providerInstabilityState,
    candidate_new_tail_direction: candidateNewTailDirection,
    candidate_provider_clean_new_tail_direction: candidateProviderCleanNewTailDirection,
    candidate_new_tail_non_regressing_every_pair: candidateNewTailNonRegressingEveryPair,
    candidate_provider_clean_new_tail_non_regressing_every_pair:
      candidateProviderCleanNewTailNonRegressingEveryPair,
    candidate_new_tail_strictly_improves_consistently:
      candidateNewTailStrictlyImprovesConsistently,
    candidate_new_tail_improvement_hypothesis: candidateNewTailImprovementHypothesis,
    candidate_new_tail_confirmed_improvement: candidateNewTailConfirmedImprovement,
    // Only a stable, provider-clean, pair-consistent new-tail improvement can
    // justify proposing a production dynamic-tail policy change.  This is
    // deliberately diagnostic; the ordinary release promotion gate remains
    // based on raw complete A/B evidence.
    hypothesis_only: hypothesisOnly
  };
}

function crossoverPhaseLabels(
  scenario,
  turn,
  lateShallowProviderWaterlineRollback = false
) {
  const labels = turn === 0 ? ["pair-start"] : [];
  if (
    scenario === "dynamic-tail-mix" &&
    turn > 0 &&
    turn % 2 === 1 &&
    !(lateShallowProviderWaterlineRollback && turn === 3)
  ) {
    labels.push(`dynamic-tail-${(turn + 1) / 2}`);
  }
  if (scenario === "dynamic-tail-mix" && lateShallowProviderWaterlineRollback) {
    if (turn === 2) labels.push("late-shallow-residual-witness");
    if (turn === 3) labels.push("late-shallow-delayed-direct-child");
  }
  return labels;
}

function validArmOrder(order) {
  const [firstArm, secondArm] = array(order);
  return (firstArm === "champion" || firstArm === "candidate") &&
    (secondArm === "champion" || secondArm === "candidate") &&
    firstArm !== secondArm;
}

function sameToolTailMaturityDispatch(actual, expected) {
  return actual?.turn === expected?.turn &&
    actual?.arm === expected?.arm &&
    actual?.stage === expected?.stage &&
    (actual?.role ?? null) === (expected?.role ?? null);
}

function sharedToolTailMaturityCrossoverEvidence(policy = {}) {
  const required = policy?.required === true;
  const observed = policy?.observed === true;
  const pairIds = unique(array(policy?.scored_pair_ids)
    .filter((pair) => Number.isInteger(pair) && pair >= 0))
    .map((pair) => Number(pair));
  const turnOrders = array(policy?.turn_orders);
  const championRuns = new Map(array(policy?.champion_runs).map((run) => [run?.pair, run]));
  const candidateRuns = new Map(array(policy?.candidate_runs).map((run) => [run?.pair, run]));
  const armRunEvidenceRequired = championRuns.size > 0 || candidateRuns.size > 0;
  const pairs = pairIds.map((pair) => {
    const schedule = turnOrders[pair] ?? {};
    const pairStart = array(schedule?.pair_start);
    const stablePredecessor = array(schedule?.stable_predecessor);
    const leader = schedule?.leader;
    const follower = schedule?.follower;
    const expectedSequence = array(schedule?.sequence);
    const actualSequence = array(schedule?.actual_sequence);
    const armRunsComplete = !armRunEvidenceRequired ||
      (championRuns.get(pair)?.pass === true && candidateRuns.get(pair)?.pass === true);
    const baseOrdersValid = schedule?.schema === "atoapi-tool-tail-maturity-dispatch-v1" &&
      validArmOrder(pairStart) && validArmOrder(stablePredecessor) &&
      leader === pairStart[0] && follower === pairStart[1] &&
      expectedSequence.length === 8 && actualSequence.length === expectedSequence.length &&
      actualSequence.every((event, index) => sameToolTailMaturityDispatch(event, expectedSequence[index]));
    const leaderChain = actualSequence[4]?.arm === leader && actualSequence[4]?.turn === 2 &&
      actualSequence[4]?.stage === "tool-tail-maturity" && actualSequence[4]?.role === "leader-tail" &&
      actualSequence[5]?.arm === leader && actualSequence[5]?.turn === 3 &&
      actualSequence[5]?.stage === "direct-successor" && actualSequence[5]?.role === "leader-successor";
    const followerChain = actualSequence[6]?.arm === follower && actualSequence[6]?.turn === 2 &&
      actualSequence[6]?.stage === "tool-tail-maturity" && actualSequence[6]?.role === "follower-tail" &&
      actualSequence[7]?.arm === follower && actualSequence[7]?.turn === 3 &&
      actualSequence[7]?.stage === "direct-successor" && actualSequence[7]?.role === "follower-successor";
    const complete = armRunsComplete && baseOrdersValid && leaderChain && followerChain;
    return {
      pair,
      arm_runs_complete: armRunsComplete,
      complete,
      leader: (leader === "champion" || leader === "candidate") ? leader : null,
      target_chain_complete: leaderChain,
      phases: [
        {
          turn: 0,
          first_arm: validArmOrder(pairStart) ? pairStart[0] : null,
          second_arm: validArmOrder(pairStart) ? pairStart[1] : null,
          labels: ["pair-start"],
          complete: validArmOrder(pairStart)
        },
        {
          turn: 1,
          first_arm: validArmOrder(stablePredecessor) ? stablePredecessor[0] : null,
          second_arm: validArmOrder(stablePredecessor) ? stablePredecessor[1] : null,
          labels: ["stable-predecessor"],
          complete: validArmOrder(stablePredecessor)
        },
        {
          turn: 2,
          first_arm: (leader === "champion" || leader === "candidate") ? leader : null,
          second_arm: (follower === "champion" || follower === "candidate") ? follower : null,
          labels: ["tool-tail-maturity-leader"],
          complete: leaderChain && followerChain
        }
      ]
    };
  });
  const completePairs = pairs.filter((pair) => pair.complete);
  const phaseCoverage = new Map();
  for (const pair of completePairs) {
    for (const phase of pair.phases) {
      for (const label of phase.labels) {
        const current = phaseCoverage.get(label) ?? {
          phase: label,
          champion_first_pairs: [],
          candidate_first_pairs: []
        };
        if (phase.first_arm === "champion") current.champion_first_pairs.push(pair.pair);
        if (phase.first_arm === "candidate") current.candidate_first_pairs.push(pair.pair);
        phaseCoverage.set(label, current);
      }
    }
  }
  const phases = [...phaseCoverage.values()]
    .map((phase) => ({
      ...phase,
      champion_first_pairs: unique(phase.champion_first_pairs).map(Number),
      candidate_first_pairs: unique(phase.candidate_first_pairs).map(Number)
    }))
    .map((phase) => ({
      ...phase,
      balanced: phase.champion_first_pairs.length > 0 && phase.candidate_first_pairs.length > 0
    }))
    .sort((left, right) => left.phase.localeCompare(right.phase));
  const pairCountSufficient = completePairs.length >= 2;
  const phaseBalanceComplete = phases.length === 3 && phases.every((phase) => phase.balanced);
  const pass = !required || (observed && pairCountSufficient && phaseBalanceComplete);
  return {
    required,
    observed,
    scenario: "tool-tail-maturity",
    required_scored_pairs: required ? 2 : 0,
    scored_pair_ids: pairIds,
    complete_scored_pair_ids: completePairs.map((pair) => pair.pair),
    scored_pair_count: pairIds.length,
    complete_scored_pair_count: completePairs.length,
    pair_count_sufficient: pairCountSufficient,
    pairs,
    phases,
    phase_balance_complete: phaseBalanceComplete,
    pass,
    reason: !required
      ? "not_required"
      : !observed
        ? "shared_turn_crossover_not_observed"
        : !pairCountSufficient
          ? "requires_at_least_two_complete_scored_pairs"
          : !phaseBalanceComplete
            ? "tool_tail_maturity_dispatch_not_balanced_or_not_contiguous"
            : "balanced"
  };
}

// A shared upstream lane deliberately lets the second arm read what the first
// arm just primed. That is useful only if every scored phase crosses direction
// symmetrically; otherwise a one-pair result can simply award second-sender
// cache reads to whichever executable happened to run later.
function sharedTurnCrossoverEvidence(policy = {}) {
  const required = policy?.required === true;
  const observed = policy?.observed === true;
  const scenario = String(policy?.scenario ?? "");
  if (scenario === "tool-tail-maturity") {
    return sharedToolTailMaturityCrossoverEvidence(policy);
  }
  const expectedTurns = Number.isInteger(policy?.turns) && policy.turns > 0
    ? policy.turns
    : 0;
  const pairIds = unique(array(policy?.scored_pair_ids)
    .filter((pair) => Number.isInteger(pair) && pair >= 0))
    .map((pair) => Number(pair));
  const turnOrders = array(policy?.turn_orders);
  const championRuns = new Map(array(policy?.champion_runs).map((run) => [run?.pair, run]));
  const candidateRuns = new Map(array(policy?.candidate_runs).map((run) => [run?.pair, run]));
  const armRunEvidenceRequired = championRuns.size > 0 || candidateRuns.size > 0;
  const pairs = pairIds.map((pair) => {
    const turns = array(turnOrders[pair]);
    const expected = expectedTurns || turns.length;
    const phases = [];
    const armRunsComplete = !armRunEvidenceRequired ||
      (championRuns.get(pair)?.pass === true && candidateRuns.get(pair)?.pass === true);
    let complete = armRunsComplete && expected > 0 && turns.length === expected;
    for (let turn = 0; turn < expected; turn += 1) {
      const order = array(turns[turn]);
      const firstArm = order[0];
      const secondArm = order[1];
      const valid = order.length === 2 &&
        (firstArm === "champion" || firstArm === "candidate") &&
        (secondArm === "champion" || secondArm === "candidate") &&
        firstArm !== secondArm;
      if (!valid) complete = false;
      phases.push({
        turn,
        first_arm: valid ? firstArm : null,
        second_arm: valid ? secondArm : null,
        labels: crossoverPhaseLabels(
          scenario,
          turn,
          policy?.candidate_late_shallow_provider_waterline_rollback_wait === true
        ),
        complete: valid
      });
    }
    return { pair, arm_runs_complete: armRunsComplete, complete, phases };
  });
  const completePairs = pairs.filter((pair) => pair.complete);
  const phaseCoverage = new Map();
  for (const pair of completePairs) {
    for (const phase of pair.phases) {
      for (const label of phase.labels) {
        const current = phaseCoverage.get(label) ?? {
          phase: label,
          champion_first_pairs: [],
          candidate_first_pairs: []
        };
        if (phase.first_arm === "champion") current.champion_first_pairs.push(pair.pair);
        if (phase.first_arm === "candidate") current.candidate_first_pairs.push(pair.pair);
        phaseCoverage.set(label, current);
      }
    }
  }
  const phases = [...phaseCoverage.values()]
    .map((phase) => ({
      ...phase,
      champion_first_pairs: unique(phase.champion_first_pairs).map(Number),
      candidate_first_pairs: unique(phase.candidate_first_pairs).map(Number)
    }))
    .map((phase) => ({
      ...phase,
      balanced: phase.champion_first_pairs.length > 0 && phase.candidate_first_pairs.length > 0
    }))
    .sort((left, right) => left.phase.localeCompare(right.phase));
  const pairCountSufficient = completePairs.length >= 2;
  const phaseBalanceComplete = phases.length > 0 && phases.every((phase) => phase.balanced);
  const pass = !required || (observed && pairCountSufficient && phaseBalanceComplete);
  return {
    required,
    observed,
    scenario,
    required_scored_pairs: required ? 2 : 0,
    scored_pair_ids: pairIds,
    complete_scored_pair_ids: completePairs.map((pair) => pair.pair),
    scored_pair_count: pairIds.length,
    complete_scored_pair_count: completePairs.length,
    pair_count_sufficient: pairCountSufficient,
    pairs,
    phases,
    phase_balance_complete: phaseBalanceComplete,
    pass,
    reason: !required
      ? "not_required"
      : !observed
        ? "shared_turn_crossover_not_observed"
        : !pairCountSufficient
          ? "requires_at_least_two_complete_scored_pairs"
          : !phaseBalanceComplete
            ? "first_second_order_not_balanced_for_every_relevant_phase"
            : "balanced"
  };
}

function sourceAwareSharedCrossoverAttribution(champion, candidate, crossover) {
  if (crossover?.scenario === "tool-tail-maturity") {
    return {
      applicable: false,
      complete: true,
      reason: "arm-grouped-tool-tail-maturity-crossover",
      after_candidate_prime: null,
      after_champion_prime: null,
      by_turn: []
    };
  }
  if (!crossover?.observed || !Array.isArray(crossover?.complete_scored_pair_ids)) {
    return { applicable: false, complete: true, after_candidate_prime: null, after_champion_prime: null, by_turn: [] };
  }
  const runsByArm = {
    champion: new Map(array(champion?.runs).map((run) => [run?.pair, run])),
    candidate: new Map(array(candidate?.runs).map((run) => [run?.pair, run]))
  };
  const buckets = {
    champion: { observations: 0, input_tokens: 0, cache_read_tokens: 0 },
    candidate: { observations: 0, input_tokens: 0, cache_read_tokens: 0 }
  };
  const byTurn = [];
  let complete = true;
  for (const pair of crossover.complete_scored_pair_ids) {
    const pairEvidence = array(crossover?.pairs).find((item) => item?.pair === pair);
    if (!pairEvidence?.complete) {
      complete = false;
      continue;
    }
    for (const phaseEvidence of array(pairEvidence?.phases)) {
      const turn = phaseEvidence?.turn;
      const firstArm = phaseEvidence?.first_arm;
      const secondArm = phaseEvidence?.second_arm;
      const secondRequests = array(runsByArm[secondArm]?.get(pair)?.requests)
        .filter((request) => !request?.request_kind || request.request_kind === "turn");
      const secondRequest = secondRequests[turn];
      const inputTokens = explicitFiniteNonNegativeNumber(secondRequest?.input_tokens);
      const cacheReadTokens = explicitFiniteNonNegativeNumber(secondRequest?.cache_read_tokens);
      const valid = (firstArm === "champion" || firstArm === "candidate") &&
        (secondArm === "champion" || secondArm === "candidate") &&
        firstArm !== secondArm && inputTokens !== null && cacheReadTokens !== null;
      if (!valid) {
        complete = false;
        byTurn.push({ pair, turn, complete: false });
        continue;
      }
      const bucket = buckets[firstArm];
      bucket.observations += 1;
      bucket.input_tokens += inputTokens;
      bucket.cache_read_tokens += cacheReadTokens;
      byTurn.push({
        pair,
        turn,
        phase: secondRequest?.phase ?? null,
        first_arm: firstArm,
        second_arm: secondArm,
        second_input_tokens: inputTokens,
        second_cache_read_tokens: cacheReadTokens,
        complete: true
      });
    }
  }
  const summarize = (bucket) => ({
    ...bucket,
    cache_128_hit_rate: ratio(bucket.cache_read_tokens, bucket.input_tokens)
  });
  return {
    applicable: true,
    complete,
    after_candidate_prime: summarize(buckets.candidate),
    after_champion_prime: summarize(buckets.champion),
    by_turn: byTurn
  };
}

function dynamicTailTerminalFollowup(events, requests) {
  const injected = array(events);
  const terminalEvent = injected.at(-1);
  if (!terminalEvent) {
    return { present: false, expected_phase: null, phase: null, input_tokens: 0 };
  }
  const requestRows = array(requests);
  const eventPhase = typeof terminalEvent.phase === "string" ? terminalEvent.phase.trim() : "";
  const matchingInjectionIndexes = requestRows
    .map((request, index) => eventPhase && request?.phase === eventPhase ? index : -1)
    .filter((index) => index >= 0);
  const injectionIndex = matchingInjectionIndexes.length === 1
    ? matchingInjectionIndexes[0]
    : -1;
  const followup = injectionIndex >= 0 ? requestRows[injectionIndex + 1] : null;
  const eventTurn = Number(terminalEvent.turn);
  const expectedPhase = Number.isInteger(eventTurn) && eventTurn >= 0
    ? `followup-${eventTurn + 1}`
    : null;
  const valid = Boolean(eventPhase) && matchingInjectionIndexes.length === 1 && Boolean(expectedPhase) && Boolean(followup) &&
    followup?.phase === expectedPhase &&
    followup?.request_kind === "turn" &&
    followup?.sse_completed === true &&
    number(followup?.input_tokens) > 0;
  return {
    present: valid,
    expected_phase: expectedPhase,
    phase: followup?.phase ?? null,
    input_tokens: number(followup?.input_tokens)
  };
}

function emptyDynamicTailMix() {
  return {
    injections: 0,
    injected_characters: 0,
    shape_counts: {},
    followups_observed: 0,
    followup_new_tail_tokens: 0,
    followup_provider_unstable_tokens: 0,
    followup_tail_lag_count: 0
  };
}

function mergeDynamicTailMix(target, source) {
  const next = source ?? emptyDynamicTailMix();
  target.injections += number(next.injections);
  target.injected_characters += number(next.injected_characters);
  target.followups_observed += number(next.followups_observed);
  target.followup_new_tail_tokens += number(next.followup_new_tail_tokens);
  target.followup_provider_unstable_tokens += number(next.followup_provider_unstable_tokens);
  target.followup_tail_lag_count += number(next.followup_tail_lag_count);
  for (const [shape, count] of Object.entries(next.shape_counts ?? {})) {
    target.shape_counts[shape] = number(target.shape_counts[shape]) + number(count);
  }
  return target;
}

// This evidence is intentionally narrower than `guarded_requests`: the
// candidate must prove that its *direct* child of the 4-8KiB tool-tail request
// used the dedicated ExactMediumToolTail reason.  A giant-root, fresh-prefix,
// or provider-recovery wait is useful telemetry, but it is not evidence that
// this particular optimization ran.
function exactMediumToolTailMaturityEvidence(requests) {
  const rows = array(requests);
  let predecessorRequests = 0;
  let directSuccessorRequests = 0;
  let maturityWaitRequests = 0;
  let nonTargetPrefixGuardWaitRequests = 0;
  for (let index = 0; index < rows.length; index += 1) {
    const request = rows[index];
    const previous = rows[index - 1];
    const toolOutputChars = Math.max(
      number(previous?.tail_tool_output_chars),
      number(previous?.tail_largest_tool_output_chars)
    );
    const exactPredecessor = previous?.phase === "tool-tail-maturity" &&
      number(previous?.input_tokens) >= 16_384 &&
      toolOutputChars >= 4_096 && toolOutputChars <= 8_191;
    if (exactPredecessor) predecessorRequests += 1;
    const directSuccessor = exactPredecessor &&
      request?.phase === "followup-3" &&
      request?.request_kind === "turn" &&
      request?.sse_completed === true;
    if (directSuccessor) directSuccessorRequests += 1;
    const exactWait = directSuccessor &&
      number(request?.prefix_guard_wait_ms) > 0 &&
      request?.prefix_guard_wait_reason === "responses_exact_medium_tool_tail_maturity_pending" &&
      request?.prefix_guard_wait_source === "exact";
    if (exactWait) maturityWaitRequests += 1;
    if (number(request?.prefix_guard_wait_ms) > 0 && !exactWait) {
      nonTargetPrefixGuardWaitRequests += 1;
    }
  }
  return {
    predecessor_requests: predecessorRequests,
    direct_successor_requests: directSuccessorRequests,
    maturity_wait_requests: maturityWaitRequests,
    non_target_prefix_guard_wait_requests: nonTargetPrefixGuardWaitRequests
  };
}

function buildDynamicRun(input) {
  const requests = input.requests;
  const comparable = requests.filter((item) => item.input_tokens > 0);
  const cacheable = comparable.filter((item) => cacheableInputTokens128(item.input_tokens) > 0);
  const warmComparable = comparable.filter((item) => item.phase !== "seed");
  const warm = cacheable.filter((item) => item.phase !== "seed");
  const seedRequests = requests.filter((item) => item.phase === "seed");
  const inputTokens = sum(comparable, "input_tokens");
  const cacheReadTokens = sum(comparable, "cache_read_tokens");
  const warmInputTokens = sum(warmComparable, "input_tokens");
  const warmCacheReadTokens = sum(warmComparable, "cache_read_tokens");
  const cacheableTokens = cacheable.reduce(
    (total, item) => total + cacheableInputTokens128(item.input_tokens),
    0
  );
  const cacheableReadTokens = cacheable.reduce(
    (total, item) => total + Math.min(item.cache_read_tokens, cacheableInputTokens128(item.input_tokens)),
    0
  );
  const warmCacheableTokens = warm.reduce(
    (total, item) => total + cacheableInputTokens128(item.input_tokens),
    0
  );
  const warmCacheableReadTokens = warm.reduce(
    (total, item) => total + Math.min(item.cache_read_tokens, cacheableInputTokens128(item.input_tokens)),
    0
  );
  const fullBuckets = cacheable.filter(
    (item) => item.cache_read_tokens >= cacheableInputTokens128(item.input_tokens)
  ).length;
  const warmFullBuckets = warm.filter(
    (item) => item.cache_read_tokens >= cacheableInputTokens128(item.input_tokens)
  ).length;
  // Scope evidence is independent of cache-score eligibility. A failed
  // request may legitimately have zero usage while still recording the
  // selected realm; dropping that observation would falsely report a realm
  // mismatch instead of the real request failure.
  const observedRealms = unique(
    requests.map((item) => item.observed_realm_id).filter(Boolean)
  );
  const allTerminal = requests.length > 0 && requests.every((item) => item.sse_completed);
  const allSingle = requests.length > 0 && requests.every(
    (item) => item.checks?.per_inbound_one_attempt_one_post && item.checks?.exact_counter_delta
  );
  const allCohortBound = requests.length > 0 && requests.every(
    (item) => item.checks?.provider_matches_cohort &&
      item.checks?.model_matches_cohort &&
      item.checks?.observed_key_realm_matches_cohort
  );
  const completeCacheReadTokenEvidence = requests.length > 0 && requests.every(
    (item) => {
      const value = explicitFiniteNonNegativeNumber(item?.cache_read_tokens);
      const hasObservationMarker = Object.prototype.hasOwnProperty.call(
        item ?? {},
        "cache_read_tokens_observed"
      );
      return value !== null && (!hasObservationMarker || item.cache_read_tokens_observed === true);
    }
  );
  const usageCoverage = requests.length === 0 ? 0 : comparable.length / requests.length;
  const timing = timingSummary(comparable);
  const staticWire = staticWireContinuity(requests);
  const seedRequest = seedRequests[0];
  const seedInputTokens = number(seedRequest?.input_tokens);
  const seedCacheReadTokens = number(seedRequest?.cache_read_tokens);
  const coldSeedRequestCount = seedRequests.filter((item) => {
    const value = explicitFiniteNonNegativeNumber(item?.cache_read_tokens);
    const hasObservationMarker = Object.prototype.hasOwnProperty.call(
      item ?? {},
      "cache_read_tokens_observed"
    );
    return value === 0 && (!hasObservationMarker || item.cache_read_tokens_observed === true);
  }).length;
  const peakInputTokens = comparable.reduce(
    (maximum, item) => Math.max(maximum, number(item.input_tokens)),
    0
  );
  const dynamicTail = dynamicTailRecoverySummary(input.dynamicTailEvents, requests);
  const terminalDynamicFollowup = dynamicTailTerminalFollowup(input.dynamicTailEvents, requests);
  const smallContextColdReadForegroundWait = requests.some((item) =>
    number(item.input_tokens) > 0 &&
    number(item.input_tokens) < 32_000 &&
    item.prefix_lag_classification === "cold_read_after_warm" &&
    item.prefix_guard_wait_reason === "responses_recent_cold_read_settle"
  );
  const upstreamAffinityTestRequests = requests.filter(
    (item) => item.upstream_affinity?.test_enabled === true
  ).length;
  const upstreamAffinityLearnedRequests = requests.filter(
    (item) => item.upstream_affinity?.learned === true
  ).length;
  const upstreamAffinityInjectedRequests = requests.filter(
    (item) => item.upstream_affinity?.injected === true
  ).length;
  const requiredCandidateCacheControlField = String(
    input.requireCandidateCacheControlField ?? ""
  ).trim();
  const candidateCacheControlFieldRequests = requiredCandidateCacheControlField
    ? requests.filter((item) => array(item.cache_control_fields).includes(
      requiredCandidateCacheControlField
    )).length
    : 0;
  const candidateCacheOptions24hRequests = input.requireCandidateCacheOptions24h
    ? requests.filter((item) => item.cache_options_24h === true).length
    : 0;
  const candidateOptions24hSiblingSettleRequests = requests.filter(
    (item) => item.prefix_guard_wait_reason === "responses_prompt_cache_options_sibling_settle"
  ).length;
  const candidateHttp1Requests = input.requireCandidateHttp1
    ? requests.filter((item) => item.transport?.upstream_http_version === "HTTP/1.1").length
    : 0;
  const candidateProviderWaterlineRecoveryWaitRequests = requests.filter(
    (item) => item.prefix_guard_wait_reason === "responses_provider_waterline_recovery_settle"
  ).length;
  const candidateExactLargeMessageTailLagRequests = requests.filter(
    (item) => item.prefix_guard_wait_reason === "responses_exact_large_message_tail_lag"
  ).length;
  const candidateLateShallowProviderWaterlineRollbackWaitRequests = requests.filter(
    (item) => item.prefix_guard_wait_reason ===
      "responses_late_shallow_provider_waterline_rollback_pending"
  ).length;
  const exactMediumToolTail = exactMediumToolTailMaturityEvidence(requests);
  const metrics = {
    requests: requests.length,
    successful_sse_requests: requests.filter((item) => item.sse_completed).length,
    input_tokens: inputTokens,
    warm_input_tokens: warmInputTokens,
    seed_input_tokens: seedInputTokens,
    seed_cache_read_tokens: seedCacheReadTokens,
    seed_request_count: seedRequests.length,
    cold_seed_request_count: coldSeedRequestCount,
    peak_input_tokens: peakInputTokens,
    dynamic_tail_terminal_followup_input_tokens: terminalDynamicFollowup.input_tokens,
    cache_read_tokens: cacheReadTokens,
    raw_token_hit_rate: ratio(cacheReadTokens, inputTokens),
    warm_cache_read_tokens: warmCacheReadTokens,
    warm_raw_token_hit_rate: ratio(warmCacheReadTokens, warmInputTokens),
    cacheable_tokens_128: cacheableTokens,
    cacheable_read_tokens_128: cacheableReadTokens,
    cache_128_hit_rate: ratio(cacheableReadTokens, cacheableTokens),
    warm_cacheable_tokens_128: warmCacheableTokens,
    warm_cacheable_read_tokens_128: warmCacheableReadTokens,
    warm_cache_128_hit_rate: ratio(warmCacheableReadTokens, warmCacheableTokens),
    warm_stable_prefix_tokens_128: warmCacheableTokens,
    warm_stable_prefix_cached_tokens_128: warmCacheableReadTokens,
    warm_stable_prefix_hit_rate: ratio(warmCacheableReadTokens, warmCacheableTokens),
    full_bucket_requests: fullBuckets,
    full_bucket_rate: ratio(fullBuckets, cacheable.length),
    warm_full_bucket_requests: warmFullBuckets,
    warm_full_bucket_rate: ratio(warmFullBuckets, warm.length),
    warm_full_bucket_denominator: warm.length,
    cacheable_request_count: cacheable.length,
    full_bucket_denominator: cacheable.length,
    avoidable_gap_tokens: sum(comparable, "cache_avoidable_gap_tokens"),
    new_tail_gap_tokens: sum(comparable, "cache_new_tail_gap_tokens"),
    provider_unstable_gap_tokens: sum(comparable, "cache_provider_unstable_gap_tokens"),
    shortfall_tokens: sum(comparable, "cache_shortfall_tokens"),
    guarded_requests: comparable.filter((item) => number(item.prefix_guard_wait_ms) > 0).length,
    upstream_affinity_test_requests: upstreamAffinityTestRequests,
    upstream_affinity_learned_requests: upstreamAffinityLearnedRequests,
    upstream_affinity_injected_requests: upstreamAffinityInjectedRequests,
    candidate_cache_control_field_requests: candidateCacheControlFieldRequests,
    candidate_cache_options_24h_requests: candidateCacheOptions24hRequests,
    candidate_options24h_sibling_settle_requests: candidateOptions24hSiblingSettleRequests,
    candidate_http1_requests: candidateHttp1Requests,
    candidate_provider_waterline_recovery_wait_requests:
      candidateProviderWaterlineRecoveryWaitRequests,
    candidate_exact_large_message_tail_lag_requests:
      candidateExactLargeMessageTailLagRequests,
    candidate_late_shallow_provider_waterline_rollback_wait_requests:
      candidateLateShallowProviderWaterlineRollbackWaitRequests,
    exact_medium_tool_tail_predecessor_requests:
      exactMediumToolTail.predecessor_requests,
    exact_medium_tool_tail_direct_successor_requests:
      exactMediumToolTail.direct_successor_requests,
    exact_medium_tool_tail_maturity_wait_requests:
      exactMediumToolTail.maturity_wait_requests,
    non_target_prefix_guard_wait_requests:
      exactMediumToolTail.non_target_prefix_guard_wait_requests,
    ...timing,
    usage_coverage: usageCoverage,
    observed_realm_ids: observedRealms,
    dynamic_tail_mix: dynamicTail
  };
  const checks = {
    no_runtime_failure: !input.fatal,
    every_sse_completed: allTerminal,
    every_inbound_one_attempt_one_main_post: allSingle,
    cohort_bound_on_every_request: allCohortBound,
    complete_usage_coverage: usageCoverage === 1,
    complete_cache_read_token_evidence: completeCacheReadTokenEvidence,
    complete_timing_coverage: metrics.timing_complete_requests === comparable.length,
    input_usage_present: inputTokens > 0,
    required_seed_input_tokens:
      seedInputTokens >= number(input.minimumSeedInputTokens),
    required_peak_input_tokens:
      peakInputTokens >= number(input.minimumPeakInputTokens),
    maximum_peak_input_tokens:
      number(input.maximumPeakInputTokens) === 0 ||
      peakInputTokens <= number(input.maximumPeakInputTokens),
    dynamic_tail_terminal_followup_peak_in_range:
      input.scenario !== "dynamic-tail-mix" ||
      (number(input.minimumPeakInputTokens) === 0 && number(input.maximumPeakInputTokens) === 0) ||
      (terminalDynamicFollowup.present &&
        terminalDynamicFollowup.input_tokens >= number(input.minimumPeakInputTokens) &&
        (number(input.maximumPeakInputTokens) === 0 ||
          terminalDynamicFollowup.input_tokens <= number(input.maximumPeakInputTokens))),
    cacheable_128_evidence_present: cacheableTokens > 0,
    warm_stable_prefix_evidence_present: warmCacheableTokens > 0,
    static_wire_continuity: staticWire.pass,
    small_context_cold_read_no_foreground_wait: !smallContextColdReadForegroundWait,
    one_observed_key_realm: observedRealms.length === 1,
    avoidable_gap_zero: metrics.avoidable_gap_tokens === 0,
    required_guarded_requests:
      metrics.guarded_requests >= number(input.minimumGuardedRequests),
    candidate_upstream_affinity_injected:
      !input.requireCandidateUpstreamAffinity ||
      (metrics.upstream_affinity_learned_requests > 0 &&
        metrics.upstream_affinity_injected_requests > 0),
    candidate_cache_control_field_injected:
      !requiredCandidateCacheControlField ||
      candidateCacheControlFieldRequests === requests.length,
    candidate_cache_options_24h_injected:
      !input.requireCandidateCacheOptions24h ||
      candidateCacheOptions24hRequests === requests.length,
    candidate_options24h_sibling_settle_observed:
      !input.requireCandidateOptions24hSiblingSettle ||
      candidateOptions24hSiblingSettleRequests > 0,
    candidate_http1_observed:
      !input.requireCandidateHttp1 || candidateHttp1Requests === requests.length,
    candidate_late_shallow_provider_waterline_rollback_wait_observed:
      !input.requireCandidateLateShallowProviderWaterlineRollbackWait ||
      candidateLateShallowProviderWaterlineRollbackWaitRequests > 0,
    compaction_observed:
      !new Set(["compacted-anchor", "compaction-root"]).has(input.scenario) ||
      input.compactionSeen === true,
    dynamic_tail_followup_coverage:
      input.scenario !== "dynamic-tail-mix" ||
      dynamicTail.followups_observed === dynamicTail.injections
  };
  return {
    schema: SCHEMA,
    kind: "dynamic-run",
    pass: Object.values(checks).every(Boolean),
    arm: input.arm,
    pair: input.pair,
    cohort: input.cohort,
    executable: input.executable,
    scenario: input.scenario,
    prompt_cache_key_used: input.promptCacheKeyUsed,
    capability_certificate: input.capabilityCertificate ?? null,
    fatal: input.fatal ?? null,
    metrics,
    static_wire_continuity: staticWire,
    checks,
    requests: requests.map(stripCompactedInput)
  };
}

function failedDynamicRun({ arm, pair, cohort, executable, reason }) {
  return {
    schema: SCHEMA,
    kind: "dynamic-run",
    pass: false,
    arm,
    pair,
    cohort,
    executable,
    scenario: cohort.request_family,
    prompt_cache_key_used: false,
    fatal: reason,
    metrics: emptyMetrics(),
    checks: {
      no_runtime_failure: false,
      every_sse_completed: false,
      every_inbound_one_attempt_one_main_post: false,
      cohort_bound_on_every_request: false,
      complete_usage_coverage: false,
    input_usage_present: false,
    required_seed_input_tokens: false,
    required_peak_input_tokens: false,
    maximum_peak_input_tokens: false,
    dynamic_tail_terminal_followup_peak_in_range: false,
      cacheable_128_evidence_present: false,
      warm_stable_prefix_evidence_present: false,
    static_wire_continuity: false,
    small_context_cold_read_no_foreground_wait: false,
    one_observed_key_realm: false,
    avoidable_gap_zero: false,
    complete_timing_coverage: false,
    compaction_observed: false,
    candidate_late_shallow_provider_waterline_rollback_wait_observed: false,
    dynamic_tail_followup_coverage: false
  },
    requests: []
  };
}

function aggregateArm(
  arm,
  cohort,
  executable,
  runs,
  minimumGuardedRequests = 0,
  minimumPeakInputTokens = 0,
  maximumPeakInputTokens = 0,
  requireCandidateExactMediumToolTailMaturityWait = false,
  requireCandidateExactLargeMessageTailLag = false,
  requireCandidateLateShallowProviderWaterlineRollbackWait = false
) {
  const normalized = runs.map((run) => validateDynamicRun(run, arm));
  const metrics = emptyMetrics();
  const dynamicTailMix = emptyDynamicTailMix();
  const observedRealms = [];
  for (const run of normalized) {
    const source = run.metrics;
    for (const key of [
      "requests",
      "successful_sse_requests",
      "input_tokens",
      "warm_input_tokens",
      "cache_read_tokens",
      "warm_cache_read_tokens",
      "cacheable_tokens_128",
      "cacheable_read_tokens_128",
      "warm_cacheable_tokens_128",
      "warm_cacheable_read_tokens_128",
      "warm_stable_prefix_tokens_128",
      "warm_stable_prefix_cached_tokens_128",
      "avoidable_gap_tokens",
      "new_tail_gap_tokens",
      "provider_unstable_gap_tokens",
      "shortfall_tokens",
      "guarded_requests",
      "upstream_affinity_test_requests",
      "upstream_affinity_learned_requests",
      "upstream_affinity_injected_requests",
      "candidate_cache_control_field_requests",
      "candidate_cache_options_24h_requests",
      "candidate_options24h_sibling_settle_requests",
      "candidate_http1_requests",
      "candidate_provider_waterline_recovery_wait_requests",
      "candidate_exact_large_message_tail_lag_requests",
      "candidate_late_shallow_provider_waterline_rollback_wait_requests",
      "exact_medium_tool_tail_predecessor_requests",
      "exact_medium_tool_tail_direct_successor_requests",
      "exact_medium_tool_tail_maturity_wait_requests",
      "non_target_prefix_guard_wait_requests"
    ]) {
      metrics[key] += number(source[key]);
    }
    metrics.cacheable_request_count += number(source.cacheable_request_count);
    metrics.full_bucket_denominator += number(source.full_bucket_denominator);
    observedRealms.push(...array(source.observed_realm_ids));
    mergeDynamicTailMix(dynamicTailMix, source.dynamic_tail_mix);
  }
  metrics.raw_token_hit_rate = ratio(metrics.cache_read_tokens, metrics.input_tokens);
  metrics.warm_raw_token_hit_rate = ratio(
    metrics.warm_cache_read_tokens,
    metrics.warm_input_tokens
  );
  metrics.cache_128_hit_rate = ratio(
    metrics.cacheable_read_tokens_128,
    metrics.cacheable_tokens_128
  );
  metrics.warm_cache_128_hit_rate = ratio(
    metrics.warm_cacheable_read_tokens_128,
    metrics.warm_cacheable_tokens_128
  );
  metrics.warm_stable_prefix_hit_rate = ratio(
    metrics.warm_stable_prefix_cached_tokens_128,
    metrics.warm_stable_prefix_tokens_128
  );
  // Per-run full bucket rates may have different denominators.  The raw run
  // object does not expose a separate count in older artifacts, so derive it
  // from the retained request evidence when available and otherwise fail closed.
  const retainedRequests = normalized.flatMap((run) => array(run.requests));
  // Seed evidence is a shared-cache crossover boundary.  Do not derive it
  // from the per-run aggregate fields: older reports could retain only the
  // first pair's seed there, and a missing counter was normalized to zero.
  // Keep every scored run's raw seed in the aggregate, while leaving an
  // incomplete counter explicitly unknown for the comparison gate below.
  const retainedSeedRows = retainedRequests.filter((item) => item?.phase === "seed");
  metrics.seed_input_tokens = retainedSeedRows.reduce(
    (total, item) => total + number(item?.input_tokens),
    0
  );
  metrics.seed_request_count = retainedSeedRows.length;
  const seedCacheReadEvidence = aggregateSeedCacheReadEvidence({ runs: normalized });
  metrics.seed_cache_read_tokens = seedCacheReadEvidence.raw_complete
    ? seedCacheReadEvidence.tokens
    : null;
  const coldSeedRows = retainedSeedRows.filter((item) => {
    const value = explicitFiniteNonNegativeNumber(item?.cache_read_tokens);
    const hasObservationMarker = Object.prototype.hasOwnProperty.call(
      item ?? {},
      "cache_read_tokens_observed"
    );
    return value === 0 && (!hasObservationMarker || item.cache_read_tokens_observed === true);
  });
  metrics.cold_seed_request_count = seedCacheReadEvidence.raw_complete
    ? coldSeedRows.length
    : null;
  const cacheableRows = retainedRequests.filter(
    (item) => cacheableInputTokens128(number(item.input_tokens)) > 0
  );
  metrics.full_bucket_requests = cacheableRows.filter(
    (item) => number(item.cache_read_tokens) >= cacheableInputTokens128(number(item.input_tokens))
  ).length;
  metrics.full_bucket_rate = ratio(metrics.full_bucket_requests, cacheableRows.length);
  metrics.cacheable_request_count = cacheableRows.length;
  metrics.full_bucket_denominator = cacheableRows.length;
  const comparableRows = retainedRequests.filter((item) => number(item.input_tokens) > 0);
  const warmRows = comparableRows.filter((item) => item.phase !== "seed");
  const warmCacheableRows = warmRows.filter(
    (item) => cacheableInputTokens128(number(item.input_tokens)) > 0
  );
  metrics.warm_full_bucket_requests = warmCacheableRows.filter(
    (item) => number(item.cache_read_tokens) >= cacheableInputTokens128(number(item.input_tokens))
  ).length;
  metrics.warm_full_bucket_denominator = warmCacheableRows.length;
  metrics.warm_full_bucket_rate = ratio(
    metrics.warm_full_bucket_requests,
    metrics.warm_full_bucket_denominator
  );
  Object.assign(metrics, timingSummary(comparableRows));
  metrics.peak_input_tokens = comparableRows.reduce(
    (maximum, item) => Math.max(maximum, number(item.input_tokens)),
    0
  );
  metrics.usage_coverage = normalized.length > 0 && normalized.every(
    (run) => number(run.metrics?.usage_coverage) === 1
  ) ? 1 : 0;
  // Prefer retained per-request observations when present. This preserves the
  // realm on zero-usage failed requests even if an older run-level metric was
  // assembled from cache-scoreable requests only.
  observedRealms.push(
    ...retainedRequests.map((item) => item?.observed_realm_id).filter(Boolean)
  );
  metrics.observed_realm_ids = unique(observedRealms);
  metrics.dynamic_tail_mix = dynamicTailMix;
  const checks = {
    every_run_passed: normalized.length > 0 && normalized.every((run) => run.pass),
    cohort_consistent: normalized.length > 0 && normalized.every((run) => sameCohort(run.cohort, cohort)),
    one_observed_key_realm: metrics.observed_realm_ids.length === 1,
    every_sse_completed: metrics.successful_sse_requests === metrics.requests && metrics.requests > 0,
    every_inbound_one_attempt_one_main_post: normalized.every(
      (run) => run.checks?.every_inbound_one_attempt_one_main_post === true
    ),
    avoidable_gap_zero: metrics.avoidable_gap_tokens === 0,
    complete_timing_coverage: metrics.timing_complete_requests === comparableRows.length,
    input_usage_present: metrics.input_tokens > 0,
    required_peak_input_tokens:
      number(minimumPeakInputTokens) === 0 ||
      (normalized.length > 0 && normalized.every(
        (run) => run.checks?.required_peak_input_tokens === true
      )),
    maximum_peak_input_tokens:
      number(maximumPeakInputTokens) === 0 ||
      (normalized.length > 0 && normalized.every(
        (run) => run.checks?.maximum_peak_input_tokens === true
      )),
    cacheable_128_evidence_present: metrics.cacheable_tokens_128 > 0,
    warm_stable_prefix_evidence_present: metrics.warm_stable_prefix_tokens_128 > 0,
    static_wire_continuity: normalized.length > 0 && normalized.every(
      (run) => run.checks?.static_wire_continuity === true
    ),
    full_bucket_denominator_present: metrics.full_bucket_denominator > 0,
    required_guarded_requests:
      metrics.guarded_requests >= number(minimumGuardedRequests),
    candidate_exact_medium_tool_tail_maturity_wait_observed:
      !requireCandidateExactMediumToolTailMaturityWait ||
      (metrics.exact_medium_tool_tail_predecessor_requests > 0 &&
        metrics.exact_medium_tool_tail_direct_successor_requests > 0 &&
        metrics.exact_medium_tool_tail_maturity_wait_requests > 0),
    candidate_exact_large_message_tail_lag_observed:
      !requireCandidateExactLargeMessageTailLag ||
      metrics.candidate_exact_large_message_tail_lag_requests > 0,
    candidate_late_shallow_provider_waterline_rollback_wait_observed:
      !requireCandidateLateShallowProviderWaterlineRollbackWait ||
      metrics.candidate_late_shallow_provider_waterline_rollback_wait_requests > 0,
    candidate_upstream_affinity_injected: normalized.length > 0 && normalized.every(
      (run) => run.checks?.candidate_upstream_affinity_injected !== false
    ),
    candidate_cache_control_field_injected: normalized.length > 0 && normalized.every(
      (run) => run.checks?.candidate_cache_control_field_injected !== false
    ),
    candidate_cache_options_24h_injected: normalized.length > 0 && normalized.every(
      (run) => run.checks?.candidate_cache_options_24h_injected !== false
    ),
    candidate_http1_observed: normalized.length > 0 && normalized.every(
      (run) => run.checks?.candidate_http1_observed !== false
    ),
    dynamic_tail_followup_coverage: normalized.length > 0 && normalized.every(
      (run) => run.scenario !== "dynamic-tail-mix" || run.checks?.dynamic_tail_followup_coverage === true
    ),
    dynamic_tail_terminal_followup_peak_in_range: normalized.length > 0 && normalized.every(
      (run) => run.scenario !== "dynamic-tail-mix" ||
        run.checks?.dynamic_tail_terminal_followup_peak_in_range === true
    )
  };
  return {
    schema: SCHEMA,
    kind: "dynamic-arm-aggregate",
    pass: Object.values(checks).every(Boolean),
    arm,
    cohort,
    executable,
    runs: normalized,
    metrics,
    checks
  };
}

function aggregateSeedCacheReadEvidence(aggregate) {
  const runs = array(aggregate?.runs);
  // Reports without retained per-pair requests can still display their old
  // aggregate metric, but they cannot prove a shared-cache crossover fair.
  if (runs.length === 0) {
    return {
      tokens: explicitFiniteNonNegativeNumber(aggregate?.metrics?.seed_cache_read_tokens),
      raw_complete: false,
      source: "aggregate_metric"
    };
  }
  let tokens = 0;
  for (const run of runs) {
    const seeds = array(run?.requests).filter((item) => item?.phase === "seed");
    if (seeds.length !== 1) {
      return { tokens: null, raw_complete: false, source: "raw_incomplete" };
    }
    const [seed] = seeds;
    const value = explicitFiniteNonNegativeNumber(seed?.cache_read_tokens);
    const hasObservationMarker = Object.prototype.hasOwnProperty.call(
      seed ?? {},
      "cache_read_tokens_observed"
    );
    if (value === null || (hasObservationMarker && seed.cache_read_tokens_observed !== true)) {
      return { tokens: null, raw_complete: false, source: "raw_incomplete" };
    }
    tokens += value;
  }
  return { tokens, raw_complete: true, source: "raw_scored_runs" };
}

// Cold starts are excluded from the dynamic hit-rate numerator and
// denominator, but only after the retained raw evidence proves both arms saw
// the same number of seed requests and no arm received an extra cold seed.
function aggregateColdSeedEvidence(aggregate) {
  const runs = array(aggregate?.runs);
  if (runs.length === 0) {
    return { count: null, seed_count: 0, raw_complete: false, source: "aggregate_metric" };
  }
  let count = 0;
  for (const run of runs) {
    const seeds = array(run?.requests).filter((item) => item?.phase === "seed");
    if (seeds.length !== 1) {
      return { count: null, seed_count: seeds.length, raw_complete: false, source: "raw_incomplete" };
    }
    const [seed] = seeds;
    const value = explicitFiniteNonNegativeNumber(seed?.cache_read_tokens);
    const hasObservationMarker = Object.prototype.hasOwnProperty.call(
      seed ?? {},
      "cache_read_tokens_observed"
    );
    if (value === null || (hasObservationMarker && seed.cache_read_tokens_observed !== true)) {
      return { count: null, seed_count: runs.length, raw_complete: false, source: "raw_incomplete" };
    }
    if (value === 0) count += 1;
  }
  return { count, seed_count: runs.length, raw_complete: true, source: "raw_scored_runs" };
}

// Native prompt-cache placement is intentionally retained only as a one-way
// fingerprint. A live isolated comparison must prove that every request in an
// arm's pair stayed on the same native placement, while the other arm used a
// different one. This does not require the two crossed-over pairs to share a
// placement: their lanes deliberately rotate between pairs.
function nativePlacementFingerprint(request) {
  const fingerprint = safeDiagnosticHash(request?.provider_prefix_key_fingerprint);
  return request?.provider_prefix_key_present === true &&
    typeof fingerprint === "string" && /^[a-f0-9]{32}$/u.test(fingerprint)
    ? fingerprint
    : null;
}

function nativePlacementRunEvidence(run) {
  const requests = array(run?.requests);
  const fingerprints = requests.map(nativePlacementFingerprint);
  const presentOnEveryRequest = requests.length > 0 && fingerprints.every(Boolean);
  const distinctFingerprints = unique(fingerprints.filter(Boolean));
  const stable = presentOnEveryRequest && distinctFingerprints.length === 1;
  return {
    request_count: requests.length,
    fingerprint_present_on_every_request: presentOnEveryRequest,
    stable,
    provider_prefix_key_fingerprint: stable ? distinctFingerprints[0] : null
  };
}

function pairedNativePlacementIsolation(champion, candidate, required = false) {
  const championRuns = array(champion?.runs);
  const candidateRuns = array(candidate?.runs);
  if (!required) {
    return {
      required: false,
      applicable: false,
      pass: true,
      pair_count: 0,
      pair_ids_valid: true,
      pair_ids_unique: true,
      pair_ids_aligned: true,
      fingerprints_present: true,
      fingerprints_stable: true,
      arms_differ: true,
      pairs: []
    };
  }
  const championPairIds = championRuns.map((run) => run?.pair);
  const candidatePairIds = candidateRuns.map((run) => run?.pair);
  const pairIdsValid = [...championPairIds, ...candidatePairIds].every(
    (pair) => Number.isInteger(pair) && pair >= 0
  );
  const championPairIdsUnique = new Set(championPairIds).size === championPairIds.length;
  const candidatePairIdsUnique = new Set(candidatePairIds).size === candidatePairIds.length;
  const championByPair = new Map(championRuns.map((run) => [run?.pair, run]));
  const candidateByPair = new Map(candidateRuns.map((run) => [run?.pair, run]));
  const pairIdsAligned = pairIdsValid && championPairIdsUnique && candidatePairIdsUnique &&
    championRuns.length === candidateRuns.length &&
    championByPair.size === championRuns.length &&
    candidateByPair.size === candidateRuns.length &&
    championPairIds.every((pair) => candidateByPair.has(pair));
  const pairIds = [...new Set([...championPairIds, ...candidatePairIds]
    .filter((pair) => Number.isInteger(pair) && pair >= 0))]
    .sort((left, right) => left - right);
  const pairs = pairIds.map((pair) => {
    const championEvidence = nativePlacementRunEvidence(championByPair.get(pair));
    const candidateEvidence = nativePlacementRunEvidence(candidateByPair.get(pair));
    const fingerprintsDiffer = championEvidence.stable && candidateEvidence.stable &&
      championEvidence.provider_prefix_key_fingerprint !==
      candidateEvidence.provider_prefix_key_fingerprint;
    return {
      pair,
      champion: championEvidence,
      candidate: candidateEvidence,
      fingerprints_differ: fingerprintsDiffer
    };
  });
  const fingerprintsPresent = pairs.length > 0 && pairs.every((pair) =>
    pair.champion.fingerprint_present_on_every_request &&
    pair.candidate.fingerprint_present_on_every_request
  );
  const fingerprintsStable = pairs.length > 0 && pairs.every((pair) =>
    pair.champion.stable && pair.candidate.stable
  );
  const armsDiffer = pairs.length > 0 && pairs.every((pair) => pair.fingerprints_differ);
  const runsPresent = championRuns.length > 0 && candidateRuns.length > 0;
  return {
    required: true,
    applicable: true,
    pass: runsPresent && pairIdsAligned && fingerprintsPresent && fingerprintsStable && armsDiffer,
    pair_count: pairs.length,
    pair_ids_valid: pairIdsValid,
    pair_ids_unique: championPairIdsUnique && candidatePairIdsUnique,
    pair_ids_aligned: pairIdsAligned,
    fingerprints_present: fingerprintsPresent,
    fingerprints_stable: fingerprintsStable,
    arms_differ: armsDiffer,
    pairs
  };
}

function outboundInputSemanticFingerprints(request) {
  const source = request?.outbound_prefix_fingerprints;
  const fields = ["input_full", "instructions", "tools_schema"];
  if (!source || typeof source !== "object") return null;
  const result = {};
  for (const field of fields) {
    const value = typeof source[field] === "string" ? source[field] : "";
    if (!value) return null;
    result[field] = value;
  }
  return result;
}

function outboundPreInputWireFingerprint(request) {
  const value = request?.outbound_prefix_fingerprints?.pre_input_wire;
  return typeof value === "string" && value.length > 0 ? value : null;
}

function outboundCacheMetadataFingerprint(request) {
  const value = request?.outbound_prefix_fingerprints?.cache_metadata;
  return typeof value === "string" && value.length > 0 ? value : null;
}

function outboundInputPrefixFingerprints(request) {
  const value = request?.outbound_prefix_fingerprints?.input_prefixes;
  if (!Array.isArray(value)) return null;
  return value.filter((item) => typeof item === "string" && item.length > 0);
}

function localPreviousResponseIdRebindWitness(
  championRuns,
  candidateRuns,
  policy = {}
) {
  const required = policy.exerciseLocalPreviousResponseIdRebind === true;
  const empty = {
    required,
    target_request_count: 0,
    expected_target_request_count: required
      ? Number(policy.expectedLocalPreviousResponseIdRebindRequests ?? 0)
      : 0,
    target_request_count_expected: !required,
    target_markers_symmetric: true,
    tool_protocol_match: true,
    local_previous_response_ids_present: true,
    regenerated_tool_pairs_present: true,
    regenerated_tool_pair_counts_match: true,
    client_input_fingerprints_match: true,
    instructions_match: true,
    tools_schema_match: true,
    pre_input_wire_match: true,
    candidate_final_input_diff_observed: true,
    candidate_predecessor_prefix_restored: true,
    champion_predecessor_prefix_not_restored: true,
    candidate_pre_input_static: true,
    pass: !required
  };
  if (!required) return empty;

  const championByPair = new Map(array(championRuns).map((run) => [run?.pair, run]));
  const candidateByPair = new Map(array(candidateRuns).map((run) => [run?.pair, run]));
  for (const [pair, championRun] of championByPair) {
    const candidateRun = candidateByPair.get(pair);
    if (!candidateRun) {
      empty.target_markers_symmetric = false;
      empty.candidate_predecessor_prefix_restored = false;
      empty.champion_predecessor_prefix_not_restored = false;
      continue;
    }
    const championRequests = array(championRun?.requests);
    const candidateRequests = array(candidateRun?.requests);
    if (championRequests.length !== candidateRequests.length) {
      empty.target_markers_symmetric = false;
      empty.candidate_predecessor_prefix_restored = false;
      empty.champion_predecessor_prefix_not_restored = false;
      continue;
    }
    for (let index = 0; index < championRequests.length; index += 1) {
      const championRequest = championRequests[index];
      const candidateRequest = candidateRequests[index];
      const championTarget = championRequest?.local_rebind_target === true;
      const candidateTarget = candidateRequest?.local_rebind_target === true;
      if (!championTarget && !candidateTarget) continue;
      empty.target_request_count += 1;
      const expectedToolProtocol = normalizeToolProtocol(policy.expectedToolProtocol ?? "function");
      if (!expectedToolProtocol ||
        championRequest?.fixture_tool_protocol !== expectedToolProtocol ||
        candidateRequest?.fixture_tool_protocol !== expectedToolProtocol) {
        empty.tool_protocol_match = false;
      }
      if (!championTarget || !candidateTarget) {
        empty.target_markers_symmetric = false;
      }
      if (!championRequest?.local_previous_response_id_sent ||
        !candidateRequest?.local_previous_response_id_sent ||
        !championRequest?.local_response_id_present ||
        !candidateRequest?.local_response_id_present) {
        empty.local_previous_response_ids_present = false;
      }
      const championRegenerated = number(championRequest?.regenerated_tool_pair_count);
      const candidateRegenerated = number(candidateRequest?.regenerated_tool_pair_count);
      if (championRegenerated <= 0 || candidateRegenerated <= 0) {
        empty.regenerated_tool_pairs_present = false;
      }
      if (championRegenerated !== candidateRegenerated) {
        empty.regenerated_tool_pair_counts_match = false;
      }
      const championOutbound = outboundInputSemanticFingerprints(championRequest);
      const candidateOutbound = outboundInputSemanticFingerprints(candidateRequest);
      if (!championOutbound || !candidateOutbound) {
        empty.instructions_match = false;
        empty.tools_schema_match = false;
        empty.candidate_final_input_diff_observed = false;
      } else {
        if (championOutbound.instructions !== candidateOutbound.instructions) {
          empty.instructions_match = false;
        }
        if (championOutbound.tools_schema !== candidateOutbound.tools_schema) {
          empty.tools_schema_match = false;
        }
        if (championOutbound.input_full === candidateOutbound.input_full) {
          empty.candidate_final_input_diff_observed = false;
        }
      }
      const championPreInputWire = outboundPreInputWireFingerprint(championRequest);
      const candidatePreInputWire = outboundPreInputWireFingerprint(candidateRequest);
      if (!championPreInputWire || championPreInputWire !== candidatePreInputWire) {
        empty.pre_input_wire_match = false;
        empty.candidate_pre_input_static = false;
      }
      if (!championRequest.input_fingerprint ||
        championRequest.input_fingerprint !== candidateRequest.input_fingerprint) {
        empty.client_input_fingerprints_match = false;
      }
      const previousChampion = championRequests[index - 1];
      const previousCandidate = candidateRequests[index - 1];
      const championPreviousFull = previousChampion?.outbound_prefix_fingerprints?.input_full;
      const candidatePreviousFull = previousCandidate?.outbound_prefix_fingerprints?.input_full;
      const championPrefixes = outboundInputPrefixFingerprints(championRequest);
      const candidatePrefixes = outboundInputPrefixFingerprints(candidateRequest);
      if (index <= 0 || !championPreviousFull || !candidatePreviousFull ||
        !championPrefixes || !candidatePrefixes) {
        empty.candidate_predecessor_prefix_restored = false;
        empty.champion_predecessor_prefix_not_restored = false;
      } else {
        if (!candidatePrefixes.includes(candidatePreviousFull)) {
          empty.candidate_predecessor_prefix_restored = false;
        }
        if (championPrefixes.includes(championPreviousFull)) {
          empty.champion_predecessor_prefix_not_restored = false;
        }
      }
    }
  }
  empty.target_request_count_expected = empty.target_request_count > 0 &&
    (empty.expected_target_request_count <= 0 ||
      empty.target_request_count === empty.expected_target_request_count);
  empty.pass = empty.target_request_count > 0 &&
    empty.target_request_count_expected &&
    empty.target_markers_symmetric &&
    empty.tool_protocol_match &&
    empty.local_previous_response_ids_present &&
    empty.regenerated_tool_pairs_present &&
    empty.regenerated_tool_pair_counts_match &&
    empty.client_input_fingerprints_match &&
    empty.instructions_match &&
    empty.tools_schema_match &&
    empty.pre_input_wire_match &&
    empty.candidate_final_input_diff_observed &&
    empty.candidate_predecessor_prefix_restored &&
    empty.champion_predecessor_prefix_not_restored &&
    empty.candidate_pre_input_static;
  return empty;
}

// A separate correctness witness for a local previous_response_id FullReplay
// whose closed tool call ids remain unchanged. This must not be folded into
// the historical rebind witness: the latter intentionally requires a real
// call-id rewrite and a candidate-only input difference.
function localPreviousResponseIdFullReplayWitness(
  championRuns,
  candidateRuns,
  policy = {}
) {
  const required = policy.exerciseLocalPreviousResponseIdFullReplay === true;
  const empty = {
    required,
    target_request_count: 0,
    expected_target_request_count: required
      ? Number(policy.expectedLocalPreviousResponseIdFullReplayRequests ?? 0)
      : 0,
    target_request_count_expected: !required,
    target_markers_symmetric: true,
    target_mode_match: true,
    tool_protocol_match: true,
    local_previous_response_ids_present: true,
    regenerated_tool_pairs_absent: true,
    regenerated_tool_pair_counts_match: true,
    client_input_fingerprints_match: true,
    instructions_match: true,
    tools_schema_match: true,
    pre_input_wire_match: true,
    final_input_match: true,
    predecessor_prefix_restored: true,
    material_tool_tail_predecessor_observed: true,
    direct_successor_observed: true,
    champion_maturity_wait_observed: true,
    candidate_maturity_wait_observed: true,
    maturity_wait_reason_match: true,
    maturity_wait_source_match: true,
    maturity_wait_bounded: true,
    pass: !required
  };
  if (!required) return empty;

  const championByPair = new Map(array(championRuns).map((run) => [run?.pair, run]));
  const candidateByPair = new Map(array(candidateRuns).map((run) => [run?.pair, run]));
  const expectedToolProtocol = normalizeToolProtocol(policy.expectedToolProtocol ?? "function");
  const isMaterialToolTailPredecessor = (request) => {
    const source = String(request?.tail_source ?? "");
    const outputChars = Math.max(
      number(request?.tail_tool_output_chars),
      number(request?.tail_largest_tool_output_chars),
      number(request?.tail_tool_call_chars)
    );
    return number(request?.input_tokens) >= 16_384 &&
      new Set(["tool_output", "mixed", "tool_call"]).has(source) &&
      outputChars >= 8_192;
  };
  const isMaturityWait = (request) =>
    number(request?.prefix_guard_wait_ms) > 0 &&
    number(request?.prefix_guard_wait_ms) <= 500 &&
    request?.prefix_guard_wait_reason === "responses_material_tool_tail_maturity_pending" &&
    request?.prefix_guard_wait_source === "exact";

  for (const [pair, championRun] of championByPair) {
    const candidateRun = candidateByPair.get(pair);
    if (!candidateRun) {
      empty.target_markers_symmetric = false;
      empty.predecessor_prefix_restored = false;
      empty.material_tool_tail_predecessor_observed = false;
      empty.direct_successor_observed = false;
      empty.champion_maturity_wait_observed = false;
      empty.candidate_maturity_wait_observed = false;
      continue;
    }
    const championRequests = array(championRun?.requests);
    const candidateRequests = array(candidateRun?.requests);
    if (championRequests.length !== candidateRequests.length) {
      empty.target_markers_symmetric = false;
      empty.predecessor_prefix_restored = false;
      empty.material_tool_tail_predecessor_observed = false;
      empty.direct_successor_observed = false;
      empty.champion_maturity_wait_observed = false;
      empty.candidate_maturity_wait_observed = false;
      continue;
    }
    for (let index = 0; index < championRequests.length; index += 1) {
      const championRequest = championRequests[index];
      const candidateRequest = candidateRequests[index];
      const championTarget = championRequest?.local_full_replay_target === true;
      const candidateTarget = candidateRequest?.local_full_replay_target === true;
      if (!championTarget && !candidateTarget) continue;
      empty.target_request_count += 1;
      if (!championTarget || !candidateTarget) empty.target_markers_symmetric = false;
      if (championRequest?.local_rebind_target === true || candidateRequest?.local_rebind_target === true) {
        empty.target_mode_match = false;
      }
      if (!expectedToolProtocol ||
        championRequest?.fixture_tool_protocol !== expectedToolProtocol ||
        candidateRequest?.fixture_tool_protocol !== expectedToolProtocol) {
        empty.tool_protocol_match = false;
      }
      if (!championRequest?.local_previous_response_id_sent ||
        !candidateRequest?.local_previous_response_id_sent ||
        !championRequest?.local_response_id_present ||
        !candidateRequest?.local_response_id_present) {
        empty.local_previous_response_ids_present = false;
      }
      const championRegenerated = number(championRequest?.regenerated_tool_pair_count);
      const candidateRegenerated = number(candidateRequest?.regenerated_tool_pair_count);
      if (championRegenerated !== 0 || candidateRegenerated !== 0) {
        empty.regenerated_tool_pairs_absent = false;
      }
      if (championRegenerated !== candidateRegenerated) {
        empty.regenerated_tool_pair_counts_match = false;
      }
      const championOutbound = outboundInputSemanticFingerprints(championRequest);
      const candidateOutbound = outboundInputSemanticFingerprints(candidateRequest);
      if (!championOutbound || !candidateOutbound) {
        empty.instructions_match = false;
        empty.tools_schema_match = false;
        empty.final_input_match = false;
      } else {
        if (championOutbound.instructions !== candidateOutbound.instructions) {
          empty.instructions_match = false;
        }
        if (championOutbound.tools_schema !== candidateOutbound.tools_schema) {
          empty.tools_schema_match = false;
        }
        if (championOutbound.input_full !== candidateOutbound.input_full) {
          empty.final_input_match = false;
        }
      }
      const championPreInputWire = outboundPreInputWireFingerprint(championRequest);
      const candidatePreInputWire = outboundPreInputWireFingerprint(candidateRequest);
      if (!championPreInputWire || championPreInputWire !== candidatePreInputWire) {
        empty.pre_input_wire_match = false;
      }
      if (!championRequest.input_fingerprint ||
        championRequest.input_fingerprint !== candidateRequest.input_fingerprint) {
        empty.client_input_fingerprints_match = false;
      }
      const previousChampion = championRequests[index - 1];
      const previousCandidate = candidateRequests[index - 1];
      const previousChampionFull = previousChampion?.outbound_prefix_fingerprints?.input_full;
      const previousCandidateFull = previousCandidate?.outbound_prefix_fingerprints?.input_full;
      const championPrefixes = outboundInputPrefixFingerprints(championRequest);
      const candidatePrefixes = outboundInputPrefixFingerprints(candidateRequest);
      if (index <= 0 || !previousChampionFull || !previousCandidateFull ||
        !championPrefixes || !candidatePrefixes ||
        !championPrefixes.includes(previousChampionFull) ||
        !candidatePrefixes.includes(previousCandidateFull)) {
        empty.predecessor_prefix_restored = false;
      }
      if (!isMaterialToolTailPredecessor(previousChampion) ||
        !isMaterialToolTailPredecessor(previousCandidate)) {
        empty.material_tool_tail_predecessor_observed = false;
      }
      if (championRequest?.phase !== "followup-2" ||
        candidateRequest?.phase !== "followup-2" ||
        championRequest?.request_kind !== "turn" ||
        candidateRequest?.request_kind !== "turn" ||
        championRequest?.sse_completed !== true ||
        candidateRequest?.sse_completed !== true) {
        empty.direct_successor_observed = false;
      }
      const championWait = isMaturityWait(championRequest);
      const candidateWait = isMaturityWait(candidateRequest);
      if (!championWait) empty.champion_maturity_wait_observed = false;
      if (!candidateWait) empty.candidate_maturity_wait_observed = false;
      if (championRequest?.prefix_guard_wait_reason !== candidateRequest?.prefix_guard_wait_reason) {
        empty.maturity_wait_reason_match = false;
      }
      if (championRequest?.prefix_guard_wait_source !== candidateRequest?.prefix_guard_wait_source) {
        empty.maturity_wait_source_match = false;
      }
      if ((number(championRequest?.prefix_guard_wait_ms) > 500) ||
        (number(candidateRequest?.prefix_guard_wait_ms) > 500)) {
        empty.maturity_wait_bounded = false;
      }
    }
  }
  empty.target_request_count_expected = empty.target_request_count > 0 &&
    (empty.expected_target_request_count <= 0 ||
      empty.target_request_count === empty.expected_target_request_count);
  empty.pass = empty.target_request_count > 0 &&
    empty.target_request_count_expected &&
    empty.target_markers_symmetric &&
    empty.target_mode_match &&
    empty.tool_protocol_match &&
    empty.local_previous_response_ids_present &&
    empty.regenerated_tool_pairs_absent &&
    empty.regenerated_tool_pair_counts_match &&
    empty.client_input_fingerprints_match &&
    empty.instructions_match &&
    empty.tools_schema_match &&
    empty.pre_input_wire_match &&
    empty.final_input_match &&
    empty.predecessor_prefix_restored &&
    empty.material_tool_tail_predecessor_observed &&
    empty.direct_successor_observed &&
    empty.champion_maturity_wait_observed &&
    empty.candidate_maturity_wait_observed &&
    empty.maturity_wait_reason_match &&
    empty.maturity_wait_source_match &&
    empty.maturity_wait_bounded;
  return empty;
}

// A candidate-only cache-control field is expected to change final metadata
// and therefore the whole pre-input wire digest. It must not make the actual
// input, instructions, or tool schema differ. This narrow policy lets the
// verifier distinguish that declared treatment difference from an accidental
// semantic-wire regression.
function normalizeCacheControlSymmetryPolicy(value) {
  const candidateField = String(value?.candidateCacheControlField ?? "").trim();
  const enabled = new Set([
    "prompt-cache-key",
    "prompt-cache-options",
    "prompt-cache-retention"
  ]).has(candidateField);
  return {
    enabled,
    expected_candidate_field: enabled ? candidateField : null,
    require_candidate_options_24h:
      enabled && candidateField === "prompt-cache-options" &&
      value?.candidateCacheOptions24h === true,
    exercise_local_previous_response_id_rebind:
      value?.exerciseLocalPreviousResponseIdRebind === true,
    expected_local_previous_response_id_rebind_requests:
      Number(value?.expectedLocalPreviousResponseIdRebindRequests ?? 0),
    exercise_local_previous_response_id_full_replay:
      value?.exerciseLocalPreviousResponseIdFullReplay === true,
    expected_local_previous_response_id_full_replay_requests:
      Number(value?.expectedLocalPreviousResponseIdFullReplayRequests ?? 0),
    expected_tool_protocol: normalizeToolProtocol(value?.toolProtocol ?? "function") ?? null
  };
}

// Compare the request evidence that actually left the proxy, not merely the
// nominal scenario fixture. The same client input can be serialized or
// transformed differently by the two versions; that is not valid A/B evidence
// even when a provider happens to return a cache hit.
function pairedInputSymmetry(
  champion,
  candidate,
  maxInputTokenDelta = 128,
  cacheControlPolicy = null
) {
  return pairedRunInputSymmetry(
    array(champion?.runs),
    array(candidate?.runs),
    maxInputTokenDelta,
    true,
    cacheControlPolicy
  );
}

// Keep the dynamic-tail-specific diagnostic because its follow-up evidence is
// useful when tuning a growing context. Promotion itself uses the all-scenario
// proof above, so a full-replay or compaction comparison cannot bypass it.
function pairedDynamicInputSymmetry(
  champion,
  candidate,
  maxInputTokenDelta = 128,
  cacheControlPolicy = null
) {
  const championRuns = array(champion?.runs).filter(
    (run) => run?.scenario === "dynamic-tail-mix"
  );
  const candidateRuns = array(candidate?.runs).filter(
    (run) => run?.scenario === "dynamic-tail-mix"
  );
  const dynamicExpected = number(champion?.metrics?.dynamic_tail_mix?.injections) > 0 ||
    number(candidate?.metrics?.dynamic_tail_mix?.injections) > 0;
  return pairedRunInputSymmetry(
    championRuns,
    candidateRuns,
    maxInputTokenDelta,
    dynamicExpected,
    cacheControlPolicy
  );
}

// The thread-stable bridge is a candidate-only placement experiment, not a
// semantic-input treatment.  Keep a separate witness so a successful cache
// result cannot be attributed when the candidate changed instructions,
// tools, pre-input metadata, or the dynamic input itself.  The only expected
// final-wire difference is the one-way provider placement fingerprint emitted
// for prompt_cache_key; raw keys are never retained.
function threadStablePromptCacheKeyBridgeWireWitness(
  champion,
  candidate,
  required = false,
  maxInputTokenDelta = 128,
  requireIdentityChurn = false
) {
  if (!required) {
    return {
      required: false,
      applicable: false,
      pass: true,
      scenario_match: true,
      dynamic_input_symmetry: true,
      pair_count: 0,
      request_pair_count: 0,
      semantic_wire_match: true,
      prompt_cache_key_present: true,
      prompt_cache_key_differs: true,
      identity_churn_required: false,
      identity_churn_observed: true,
      unexpected_wire_differences: []
    };
  }
  const championRuns = array(champion?.runs);
  const candidateRuns = array(candidate?.runs);
  const dynamicInput = pairedDynamicInputSymmetry(
    champion,
    candidate,
    maxInputTokenDelta,
    null
  );
  const championByPair = new Map(championRuns.map((run) => [run?.pair, run]));
  const candidateByPair = new Map(candidateRuns.map((run) => [run?.pair, run]));
  const pairIds = [...new Set([
    ...championRuns.map((run) => run?.pair),
    ...candidateRuns.map((run) => run?.pair)
  ])].filter((pair) => Number.isInteger(pair)).sort((a, b) => a - b);
  const unexpected = new Set();
  let scenarioMatch = championRuns.length > 0 && candidateRuns.length > 0;
  let pairAligned = championRuns.length === candidateRuns.length &&
    championRuns.length > 0 &&
    championRuns.every((run) => candidateByPair.has(run?.pair));
  let requestPairCount = 0;
  let promptCacheKeyPresent = true;
  let promptCacheKeyDiffers = true;
  let identityChurnObserved = !requireIdentityChurn;
  let identityThreadStable = true;
  const championIdentityPhases = [];
  const candidateIdentityPhases = [];
  let semanticWireMatch = true;
  const equalStringWireFields = [
    "input_full",
    "instructions",
    "tools_schema",
    "pre_input_wire",
    "cache_metadata"
  ];
  for (const run of [...championRuns, ...candidateRuns]) {
    if (run?.scenario !== "dynamic-tail-mix") scenarioMatch = false;
  }
  if (championRuns.some((run) => run?.arm && run.arm !== "champion") ||
    candidateRuns.some((run) => run?.arm && run.arm !== "candidate")) {
    scenarioMatch = false;
    unexpected.add("arm_label");
  }
  for (const championRun of championRuns) {
    const candidateRun = candidateByPair.get(championRun?.pair);
    if (!candidateRun) {
      pairAligned = false;
      continue;
    }
    const championRequests = array(championRun.requests);
    const candidateRequests = array(candidateRun.requests);
    if (championRequests.length !== candidateRequests.length || championRequests.length === 0) {
      pairAligned = false;
      continue;
    }
    for (let index = 0; index < championRequests.length; index += 1) {
      const baseline = championRequests[index];
      const contender = candidateRequests[index];
      requestPairCount += 1;
      championIdentityPhases.push(baseline?.fixture_identity_phase ?? null);
      candidateIdentityPhases.push(contender?.fixture_identity_phase ?? null);
      if (baseline?.fixture_identity_thread_stable !== true ||
        contender?.fixture_identity_thread_stable !== true) {
        identityThreadStable = false;
        unexpected.add("thread_identity_not_stable");
      }
      if (baseline?.fixture_identity_phase !== contender?.fixture_identity_phase) {
        unexpected.add("identity_phase_mismatch");
      }
      if (baseline?.phase !== contender?.phase || baseline?.request_kind !== contender?.request_kind) {
        unexpected.add("phase_or_request_kind");
      }
      const baselineWire = baseline?.outbound_prefix_fingerprints;
      const contenderWire = contender?.outbound_prefix_fingerprints;
      for (const field of equalStringWireFields) {
        if (typeof baselineWire?.[field] !== "string" ||
          typeof contenderWire?.[field] !== "string") {
          semanticWireMatch = false;
          unexpected.add(`missing_${field}`);
        } else if (baselineWire[field] !== contenderWire[field]) {
          semanticWireMatch = false;
          unexpected.add(field);
        }
      }
      const baselinePrefixes = baselineWire?.input_prefixes;
      const contenderPrefixes = contenderWire?.input_prefixes;
      if (!Array.isArray(baselinePrefixes) || !Array.isArray(contenderPrefixes)) {
        semanticWireMatch = false;
        unexpected.add("missing_input_prefixes");
      } else if (JSON.stringify(baselinePrefixes) !== JSON.stringify(contenderPrefixes)) {
        semanticWireMatch = false;
        unexpected.add("input_prefixes");
      }
      if (baseline?.provider_prefix_fingerprint !== contender?.provider_prefix_fingerprint) {
        semanticWireMatch = false;
        unexpected.add("provider_prefix");
      }
      const championKey = typeof baseline?.provider_prefix_key_fingerprint === "string"
        ? baseline.provider_prefix_key_fingerprint : "";
      const candidateKey = typeof contender?.provider_prefix_key_fingerprint === "string"
        ? contender.provider_prefix_key_fingerprint : "";
      if (!championKey || !candidateKey) {
        promptCacheKeyPresent = false;
        unexpected.add("prompt_cache_key_missing");
      } else if (championKey === candidateKey) {
        promptCacheKeyDiffers = false;
        unexpected.add("prompt_cache_key_not_different");
      }
      const baselineTokens = number(baseline?.input_tokens);
      const candidateTokens = number(contender?.input_tokens);
      if (baselineTokens <= 0 || candidateTokens <= 0 ||
        Math.abs(baselineTokens - candidateTokens) > maxInputTokenDelta) {
        unexpected.add("dynamic_input_token_delta");
      }
    }
  }
  if (requireIdentityChurn) {
    const hasBase = championIdentityPhases.includes("base") &&
      candidateIdentityPhases.includes("base");
    const hasRotated = championIdentityPhases.includes("rotated") &&
      candidateIdentityPhases.includes("rotated");
    identityChurnObserved = hasBase && hasRotated && identityThreadStable;
    if (!identityChurnObserved) unexpected.add("identity_churn_missing");
  }
  const pass = scenarioMatch && dynamicInput.pass && pairAligned && requestPairCount > 0 &&
    semanticWireMatch && promptCacheKeyPresent && promptCacheKeyDiffers &&
    identityChurnObserved && unexpected.size === 0;
  return {
    required: true,
    applicable: true,
    pass,
    scenario_match: scenarioMatch,
    dynamic_input_symmetry: dynamicInput.pass,
    pair_count: pairIds.length,
    request_pair_count: requestPairCount,
    pair_ids: pairIds,
    semantic_wire_match: semanticWireMatch,
    prompt_cache_key_present: promptCacheKeyPresent,
    prompt_cache_key_differs: promptCacheKeyDiffers,
    identity_churn_required: requireIdentityChurn,
    identity_churn_observed: identityChurnObserved,
    identity_thread_stable: identityThreadStable,
    champion_identity_phases: [...new Set(championIdentityPhases)],
    candidate_identity_phases: [...new Set(candidateIdentityPhases)],
    unexpected_wire_differences: [...unexpected].sort(),
    dynamic_input_evidence: dynamicInput
  };
}

function pairedRunInputSymmetry(
  championRuns,
  candidateRuns,
  maxInputTokenDelta,
  required,
  cacheControlPolicy = null
) {
  const controlPolicy = normalizeCacheControlSymmetryPolicy(cacheControlPolicy);
  const localRebindPolicy = {
    exerciseLocalPreviousResponseIdRebind:
      controlPolicy.exercise_local_previous_response_id_rebind,
    expectedLocalPreviousResponseIdRebindRequests:
      controlPolicy.expected_local_previous_response_id_rebind_requests,
    expectedToolProtocol: controlPolicy.expected_tool_protocol
  };
  const localFullReplayPolicy = {
    exerciseLocalPreviousResponseIdFullReplay:
      controlPolicy.exercise_local_previous_response_id_full_replay,
    expectedLocalPreviousResponseIdFullReplayRequests:
      controlPolicy.expected_local_previous_response_id_full_replay_requests,
    expectedToolProtocol: controlPolicy.expected_tool_protocol
  };
  const championPairIds = championRuns.map((run) => run?.pair);
  const candidatePairIds = candidateRuns.map((run) => run?.pair);
  const anyRuns = championRuns.length > 0 || candidateRuns.length > 0;
  const applicable = required || anyRuns;
  if (!applicable) {
    return {
      applicable: false,
      pass: true,
      pair_count: 0,
      request_pair_count: 0,
      max_input_token_delta: 0,
      max_warm_input_token_delta: 0,
      max_cold_seed_input_token_delta: 0,
      allowed_input_token_delta: maxInputTokenDelta,
      phases_match: true,
      input_fingerprints_match: true,
      client_input_fingerprints_match: true,
      outbound_semantic_fingerprints_match: true,
      actual_outbound_semantic_fingerprints_match: true,
      pre_input_wire_fingerprints_match: true,
      declared_cache_control_difference: {
        required: controlPolicy.enabled,
        expected_candidate_field: controlPolicy.expected_candidate_field,
        require_candidate_options_24h: controlPolicy.require_candidate_options_24h,
        pre_input_wire_differences: 0,
        attributed_differences: 0,
        unattributed_differences: 0,
        pass: true
      },
      request_kinds_match: true,
      terminal_sse_complete: true,
      pair_ids_valid: true,
      pair_ids_unique: true,
      all_pairs_have_requests: true,
      input_tokens_present: true,
      runs_present: false,
      scored_pair_ids: [],
      local_previous_response_id_rebind:
        localPreviousResponseIdRebindWitness([], [], localRebindPolicy),
      local_previous_response_id_full_replay:
        localPreviousResponseIdFullReplayWitness([], [], localFullReplayPolicy)
    };
  }
  const pairIdsValid = [...championPairIds, ...candidatePairIds].every(
    (pair) => Number.isInteger(pair) && pair >= 0
  );
  const championPairIdsUnique = new Set(championPairIds).size === championPairIds.length;
  const candidatePairIdsUnique = new Set(candidatePairIds).size === candidatePairIds.length;
  const championByPair = new Map(championRuns.map((run) => [run?.pair, run]));
  const candidateByPair = new Map(candidateRuns.map((run) => [run?.pair, run]));
  const runsPresent = championRuns.length > 0 && candidateRuns.length > 0;
  let requestPairCount = 0;
  let maxDelta = 0;
  let maxWarmInputTokenDelta = 0;
  let maxColdSeedInputTokenDelta = 0;
  let pairsAligned = pairIdsValid && championPairIdsUnique && candidatePairIdsUnique &&
    championRuns.length === candidateRuns.length &&
    championByPair.size === championRuns.length &&
    candidateByPair.size === candidateRuns.length &&
    championPairIds.every((pair) => candidateByPair.has(pair));
  let phasesMatch = true;
  let inputFingerprintsMatch = true;
  let outboundSemanticFingerprintsMatch = true;
  let preInputWireFingerprintsMatch = true;
  let preInputWireDifferences = 0;
  let attributedCacheControlDifferences = 0;
  let unattributedCacheControlDifferences = 0;
  let requestKindsMatch = true;
  let terminalSseComplete = true;
  let inputTokensPresent = true;
  let allPairsHaveRequests = true;
  for (const championRun of championRuns) {
    const candidateRun = candidateByPair.get(championRun?.pair);
    if (!candidateRun) {
      pairsAligned = false;
      continue;
    }
    const championRequests = array(championRun.requests);
    const candidateRequests = array(candidateRun.requests);
    if (championRequests.length === 0 || candidateRequests.length === 0) {
      allPairsHaveRequests = false;
      pairsAligned = false;
    }
    if (championRequests.length !== candidateRequests.length) {
      pairsAligned = false;
      continue;
    }
    for (let index = 0; index < championRequests.length; index += 1) {
      const baseline = championRequests[index];
      const contender = candidateRequests[index];
      requestPairCount += 1;
      const baselinePhase = typeof baseline?.phase === "string" ? baseline.phase : "";
      const contenderPhase = typeof contender?.phase === "string" ? contender.phase : "";
      if (!baselinePhase || baselinePhase !== contenderPhase) {
        phasesMatch = false;
        pairsAligned = false;
      }
      const baselineKind = typeof baseline?.request_kind === "string" ? baseline.request_kind : "";
      const contenderKind = typeof contender?.request_kind === "string" ? contender.request_kind : "";
      if (!baselineKind || baselineKind !== contenderKind) {
        requestKindsMatch = false;
        pairsAligned = false;
      }
      if (baseline?.sse_completed !== true || contender?.sse_completed !== true) {
        terminalSseComplete = false;
      }
      const baselineFingerprint = String(baseline?.input_fingerprint ?? "");
      const contenderFingerprint = String(contender?.input_fingerprint ?? "");
      if (!baselineFingerprint || baselineFingerprint !== contenderFingerprint) {
        inputFingerprintsMatch = false;
      }
      const baselineOutbound = outboundInputSemanticFingerprints(baseline);
      const contenderOutbound = outboundInputSemanticFingerprints(contender);
      const localRebindTarget = controlPolicy.exercise_local_previous_response_id_rebind &&
        baseline?.local_rebind_target === true && contender?.local_rebind_target === true;
      const nonInputSemanticDifference = Boolean(baselineOutbound && contenderOutbound) &&
        ["instructions", "tools_schema"].some(
          (field) => baselineOutbound[field] !== contenderOutbound[field]
        );
      const inputFullDifference = Boolean(baselineOutbound && contenderOutbound) &&
        baselineOutbound.input_full !== contenderOutbound.input_full;
      if (!baselineOutbound || !contenderOutbound ||
        nonInputSemanticDifference || (inputFullDifference && !localRebindTarget)) {
        outboundSemanticFingerprintsMatch = false;
      }
      const baselinePreInputWire = outboundPreInputWireFingerprint(baseline);
      const contenderPreInputWire = outboundPreInputWireFingerprint(contender);
      const baselineCacheMetadata = outboundCacheMetadataFingerprint(baseline);
      const contenderCacheMetadata = outboundCacheMetadataFingerprint(contender);
      const baselineProviderPrefixKey = typeof baseline?.provider_prefix_key_fingerprint === "string"
        ? baseline.provider_prefix_key_fingerprint
        : "";
      const contenderProviderPrefixKey = typeof contender?.provider_prefix_key_fingerprint === "string"
        ? contender.provider_prefix_key_fingerprint
        : "";
      const candidateFieldObserved = array(contender?.cache_control_fields).includes(
        controlPolicy.expected_candidate_field
      );
      const candidateOptions24hObserved =
        !controlPolicy.require_candidate_options_24h || contender?.cache_options_24h === true;
      const metadataActuallyDiffers = Boolean(
        baselineCacheMetadata && contenderCacheMetadata &&
        baselineCacheMetadata !== contenderCacheMetadata
      );
      // prompt_cache_key is represented separately from the semantic
      // pre-input fingerprint: it is intentionally redacted from the
      // metadata digest, while its provider-prefix fingerprint remains a
      // first-class wire witness. Treat that narrow, expected difference as
      // attributable PCK evidence; no other field gets this exception.
      const providerPrefixKeyActuallyDiffers = Boolean(
        baselineProviderPrefixKey && contenderProviderPrefixKey &&
        baselineProviderPrefixKey !== contenderProviderPrefixKey
      );
      const controlDifferenceActuallyDiffers = metadataActuallyDiffers || (
        controlPolicy.expected_candidate_field === "prompt-cache-key" &&
        providerPrefixKeyActuallyDiffers
      );
      const semanticFieldsMatch = Boolean(baselineOutbound && contenderOutbound) &&
        Object.keys(baselineOutbound).every(
          (field) => baselineOutbound[field] === contenderOutbound[field]
        );
      const preInputWireDiffers = !baselinePreInputWire || !contenderPreInputWire ||
        baselinePreInputWire !== contenderPreInputWire;
      if (preInputWireDiffers) {
        preInputWireFingerprintsMatch = false;
        preInputWireDifferences += 1;
      }
      if (controlPolicy.enabled && candidateFieldObserved && candidateOptions24hObserved &&
        controlDifferenceActuallyDiffers && semanticFieldsMatch) {
        attributedCacheControlDifferences += 1;
      } else if (preInputWireDiffers || controlPolicy.enabled) {
        unattributedCacheControlDifferences += 1;
        if (preInputWireDiffers) outboundSemanticFingerprintsMatch = false;
      }
      const baselineTokens = number(baseline?.input_tokens);
      const contenderTokens = number(contender?.input_tokens);
      if (baselineTokens <= 0 || contenderTokens <= 0) {
        inputTokensPresent = false;
        continue;
      }
      const inputTokenDelta = Math.abs(baselineTokens - contenderTokens);
      maxDelta = Math.max(maxDelta, inputTokenDelta);
      // Cold seeds do not enter cache-hit scoring. When the complete client
      // input and the emitted semantic wire are already equal, a different
      // upstream-reported seed token total is accounting noise, not evidence
      // that either binary sent a different request. Keep it visible, but do
      // not let it veto the warm dynamic comparison. Every non-seed request
      // remains bound to the configured token-delta ceiling.
      if (baselinePhase === "seed") {
        maxColdSeedInputTokenDelta = Math.max(maxColdSeedInputTokenDelta, inputTokenDelta);
      } else {
        maxWarmInputTokenDelta = Math.max(maxWarmInputTokenDelta, inputTokenDelta);
      }
    }
  }
  const scoredPairIds = pairedRunIds(championRuns, candidateRuns);
  const declaredCacheControlDifference = {
    required: controlPolicy.enabled,
    expected_candidate_field: controlPolicy.expected_candidate_field,
    require_candidate_options_24h: controlPolicy.require_candidate_options_24h,
    pre_input_wire_differences: preInputWireDifferences,
    attributed_differences: attributedCacheControlDifferences,
    unattributed_differences: unattributedCacheControlDifferences,
    // When a candidate-only cache control is requested, every compared
    // request must carry a real, attributable final-wire difference.  A
    // receipt/field marker without a wire change is a no-op and must fail
    // closed instead of being accepted as treatment evidence.
    pass: controlPolicy.enabled
      ? requestPairCount > 0 &&
        attributedCacheControlDifferences === requestPairCount &&
        unattributedCacheControlDifferences === 0
      : unattributedCacheControlDifferences === 0
  };
  const localPreviousResponseIdRebind = localPreviousResponseIdRebindWitness(
    championRuns,
    candidateRuns,
    localRebindPolicy
  );
  const localPreviousResponseIdFullReplay = localPreviousResponseIdFullReplayWitness(
    championRuns,
    candidateRuns,
    localFullReplayPolicy
  );
  return {
    applicable: true,
    pass: runsPresent && pairsAligned && allPairsHaveRequests && requestPairCount > 0 && phasesMatch &&
      inputFingerprintsMatch && outboundSemanticFingerprintsMatch && requestKindsMatch &&
      declaredCacheControlDifference.pass && terminalSseComplete && inputTokensPresent &&
      maxWarmInputTokenDelta <= maxInputTokenDelta &&
      localPreviousResponseIdRebind.pass &&
      localPreviousResponseIdFullReplay.pass,
    pair_count: championRuns.length,
    request_pair_count: requestPairCount,
    max_input_token_delta: maxDelta,
    max_warm_input_token_delta: maxWarmInputTokenDelta,
    max_cold_seed_input_token_delta: maxColdSeedInputTokenDelta,
    allowed_input_token_delta: maxInputTokenDelta,
    phases_match: phasesMatch,
    input_fingerprints_match: inputFingerprintsMatch,
    client_input_fingerprints_match: inputFingerprintsMatch,
    outbound_semantic_fingerprints_match: outboundSemanticFingerprintsMatch,
    actual_outbound_semantic_fingerprints_match: outboundSemanticFingerprintsMatch,
    pre_input_wire_fingerprints_match: preInputWireFingerprintsMatch,
    declared_cache_control_difference: declaredCacheControlDifference,
    request_kinds_match: requestKindsMatch,
    terminal_sse_complete: terminalSseComplete,
    pair_ids_valid: pairIdsValid,
    pair_ids_unique: championPairIdsUnique && candidatePairIdsUnique,
    all_pairs_have_requests: allPairsHaveRequests,
    input_tokens_present: inputTokensPresent,
    runs_present: runsPresent,
    scored_pair_ids: scoredPairIds,
    local_previous_response_id_rebind: localPreviousResponseIdRebind,
    local_previous_response_id_full_replay: localPreviousResponseIdFullReplay
  };
}

function compareArmResults(
  champion,
  candidate,
  maxTtftRegressionMs,
  maxLocalProxyOverheadRegressionMs = 0,
  maxFullBucketRegressionRequests = 0,
  requireTtftNoRegression = true,
  maxInputTokenDelta = 128,
  nativePlacementIsolationRequired = false,
  comparisonPolicy = {}
) {
  const effectiveRequireTtftNoRegression = requireTtftNoRegression === true;
  // A cache improvement may not buy local foreground delay. This stays strict
  // even for internal/offline callers so there is no alternate promotion path
  // that can accidentally reintroduce a local allowance.
  const effectiveMaxLocalProxyOverheadRegressionMs = 0;
  void maxLocalProxyOverheadRegressionMs;
  const cohortMatches = sameCohort(champion.cohort, candidate.cohort);
  const observedRealmMatches = champion.metrics.observed_realm_ids.length === 1 &&
    candidate.metrics.observed_realm_ids.length === 1 &&
    champion.metrics.observed_realm_ids[0] === candidate.metrics.observed_realm_ids[0];
  const championSeedCacheReadEvidence = aggregateSeedCacheReadEvidence(champion);
  const candidateSeedCacheReadEvidence = aggregateSeedCacheReadEvidence(candidate);
  const seedCacheReadEvidenceComplete =
    championSeedCacheReadEvidence.raw_complete && candidateSeedCacheReadEvidence.raw_complete;
  const seedCacheReadSymmetry = seedCacheReadEvidenceComplete &&
    championSeedCacheReadEvidence.tokens === candidateSeedCacheReadEvidence.tokens;
  const championColdSeedEvidence = aggregateColdSeedEvidence(champion);
  const candidateColdSeedEvidence = aggregateColdSeedEvidence(candidate);
  const coldSeedEvidenceComplete =
    championColdSeedEvidence.raw_complete && candidateColdSeedEvidence.raw_complete;
  const coldSeedRequestSymmetry = coldSeedEvidenceComplete &&
    championColdSeedEvidence.seed_count === candidateColdSeedEvidence.seed_count;
  const coldSeedSymmetry = coldSeedRequestSymmetry &&
    championColdSeedEvidence.count === candidateColdSeedEvidence.count;
  const candidateNoExtraColdStart = coldSeedEvidenceComplete &&
    candidateColdSeedEvidence.count <= championColdSeedEvidence.count;
  const cacheControlSymmetryPolicy = {
    candidateCacheControlField: comparisonPolicy?.candidate_cache_control_field,
    candidateCacheOptions24h: comparisonPolicy?.candidate_cache_options_24h === true,
    exerciseLocalPreviousResponseIdRebind:
      comparisonPolicy?.exercise_local_previous_response_id_rebind === true,
    expectedLocalPreviousResponseIdRebindRequests:
      comparisonPolicy?.expected_local_previous_response_id_rebind_requests ?? 0,
    exerciseLocalPreviousResponseIdFullReplay:
      comparisonPolicy?.exercise_local_previous_response_id_full_replay === true,
    expectedLocalPreviousResponseIdFullReplayRequests:
      comparisonPolicy?.expected_local_previous_response_id_full_replay_requests ?? 0,
    toolProtocol: comparisonPolicy?.tool_protocol ?? "function"
  };
  const dynamicInputSymmetry = pairedDynamicInputSymmetry(
    champion,
    candidate,
    maxInputTokenDelta,
    cacheControlSymmetryPolicy
  );
  const threadStablePromptCacheKeyBridgeWire =
    threadStablePromptCacheKeyBridgeWireWitness(
      champion,
      candidate,
      comparisonPolicy?.candidate_thread_stable_pck_bridge === true,
      maxInputTokenDelta,
      comparisonPolicy?.fixture_identity_churn === true
    );
  const actualOutboundInputSymmetry = pairedInputSymmetry(
    champion,
    candidate,
    maxInputTokenDelta,
    cacheControlSymmetryPolicy
  );
  const localPreviousResponseIdRebind =
    actualOutboundInputSymmetry.local_previous_response_id_rebind ??
    localPreviousResponseIdRebindWitness([], [], cacheControlSymmetryPolicy);
  const localPreviousResponseIdFullReplay =
    actualOutboundInputSymmetry.local_previous_response_id_full_replay ??
    localPreviousResponseIdFullReplayWitness([], [], cacheControlSymmetryPolicy);
  const nativePlacementIsolation = pairedNativePlacementIsolation(
    champion,
    candidate,
    nativePlacementIsolationRequired === true
  );
  const upstreamPlacementCrossoverRequired =
    comparisonPolicy?.require_shared_upstream_placement_crossover === true;
  const upstreamPlacementCrossoverObserved =
    comparisonPolicy?.shared_upstream_placement_crossover_observed === true;
  const upstreamPlacementCrossover = {
    required: upstreamPlacementCrossoverRequired,
    observed: upstreamPlacementCrossoverObserved,
    pass: !upstreamPlacementCrossoverRequired || upstreamPlacementCrossoverObserved,
    // A prompt-cache placement fingerprint is an Atoapi-local fact. It is
    // not a provider capability certificate: the provider can still make one
    // sequential arm's completed request visible to the other arm. Only the
    // interleaved shared crossover balances that effect turn by turn.
    reason: upstreamPlacementCrossoverRequired && !upstreamPlacementCrossoverObserved
      ? "shared_turn_crossover_required_for_live_promotion"
      : "not_applicable_or_observed"
  };
  const sharedTurnCrossover = sharedTurnCrossoverEvidence(
    comparisonPolicy?.shared_turn_crossover
  );
  const sourceAwareSharedCrossover = sourceAwareSharedCrossoverAttribution(
    champion,
    candidate,
    sharedTurnCrossover
  );
  // This is attribution-only evidence for a changing-context run.  It is
  // deliberately kept out of checks, baseline_pass, and promotion gating.
  const dynamicTailWarmAttribution = pairedDynamicTailAttribution(champion, candidate);
  const fullBucketRequestDelta =
    candidate.metrics.full_bucket_requests - champion.metrics.full_bucket_requests;
  const warmFullBucketRequestDelta =
    candidate.metrics.warm_full_bucket_requests - champion.metrics.warm_full_bucket_requests;
  const fullBucketDenominatorsMatch =
    candidate.metrics.warm_full_bucket_denominator === champion.metrics.warm_full_bucket_denominator;
  const fullBucketCountWithinTolerance =
    fullBucketDenominatorsMatch && warmFullBucketRequestDelta >= -maxFullBucketRegressionRequests;
  // A full-bucket request is a useful discrete signal, but it must not veto a
  // demonstrably better aggregate cache result merely because one request
  // crossed a 128-token boundary differently. A stable-prefix ratio can also
  // already be saturated: equal is the best possible result there, so require
  // it not to regress while raw/cacheable hit and a real shortfall measure
  // strictly improve.
  const aggregateTokenHitStrictlyImproves =
    candidate.metrics.warm_raw_token_hit_rate > champion.metrics.warm_raw_token_hit_rate &&
    candidate.metrics.warm_cache_128_hit_rate > champion.metrics.warm_cache_128_hit_rate &&
    candidate.metrics.warm_stable_prefix_hit_rate >= champion.metrics.warm_stable_prefix_hit_rate &&
    candidate.metrics.shortfall_tokens < champion.metrics.shortfall_tokens;
  // A cold read after an otherwise warm prefix belongs to the selected
  // upstream, not to either executable. It may make the opposite arm look
  // artificially superior in a sequential pair, so it cannot qualify as a
  // release-promoting cache improvement. Keep no-regression evidence useful,
  // but require both arms to be free of such a confound before promotion.
  const providerInstabilityFree =
    champion.metrics.provider_unstable_gap_tokens === 0 &&
    candidate.metrics.provider_unstable_gap_tokens === 0;
  const requireCandidateProviderWaterlineRecoveryWait =
    comparisonPolicy?.require_candidate_provider_waterline_recovery_wait === true;
  const candidateProviderWaterlineRecoveryWaitObserved =
    !requireCandidateProviderWaterlineRecoveryWait ||
    candidate.metrics.candidate_provider_waterline_recovery_wait_requests > 0;
  const requireCandidateOptions24hSiblingSettle =
    comparisonPolicy?.require_candidate_options24h_sibling_settle === true;
  const candidateOptions24hSiblingSettleObserved =
    !requireCandidateOptions24hSiblingSettle ||
    candidate.metrics.candidate_options24h_sibling_settle_requests > 0;
  const requireCandidateExactMediumToolTailMaturityWait =
    comparisonPolicy?.require_candidate_exact_medium_tool_tail_maturity_wait === true;
  const candidateExactMediumToolTailMaturityWaitObserved =
    !requireCandidateExactMediumToolTailMaturityWait ||
    candidate.checks?.candidate_exact_medium_tool_tail_maturity_wait_observed === true;
  const requireCandidateExactLargeMessageTailLag =
    comparisonPolicy?.require_candidate_exact_large_message_tail_lag === true;
  const candidateExactLargeMessageTailLagObserved =
    !requireCandidateExactLargeMessageTailLag ||
    candidate.checks?.candidate_exact_large_message_tail_lag_observed === true;
  const requireCandidateLateShallowProviderWaterlineRollbackWait =
    comparisonPolicy?.require_candidate_late_shallow_provider_waterline_rollback_wait === true;
  const candidateLateShallowProviderWaterlineRollbackWaitObserved =
    !requireCandidateLateShallowProviderWaterlineRollbackWait ||
    candidate.checks?.candidate_late_shallow_provider_waterline_rollback_wait_observed === true;
  // A treatment declaration is not evidence. If its required final-wire
  // witness is absent, the arm is a no-op diagnostic and must never receive a
  // positive cache verdict from an unrelated upstream cache waterline.
  const requestedCandidateCacheControlField = String(
    comparisonPolicy?.candidate_cache_control_field ?? ""
  ).trim();
  const candidateTreatmentInjected = !requestedCandidateCacheControlField ||
    candidate.checks?.candidate_cache_control_field_injected === true;
  const candidateTreatmentOptions24hInjected =
    comparisonPolicy?.candidate_cache_options_24h !== true ||
    candidate.checks?.candidate_cache_options_24h_injected === true;
  const candidateTreatmentValid =
    candidateTreatmentInjected && candidateTreatmentOptions24hInjected &&
    threadStablePromptCacheKeyBridgeWire.pass &&
    candidateExactLargeMessageTailLagObserved &&
    candidateExactMediumToolTailMaturityWaitObserved &&
    candidateLateShallowProviderWaterlineRollbackWaitObserved &&
    candidateOptions24hSiblingSettleObserved;
  const candidateTreatmentReason = candidateTreatmentValid
    ? "observed_or_not_requested"
    : !threadStablePromptCacheKeyBridgeWire.pass
      ? "candidate_thread_stable_pck_bridge_wire_witness_failed"
    : !candidateExactLargeMessageTailLagObserved
      ? "candidate_exact_large_message_tail_lag_not_observed"
      : !candidateExactMediumToolTailMaturityWaitObserved
        ? "candidate_exact_medium_tool_tail_maturity_wait_not_observed"
      : !candidateLateShallowProviderWaterlineRollbackWaitObserved
        ? "candidate_late_shallow_provider_waterline_rollback_wait_not_observed"
      : !candidateOptions24hSiblingSettleObserved
        ? "candidate_options24h_sibling_settle_not_observed"
      : "candidate_treatment_not_injected";
  const positiveCacheEvidence =
    candidateTreatmentValid && aggregateTokenHitStrictlyImproves && providerInstabilityFree &&
    seedCacheReadEvidenceComplete && coldSeedEvidenceComplete &&
    coldSeedRequestSymmetry && candidateNoExtraColdStart;
  const championLocalPreUpstreamOverhead = finiteNonNegativeNumber(
    champion.metrics.local_pre_upstream_overhead_p95_ms
  );
  const candidateLocalPreUpstreamOverhead = finiteNonNegativeNumber(
    candidate.metrics.local_pre_upstream_overhead_p95_ms
  );
  const localPreUpstreamTimingComplete =
    championLocalPreUpstreamOverhead !== null && candidateLocalPreUpstreamOverhead !== null;
  const checks = {
    champion_valid: champion.pass,
    candidate_valid: candidate.pass,
    cohort_matches: cohortMatches,
    observed_key_realm_matches: observedRealmMatches,
    seed_cache_read_evidence_complete: seedCacheReadEvidenceComplete,
    seed_cache_read_symmetry: seedCacheReadSymmetry,
    cold_seed_evidence_complete: coldSeedEvidenceComplete,
    cold_seed_request_symmetry: coldSeedRequestSymmetry,
    cold_seed_symmetry: coldSeedSymmetry,
    candidate_no_extra_cold_start: candidateNoExtraColdStart,
    dynamic_input_symmetry: dynamicInputSymmetry.pass,
    candidate_thread_stable_pck_bridge_wire_witness:
      threadStablePromptCacheKeyBridgeWire.pass,
    actual_outbound_input_symmetry: actualOutboundInputSymmetry.pass,
    candidate_declared_cache_control_difference:
      actualOutboundInputSymmetry.declared_cache_control_difference.pass,
    local_previous_response_id_rebind: localPreviousResponseIdRebind.pass,
    local_previous_response_id_full_replay: localPreviousResponseIdFullReplay.pass,
    native_placement_isolation: nativePlacementIsolation.pass,
    upstream_placement_crossover: upstreamPlacementCrossover.pass,
    shared_turn_crossover_balanced: sharedTurnCrossover.pass,
    candidate_raw_token_hit_not_lower:
      candidate.metrics.warm_raw_token_hit_rate >= champion.metrics.warm_raw_token_hit_rate,
    candidate_cache_128_hit_not_lower:
      candidate.metrics.warm_cache_128_hit_rate >= champion.metrics.warm_cache_128_hit_rate,
    candidate_warm_stable_prefix_hit_not_lower:
      candidate.metrics.warm_stable_prefix_hit_rate >= champion.metrics.warm_stable_prefix_hit_rate,
    candidate_full_bucket_rate_not_lower:
      candidate.metrics.warm_full_bucket_rate >= champion.metrics.warm_full_bucket_rate,
    candidate_full_bucket_count_within_tolerance: fullBucketCountWithinTolerance,
    candidate_full_bucket_loss_explained_by_token_gain: aggregateTokenHitStrictlyImproves,
    candidate_full_bucket_gate:
      fullBucketCountWithinTolerance || aggregateTokenHitStrictlyImproves,
    provider_instability_free: providerInstabilityFree,
    candidate_provider_waterline_recovery_wait_observed:
      candidateProviderWaterlineRecoveryWaitObserved,
    candidate_options24h_sibling_settle_observed:
      candidateOptions24hSiblingSettleObserved,
    candidate_exact_medium_tool_tail_maturity_wait_observed:
      candidateExactMediumToolTailMaturityWaitObserved,
    candidate_exact_large_message_tail_lag_observed:
      candidateExactLargeMessageTailLagObserved,
    candidate_late_shallow_provider_waterline_rollback_wait_observed:
      candidateLateShallowProviderWaterlineRollbackWaitObserved,
    candidate_treatment_injected: candidateTreatmentValid,
    candidate_cache_strictly_improves: aggregateTokenHitStrictlyImproves,
    candidate_positive_cache_evidence: positiveCacheEvidence,
    candidate_avoidable_gap_zero: candidate.metrics.avoidable_gap_tokens === 0,
    candidate_all_sse_completed:
      candidate.metrics.successful_sse_requests === candidate.metrics.requests && candidate.metrics.requests > 0,
    candidate_one_attempt_one_main_post:
      candidate.checks.every_inbound_one_attempt_one_main_post === true,
    local_pre_upstream_timing_complete: localPreUpstreamTimingComplete,
    candidate_local_pre_upstream_overhead_p95_not_regressed:
      localPreUpstreamTimingComplete &&
      candidateLocalPreUpstreamOverhead <=
      championLocalPreUpstreamOverhead + effectiveMaxLocalProxyOverheadRegressionMs,
    // Compatibility alias: this now measures the complete local pre-upstream
    // path, not only the prefix guard and request-plan preparation.
    candidate_local_proxy_overhead_p95_not_regressed:
      localPreUpstreamTimingComplete &&
      candidateLocalPreUpstreamOverhead <=
      championLocalPreUpstreamOverhead + effectiveMaxLocalProxyOverheadRegressionMs,
    candidate_ttft_p95_not_regressed:
      candidate.metrics.ttft_p95_ms <= champion.metrics.ttft_p95_ms + maxTtftRegressionMs
  };
  const gatingChecks = { ...checks };
  delete gatingChecks.candidate_full_bucket_rate_not_lower;
  delete gatingChecks.candidate_full_bucket_count_within_tolerance;
  delete gatingChecks.candidate_full_bucket_loss_explained_by_token_gain;
  delete gatingChecks.provider_instability_free;
  // Cold-start cache reads are intentionally outside hit scoring. They remain
  // observable and complete, but an extra cold start is the only candidate
  // regression: a candidate with the same number of seeds and fewer cold
  // reads must not be rejected merely for reaching a warm cache earlier.
  delete gatingChecks.seed_cache_read_symmetry;
  delete gatingChecks.cold_seed_symmetry;
  // An isolated candidate experiment is valid only if its target path was
  // actually observed. This does not claim a benefit; it simply prevents a
  // no-op flag from entering a promotion decision.
  if (!requireCandidateProviderWaterlineRecoveryWait) {
    delete gatingChecks.candidate_provider_waterline_recovery_wait_observed;
  }
  if (!requireCandidateOptions24hSiblingSettle) {
    delete gatingChecks.candidate_options24h_sibling_settle_observed;
  }
  if (!requireCandidateExactMediumToolTailMaturityWait) {
    delete gatingChecks.candidate_exact_medium_tool_tail_maturity_wait_observed;
  }
  if (!requireCandidateExactLargeMessageTailLag) {
    delete gatingChecks.candidate_exact_large_message_tail_lag_observed;
  }
  if (!requireCandidateLateShallowProviderWaterlineRollbackWait) {
    delete gatingChecks.candidate_late_shallow_provider_waterline_rollback_wait_observed;
  }
  delete gatingChecks.candidate_cache_strictly_improves;
  delete gatingChecks.candidate_positive_cache_evidence;
  if (!effectiveRequireTtftNoRegression) {
    delete gatingChecks.candidate_ttft_p95_not_regressed;
  }
  // Remote TTFT belongs to the hand-selected upstream and has its own visible
  // verdict below. A cache-preserving local candidate must not be rejected
  // solely because this pair saw provider-side timing variance.
  const cacheCheckNames = [
    "champion_valid",
    "candidate_valid",
    "cohort_matches",
    "observed_key_realm_matches",
    "seed_cache_read_evidence_complete",
    "cold_seed_evidence_complete",
    "cold_seed_request_symmetry",
    "candidate_no_extra_cold_start",
    "actual_outbound_input_symmetry",
    "candidate_thread_stable_pck_bridge_wire_witness",
    "candidate_declared_cache_control_difference",
    "local_previous_response_id_rebind",
    "native_placement_isolation",
    "upstream_placement_crossover",
    "shared_turn_crossover_balanced",
    "candidate_raw_token_hit_not_lower",
    "candidate_cache_128_hit_not_lower",
    "candidate_warm_stable_prefix_hit_not_lower",
    "candidate_full_bucket_gate",
    "candidate_provider_waterline_recovery_wait_observed",
    "candidate_options24h_sibling_settle_observed",
    "candidate_exact_medium_tool_tail_maturity_wait_observed",
    "candidate_exact_large_message_tail_lag_observed",
    "candidate_late_shallow_provider_waterline_rollback_wait_observed",
    "candidate_treatment_injected",
    "candidate_avoidable_gap_zero",
    "candidate_all_sse_completed",
    "candidate_one_attempt_one_main_post"
  ];
  // Cache behavior and end-to-end latency are intentionally reported as
  // separate verdicts. The latter includes remote provider TTFT variance;
  // hiding a cache regression behind a fast upstream, or vice versa, would
  // make the release evidence misleading.
  const cachePass = cacheCheckNames.every((name) => checks[name] === true);
  const localLatencyPass = checks.candidate_local_pre_upstream_overhead_p95_not_regressed;
  const endToEndLatencyPass = checks.candidate_ttft_p95_not_regressed;
  const latencyPass = effectiveRequireTtftNoRegression ? endToEndLatencyPass : localLatencyPass;
  const baselinePass = Object.values(gatingChecks).every(Boolean);
  const promotionEligible =
    upstreamPlacementCrossover.pass && sharedTurnCrossover.pass;
  const promotionIneligibilityReasons = [];
  if (!upstreamPlacementCrossover.pass) {
    promotionIneligibilityReasons.push({
      code: upstreamPlacementCrossover.reason,
      message: "live promotion requires shared upstream placement crossover"
    });
  }
  if (!sharedTurnCrossover.pass) {
    promotionIneligibilityReasons.push({
      code: sharedTurnCrossover.reason,
      message: "live promotion requires two complete scored pairs with balanced first/second order"
    });
  }
  return {
    // A passing baseline only means "not measurably worse". The user's
    // release rule is stronger: promotion additionally needs a strict,
    // provider-unconfounded cache gain.
    pass: promotionEligible && baselinePass && positiveCacheEvidence,
    promotion_eligible: promotionEligible,
    promotion_ineligibility_reasons: promotionIneligibilityReasons,
    baseline_pass: baselinePass,
    cache_pass: cachePass,
    positive_cache_evidence: positiveCacheEvidence,
    effect_evaluable: candidateTreatmentValid,
    effect_evaluation_reason: candidateTreatmentReason,
    evidence_confounded_by_provider_instability: !providerInstabilityFree,
    local_latency_pass: localLatencyPass,
    latency_pass: latencyPass,
    end_to_end_ttft_pass: endToEndLatencyPass,
    end_to_end_ttft_regression_exempted:
      !effectiveRequireTtftNoRegression && !endToEndLatencyPass && localLatencyPass,
    promotion_latency_policy: effectiveRequireTtftNoRegression
      ? "end-to-end-no-regression"
      : "local-no-regression-upstream-exempt",
    ttft_no_regression_required: effectiveRequireTtftNoRegression,
    input_symmetry: dynamicInputSymmetry,
    actual_outbound_input_symmetry: actualOutboundInputSymmetry,
    thread_stable_prompt_cache_key_bridge_wire: threadStablePromptCacheKeyBridgeWire,
    local_previous_response_id_rebind: localPreviousResponseIdRebind,
    local_previous_response_id_full_replay: localPreviousResponseIdFullReplay,
    native_placement_isolation: nativePlacementIsolation,
    upstream_placement_crossover: upstreamPlacementCrossover,
    shared_turn_crossover: sharedTurnCrossover,
    source_aware_shared_crossover: sourceAwareSharedCrossover,
    dynamic_tail_warm_attribution: dynamicTailWarmAttribution,
    cold_start_accounting: {
      excluded_from_hit_comparison: true,
      evidence_complete: coldSeedEvidenceComplete,
      champion_seed_requests: championColdSeedEvidence.seed_count,
      candidate_seed_requests: candidateColdSeedEvidence.seed_count,
      champion_cold_seed_requests: championColdSeedEvidence.count,
      candidate_cold_seed_requests: candidateColdSeedEvidence.count,
      symmetric: coldSeedSymmetry,
      candidate_no_extra_cold_start: candidateNoExtraColdStart
    },
    checks,
    deltas: {
      raw_token_hit_rate: candidateTreatmentValid
        ? candidate.metrics.raw_token_hit_rate - champion.metrics.raw_token_hit_rate
        : null,
      cache_128_hit_rate: candidateTreatmentValid
        ? candidate.metrics.cache_128_hit_rate - champion.metrics.cache_128_hit_rate
        : null,
      warm_raw_token_hit_rate:
        candidateTreatmentValid
          ? candidate.metrics.warm_raw_token_hit_rate - champion.metrics.warm_raw_token_hit_rate
          : null,
      warm_cache_128_hit_rate:
        candidateTreatmentValid
          ? candidate.metrics.warm_cache_128_hit_rate - champion.metrics.warm_cache_128_hit_rate
          : null,
      warm_stable_prefix_hit_rate:
        candidateTreatmentValid
          ? candidate.metrics.warm_stable_prefix_hit_rate - champion.metrics.warm_stable_prefix_hit_rate
          : null,
      full_bucket_rate: candidate.metrics.full_bucket_rate - champion.metrics.full_bucket_rate,
      full_bucket_requests: fullBucketRequestDelta,
      warm_full_bucket_rate:
        candidate.metrics.warm_full_bucket_rate - champion.metrics.warm_full_bucket_rate,
      warm_full_bucket_requests: warmFullBucketRequestDelta,
      avoidable_gap_tokens:
        candidate.metrics.avoidable_gap_tokens - champion.metrics.avoidable_gap_tokens,
      new_tail_gap_tokens:
        candidate.metrics.new_tail_gap_tokens - champion.metrics.new_tail_gap_tokens,
      provider_unstable_gap_tokens:
        candidate.metrics.provider_unstable_gap_tokens - champion.metrics.provider_unstable_gap_tokens,
      seed_cache_read_tokens:
        seedCacheReadEvidenceComplete
          ? candidateSeedCacheReadEvidence.tokens - championSeedCacheReadEvidence.tokens
          : null,
      cold_seed_request_count:
        coldSeedEvidenceComplete
          ? candidateColdSeedEvidence.count - championColdSeedEvidence.count
          : null,
      local_proxy_overhead_p95_ms:
        candidateLocalPreUpstreamOverhead - championLocalPreUpstreamOverhead,
      local_pre_upstream_overhead_p95_ms:
        candidateLocalPreUpstreamOverhead - championLocalPreUpstreamOverhead,
      upstream_ttft_p95_ms:
        candidate.metrics.upstream_ttft_p95_ms - champion.metrics.upstream_ttft_p95_ms,
      ttft_p95_ms: candidate.metrics.ttft_p95_ms - champion.metrics.ttft_p95_ms
    }
  };
}

async function compareOfflineArtifacts(options) {
  if (!options["champion-result"] || !options["candidate-result"]) {
    throw new FailClosedError(
      "incomplete_offline_artifacts",
      "offline comparison requires both --champion-result and --candidate-result"
    );
  }
  const champion = extractArmArtifact(
    JSON.parse(await readRequiredText(resolve(String(options["champion-result"])), "champion result")),
    "champion"
  );
  const candidate = extractArmArtifact(
    JSON.parse(await readRequiredText(resolve(String(options["candidate-result"])), "candidate result")),
    "candidate"
  );
  const expected = options["key-realm-hash"]
    ? validateOpaqueHash(options["key-realm-hash"], "--key-realm-hash")
    : null;
  if (expected && (champion.cohort.key_realm_hash !== expected || candidate.cohort.key_realm_hash !== expected)) {
    throw new FailClosedError(
      "offline_key_realm_mismatch",
      "offline evidence does not match the explicitly requested --key-realm-hash"
    );
  }
  const maxTtftRegressionMs = boundedInteger(
    options["max-ttft-regression-ms"] ?? 0,
    "--max-ttft-regression-ms",
    0,
    120_000
  );
  const maxLocalProxyOverheadRegressionMs = strictLocalLatencyRegressionBudget(options);
  const maxFullBucketRegressionRequests = boundedInteger(
    options["max-full-bucket-regression-requests"] ?? 0,
    "--max-full-bucket-regression-requests",
    0,
    Math.max(champion.metrics.full_bucket_denominator, candidate.metrics.full_bucket_denominator)
  );
  const maxInputTokenDelta = boundedInteger(
    options["max-input-token-delta"] ?? 128,
    "--max-input-token-delta",
    0,
    10_000
  );
  const requireTtftNoRegression = resolvePromotionTtftPolicy(options);
  const comparison = compareArmResults(
    champion,
    candidate,
    maxTtftRegressionMs,
    maxLocalProxyOverheadRegressionMs,
    maxFullBucketRegressionRequests,
    requireTtftNoRegression,
    maxInputTokenDelta
  );
  return {
    schema: SCHEMA,
    kind: "release-champion-comparison",
    mode: "offline-artifacts",
    pass: comparison.pass,
    cohort: champion.cohort,
    champion,
    candidate,
    comparison,
    settings: {
      max_ttft_regression_ms: maxTtftRegressionMs,
      max_local_proxy_overhead_regression_ms: maxLocalProxyOverheadRegressionMs,
      max_full_bucket_regression_requests: maxFullBucketRegressionRequests,
      max_input_token_delta: maxInputTokenDelta,
      require_ttft_no_regression: requireTtftNoRegression,
      promotion_latency_policy: requireTtftNoRegression
        ? "end-to-end-no-regression"
        : "local-no-regression-upstream-exempt"
    }
  };
}

function extractArmArtifact(value, expectedArm) {
  const candidate = value?.kind === "dynamic-arm-aggregate"
    ? value
    : value?.[expectedArm] ?? value?.arms?.[expectedArm] ?? null;
  if (!candidate) {
    throw new FailClosedError(
      "invalid_offline_artifact",
      `could not find a ${expectedArm} dynamic-arm-aggregate in the supplied JSON`
    );
  }
  return validateAggregate(candidate, expectedArm);
}

function validateAggregate(value, expectedArm) {
  if (value?.schema !== SCHEMA || value?.kind !== "dynamic-arm-aggregate") {
    throw new FailClosedError(
      "invalid_offline_artifact_schema",
      "result JSON must be an atoapi-release-champion-v1 dynamic-arm-aggregate"
    );
  }
  if (value?.diagnostic_only === true) {
    throw new FailClosedError(
      "diagnostic_only_artifact",
      "diagnostic-only isolated evidence cannot be used for offline promotion"
    );
  }
  if (value.arm !== expectedArm) {
    throw new FailClosedError(
      "invalid_offline_artifact_arm",
      `expected ${expectedArm} result but received ${String(value.arm)}`
    );
  }
  if (!isCompleteCohort(value.cohort)) {
    throw new FailClosedError("invalid_offline_cohort", "result JSON has no complete comparison cohort");
  }
  const metrics = { ...(value.metrics ?? {}) };
  // r2 evidence from August 13, 2026 predates the explicit
  // local_pre_upstream_overhead_p95_ms field.  At that point the retained
  // local_proxy_overhead_p95_ms field had the same full local-path meaning,
  // so preserve offline comparability without rewriting the artifact.
  const legacyLocalTimingAlias =
    !Number.isFinite(Number(metrics.local_pre_upstream_overhead_p95_ms)) &&
    Number.isFinite(Number(metrics.local_proxy_overhead_p95_ms));
  if (legacyLocalTimingAlias) {
    metrics.local_pre_upstream_overhead_p95_ms = metrics.local_proxy_overhead_p95_ms;
  }
  for (const field of [
    "input_tokens",
    "cache_read_tokens",
    "raw_token_hit_rate",
    "cache_128_hit_rate",
    "warm_stable_prefix_hit_rate",
    "full_bucket_rate",
    "avoidable_gap_tokens",
    "local_pre_upstream_overhead_p95_ms",
    "ttft_p95_ms"
  ]) {
    if (!Number.isFinite(Number(metrics[field]))) {
      throw new FailClosedError("invalid_offline_metrics", `result JSON metric ${field} is missing`);
    }
  }
  return legacyLocalTimingAlias
    ? {
      ...value,
      metrics: {
        ...metrics,
        local_pre_upstream_overhead_p95_source: "legacy_local_proxy_overhead_p95_ms"
      }
    }
    : value;
}

function validateDynamicRun(value, expectedArm) {
  if (value?.schema !== SCHEMA || value?.kind !== "dynamic-run") {
    throw new FailClosedError("invalid_dynamic_run", "live runner produced an incomplete dynamic result");
  }
  if (value.arm !== expectedArm || !isCompleteCohort(value.cohort)) {
    throw new FailClosedError("invalid_dynamic_run_scope", "dynamic result has an invalid cohort or arm");
  }
  return value;
}

/// Captures the mutable user config exactly once before the first arm starts.
/// Each arm clones the same sealed input. The caller separately verifies that
/// the user's live Codex selection remains unchanged throughout the run, so a
/// comparison never mixes a stale snapshot with a newly hand-selected route.
async function snapshotLiveConfig(sourceConfigDir) {
  const sourceConfig = join(sourceConfigDir, "config.toml");
  const configText = await readRequiredText(sourceConfig, "source config.toml");
  const root = await mkdtemp(join(tmpdir(), "atoapi-release-champion-source-"));
  const configDir = join(root, "config");
  try {
    await mkdir(configDir, { recursive: true });
    await writeFile(join(configDir, "config.toml"), configText, "utf8");
    const sourceKey = join(sourceConfigDir, "cache-key.dpapi");
    if (await fileExists(sourceKey)) {
      await copyFile(sourceKey, join(configDir, basename(sourceKey)));
    }
    return { root, configDir };
  } catch (error) {
    await removeTemporaryDirectory(root);
    throw error;
  }
}

async function currentLiveSelectionScopeFingerprint(
  sourceConfigDir,
  configText = null,
  providerScope = "codex-agent",
  pinnedKeyId = null
) {
  const normalizedProviderScope = normalizeProviderScope(providerScope);
  const text = configText ?? await readRequiredText(
    join(sourceConfigDir, "config.toml"),
    "live source config.toml"
  );
  const route = codexAgentRoute(text);
  if (normalizedProviderScope === "codex-agent" && (!route?.enabled || !route.provider_id)) {
    throw new FailClosedError(
      "live_selection_scope_unavailable",
      "the live Codex route is not enabled or has no selected Provider"
    );
  }
  const activeProviderId = extractTomlString(text, "active_provider_id");
  const selectedProviderId = normalizedProviderScope === "active-provider"
    ? activeProviderId
    : route?.provider_id ?? "";
  if (!selectedProviderId) {
    throw new FailClosedError(
      "live_selection_scope_unavailable",
      normalizedProviderScope === "active-provider"
        ? "the live config has no active_provider_id"
        : "the live Codex route has no selected Provider"
    );
  }
  const providerBlock = tomlArrayBlocks(text, "providers")
    .map((item) => item.body)
    .find((item) => extractTomlString(item, "id") === selectedProviderId);
  if (!providerBlock) {
    throw new FailClosedError(
      "live_selection_scope_unavailable",
      `the selected Provider ${selectedProviderId} is not present in the source config`
    );
  }
  if (extractTomlBoolean(providerBlock, "enabled") !== true) {
    throw new FailClosedError(
      "live_selection_scope_unavailable",
      `the selected Provider ${selectedProviderId} is not enabled in the source config`
    );
  }
  const keyPool = providerKeyPoolContext(text, selectedProviderId);
  const keyPoolMaterial = keyPool
    ? {
      enabled: extractTomlBoolean(keyPool.pool.body, "enabled"),
      ...(pinnedKeyId
        ? {
          pinned_key: (() => {
            const pinned = keyPool.keys.find(
              (key) => extractTomlString(key.body, "id") === pinnedKeyId
            );
            return pinned
              ? {
                id: pinnedKeyId,
                enabled: extractTomlBoolean(pinned.body, "enabled"),
                disabled_until: extractTomlRawValue(pinned.body, "disabled_until"),
                material_digest: sha256Text(extractTomlString(pinned.body, "key_encrypted"))
              }
              : { id: pinnedKeyId, missing: true };
          })()
        }
        : {
          strategy: extractTomlString(keyPool.pool.body, "strategy"),
          keys: keyPool.keys.map((key) => ({
            id: extractTomlString(key.body, "id"),
            enabled: extractTomlBoolean(key.body, "enabled"),
            disabled_until: extractTomlRawValue(key.body, "disabled_until"),
            material_digest: sha256Text(extractTomlString(key.body, "key_encrypted"))
          }))
        })
    }
    : null;
  return sha256Parts([
    "atoapi-release-champion-live-selection-scope-v3",
    JSON.stringify({
      provider_scope: normalizedProviderScope,
      selected_provider_id: selectedProviderId,
      active_provider_id: normalizedProviderScope === "active-provider"
        ? activeProviderId
        : "",
      route: normalizedProviderScope === "codex-agent"
        ? {
          provider_id: route?.provider_id ?? "",
          model_id: route?.model_id ?? "",
          enabled: route?.enabled ?? false,
          target_path_digest: sha256Text(route?.target_path ?? "")
        }
        : null,
      provider: {
        enabled: extractTomlBoolean(providerBlock, "enabled"),
        channel: extractTomlString(providerBlock, "channel"),
        use_system_proxy: extractTomlBoolean(providerBlock, "use_system_proxy"),
        base_url_digest: sha256Text(extractTomlString(providerBlock, "base_url")),
        direct_key_material_digest: sha256Text([
          extractTomlString(providerBlock, "api_key_encrypted"),
          extractTomlString(providerBlock, "api_key")
        ].join("\u0000"))
      },
      key_pool: keyPoolMaterial
    })
  ]);
}

async function assertLiveSelectionScopeUnchanged(
  sourceConfigDir,
  expectedFingerprint,
  checkpoint,
  providerScope = "codex-agent",
  pinnedKeyId = null
) {
  const currentFingerprint = await currentLiveSelectionScopeFingerprint(
    sourceConfigDir,
    null,
    providerScope,
    pinnedKeyId
  );
  if (currentFingerprint !== expectedFingerprint) {
    throw new FailClosedError(
      "live_selection_scope_changed",
      `the hand-selected Codex route, Provider, model, or Key selection changed at ${checkpoint}; live comparison stopped before more traffic was sent (expected=${expectedFingerprint.slice(0, 16)}, current=${currentFingerprint.slice(0, 16)})`
    );
  }
}

async function copyIsolatedConfig(
  sourceConfigDir,
  targetConfigDir,
  {
    providerId = "",
    upstreamUserAgent = null,
    pinnedKeyId = null,
    forceUseSystemProxy = null
  } = {}
) {
  const sourceConfig = join(sourceConfigDir, "config.toml");
  await assertFile(sourceConfig, "source config.toml");
  await mkdir(targetConfigDir, { recursive: true });
  const targetConfig = join(targetConfigDir, "config.toml");
  await copyFile(sourceConfig, targetConfig);
  let rewrittenConfig = await readRequiredText(targetConfig, "isolated config.toml");
  if (upstreamUserAgent) {
    if (!providerId) {
      throw new FailClosedError(
        "missing_provider_for_user_agent_probe",
        "--upstream-user-agent requires the current Codex Provider binding"
      );
    }
    rewrittenConfig = replaceProviderTomlString(
      rewrittenConfig,
      providerId,
      "custom_user_agent",
      upstreamUserAgent
    );
  }
  if (pinnedKeyId) {
    rewrittenConfig = pinProviderKeyInToml(rewrittenConfig, providerId, pinnedKeyId);
  }
  if (forceUseSystemProxy !== null) {
    rewrittenConfig = replaceProviderTomlBoolean(
      rewrittenConfig,
      providerId,
      "use_system_proxy",
      forceUseSystemProxy
    );
  }
  if (rewrittenConfig !== await readRequiredText(targetConfig, "isolated config.toml")) {
    await writeFile(targetConfig, rewrittenConfig, "utf8");
  }
  const sourceKey = join(sourceConfigDir, "cache-key.dpapi");
  if (await fileExists(sourceKey)) {
    await copyFile(sourceKey, join(targetConfigDir, basename(sourceKey)));
  }
}

function replaceProviderTomlString(configText, providerId, key, value) {
  const marker = "[[providers]]";
  const starts = [];
  let offset = 0;
  while ((offset = configText.indexOf(marker, offset)) >= 0) {
    starts.push(offset);
    offset += marker.length;
  }
  for (const start of starts) {
    const next = configText.indexOf("\n[[", start + marker.length);
    const end = next < 0 ? configText.length : next + 1;
    const block = configText.slice(start, end);
    if (extractTomlString(block, "id") !== providerId) continue;
    const escapedKey = escapeRegExp(key);
    const escapedValue = value.replace(/\\/gu, "\\\\").replace(/"/gu, '\\"');
    const field = new RegExp(`^${escapedKey}\\s*=\\s*"[^"]*"\\s*$`, "mu");
    const replacement = `${key} = "${escapedValue}"`;
    const rewritten = field.test(block)
      ? block.replace(field, replacement)
      : `${block.trimEnd()}\n${replacement}\n`;
    return `${configText.slice(0, start)}${rewritten}${configText.slice(end)}`;
  }
  throw new FailClosedError(
    "provider_not_found_for_user_agent_probe",
    "current Codex Provider was not found in the copied isolated config"
  );
}

function replaceProviderTomlBoolean(configText, providerId, key, value) {
  const marker = "[[providers]]";
  const starts = [];
  let offset = 0;
  while ((offset = configText.indexOf(marker, offset)) >= 0) {
    starts.push(offset);
    offset += marker.length;
  }
  for (const start of starts) {
    const next = configText.indexOf("\n[[", start + marker.length);
    const end = next < 0 ? configText.length : next + 1;
    const block = configText.slice(start, end);
    if (extractTomlString(block, "id") !== providerId) continue;
    const escapedKey = escapeRegExp(key);
    const field = new RegExp(`^${escapedKey}\\s*=\\s*(?:true|false)\\s*$`, "mu");
    const replacement = `${key} = ${value ? "true" : "false"}`;
    const rewritten = field.test(block)
      ? block.replace(field, replacement)
      : `${block.trimEnd()}\n${replacement}\n`;
    return `${configText.slice(0, start)}${rewritten}${configText.slice(end)}`;
  }
  throw new FailClosedError(
    "provider_not_found_for_transport_probe",
    "current Codex Provider was not found in the copied isolated config"
  );
}

function optionalOpaqueIdentifier(value, label) {
  if (value === undefined || value === null) return null;
  const normalized = String(value).trim();
  if (!normalized || normalized.length > 128 || !/^[A-Za-z0-9._:-]+$/u.test(normalized)) {
    throw new FailClosedError(
      "invalid_opaque_identifier",
      `${label} must be a non-empty opaque identifier up to 128 safe characters`
    );
  }
  return normalized;
}

function optionalCandidateCacheControlField(value) {
  if (value === undefined || value === null) return null;
  const normalized = String(value).trim();
  if (new Set(["prompt-cache-key", "prompt-cache-options", "prompt-cache-retention"]).has(normalized)) {
    return normalized;
  }
  throw new FailClosedError(
    "invalid_candidate_cache_control_field",
    "--candidate-cache-control-field supports prompt-cache-key, prompt-cache-options, or prompt-cache-retention"
  );
}

function optionalReasoningEffort(value) {
  if (value === undefined || value === null) return null;
  const normalized = String(value).trim().toLowerCase();
  if (!normalized) return null;
  if (new Set(["none", "minimal", "low", "medium", "high", "xhigh", "max", "ultra"]).has(normalized)) {
    return normalized;
  }
  throw new FailClosedError(
    "invalid_reasoning_effort",
    "--reasoning-effort must be none, minimal, low, medium, high, xhigh, max, or ultra"
  );
}

function cacheCapabilityProbeChannel(providerTomlBlock) {
  const channel = extractTomlString(providerTomlBlock, "channel").trim();
  return new Set(["responses", "chat"]).has(channel) ? channel : null;
}

function extractTomlBoolean(text, key) {
  const escaped = escapeRegExp(key);
  const value = text.match(new RegExp(`^${escaped}\\s*=\\s*(true|false)`, "mu"))?.[1];
  return value === undefined ? null : value === "true";
}

function extractTomlValuePresent(text, key) {
  const escaped = escapeRegExp(key);
  return new RegExp(`^${escaped}\\s*=\\s*(?:"[^"]*"|[^\\r\\n]+)`, "mu").test(text);
}

function extractTomlRawValue(text, key) {
  const escaped = escapeRegExp(key);
  return text.match(new RegExp(`^${escaped}\\s*=\\s*([^\\r\\n#]+)`, "mu"))?.[1].trim() ?? "";
}

function tomlArrayBlocksWithOffsets(text, section) {
  const marker = `[[${section}]]`;
  const starts = [];
  let offset = 0;
  while ((offset = text.indexOf(marker, offset)) >= 0) {
    const lineStart = offset === 0 || text[offset - 1] === "\n";
    if (lineStart) starts.push(offset);
    offset += marker.length;
  }
  return starts.map((start) => {
    const next = text.indexOf("\n[[", start + marker.length);
    const end = next < 0 ? text.length : next + 1;
    return { start, end, body: text.slice(start, end) };
  });
}

function providerKeyPoolContext(configText, providerId) {
  const pools = tomlArrayBlocksWithOffsets(configText, "provider_key_pools")
    .filter((block) => extractTomlString(block.body, "provider_id") === providerId);
  if (pools.length === 0) return null;
  if (pools.length !== 1) {
    throw new FailClosedError(
      "ambiguous_provider_key_pool",
      "isolated key pin requires exactly one provider key pool"
    );
  }
  const pool = pools[0];
  const nextPool = tomlArrayBlocksWithOffsets(configText, "provider_key_pools")
    .map((block) => block.start)
    .filter((start) => start > pool.start)
    .sort((left, right) => left - right)[0] ?? configText.length;
  const keys = tomlArrayBlocksWithOffsets(configText, "provider_key_pools.keys")
    .filter((block) => block.start > pool.start && block.start < nextPool);
  return { pool, keys };
}

function validatePinnedKeyConfiguration(configText, providerId, pinnedKeyId) {
  const context = providerKeyPoolContext(configText, providerId);
  if (!context) {
    if (pinnedKeyId) {
      throw new FailClosedError(
        "pinned_key_pool_missing",
        "--key-id requires an enabled Provider Key pool for the selected Codex Provider"
      );
    }
    return;
  }
  const poolEnabled = extractTomlBoolean(context.pool.body, "enabled");
  if (poolEnabled !== true) {
    if (pinnedKeyId) {
      throw new FailClosedError(
        "pinned_key_pool_disabled",
        "--key-id requires the selected Provider Key pool to be enabled"
      );
    }
    return;
  }
  if (!pinnedKeyId) {
    throw new FailClosedError(
      "key_pin_required_for_pool",
      "multi-Key live verification requires explicit --key-id; no Key is selected implicitly"
    );
  }
  const target = context.keys.find((block) => extractTomlString(block.body, "id") === pinnedKeyId);
  if (!target || !extractTomlValuePresent(target.body, "key_encrypted")) {
    throw new FailClosedError(
      "pinned_key_not_found",
      "the explicit --key-id is not a saved Key in the selected Provider Key pool"
    );
  }
  assertPinnedKeyUsable(target.body, pinnedKeyId);
}

function assertPinnedKeyUsable(keyBody, keyId) {
  if (extractTomlBoolean(keyBody, "enabled") !== true) {
    throw new FailClosedError(
      "pinned_key_unavailable",
      `--key-id ${keyId} is disabled or cooling down; live verification must not revive it`
    );
  }
  const disabledUntil = extractTomlRawValue(keyBody, "disabled_until");
  if (!disabledUntil) return;
  const timestamp = Date.parse(disabledUntil.replace(/^['"]|['"]$/gu, ""));
  if (!Number.isFinite(timestamp) || timestamp > Date.now()) {
    throw new FailClosedError(
      "pinned_key_unavailable",
      `--key-id ${keyId} is disabled or cooling down; live verification must not revive it`
    );
  }
}

function replaceTomlBooleanField(block, key, value) {
  const escaped = escapeRegExp(key);
  const field = new RegExp(`^${escaped}[\\t ]*=[\\t ]*(?:true|false)[\\t ]*$`, "mu");
  const replacement = `${key} = ${value ? "true" : "false"}`;
  return field.test(block)
    ? block.replace(field, replacement)
    : `${block.trimEnd()}\n${replacement}\n`;
}

function removeTomlField(block, key) {
  const escaped = escapeRegExp(key);
  return block.replace(new RegExp(`^${escaped}\\s*=.*(?:\\r?\\n|$)`, "gmu"), "");
}

function pinProviderKeyInToml(configText, providerId, pinnedKeyId) {
  const context = providerKeyPoolContext(configText, providerId);
  if (!context) {
    throw new FailClosedError(
      "pinned_key_pool_missing",
      "cannot pin a Key without the selected Provider Key pool"
    );
  }
  const target = context.keys.find((block) => extractTomlString(block.body, "id") === pinnedKeyId);
  if (!target || !extractTomlValuePresent(target.body, "key_encrypted")) {
    throw new FailClosedError(
      "pinned_key_not_found",
      "the explicit --key-id is not a saved Key in the selected Provider Key pool"
    );
  }
  assertPinnedKeyUsable(target.body, pinnedKeyId);
  let rewritten = configText;
  for (const block of [...context.keys].sort((left, right) => right.start - left.start)) {
    const isTarget = extractTomlString(block.body, "id") === pinnedKeyId;
    let body = replaceTomlBooleanField(block.body, "enabled", isTarget);
    if (isTarget) body = removeTomlField(body, "disabled_until");
    rewritten = `${rewritten.slice(0, block.start)}${body}${rewritten.slice(block.end)}`;
  }
  return rewritten;
}

async function startIsolatedRuntime({
  executable,
  configDir,
  requestedPort,
  upstreamAffinityTestEnabled = false,
  cacheControlField = null,
  cacheOptions24hTestEnabled = false,
  upstreamHttp1TestEnabled = false,
  providerWaterlineRecoveryWaitTestEnabled = false,
  promptCacheKeyOverride = null,
  threadStablePromptCacheKeyBridgeTestEnabled = false
}) {
  const config = await readRequiredText(join(configDir, "config.toml"), "isolated config.toml");
  const localKey = extractTomlString(config, "local_key");
  if (!localKey) {
    throw new FailClosedError(
      "missing_local_key",
      "copied config.toml has no local_key; live verification cannot authenticate safely"
    );
  }
  const port = await findAvailablePort(requestedPort);
  // v1.4.33 predates the runtime-side WebView2 isolation added for newer
  // candidates.  Give every temporary arm its own profile here as well, so
  // both binaries can start beside the live desktop process without sharing
  // its renderer/GPU state.  The profile lives under the arm's disposable
  // config directory and never changes the live application's profile.
  const webviewUserDataFolder = join(configDir, "webview2-profile");
  await mkdir(webviewUserDataFolder, { recursive: true });
  const child = spawn(executable, [], {
    cwd: repoRoot,
    windowsHide: true,
    stdio: "ignore",
    env: {
      ...process.env,
      ATOAPI_CONFIG_DIR: configDir,
      ATOAPI_ISOLATED_TEST_INSTANCE: "1",
      ATOAPI_HEADLESS_ISOLATED_TEST: "1",
      ATOAPI_TEST_LISTEN_PORT: String(port),
      ATOAPI_PREFIX_DIAGNOSTICS: "1",
      ATOAPI_AUTOMATIC_CACHE_CANARY: "0",
      ...isolatedRuntimeExperimentEnvironment({
        upstreamAffinityTestEnabled,
        cacheControlField,
        promptCacheKeyOverride,
        cacheOptions24hTestEnabled,
        upstreamHttp1TestEnabled,
        providerWaterlineRecoveryWaitTestEnabled,
        threadStablePromptCacheKeyBridgeTestEnabled
      }),
      WEBVIEW2_USER_DATA_FOLDER: webviewUserDataFolder
    }
  });
  const baseUrl = `http://127.0.0.1:${port}`;
  try {
    await waitForHealth(baseUrl, child);
  } catch (error) {
    await stopChild(child, "isolated startup");
    throw error;
  }
  return { child, baseUrl, localKey, port, configDir };
}

// Every candidate-only experiment belongs to a disposable isolated process.
// This pure helper intentionally has no access to desktop configuration, so
// the "candidate only" environment contract can be verified without starting
// Atoapi or using an upstream request.
function isolatedRuntimeExperimentEnvironment({
  upstreamAffinityTestEnabled = false,
  cacheControlField = null,
  promptCacheKeyOverride = null,
  cacheOptions24hTestEnabled = false,
  upstreamHttp1TestEnabled = false,
  providerWaterlineRecoveryWaitTestEnabled = false,
  threadStablePromptCacheKeyBridgeTestEnabled = false
} = {}) {
  return {
    ATOAPI_EXPERIMENTAL_UPSTREAM_AFFINITY: upstreamAffinityTestEnabled ? "1" : "0",
    ATOAPI_FORCE_ISOLATED_CACHE_CONTROL_FIELD: cacheControlField ?? "",
    ATOAPI_FORCE_ISOLATED_PROMPT_CACHE_KEY: promptCacheKeyOverride ?? "",
    ATOAPI_FORCE_ISOLATED_CACHE_OPTIONS_TTL24H:
      cacheOptions24hTestEnabled ? "1" : "0",
    ATOAPI_EXPERIMENTAL_UPSTREAM_HTTP1: upstreamHttp1TestEnabled ? "1" : "0",
    ATOAPI_EXPERIMENTAL_PROVIDER_WATERLINE_RECOVERY_SETTLE:
      providerWaterlineRecoveryWaitTestEnabled ? "1" : "0",
    ATOAPI_EXPERIMENTAL_THREAD_STABLE_PCK_BRIDGE:
      threadStablePromptCacheKeyBridgeTestEnabled ? "1" : "0"
  };
}

async function executableArtifact(path) {
  const contents = await readFile(path);
  return {
    path: resolve(path),
    sha256: createHash("sha256").update(contents).digest("hex")
  };
}

function requestCounters(metrics) {
  return {
    inbound_requests: number(metrics?.agent_generation?.inbound_requests),
    generation_attempts: number(metrics?.agent_generation?.generation_attempts),
    upstream_requests: number(metrics?.upstream_requests)
  };
}

function subtractCounters(after, before) {
  return {
    inbound_requests: after.inbound_requests - before.inbound_requests,
    generation_attempts: after.generation_attempts - before.generation_attempts,
    upstream_requests: after.upstream_requests - before.upstream_requests
  };
}

function cacheableInputTokens128(inputTokens) {
  return inputTokens < 1_024
    ? 0
    : 1_024 + Math.floor((inputTokens - 1_024) / 128) * 128;
}

function localPreUpstreamOverheadMs(timing) {
  const components = [
    timing?.prefix_guard_wait_ms,
    timing?.local_prepare_ms,
    timing?.request_body_encode_ms,
    timing?.gzip_encode_ms
  ];
  if (components.some((value) => finiteNonNegativeNumber(value) === null)) return null;
  return components.reduce((total, value) => total + Number(value), 0);
}

function timingSummary(rows) {
  const comparable = array(rows);
  const compaction = comparable.filter((item) => item.phase === "compaction");
  const p95 = (items, project, empty = 0) => {
    const values = items.map(project).filter((value) => Number.isFinite(value) && value >= 0);
    return values.length === items.length && values.length > 0 ? percentile(values, 95) : empty;
  };
  return {
    timing_complete_requests: comparable.filter((item) => item.checks?.timing_present).length,
    local_pre_upstream_overhead_p95_ms: p95(
      comparable,
      (item) => finiteNonNegativeNumber(item.local_pre_upstream_overhead_ms)
    ),
    local_proxy_overhead_p95_ms: p95(
      comparable,
      (item) => {
        const guard = finiteNonNegativeNumber(item.prefix_guard_wait_ms);
        const prepare = finiteNonNegativeNumber(item.local_prepare_ms);
        return guard === null || prepare === null ? null : guard + prepare;
      }
    ),
    upstream_ttft_p95_ms: p95(comparable, (item) => item.upstream_ttft_ms),
    ttft_p95_ms: p95(comparable, (item) => item.ttft_ms),
    compaction_request_count: compaction.length,
    compaction_local_proxy_overhead_p95_ms: p95(
      compaction,
      (item) => {
        const guard = finiteNonNegativeNumber(item.prefix_guard_wait_ms);
        const prepare = finiteNonNegativeNumber(item.local_prepare_ms);
        return guard === null || prepare === null ? null : guard + prepare;
      },
      null
    ),
    compaction_upstream_ttft_p95_ms: p95(
      compaction,
      (item) => item.upstream_ttft_ms,
      null
    ),
    compaction_ttft_p95_ms: p95(compaction, (item) => item.ttft_ms, null)
  };
}

function emptyMetrics() {
  return {
    requests: 0,
    successful_sse_requests: 0,
    input_tokens: 0,
    warm_input_tokens: 0,
    seed_input_tokens: 0,
    seed_cache_read_tokens: 0,
    seed_request_count: 0,
    cold_seed_request_count: 0,
    peak_input_tokens: 0,
    cache_read_tokens: 0,
    raw_token_hit_rate: 0,
    warm_cache_read_tokens: 0,
    warm_raw_token_hit_rate: 0,
    cacheable_tokens_128: 0,
    cacheable_read_tokens_128: 0,
    cache_128_hit_rate: 0,
    warm_cacheable_tokens_128: 0,
    warm_cacheable_read_tokens_128: 0,
    warm_cache_128_hit_rate: 0,
    warm_stable_prefix_tokens_128: 0,
    warm_stable_prefix_cached_tokens_128: 0,
    warm_stable_prefix_hit_rate: 0,
    full_bucket_requests: 0,
    full_bucket_rate: 0,
    warm_full_bucket_requests: 0,
    warm_full_bucket_rate: 0,
    warm_full_bucket_denominator: 0,
    cacheable_request_count: 0,
    full_bucket_denominator: 0,
    avoidable_gap_tokens: 0,
    new_tail_gap_tokens: 0,
    provider_unstable_gap_tokens: 0,
    shortfall_tokens: 0,
    guarded_requests: 0,
    upstream_affinity_test_requests: 0,
    upstream_affinity_learned_requests: 0,
    upstream_affinity_injected_requests: 0,
    candidate_cache_control_field_requests: 0,
    candidate_cache_options_24h_requests: 0,
    candidate_options24h_sibling_settle_requests: 0,
    candidate_http1_requests: 0,
    candidate_provider_waterline_recovery_wait_requests: 0,
    candidate_exact_large_message_tail_lag_requests: 0,
    candidate_late_shallow_provider_waterline_rollback_wait_requests: 0,
    exact_medium_tool_tail_predecessor_requests: 0,
    exact_medium_tool_tail_direct_successor_requests: 0,
    exact_medium_tool_tail_maturity_wait_requests: 0,
    non_target_prefix_guard_wait_requests: 0,
    timing_complete_requests: 0,
    local_pre_upstream_overhead_p95_ms: 0,
    local_proxy_overhead_p95_ms: 0,
    upstream_ttft_p95_ms: 0,
    ttft_p95_ms: 0,
    compaction_request_count: 0,
    compaction_local_proxy_overhead_p95_ms: null,
    compaction_upstream_ttft_p95_ms: null,
    compaction_ttft_p95_ms: null,
    usage_coverage: 0,
    observed_realm_ids: [],
    dynamic_tail_mix: emptyDynamicTailMix()
  };
}

function stripCompactedInput(record) {
  const { compacted_input, ...safe } = record;
  return safe;
}

function message(text) {
  return {
    type: "message",
    role: "user",
    content: [{ type: "input_text", text }]
  };
}

function buildSeedContext(targetChars, fixtureFamily = null, profile = "natural") {
  const fixturePrefix = fixtureFamily ? `[fixture ${fixtureFamily}] ` : "";
  if (targetChars <= 0) {
    return `${fixturePrefix}Release champion seed. Reply with OK only.`;
  }
  if (profile === "natural-dense") {
    // Keep the seed natural and pair-specific, but use the same compact,
    // non-repeating CJK-rich record shape as the validated dynamic tail. This
    // reaches the requested token class without recreating the oversized
    // 2MB+ ASCII seed that the relay rejects before usage is returned.
    return buildNaturalDenseToolOutput(targetChars, fixtureFamily, 0, 1);
  }
  if (profile !== "legacy-repeated") {
    return buildNaturalFixtureText(targetChars, fixtureFamily, "history");
  }
  const sections = [
    "Architecture notes: preserve established behavior and append only new facts.",
    "Repository inventory: keep prior decisions, tool outcomes, and constraints in order.",
    "Validation record: report only evidence-backed conclusions without rewriting history.",
    "Continuation rule: retain stable context verbatim and answer the newest request."
  ];
  let output = fixturePrefix;
  for (let index = 0; output.length < targetChars; index += 1) {
    output += `\n[section ${index + 1}] ${sections[index % sections.length]}`;
  }
  return output.slice(0, targetChars);
}

function buildStableInstructions(targetChars, fixtureFamily = null, profile = "natural") {
  // The lane belongs only in the opaque prompt cache key. Keeping the body
  // identical across arms makes raw upstream token telemetry comparable. A
  // pair-scoped fixture may vary across pairs, but never across the two arms
  // inside one pair, so stale upstream context cannot warm a later pair.
  if (profile !== "legacy-repeated") {
    return buildNaturalFixtureText(targetChars, fixtureFamily, "instruction");
  }
  const prefix = fixtureFamily
    ? `Release-cache validation fixture ${fixtureFamily}. Preserve the supplied history. `
    : "Release-cache validation fixture. Preserve the supplied history. ";
  const unit = "Follow the existing instructions exactly; reply with OK only when asked. ";
  return (prefix + unit.repeat(Math.ceil(Math.max(0, targetChars - prefix.length) / unit.length)))
    .slice(0, targetChars);
}

// The default live fixture deliberately avoids repeated long sentences,
// timestamp/path/hash noise, and user-derived text. It is deterministic and
// equal across both arms of one pair, while each record has a unique ordinal
// so generic WAF/replay heuristics do not mistake the benchmark for a repeated
// payload flood. It changes only verifier input, never Atoapi user traffic.
function buildNaturalFixtureText(targetChars, fixtureFamily, kind) {
  const label = fixtureFamily
    ? `Release validation fixture ${fixtureFamily}. `
    : "Release validation fixture. ";
  const subjects = [
    "The design record",
    "The implementation note",
    "The review summary",
    "The compatibility statement",
    "The test observation",
    "The operating constraint",
    "The interface contract",
    "The continuation boundary"
  ];
  const predicates = [
    "keeps confirmed behavior intact before new work is appended",
    "preserves the established order of facts and decisions",
    "requires the latest request to be evaluated against prior context",
    "keeps tool results associated with their completed calls",
    "separates stable context from the newest input",
    "avoids changing route or key selection implicitly",
    "records only evidence that is relevant to the active task",
    "retains the existing response contract without reinterpretation"
  ];
  const closing = kind === "history"
    ? "This history entry remains available to the next turn."
    : kind === "tool"
      ? "This completed tool record remains available to later reasoning."
      : "Apply this instruction while answering only the newest request.";
  let output = label;
  for (let index = 0; output.length < targetChars; index += 1) {
    const ordinal = String(index + 1).padStart(6, "0");
    const subject = subjects[(index * 5 + 1) % subjects.length];
    const predicate = predicates[(index * 3 + 2) % predicates.length];
    output += `\nRecord ${ordinal}: ${subject} ${predicate}. ${closing}`;
  }
  return output.slice(0, targetChars);
}

function fixtureLineStats(text) {
  const counts = new Map();
  for (const line of String(text ?? "").split(/\r?\n/u)) {
    if (!line) continue;
    counts.set(line, (counts.get(line) ?? 0) + 1);
  }
  return {
    line_count: [...counts.values()].reduce((total, count) => total + count, 0),
    unique_line_count: counts.size,
    max_repeated_line_count: Math.max(0, ...counts.values())
  };
}

function buildToolFixtureItems({
  pair,
  fixtureFamily = null,
  targetChars,
  shape,
  calls,
  eventOrdinal = 0,
  toolProtocol = "function"
}) {
  const fixtureTool = releaseFixtureToolProtocol(toolProtocol);
  const items = [];
  let remainingChars = targetChars;
  for (let index = 0; index < calls; index += 1) {
    const remainingCalls = calls - index;
    const chars = Math.floor(remainingChars / remainingCalls);
    remainingChars -= chars;
    const callId = releaseFixtureCallId(pair, fixtureFamily, index, calls, eventOrdinal);
    const call = fixtureTool.protocol === "custom"
      ? {
        type: fixtureTool.call_type,
        call_id: callId,
        name: "read_release_fixture",
        input: calls > 1
          ? JSON.stringify({ part: index + 1, total_parts: calls })
          : "{}"
      }
      : {
        type: fixtureTool.call_type,
        call_id: callId,
        name: "read_release_fixture",
        arguments: calls > 1 ? JSON.stringify({ part: index + 1, total_parts: calls }) : "{}"
      };
    items.push(
      call,
      {
        type: fixtureTool.output_type,
        call_id: callId,
        output: buildToolOutput(chars, shape, fixtureFamily, index, calls)
      }
    );
  }
  return items;
}

// Some compatible upstreams accept long full-replay context but reject a
// synthetic function_call history at that same size. Text mode keeps the
// growing, shape-varied tail and the exact A/B symmetry without claiming to
// exercise tool-history behavior on an upstream that cannot replay it.
function buildDynamicTextTail({ targetChars, shape, fixtureFamily = null, eventOrdinal = 0 }) {
  const scopedFixture = fixtureFamily
    ? `${fixtureFamily}-text-tail-${eventOrdinal}`
    : `text-tail-${eventOrdinal}`;
  return buildToolOutput(targetChars, shape, scopedFixture, 0, 1);
}

function buildToolOutput(targetChars, shape, fixtureFamily = null, partIndex = 0, partCount = 1) {
  const partLabel = partCount > 1 ? ` tool_part=${partIndex + 1}/${partCount}` : "";
  if (shape === "natural-dense") {
    return buildNaturalDenseToolOutput(targetChars, fixtureFamily, partIndex, partCount);
  }
  if (shape === "natural") {
    const scopedFixture = fixtureFamily
      ? `${fixtureFamily}-tool-${partIndex + 1}`
      : `tool-${partIndex + 1}`;
    return buildNaturalFixtureText(targetChars, scopedFixture, "tool");
  }
  if (shape === "structured") {
    let output = "";
    for (let index = 0; output.length < targetChars; index += 1) {
      const record = {
        fixture: "structured-tool-output",
        index,
        status: index % 5 === 0 ? "changed" : "unchanged",
        path: `fixture/module-${index % 37}/item-${index}.json`,
        values: [index % 11, (index * 7) % 101, "stable"]
      };
      if (fixtureFamily) record.fixture_family = fixtureFamily;
      if (partCount > 1) record.tool_part = partIndex + 1;
      output += JSON.stringify(record) + "\n";
    }
    return output.slice(0, targetChars);
  }
  if (shape === "noisy") {
    let output = "";
    for (let index = 0; output.length < targetChars; index += 1) {
      const day = String(index % 28 + 1).padStart(2, "0");
      const stamp = `2026-01-${day}T12:${String(index % 60).padStart(2, "0")}:00Z`;
      const hash = String(index.toString(16)).padStart(16, "0");
      const family = fixtureFamily ? ` fixture_family=${fixtureFamily}` : "";
      output += `${stamp} INFO fixture${family}${partLabel} path=/workspace/fixture/${index % 53}/file-${index}.ts hash=${hash} payload={"line":${index},"state":"ok"}\n`;
    }
    return output.slice(0, targetChars);
  }
  const unit = fixtureFamily
    ? `tool-output [${fixtureFamily}]${partLabel}: stable release validation data; `
    : `tool-output${partLabel}: stable release validation data; `;
  return unit.repeat(Math.ceil(targetChars / unit.length)).slice(0, targetChars);
}

// A CJK-rich, prose-like tool record produces a high token density without
// falling back to repeated filler, timestamps, paths, hashes, or user text.
// The opaque pair fixture is converted into a compact Chinese marker so fresh
// pairs still have distinct bodies while no raw cache-placement identity is
// written into the fixture.
function buildNaturalDenseToolOutput(targetChars, fixtureFamily = null, partIndex = 0, partCount = 1) {
  const marker = naturalDenseFixtureMarker(fixtureFamily);
  const part = partCount > 1 ? `第${partIndex + 1}段` : "本段";
  const subjects = [
    "依赖清单",
    "接口约束",
    "回归记录",
    "变更说明",
    "兼容结论",
    "验证要点",
    "状态快照",
    "审阅意见"
  ];
  const actions = [
    "已核对前序事实",
    "保持既有边界",
    "补充必要证据",
    "确认调用顺序",
    "保留稳定上下文",
    "标记待复核条件",
    "关联完成结果",
    "排除无关变更"
  ];
  const outcomes = [
    "等待下一项任务继续使用",
    "不改写已经确认的记录",
    "只允许新增信息追加",
    "保持结果可以逐项追溯",
    "避免把临时信号当成结论",
    "保留完整工具语义",
    "不改变既有调用契约",
    "以最新请求为处理焦点"
  ];
  let output = "";
  for (let index = 0; output.length < targetChars; index += 1) {
    const ordinal = String(index + 1).padStart(6, "0");
    const subject = subjects[(index * 5 + partIndex) % subjects.length];
    const action = actions[(index * 3 + partCount) % actions.length];
    const outcome = outcomes[(index * 7 + partIndex + partCount) % outcomes.length];
    output += `记录${ordinal}：${part}${subject}${action}，${outcome}。批次${marker}\n`;
  }
  return output.slice(0, targetChars);
}

function naturalDenseFixtureMarker(fixtureFamily) {
  if (!fixtureFamily) return "基线";
  const syllables = [
    "甲", "乙", "丙", "丁", "戊", "己", "庚", "辛",
    "壬", "癸", "东", "南", "西", "北", "春", "秋"
  ];
  return [...sha256Text(String(fixtureFamily)).slice(0, 12)]
    .map((value) => syllables[Number.parseInt(value, 16)])
    .join("");
}

function dynamicTailProfileForTurn(
  turn,
  baseChars,
  baseCalls,
  tailProfile = "mixed",
  lateShallowProviderWaterlineRollback = false
) {
  // This four-turn fixture is intentionally seed -> changing text tail ->
  // quiet rollback witness -> delayed quiet direct child.  Adding a second
  // dynamic tail on turn three would turn the supposed direct child into a
  // noisy message and make the target path impossible to exercise.
  if (lateShallowProviderWaterlineRollback && turn >= 3) return null;
  if (turn <= 0 || turn % 2 === 0) return null;
  const mixedProfiles = [
    { shape: "natural", scale: 0.25, calls_delta: 0 },
    { shape: "structured", scale: 0.5, calls_delta: 1 },
    { shape: "noisy", scale: 0.75, calls_delta: 2 },
    { shape: "flat", scale: 1, calls_delta: 3 },
    { shape: "natural", scale: 0.35, calls_delta: 1 }
  ];
  const profiles = tailProfile === "natural-dense"
    ? mixedProfiles.map((item) => ({ ...item, shape: "natural-dense" }))
    : mixedProfiles;
  const ordinal = Math.floor((turn - 1) / 2) + 1;
  const profile = profiles[(ordinal - 1) % profiles.length];
  return {
    ordinal,
    shape: profile.shape,
    targetChars: Math.max(1_024, Math.round(Number(baseChars) * profile.scale)),
    calls: Math.min(8, Math.max(1, Number(baseCalls) + profile.calls_delta))
  };
}

function releaseFixtureCallId(
  pair,
  fixtureFamily = null,
  partIndex = 0,
  partCount = 1,
  eventOrdinal = 0
) {
  const family = fixtureFamily ? `_${safeSegment(fixtureFamily)}` : "";
  const event = eventOrdinal > 0 ? `_event_${Number(eventOrdinal)}` : "";
  const part = partCount > 1 ? `_part_${partIndex + 1}` : "";
  return `call_release_fixture${family}${event}_pair_${Number(pair)}${part}`;
}

function effectiveReuseRuntimePerArm(requested, isolateUpstreamCache) {
  return Boolean(requested) && !Boolean(isolateUpstreamCache);
}

function validateSeedToReuseDelay({
  seedToReuseDelayMs,
  scenario,
  turns,
  scoredPairCount,
  sharedCacheCrossover,
  reuseRuntimePerArm,
  candidateCacheControlField,
  turnDelayMs,
  interArmDelayMs,
  liveCodexMetricsConfigured
}) {
  if (seedToReuseDelayMs === 0) return;
  if (scenario !== "dynamic-tail-mix") {
    throw new FailClosedError(
      "seed_to_reuse_delay_scenario_invalid",
      "--seed-to-reuse-delay-ms requires --scenario dynamic-tail-mix"
    );
  }
  if (turns < 3) {
    throw new FailClosedError(
      "seed_to_reuse_delay_turn_count_invalid",
      "--seed-to-reuse-delay-ms requires at least three turns (seed, changing tail, direct successor)"
    );
  }
  if (scoredPairCount < 2) {
    throw new FailClosedError(
      "seed_to_reuse_delay_scored_pair_count_invalid",
      "--seed-to-reuse-delay-ms requires at least two scored pairs for reversed prime order"
    );
  }
  if (!sharedCacheCrossover || !reuseRuntimePerArm) {
    throw new FailClosedError(
      "seed_to_reuse_delay_shared_crossover_required",
      "--seed-to-reuse-delay-ms requires --shared-cache-crossover with --reuse-runtime-per-arm"
    );
  }
  if (candidateCacheControlField !== "prompt-cache-retention") {
    throw new FailClosedError(
      "seed_to_reuse_delay_retention_required",
      "--seed-to-reuse-delay-ms requires --candidate-cache-control-field prompt-cache-retention"
    );
  }
  if (turnDelayMs !== 0 || interArmDelayMs !== 0) {
    throw new FailClosedError(
      "seed_to_reuse_delay_pacing_conflict",
      "--seed-to-reuse-delay-ms requires zero turn and inter-arm pacing so the horizon is the only injected timing condition"
    );
  }
  if (!liveCodexMetricsConfigured) {
    throw new FailClosedError(
      "seed_to_reuse_delay_live_scope_gate_required",
      "--seed-to-reuse-delay-ms requires --live-codex-metrics-url so scope drift can be rejected before the delayed tail"
    );
  }
}

function seedToReuseDelayEvidence({
  pair,
  requestedMs,
  observedMs,
  seedTurnOrder,
  postDelaySelectionScopeVerified = false,
  postDelayLiveScopeVerified = false
}) {
  if (!Number.isInteger(pair) || pair < 0) {
    throw new FailClosedError("invalid_pair", "seed-to-reuse delay evidence requires a non-negative pair id");
  }
  if (!Number.isInteger(requestedMs) || requestedMs <= 0) {
    throw new FailClosedError("invalid_seed_to_reuse_delay", "seed-to-reuse delay evidence requires a positive integer delay");
  }
  if (!Number.isFinite(observedMs) || observedMs < 0) {
    throw new FailClosedError("invalid_seed_to_reuse_delay", "seed-to-reuse delay evidence requires a non-negative observed delay");
  }
  const order = array(seedTurnOrder).filter((arm) => arm === "champion" || arm === "candidate");
  if (order.length !== 2) {
    throw new FailClosedError("invalid_seed_to_reuse_order", "seed-to-reuse delay evidence requires both seed arms in order");
  }
  return {
    schema: "atoapi-seed-to-reuse-delay-evidence-v1",
    pair,
    requested_ms: requestedMs,
    observed_ms: Math.round(observedMs),
    after_turn: 0,
    before_turn: 1,
    seed_turn_order: order,
    post_delay_selection_scope_verified: postDelaySelectionScopeVerified === true,
    post_delay_live_scope_verified: postDelayLiveScopeVerified === true
  };
}

function isolationLaneForPair(pair, arm) {
  if (arm !== "champion" && arm !== "candidate") {
    throw new FailClosedError("invalid_arm", "isolation lane requires champion or candidate arm");
  }
  const championGetsLaneA = Number(pair) % 2 === 0;
  return (arm === "champion") === championGetsLaneA ? "lane-a" : "lane-b";
}

function releaseCachePlacementLane({
  runId,
  keyRealmHash,
  requestFamily,
  pair,
  arm,
  isolationLane,
  isolateUpstreamCache,
  sharedCacheCrossover = false
}) {
  if (Boolean(sharedCacheCrossover)) {
    // Both binaries replay the exact same turn into one upstream cache key.
    // runInterleavedDynamicPair rotates the first sender every turn, so cache
    // placement and short capacity windows are balanced without relying on
    // an unverified provider interpretation of separate prompt-cache keys.
    return sha256Parts([
      "release-champion-lane-v3",
      runId,
      keyRealmHash,
      requestFamily,
      "shared-turn-crossover",
      pair
    ]);
  }
  if (Boolean(isolateUpstreamCache)) {
    if (isolationLane !== "lane-a" && isolationLane !== "lane-b") {
      throw new FailClosedError(
        "invalid_isolation_lane",
        "an isolated upstream-cache comparison requires lane-a or lane-b"
      );
    }
    // A/B lanes remain distinct within one pair, but the physical lane keeps
    // its generated placement identity after the crossover. Pair-specific
    // fixtures and session/thread identities are intentionally separate.
    return sha256Parts([
      "release-champion-lane-v3",
      runId,
      keyRealmHash,
      requestFamily,
      "isolated",
      isolationLane
    ]);
  }
  return sha256Parts([
    "release-champion-lane-v3",
    runId,
    keyRealmHash,
    requestFamily,
    "shared",
    pair,
    arm
  ]);
}

function releaseFixtureConversationIdentity(pair, fixtureFamily = null, isolationLane = null) {
  const family = fixtureFamily ? safeSegment(fixtureFamily) : `pair-${Number(pair)}`;
  const identityFamily = isolationLane
    ? `${family}-${safeSegment(isolationLane)}`
    : family;
  return {
    session_id: `release-champion-session-${identityFamily}`,
    conversation_id: `release-champion-conversation-${identityFamily}`,
    thread_id: `release-champion-thread-${identityFamily}`
  };
}

// The thread-stable prompt-cache-key bridge is specifically intended to
// survive a client-side session/conversation rollover.  Keep the first seed
// on the original identity, then rotate both metadata dimensions once on the
// first successor while preserving the explicit thread_id.  This is enabled
// only by the isolated bridge fixture; all other release scenarios retain the
// historical identity byte-for-byte.
function releaseFixtureTurnIdentity({
  pair,
  fixtureFamily = null,
  isolationLane = null,
  turn,
  scenario,
  identityChurn = false,
  baseSessionId,
  baseConversationId,
  threadId
}) {
  const base = releaseFixtureConversationIdentity(pair, fixtureFamily, isolationLane);
  const session = typeof baseSessionId === "string" && baseSessionId
    ? baseSessionId
    : base.session_id;
  const conversation = typeof baseConversationId === "string" && baseConversationId
    ? baseConversationId
    : identityChurn === true ? base.conversation_id : null;
  const thread = typeof threadId === "string" && threadId ? threadId : base.thread_id;
  const rotate = identityChurn === true && scenario === "dynamic-tail-mix" && Number(turn) > 0;
  return rotate
    ? {
      session_id: `${session}-rotated`,
      conversation_id: `${conversation}-rotated`,
      thread_id: thread,
      phase: "rotated",
      thread_stable: true
    }
    : {
      session_id: session,
      conversation_id: conversation,
      thread_id: thread,
      phase: "base",
      thread_stable: true
    };
}

function extractCompactionItems(responseText) {
  const items = [];
  const seen = new Set();
  const collect = (value) => {
    if (Array.isArray(value)) {
      for (const item of value) collect(item);
      return;
    }
    if (!value || typeof value !== "object") return;
    if (value.type === "compaction" && typeof value.encrypted_content === "string") {
      const fingerprint = JSON.stringify(value);
      if (!seen.has(fingerprint)) {
        seen.add(fingerprint);
        items.push(value);
      }
      return;
    }
    for (const child of Object.values(value)) collect(child);
  };
  // Third-party SSE relays do not consistently preserve blank event
  // separators. Parse each data line independently so an otherwise valid
  // compaction item is not discarded merely because frames were coalesced.
  for (const line of String(responseText).split(/\r?\n/u)) {
    const trimmed = line.trimStart();
    const payload = trimmed.startsWith("data:")
      ? trimmed.slice(5).trim()
      : "";
    if (!payload || payload === "[DONE]") continue;
    try {
      collect(JSON.parse(payload));
    } catch {
      // SSE text fragments which are not JSON cannot contain a usable compaction item.
    }
  }
  return items;
}

function generatedPromptCacheKey(prefix, lane) {
  const normalized = String(prefix).trim();
  if (!normalized) {
    throw new FailClosedError(
      "invalid_prompt_cache_key_prefix",
      "--prompt-cache-key-prefix must not be blank when it is supplied"
    );
  }
  return `atoapi-${sha256Parts(["release-client-key-v1", normalized, lane]).slice(0, 48)}`;
}

function requestFamilyForScenario(scenario) {
  return {
    "full-replay": "codex-responses-full-replay",
    "tool-burst": "codex-responses-tool-burst",
    "tool-tail-maturity": "codex-responses-tool-burst",
    "dynamic-tail-mix": "codex-responses-dynamic-tail-mix",
    "compacted-anchor": "codex-responses-compacted-anchor",
    "compaction-root": "codex-responses-compaction-root"
  }[scenario];
}

function normalizeScenario(value) {
  const normalized = String(value).trim().toLowerCase().replace(/_/gu, "-");
  return new Set([
    "full-replay",
    "tool-burst",
    "tool-tail-maturity",
    "dynamic-tail-mix",
    "compacted-anchor",
    "compaction-root"
  ]).has(normalized)
    ? normalized
    : null;
}

function normalizeProviderScope(value) {
  const normalized = String(value).trim().toLowerCase().replace(/_/gu, "-");
  if (normalized === "codex-agent" || normalized === "active-provider") return normalized;
  throw new FailClosedError(
    "invalid_provider_scope",
    "--provider-scope must be codex-agent or active-provider"
  );
}

function normalizeToolOutputShape(value) {
  const normalized = String(value).trim().toLowerCase().replace(/_/gu, "-");
  if (new Set(["natural", "natural-dense", "flat", "structured", "noisy"]).has(normalized)) return normalized;
  throw new FailClosedError(
    "invalid_tool_output_shape",
    "--tool-output-shape must be natural, natural-dense, flat, structured, or noisy"
  );
}

function normalizeToolProtocol(value) {
  const normalized = String(value).trim().toLowerCase().replace(/_/gu, "-");
  return new Set(["function", "custom"]).has(normalized) ? normalized : null;
}

function releaseFixtureToolProtocol(value = "function") {
  const protocol = normalizeToolProtocol(value);
  if (!protocol) {
    throw new FailClosedError(
      "invalid_tool_protocol",
      "release fixture tool protocol must be function or custom"
    );
  }
  return protocol === "custom"
    ? {
      protocol,
      label: "custom-tool",
      call_type: "custom_tool_call",
      output_type: "custom_tool_call_output"
    }
    : {
      protocol,
      label: "function",
      call_type: "function_call",
      output_type: "function_call_output"
    };
}

function isFixtureToolHistoryItemType(value) {
  return new Set([
    "function_call",
    "function_call_output",
    "custom_tool_call",
    "custom_tool_call_output"
  ]).has(String(value ?? ""));
}

function normalizeDynamicTailProfile(value) {
  const normalized = String(value).trim().toLowerCase().replace(/_/gu, "-");
  return new Set(["mixed", "natural-dense"]).has(normalized) ? normalized : null;
}

function normalizeDynamicTailMode(value) {
  const normalized = String(value).trim().toLowerCase().replace(/_/gu, "-");
  return new Set(["tool", "text"]).has(normalized) ? normalized : null;
}

function normalizeFixtureProfile(value) {
  const normalized = String(value).trim().toLowerCase().replace(/_/gu, "-");
  return new Set(["natural", "natural-dense", "legacy-repeated"]).has(normalized)
    ? normalized
    : null;
}

function normalizeFirstArm(value) {
  const normalized = String(value).trim().toLowerCase();
  if (normalized === "champion" || normalized === "candidate") return normalized;
  throw new FailClosedError(
    "invalid_first_arm",
    "--first-arm must be champion or candidate"
  );
}

function codexAgentRoute(configText) {
  const block = tomlArrayBlocks(configText, "agent_injections")
    .map((item) => item.body)
    .find((item) => extractTomlString(item, "id") === "codex");
  if (!block) return null;
  return {
    provider_id: extractTomlString(block, "provider_id"),
    model_id: extractTomlString(block, "model_id"),
    enabled: extractTomlBoolean(block, "enabled") === true,
    target_path: extractTomlQuotedString(block, "target_path")
  };
}

function assertCodexRouteModelScope(route, model) {
  if (route?.model_id && model !== route.model_id) {
    throw new FailClosedError(
      "model_scope_mismatch",
      "--model must match the Codex model_id in the snapshotted source config"
    );
  }
}

function tomlArrayBlocks(text, section) {
  const marker = `[[${section}]]`;
  const starts = [];
  let offset = 0;
  while ((offset = text.indexOf(marker, offset)) >= 0) {
    starts.push(offset);
    offset += marker.length;
  }
  return starts.map((start) => {
    const next = text.indexOf("\n[[", start + marker.length);
    const end = next < 0 ? text.length : next + 1;
    return { body: text.slice(start, end) };
  });
}

function extractTomlString(text, key) {
  const escaped = escapeRegExp(key);
  return text.match(new RegExp(`^${escaped}\\s*=\\s*"([^"]*)"`, "mu"))?.[1] ?? "";
}

function extractTomlQuotedString(text, key) {
  const escaped = escapeRegExp(key);
  const match = text.match(new RegExp(`^${escaped}\\s*=\\s*(?:"([^"]*)"|'([^']*)')`, "mu"));
  return match?.[1] ?? match?.[2] ?? "";
}

async function findAvailablePort(start) {
  for (let port = start; port <= Math.min(start + 64, 65_500); port += 1) {
    if (port === 18_883) continue;
    if (await portIsAvailable(port)) return port;
  }
  throw new FailClosedError(
    "no_isolated_port",
    `could not find an isolated loopback port from ${start} through ${Math.min(start + 64, 65_500)}`
  );
}

// WebView2 child processes can release their profile lock a short time after
// the desktop parent exits.  Node's Windows-aware retry is bounded here so a
// successful isolated comparison is not misreported as a failure merely
// because its disposable renderer profile is still closing.
async function removeTemporaryDirectory(path) {
  await rm(path, {
    recursive: true,
    force: true,
    maxRetries: 20,
    retryDelay: 100
  });
}

function portIsAvailable(port) {
  return new Promise((resolvePort) => {
    const server = createNetServer();
    server.once("error", () => resolvePort(false));
    server.listen(port, "127.0.0.1", () => {
      server.close(() => resolvePort(true));
    });
  });
}

async function waitForHealth(baseUrl, child) {
  const deadline = Date.now() + 30_000;
  while (Date.now() < deadline) {
    if (!processIsAlive(child.pid)) {
      throw new FailClosedError("isolated_startup_failed", "isolated Atoapi exited before health became ready");
    }
    try {
      const health = await getJson(`${baseUrl}/health`, 1_000);
      if (health?.ok === true) return;
    } catch {
      // The isolated child is still starting.  The live 18883 service is never used.
    }
    await delay(100);
  }
  throw new FailClosedError("isolated_health_timeout", `isolated Atoapi did not become healthy at ${baseUrl}`);
}

async function stopChild(child, label) {
  if (!child || !processIsAlive(child.pid)) return;
  child.kill();
  const deadline = Date.now() + 15_000;
  while (processIsAlive(child.pid) && Date.now() < deadline) {
    await delay(50);
  }
  if (processIsAlive(child.pid)) {
    throw new FailClosedError("isolated_shutdown_timeout", `${label} did not stop cleanly`);
  }
}

function processIsAlive(pid) {
  if (!pid) return false;
  try {
    process.kill(pid, 0);
    return true;
  } catch {
    return false;
  }
}

function optionalLiveCodexMetricsUrl(value) {
  if (value === undefined || value === null) return null;
  const normalized = String(value).trim();
  if (!normalized) {
    throw new FailClosedError(
      "invalid_live_codex_metrics_url",
      "--live-codex-metrics-url must be a local Atoapi metrics URL"
    );
  }
  let parsed;
  try {
    parsed = new URL(normalized);
  } catch {
    throw new FailClosedError(
      "invalid_live_codex_metrics_url",
      "--live-codex-metrics-url must be a valid local Atoapi metrics URL"
    );
  }
  if (
    parsed.protocol !== "http:" ||
    parsed.hostname !== "127.0.0.1" ||
    parsed.port !== "18883" ||
    parsed.pathname !== "/admin/metrics" ||
    parsed.search ||
    parsed.hash ||
    parsed.username ||
    parsed.password
  ) {
    throw new FailClosedError(
      "invalid_live_codex_metrics_url",
      "--live-codex-metrics-url must be exactly http://127.0.0.1:18883/admin/metrics"
    );
  }
  return parsed.toString();
}

function selectLatestLiveCodexMainRecord(metrics) {
  const rows = [
    ...array(metrics?.recent_requests).map((record) => ({
      source: "recent_requests",
      record
    })),
    ...array(metrics?.recent_failed_requests).map((record) => ({
      source: "recent_failed_requests",
      record
    }))
  ];
  const candidates = [];
  for (const row of rows) {
    const record = row.record;
    if (
      String(record?.agent_id ?? "") !== "codex" ||
      String(record?.upstream_call_source ?? "") !== "main"
    ) {
      continue;
    }
    const observedAt = String(record?.at ?? "").trim();
    if (!observedAt) {
      throw new FailClosedError(
        "live_codex_metrics_timestamp_missing",
        "a Codex main metrics record has no timestamp; live scope cannot fall back to an older record"
      );
    }
    const observedAtMs = Date.parse(observedAt);
    if (!Number.isFinite(observedAtMs)) {
      throw new FailClosedError(
        "live_codex_metrics_timestamp_invalid",
        "a Codex main metrics record has an invalid timestamp; live scope cannot fall back to an older record"
      );
    }
    candidates.push({ source: row.source, record, observedAtMs });
  }
  if (candidates.length === 0) {
    throw new FailClosedError(
      "live_codex_metrics_missing",
      "no Codex main request is available in live metrics"
    );
  }
  candidates.sort((left, right) => right.observedAtMs - left.observedAtMs);
  return candidates[0];
}

function selectLatestLiveCodexMainRecordForExpectedScope(metrics, {
  expectedProviderId,
  expectedModel,
  expectedRealm
}) {
  const rows = [
    ...array(metrics?.recent_requests).map((record) => ({
      source: "recent_requests",
      record
    })),
    ...array(metrics?.recent_failed_requests).map((record) => ({
      source: "recent_failed_requests",
      record
    }))
  ];
  const candidates = [];
  for (const row of rows) {
    const record = row.record;
    if (
      String(record?.agent_id ?? "") !== "codex" ||
      String(record?.upstream_call_source ?? "") !== "main" ||
      String(record?.provider_id ?? "").trim() !== expectedProviderId ||
      String(record?.model ?? "").trim() !== expectedModel ||
      String(record?.shadow_affinity_realm_id ?? "").trim() !== expectedRealm
    ) {
      continue;
    }
    const observedAt = String(record?.at ?? "").trim();
    if (!observedAt) {
      throw new FailClosedError(
        "live_codex_metrics_timestamp_missing",
        "the expected Codex main metrics record has no timestamp; live scope cannot fall back to an older record"
      );
    }
    const observedAtMs = Date.parse(observedAt);
    if (!Number.isFinite(observedAtMs)) {
      throw new FailClosedError(
        "live_codex_metrics_timestamp_invalid",
        "the expected Codex main metrics record has an invalid timestamp; live scope cannot fall back to an older record"
      );
    }
    candidates.push({ source: row.source, record, observedAtMs });
  }
  if (candidates.length === 0) {
    throw new FailClosedError(
      "live_codex_metrics_expected_scope_missing",
      "no Codex main request matches the expected provider/model/realm in live metrics"
    );
  }
  candidates.sort((left, right) => right.observedAtMs - left.observedAtMs);
  return candidates[0];
}

function safeLiveCodexLabel(value) {
  const normalized = String(value ?? "").trim();
  return /^[A-Za-z0-9_-]{1,96}$/u.test(normalized) ? normalized : null;
}

function liveCodexGateRecency(ageMs, maxAgeSeconds) {
  if (!Number.isFinite(ageMs)) return "unknown";
  if (ageMs < -60_000) return "clock_skew";
  return ageMs > Number(maxAgeSeconds) * 1_000 ? "stale" : "current";
}

function liveCodexGateTransportEvidence(record) {
  const projected = {};
  for (const [target, source] of Object.entries({
    request_bytes: "request_body_bytes",
    sent_bytes: "sent_body_bytes",
    request_encode_ms: "request_body_encode_ms",
    gzip_encode_ms: "gzip_encode_ms",
    upstream_headers_ms: "upstream_attempt_headers_ms",
    upstream_wait_ms: "stream_upstream_wait_ms",
    client_backpressure_ms: "stream_client_backpressure_ms"
  })) {
    const value = finiteNonNegativeNumber(record?.[source]);
    if (value !== null) projected[target] = value;
  }
  for (const field of ["gzip_attempted", "gzip_fallback_used", "downstream_disconnected"]) {
    if (typeof record?.[field] === "boolean") projected[field] = record[field];
  }
  return projected;
}

function liveCodexGateEvidence({
  checkpoint,
  latest,
  expectedProviderId,
  expectedModel,
  expectedRealm,
  maxAgeSeconds,
  requireFresh = true,
  selectionMode = "expected-scope"
}) {
  const record = latest?.record;
  if (!record || !latest) {
    return {
      schema: "atoapi-live-codex-gate-evidence-v1",
      checkpoint,
      record_available: false
    };
  }
  const providerId = String(record?.provider_id ?? "").trim();
  const model = String(record?.model ?? "").trim();
  const realm = String(record?.shadow_affinity_realm_id ?? "").trim();
  const ageMs = Date.now() - latest.observedAtMs;
  const status = Number(record?.status);
  return {
    schema: "atoapi-live-codex-gate-evidence-v1",
    checkpoint,
    record_available: true,
    record_source: latest.source === "recent_failed_requests"
      ? "recent_failed_requests"
      : "recent_requests",
    recency: liveCodexGateRecency(ageMs, maxAgeSeconds),
    freshness_required: requireFresh === true,
    selection_mode: selectionMode === "latest-main" ? "latest-main" : "expected-scope",
    http_status: Number.isInteger(status) && status >= 100 && status <= 599 ? status : null,
    protocol: {
      client_channel: safeLiveCodexLabel(record?.client_channel),
      upstream_channel: safeLiveCodexLabel(record?.upstream_channel),
      upstream_call_kind: safeLiveCodexLabel(record?.upstream_call_kind)
    },
    terminal_sse: {
      completed_event_seen: record?.sse_completed_event_seen === true,
      done_marker_seen: record?.sse_done_marker_seen === true,
      end_reason: safeLiveCodexLabel(record?.sse_end_reason),
      chunks: finiteNonNegativeNumber(record?.sse_chunks)
    },
    cache_status: safeLiveCodexLabel(record?.cache_status),
    scope: {
      provider_present: Boolean(providerId),
      model_present: Boolean(model),
      key_realm_present: /^[0-9a-f]{64}$/u.test(realm),
      provider_matches_expected: Boolean(providerId) && providerId === expectedProviderId,
      model_matches_expected: Boolean(model) && model === expectedModel,
      key_realm_matches_expected: /^[0-9a-f]{64}$/u.test(realm) && realm === expectedRealm
    },
    transport: liveCodexGateTransportEvidence(record)
  };
}

function liveCodexGateError(code, message, evidence) {
  return new LiveCodexMetricsGateError(code, message, evidence);
}

function validateLiveCodexMetricsScopeRecord({
  checkpoint,
  latest,
  expectedProviderId,
  expectedModel,
  expectedRealm,
  maxAgeSeconds,
  requireFresh = true,
  selectionMode = "expected-scope"
}) {
  const record = latest.record;
  const ageMs = Date.now() - latest.observedAtMs;
  const evidence = liveCodexGateEvidence({
    checkpoint,
    latest,
    expectedProviderId,
    expectedModel,
    expectedRealm,
    maxAgeSeconds,
    requireFresh,
    selectionMode
  });
  if (requireFresh && (ageMs > maxAgeSeconds * 1_000 || ageMs < -60_000)) {
    throw liveCodexGateError(
      "live_codex_metrics_stale",
      "the latest matching Codex main metrics record is not current at " + checkpoint,
      evidence
    );
  }
  const status = Number(record?.status);
  if (!Number.isInteger(status) || status < 200 || status >= 300) {
    throw liveCodexGateError(
      "live_codex_metrics_failed",
      "the latest matching Codex main request failed at " + checkpoint,
      evidence
    );
  }
  if (
    String(record?.client_channel ?? "") !== "responses" ||
    String(record?.upstream_channel ?? "") !== "responses" ||
    String(record?.upstream_call_kind ?? "") !== "stream" ||
    record?.sse_completed_event_seen !== true
  ) {
    throw liveCodexGateError(
      "live_codex_metrics_incomplete",
      "the latest matching Codex main request is not a completed Responses streaming request at " + checkpoint,
      evidence
    );
  }
  const providerId = String(record?.provider_id ?? "").trim();
  const model = String(record?.model ?? "").trim();
  const realm = String(record?.shadow_affinity_realm_id ?? "").trim();
  if (!providerId || !model || !/^[0-9a-f]{64}$/u.test(realm)) {
    throw liveCodexGateError(
      "live_codex_metrics_scope_incomplete",
      "the latest matching Codex main request has incomplete provider/model/realm evidence at " + checkpoint,
      evidence
    );
  }
  if (
    providerId !== expectedProviderId ||
    model !== expectedModel ||
    realm !== expectedRealm
  ) {
    throw liveCodexGateError(
      "live_codex_metrics_scope_changed",
      "the expected Codex provider/model/realm changed at " + checkpoint + "; live comparison stopped before more traffic was sent",
      evidence
    );
  }
  return evidence;
}

async function assertLiveCodexMetricsScopeUnchanged({
  metricsUrl,
  localKey,
  expectedProviderId,
  expectedModel,
  expectedRealm,
  maxAgeSeconds,
  checkpoint,
  requireFresh = true,
  selectionMode = "expected-scope"
}) {
  if (selectionMode !== "expected-scope" && selectionMode !== "latest-main") {
    throw new FailClosedError(
      "invalid_live_codex_selection_mode",
      "live Codex scope selection must be expected-scope or latest-main"
    );
  }
  let metrics;
  try {
    metrics = await getJson(metricsUrl, 5_000, localKey);
  } catch (error) {
    const code = error instanceof FailClosedError
      ? error.code
      : "live_codex_metrics_unavailable";
    throw liveCodexGateError(
      code,
      "the live Codex metrics gate could not be evaluated at " + checkpoint,
      liveCodexGateEvidence({ checkpoint, maxAgeSeconds, requireFresh, selectionMode })
    );
  }
  let latest;
  try {
    latest = selectionMode === "latest-main"
      ? selectLatestLiveCodexMainRecord(metrics)
      : selectLatestLiveCodexMainRecordForExpectedScope(metrics, {
        expectedProviderId,
        expectedModel,
        expectedRealm
      });
  } catch (error) {
    const code = error instanceof FailClosedError
      ? error.code
      : "live_codex_metrics_unavailable";
    throw liveCodexGateError(
      code,
      "the live Codex metrics gate could not select a current matching Codex record at " + checkpoint,
      liveCodexGateEvidence({ checkpoint, maxAgeSeconds, requireFresh, selectionMode })
    );
  }
  return validateLiveCodexMetricsScopeRecord({
    checkpoint,
    latest,
    expectedProviderId,
    expectedModel,
    expectedRealm,
    maxAgeSeconds,
    requireFresh,
    selectionMode
  });
}

async function getJson(url, timeoutMs, localKey = "") {
  const response = await fetch(url, {
    headers: localKey ? { authorization: `Bearer ${localKey}` } : undefined,
    signal: AbortSignal.timeout(timeoutMs)
  });
  if (!response.ok) {
    throw new FailClosedError("admin_endpoint_failed", `${url} returned HTTP ${response.status}`);
  }
  return response.json();
}

async function assertFile(path, label) {
  try {
    const metadata = await stat(path);
    if (!metadata.isFile() || metadata.size <= 0) {
      throw new Error("not a non-empty file");
    }
  } catch {
    throw new FailClosedError("missing_file", `${label} is missing or not a non-empty file`);
  }
}

async function readRequiredText(path, label) {
  try {
    return await readFile(path, "utf8");
  } catch {
    throw new FailClosedError("missing_file", `${label} is not readable`);
  }
}

async function fileExists(path) {
  try {
    await stat(path);
    return true;
  } catch {
    return false;
  }
}

function hasOfflineArtifacts(options) {
  return Boolean(options["champion-result"] || options["candidate-result"]);
}

function sameCohort(left, right) {
  return isCompleteCohort(left) && isCompleteCohort(right) &&
    left.provider_id === right.provider_id &&
    left.model === right.model &&
    left.key_realm_hash === right.key_realm_hash &&
    left.request_family === right.request_family;
}

function isCompleteCohort(value) {
  return Boolean(
    value &&
    typeof value.provider_id === "string" && value.provider_id &&
    typeof value.model === "string" && value.model &&
    typeof value.key_realm_hash === "string" && value.key_realm_hash &&
    typeof value.request_family === "string" && value.request_family
  );
}

function validateOpaqueHash(value, label) {
  const normalized = String(value ?? "").trim();
  if (!/^[A-Za-z0-9._:-]{8,256}$/u.test(normalized)) {
    throw new FailClosedError(
      "invalid_key_realm_hash",
      `${label} must be an opaque 8-256 character identifier; never pass the raw provider Key`
    );
  }
  return normalized;
}

function optionalUpstreamUserAgent(value) {
  if (value === undefined) return null;
  const normalized = String(value).trim();
  if (!normalized || normalized.length > 256 || /[\r\n]/u.test(normalized)) {
    throw new FailClosedError(
      "invalid_upstream_user_agent",
      "--upstream-user-agent must be a non-empty single-line value up to 256 characters"
    );
  }
  return normalized;
}

function evaluateUpstreamUserAgentParity({
  championUpstreamUserAgent,
  candidateUpstreamUserAgent,
  sourceCustomUserAgent,
  championExecutableSha256,
  candidateExecutableSha256,
  diagnosticUserAgentSplit = false
}) {
  const champion = String(championUpstreamUserAgent ?? "").trim();
  const candidate = String(candidateUpstreamUserAgent ?? "").trim();
  const source = String(sourceCustomUserAgent ?? "").trim();

  if (diagnosticUserAgentSplit) {
    const sameBinary = championExecutableSha256 === candidateExecutableSha256;
    const exactlyOneOverride = Boolean(champion) !== Boolean(candidate);
    if (!sameBinary) {
      return {
        ok: false,
        code: "diagnostic_user_agent_split_requires_same_binary",
        message: "--diagnostic-user-agent-split requires identical champion and candidate executable hashes"
      };
    }
    if (source) {
      return {
        ok: false,
        code: "diagnostic_user_agent_split_requires_source_default",
        message: "--diagnostic-user-agent-split requires the selected Provider to have no custom_user_agent"
      };
    }
    if (!exactlyOneOverride) {
      return {
        ok: false,
        code: "diagnostic_user_agent_split_requires_one_override",
        message: "--diagnostic-user-agent-split requires exactly one arm to set an explicit upstream User-Agent"
      };
    }
    return { ok: true, mode: "same-binary-diagnostic-split", promotion_eligible: false };
  }

  // A one-arm override is never a valid A/B: it creates an intentional
  // header/cache-lane split. Require both values and exact equality.
  if (champion || candidate) {
    if (!champion || !candidate || champion !== candidate) {
      return {
        ok: false,
        code: "upstream_user_agent_mismatch",
        message: "champion and candidate must use the same explicit upstream User-Agent"
      };
    }
    return { ok: true, mode: "explicit-common" };
  }

  if (source) return { ok: true, mode: "source-config-common" };
  if (championExecutableSha256 === candidateExecutableSha256) {
    return { ok: true, mode: "same-binary-default" };
  }

  return {
    ok: false,
    code: "cross_binary_upstream_user_agent_unproven",
    message: "cross-binary live comparison requires a common --upstream-user-agent when the selected Provider has no custom_user_agent"
  };
}

function boundedInteger(value, label, minimum, maximum) {
  const parsed = Number.parseInt(String(value), 10);
  if (!Number.isSafeInteger(parsed) || parsed < minimum || parsed > maximum) {
    throw new FailClosedError("invalid_parameter", `${label} must be an integer from ${minimum} to ${maximum}`);
  }
  return parsed;
}

function parseArgs(items) {
  const parsed = {};
  for (let index = 0; index < items.length; index += 1) {
    const item = items[index];
    if (!item.startsWith("--")) continue;
    const [key, inline] = item.slice(2).split("=", 2);
    if (inline !== undefined) {
      parsed[key] = inline;
    } else if (items[index + 1] && !items[index + 1].startsWith("--")) {
      parsed[key] = items[index + 1];
      index += 1;
    } else {
      parsed[key] = true;
    }
  }
  return parsed;
}

function booleanArg(value) {
  return value === true || new Set(["1", "true", "on", "yes"]).has(String(value).toLowerCase());
}

function optionalBoolean(value, label) {
  if (value === undefined || value === null) return null;
  const normalized = String(value).trim().toLowerCase();
  if (new Set(["1", "true", "on", "yes"]).has(normalized)) return true;
  if (new Set(["0", "false", "off", "no"]).has(normalized)) return false;
  throw new FailClosedError(
    "invalid_boolean_parameter",
    `${label} must be true or false when supplied`
  );
}

function resolveFreshFixturePerPair(options) {
  const reuseFixtureAcrossPairs = booleanArg(options["reuse-fixture-across-pairs"]);
  if (reuseFixtureAcrossPairs && booleanArg(options["fresh-fixture-per-pair"])) {
    throw new FailClosedError(
      "conflicting_fixture_reuse_flags",
      "--fresh-fixture-per-pair and --reuse-fixture-across-pairs cannot be used together"
    );
  }
  return !reuseFixtureAcrossPairs;
}

function resolveIncludeToolSchema(options) {
  return optionalBoolean(
    options["include-tool-schema"],
    "--include-tool-schema"
  ) ?? true;
}

function resolvePromotionTtftPolicy(options) {
  const strict = optionalBoolean(
    options["require-ttft-no-regression"],
    "--require-ttft-no-regression"
  );
  const upstreamExempt = optionalBoolean(
    options["allow-upstream-ttft-regression"],
    "--allow-upstream-ttft-regression"
  );
  if (strict === true && upstreamExempt === true) {
    throw new FailClosedError(
      "conflicting_ttft_promotion_policy",
      "--require-ttft-no-regression and --allow-upstream-ttft-regression cannot both be true"
    );
  }
  if (strict === false) {
    throw new FailClosedError(
      "invalid_ttft_promotion_policy",
      "use --allow-upstream-ttft-regression to exempt only upstream TTFT; local latency remains mandatory"
    );
  }
  return upstreamExempt !== true;
}

function strictLocalLatencyRegressionBudget(options) {
  const value = boundedInteger(
    options["max-local-proxy-overhead-regression-ms"] ?? 0,
    "--max-local-proxy-overhead-regression-ms",
    0,
    500
  );
  if (value !== 0) {
    throw new FailClosedError(
      "local_latency_regression_budget_must_be_zero",
      "release promotion requires --max-local-proxy-overhead-regression-ms 0"
    );
  }
  return 0;
}

function ratio(numerator, denominator) {
  return denominator > 0 ? numerator / denominator : 0;
}

function finiteNonNegativeNumber(value) {
  const parsed = Number(value);
  return Number.isFinite(parsed) && parsed >= 0 ? parsed : null;
}

function explicitFiniteNonNegativeNumber(value) {
  return typeof value === "number" && Number.isFinite(value) && value >= 0 ? value : null;
}

function number(value) {
  const parsed = Number(value ?? 0);
  return Number.isFinite(parsed) ? parsed : 0;
}

function sum(items, key) {
  return array(items).reduce((total, item) => total + number(item?.[key]), 0);
}

function array(value) {
  return Array.isArray(value) ? value : [];
}

function unique(values) {
  return [...new Set(values.map((item) => String(item)).filter(Boolean))].sort();
}

function percentile(values, percentage) {
  const sorted = values.filter((value) => Number.isFinite(value) && value >= 0).sort((a, b) => a - b);
  if (sorted.length === 0) return 0;
  const index = Math.min(sorted.length - 1, Math.max(0, Math.ceil(sorted.length * percentage / 100) - 1));
  return sorted[index];
}

function sha256Text(value) {
  return createHash("sha256").update(String(value)).digest("hex");
}

function sha256Parts(parts) {
  const hash = createHash("sha256");
  for (const part of parts) {
    hash.update(String(part));
    hash.update(Buffer.from([0]));
  }
  return hash.digest("hex");
}

function safeSegment(value) {
  return String(value).replace(/[^A-Za-z0-9_-]/gu, "-").slice(0, 64) || "run";
}

function escapeRegExp(value) {
  return String(value).replace(/[.*+?^${}()|[\]\\]/gu, "\\$&");
}

function delay(ms) {
  return new Promise((resolveDelay) => setTimeout(resolveDelay, ms));
}

function safeErrorMessage(error) {
  const message = error instanceof Error ? error.message : String(error);
  return message
    .replace(/Bearer\s+[^\s,;]+/giu, "Bearer [redacted]")
    .replace(/(api[_-]?key\s*[=:]\s*)[^\s,;]+/giu, "$1[redacted]")
    .slice(0, 300);
}

function failClosedReport(error) {
  const known = error instanceof FailClosedError;
  return {
    schema: SCHEMA,
    kind: "release-champion-comparison",
    pass: false,
    fail_closed: true,
    error: {
      code: known ? error.code : "unexpected_verifier_error",
      message: safeErrorMessage(error),
      missing_parameters: known ? error.missing : []
    }
  };
}

function printUsage() {
  console.log(`Usage:
  node scripts/verify-release-champion.mjs --live \\
    --champion-exe <old.exe> --candidate-exe <new.exe> \\
    --source-config-dir <Atoapi-config-dir> --model <model> \\
    --key-realm-hash <opaque-hash> [--provider-scope codex-agent|active-provider] [--provider-id <id>] \\
    [--reasoning-effort none|minimal|low|medium|high|xhigh|max|ultra] \\
    [--scenario full-replay|tool-burst|dynamic-tail-mix|tool-tail-maturity|compacted-anchor|compaction-root] [--pairs 2] [--warmup-pairs 0] [--turns 6] \\
    [--pair-offset 0|1] [--first-arm champion|candidate] \\
    [--seed-context-chars <0-2500000>] [--minimum-seed-input-tokens <0-1000000>] \\
    [--minimum-peak-input-tokens <0-1000000>] [--maximum-peak-input-tokens <0-1000000>] \\
    [--max-input-token-delta <0-10000>] \\
    [--fixture-profile natural|natural-dense|legacy-repeated] [--dynamic-tail-profile mixed|natural-dense] \\
    [--tool-calls <1-8>] [--tool-output-shape natural|natural-dense|flat|structured|noisy] [--tool-protocol function|custom] \\
    [--include-tool-schema true|false] \\
    [--turn-delay-ms <0-5000>] [--inter-arm-delay-ms <0-5000>] [--pair-delay-ms <0-60000>] \\
    [--seed-to-reuse-delay-ms <0-3600000>] \\
    [--response-timeout-ms <30000-600000>] \\
    [--reuse-fixture-across-pairs] \\
    [--isolate-upstream-cache] \\
    [--reuse-runtime-per-arm] [--shared-cache-crossover] \\
    [--require-candidate-exact-medium-tool-tail-maturity-wait] \\
    [--require-candidate-exact-large-message-tail-lag] \\
    [--exercise-local-previous-response-id-rebind|--exercise-local-previous-response-id-full-replay] \\
    [--candidate-cache-control-field prompt-cache-key|prompt-cache-options|prompt-cache-retention] [--candidate-cache-options-24h] \\
    [--candidate-thread-stable-pck-bridge] \\
    [--require-candidate-options24h-sibling-settle] \\
    [--max-local-proxy-overhead-regression-ms <0-500>] \\
    [--require-ttft-no-regression] \\
    [--max-full-bucket-regression-requests <calibrated-count>] \\
    [--upstream-user-agent <test-only-stable-value>] \\
    [--champion-upstream-user-agent <value>] [--candidate-upstream-user-agent <value>] \\
    [--diagnostic-user-agent-split --isolate-upstream-cache] \\
    [--force-use-system-proxy true|false]

Offline comparison (does not start any process):
  node scripts/verify-release-champion.mjs \\
    --champion-result <champion-arm.json> --candidate-result <candidate-arm.json>

Safety:
  --live is required before an upstream-backed isolated run.  Missing config,
  usage, terminal SSE, key-realm evidence, or one-attempt/one-POST evidence
  fails closed.  The script only starts temporary isolated ports; it never
  sends signals to the existing 18883 process.  Each pair uses a fresh fixture
  by default; --reuse-fixture-across-pairs is an explicit deterministic-repro
  override. --isolate-upstream-cache gives the two arms distinct metadata-only
  session identities, preventing generated placement-key cache sharing; it
  also forces fresh isolated processes per pair so an arm-owned connection
  pool cannot bias the cache-placement result.
  --reuse-runtime-per-arm keeps one isolated process per arm across fresh
  pairs only when upstream-cache isolation is off.
  --persistent-runtime-start-order changes only which persistent isolated
  process is created first. It is a transport-placement diagnostic for reused
  runtimes; it never changes the Key, request body, cache placement, or
  turn-by-turn sender order.
  --candidate-cache-options-24h is an isolated candidate-only probe. It
  requires --candidate-cache-control-field prompt-cache-options and records a
  payload-free final-wire diagnostic for the exact 24h variant.
   --require-candidate-options24h-sibling-settle requires that exact 24h probe
   and zero turn/inter-arm pacing. The live report fails closed unless at least
   one candidate request records responses_prompt_cache_options_sibling_settle.
   --require-candidate-exact-large-message-tail-lag is the dynamic text-tail
   witness gate. It requires the bounded natural/mixed/text three-turn fixture,
   both 262144-token gates, zero turn/inter-arm pacing, and the exact candidate
   reason responses_exact_large_message_tail_lag; a generic prefix wait does
   not qualify.
  --candidate-thread-stable-pck-bridge is an isolated, non-default candidate
  treatment. It requires dynamic-tail-mix with --shared-cache-crossover and
  injects ATOAPI_EXPERIMENTAL_THREAD_STABLE_PCK_BRIDGE only into the candidate
  runtime. The report fails closed unless dynamic input is symmetric and the
  only final-wire difference is the redacted prompt_cache_key placement
  fingerprint.
  --fixture-profile natural is the default: an equal-length, deterministic,
  non-repeated synthetic context. legacy-repeated exists only to reproduce
  older fixture behavior; neither profile uses user context or changes Atoapi.
  --exercise-local-previous-response-id-full-replay is a verifier-only,
  non-promotable correctness fixture. It preserves closed tool call ids,
  requires --tool-chars >=32768 and --minimum-peak-input-tokens >=16384,
  and is mutually exclusive with the regenerated-id rebind fixture.
  --include-tool-schema is on by default for historical compatibility. In a
  tool-history scenario it adds the read_release_fixture schema plus
  tool_choice=none from the seed onward. Pass false only for an explicit
  schema-free protocol probe, because that intentionally changes the wire.
  --pair-delay-ms is test-only pacing between fresh pairs; it never changes a
  request body, retries an inbound, or touches the running service. --first-arm
  and --pair-offset only choose which isolated arm sends first; they never alter
  the selected Provider, Key, proxy, request body, or upstream call count.
  --warmup-pairs executes the earliest pairs but excludes their raw results
  from aggregates and promotion; it may be set from 0 through --pairs minus 1.`);
}

async function runSelfTest() {
  assert.deepEqual(parseArgs(["--pairs=2", "--live", "--model", "m"]), {
    pairs: "2",
    live: true,
    model: "m"
  });
  const horizonDelayConfig = {
    seedToReuseDelayMs: 2_100_000,
    scenario: "dynamic-tail-mix",
    turns: 3,
    scoredPairCount: 2,
    sharedCacheCrossover: true,
    reuseRuntimePerArm: true,
    candidateCacheControlField: "prompt-cache-retention",
    turnDelayMs: 0,
    interArmDelayMs: 0,
    liveCodexMetricsConfigured: true
  };
  assert.doesNotThrow(() => validateSeedToReuseDelay(horizonDelayConfig));
  assert.throws(
    () => validateSeedToReuseDelay({ ...horizonDelayConfig, scenario: "full-replay" }),
    (error) => error?.code === "seed_to_reuse_delay_scenario_invalid"
  );
  assert.throws(
    () => validateSeedToReuseDelay({ ...horizonDelayConfig, scoredPairCount: 1 }),
    (error) => error?.code === "seed_to_reuse_delay_scored_pair_count_invalid"
  );
  assert.throws(
    () => validateSeedToReuseDelay({ ...horizonDelayConfig, sharedCacheCrossover: false }),
    (error) => error?.code === "seed_to_reuse_delay_shared_crossover_required"
  );
  assert.throws(
    () => validateSeedToReuseDelay({ ...horizonDelayConfig, candidateCacheControlField: "prompt-cache-key" }),
    (error) => error?.code === "seed_to_reuse_delay_retention_required"
  );
  assert.throws(
    () => boundedInteger(3_600_001, "--seed-to-reuse-delay-ms", 0, 3_600_000),
    (error) => error?.code === "invalid_parameter"
  );
  assert.deepEqual(
    seedToReuseDelayEvidence({
      pair: 1,
      requestedMs: 2_100_000,
      observedMs: 2_100_024.7,
      seedTurnOrder: ["candidate", "champion"],
      postDelaySelectionScopeVerified: true,
      postDelayLiveScopeVerified: true
    }),
    {
      schema: "atoapi-seed-to-reuse-delay-evidence-v1",
      pair: 1,
      requested_ms: 2_100_000,
      observed_ms: 2_100_025,
      after_turn: 0,
      before_turn: 1,
      seed_turn_order: ["candidate", "champion"],
      post_delay_selection_scope_verified: true,
      post_delay_live_scope_verified: true
    }
  );
  const balancedSharedCrossover = sharedTurnCrossoverEvidence({
    required: true,
    observed: true,
    scenario: "dynamic-tail-mix",
    turns: 3,
    scored_pair_ids: [0, 1],
    turn_orders: [
      [["champion", "candidate"], ["champion", "candidate"], ["candidate", "champion"]],
      [["candidate", "champion"], ["candidate", "champion"], ["champion", "candidate"]]
    ]
  });
  assert.equal(balancedSharedCrossover.pass, true);
  assert.equal(balancedSharedCrossover.pair_count_sufficient, true);
  assert.equal(balancedSharedCrossover.phase_balance_complete, true);
  const onePairSharedCrossover = sharedTurnCrossoverEvidence({
    required: true,
    observed: true,
    scenario: "dynamic-tail-mix",
    turns: 3,
    scored_pair_ids: [0],
    turn_orders: [
      [["champion", "candidate"], ["champion", "candidate"], ["candidate", "champion"]]
    ]
  });
  assert.equal(onePairSharedCrossover.pass, false);
  assert.equal(onePairSharedCrossover.reason, "requires_at_least_two_complete_scored_pairs");
  const phaseSkewedSharedCrossover = sharedTurnCrossoverEvidence({
    required: true,
    observed: true,
    scenario: "dynamic-tail-mix",
    turns: 3,
    scored_pair_ids: [0, 1],
    turn_orders: [
      [["champion", "candidate"], ["champion", "candidate"], ["candidate", "champion"]],
      [["candidate", "champion"], ["champion", "candidate"], ["champion", "candidate"]]
    ]
  });
  assert.equal(phaseSkewedSharedCrossover.pass, false);
  assert.equal(
    phaseSkewedSharedCrossover.reason,
    "first_second_order_not_balanced_for_every_relevant_phase"
  );
  const maturitySchedule = (pair) => {
    const schedule = toolTailMaturityDispatchSchedule(pair, 0, "champion");
    return {
      ...schedule,
      actual_sequence: schedule.sequence.map((event) => ({ ...event, role: event.role ?? null }))
    };
  };
  const balancedMaturityCrossover = sharedTurnCrossoverEvidence({
    required: true,
    observed: true,
    scenario: "tool-tail-maturity",
    turns: 4,
    scored_pair_ids: [0, 1],
    turn_orders: [maturitySchedule(0), maturitySchedule(1)]
  });
  assert.equal(balancedMaturityCrossover.pass, true);
  assert.equal(balancedMaturityCrossover.pairs[0].leader, "champion");
  assert.equal(balancedMaturityCrossover.pairs[1].leader, "candidate");
  const interruptedMaturitySchedule = maturitySchedule(1);
  [interruptedMaturitySchedule.actual_sequence[5], interruptedMaturitySchedule.actual_sequence[6]] = [
    interruptedMaturitySchedule.actual_sequence[6],
    interruptedMaturitySchedule.actual_sequence[5]
  ];
  const interruptedMaturityCrossover = sharedTurnCrossoverEvidence({
    required: true,
    observed: true,
    scenario: "tool-tail-maturity",
    turns: 4,
    scored_pair_ids: [0, 1],
    turn_orders: [maturitySchedule(0), interruptedMaturitySchedule]
  });
  assert.equal(interruptedMaturityCrossover.pass, false);
  const exactMediumRows = [
    { phase: "seed", input_tokens: 20_000, prefix_guard_wait_ms: 0 },
    { phase: "followup-1", input_tokens: 20_016, prefix_guard_wait_ms: 0 },
    {
      phase: "tool-tail-maturity",
      input_tokens: 20_128,
      tail_tool_output_chars: 6_144,
      tail_largest_tool_output_chars: 6_144,
      prefix_guard_wait_ms: 0
    },
    {
      phase: "followup-3",
      request_kind: "turn",
      sse_completed: true,
      prefix_guard_wait_ms: 250,
      prefix_guard_wait_reason: "responses_exact_medium_tool_tail_maturity_pending",
      prefix_guard_wait_source: "exact"
    }
  ];
  const exactMediumEvidence = exactMediumToolTailMaturityEvidence(exactMediumRows);
  assert.equal(exactMediumEvidence.predecessor_requests, 1);
  assert.equal(exactMediumEvidence.direct_successor_requests, 1);
  assert.equal(exactMediumEvidence.maturity_wait_requests, 1);
  assert.equal(exactMediumEvidence.non_target_prefix_guard_wait_requests, 0);
  const genericGuardEvidence = exactMediumToolTailMaturityEvidence([
    ...exactMediumRows.slice(0, 3),
    {
      ...exactMediumRows[3],
      prefix_guard_wait_reason: "responses_fresh_exact_prefix_settle"
    }
  ]);
  assert.equal(genericGuardEvidence.maturity_wait_requests, 0);
  assert.equal(genericGuardEvidence.non_target_prefix_guard_wait_requests, 1);
  const isolatedCertificate = validateIsolatedCacheControlCertificatePayload({
    provider_id: "provider-a",
    model_id: "model-a",
    channel: "responses",
    key_id: "key-a",
    fields: [{
      field: "prompt-cache-options",
      status: "verified",
      http_status: 200
    }]
  }, {
    httpStatus: 200,
    providerId: "provider-a",
    modelId: "model-a",
    channel: "responses",
    expectedKeyId: "key-a",
    field: "prompt-cache-options"
  });
  assert.deepEqual(isolatedCertificate, {
    schema: "atoapi-isolated-cache-control-certificate-v1",
    state: "verified",
    provider_id: "provider-a",
    model_id: "model-a",
    channel: "responses",
    field: "prompt-cache-options",
    selected_key_scope: "pinned",
    management_request_count: 2,
    field_http_status: 200
  });
  assert.throws(
    () => validateIsolatedCacheControlCertificatePayload({
      provider_id: "provider-a",
      model_id: "model-a",
      channel: "responses",
      key_id: "other-key",
      fields: [{ field: "prompt-cache-options", status: "verified", http_status: 200 }]
    }, {
      httpStatus: 200,
      providerId: "provider-a",
      modelId: "model-a",
      channel: "responses",
      expectedKeyId: "key-a",
      field: "prompt-cache-options"
    }),
    (error) => error?.code === "candidate_cache_control_capability_key_mismatch"
  );
  assert.throws(
    () => validateIsolatedCacheControlCertificatePayload({
      provider_id: "provider-a",
      model_id: "model-a",
      channel: "responses",
      key_id: "key-a",
      fields: [{ field: "prompt-cache-options", status: "unsupported", http_status: 400 }]
    }, {
      httpStatus: 200,
      providerId: "provider-a",
      modelId: "model-a",
      channel: "responses",
      expectedKeyId: "key-a",
      field: "prompt-cache-options"
    }),
    (error) => error?.code === "candidate_cache_control_capability_unverified"
  );
  assert.throws(
    () => validateIsolatedCacheControlCertificatePayload({
      error: {
        message: "failed to select the requested provider key: current scope changed"
      }
    }, {
      httpStatus: 502,
      providerId: "provider-a",
      modelId: "model-a",
      channel: "responses",
      expectedKeyId: "key-a",
      field: "prompt-cache-options"
    }),
    (error) => error?.code === "candidate_cache_control_capability_probe_failed" &&
      error?.message.includes("HTTP 502") &&
      error?.message.includes("selected_key_scope_mismatch")
  );
  assert.equal(
    isolatedCacheControlProbeFailureCategory({
      error: { message: "proxy tunnel connection timed out" }
    }),
    "upstream_transport_failure",
    "capability probe diagnostics must retain only a safe transport category"
  );
  const oldRuntimeError = {
    at: "2026-08-10T00:00:00Z",
    scope: "upstream_transport",
    message: "dns lookup failed"
  };
  assert.deepEqual(
    freshRuntimeErrorEvidence({
      recent_errors: [
        oldRuntimeError,
        { at: "2026-08-10T00:00:01Z", scope: "upstream_transport", message: "proxy tunnel failed" },
        { at: "2026-08-10T00:00:02Z", scope: "untrusted scope value", message: "request timeout" }
      ]
    }, new Set([errorFingerprint(oldRuntimeError)])),
    { scopes: ["upstream_transport"], classes: ["proxy", "timeout"] },
    "release evidence must retain only new, allow-listed runtime error categories"
  );
  assert.equal(
    optionalLiveCodexMetricsUrl("http://127.0.0.1:18883/admin/metrics"),
    "http://127.0.0.1:18883/admin/metrics",
    "the live Codex scope guard must stay on the protected local metrics endpoint"
  );
  assert.throws(
    () => optionalLiveCodexMetricsUrl("http://localhost:18883/admin/metrics"),
    (error) => error?.code === "invalid_live_codex_metrics_url"
  );
  const liveRealm = "a".repeat(64);
  const latestLiveCodex = selectLatestLiveCodexMainRecord({
    recent_requests: [{
      at: "2026-08-10T00:00:00Z",
      agent_id: "codex",
      upstream_call_source: "main",
      status: 200,
      provider_id: "provider-a",
      model: "model-a",
      shadow_affinity_realm_id: liveRealm,
      client_channel: "responses",
      upstream_channel: "responses",
      upstream_call_kind: "stream",
      sse_completed_event_seen: true
    }],
    recent_failed_requests: [{
      at: "2026-08-10T00:00:01Z",
      agent_id: "codex",
      upstream_call_source: "main",
      status: 502,
      provider_id: "provider-a",
      model: "model-a",
      shadow_affinity_realm_id: liveRealm,
      client_channel: "responses",
      upstream_channel: "responses",
      upstream_call_kind: "stream",
      sse_completed_event_seen: false
    }]
  });
  assert.equal(
    latestLiveCodex.record.status,
    502,
    "a newer failed Codex request must not fall back to an older success"
  );
  assert.equal(
    latestLiveCodex.source,
    "recent_failed_requests",
    "the retained gate evidence must identify the bounded metrics collection"
  );
  const liveNow = Date.now();
  const liveAt = (offsetMs = 0) => new Date(liveNow + offsetMs).toISOString();
  const expectedLiveRecord = (overrides = {}) => ({
    at: liveAt(),
    agent_id: "codex",
    upstream_call_source: "main",
    status: 200,
    provider_id: "provider-a",
    model: "model-a",
    shadow_affinity_realm_id: liveRealm,
    client_channel: "responses",
    upstream_channel: "responses",
    upstream_call_kind: "stream",
    sse_completed_event_seen: true,
    ...overrides
  });
  const independentLiveRecord = expectedLiveRecord({
    at: liveAt(1_000),
    provider_id: "provider-b",
    model: "model-b",
    shadow_affinity_realm_id: "b".repeat(64)
  });
  const expectedLiveScope = {
    expectedProviderId: "provider-a",
    expectedModel: "model-a",
    expectedRealm: liveRealm
  };
  const matchingHealthyWithNewerIndependent = selectLatestLiveCodexMainRecordForExpectedScope({
    recent_requests: [expectedLiveRecord({ at: liveAt(-1_000) }), independentLiveRecord],
    recent_failed_requests: []
  }, expectedLiveScope);
  assert.equal(
    matchingHealthyWithNewerIndependent.record.provider_id,
    "provider-a",
    "a newer independent Codex scope must not replace the healthy expected scope"
  );
  assert.doesNotThrow(() => validateLiveCodexMetricsScopeRecord({
    checkpoint: "self_test_healthy_expected_scope",
    latest: matchingHealthyWithNewerIndependent,
    ...expectedLiveScope,
    maxAgeSeconds: 600
  }));
  const newestExpectedFailure = selectLatestLiveCodexMainRecordForExpectedScope({
    recent_requests: [expectedLiveRecord({ at: liveAt(-2_000) }), independentLiveRecord],
    recent_failed_requests: [expectedLiveRecord({ at: liveAt(-500), status: 502, sse_completed_event_seen: false })]
  }, expectedLiveScope);
  assert.equal(
    newestExpectedFailure.record.status,
    502,
    "a new matching-scope failure must not fall back to an older matching success"
  );
  assert.throws(
    () => validateLiveCodexMetricsScopeRecord({
      checkpoint: "self_test_newest_expected_failure",
      latest: newestExpectedFailure,
      ...expectedLiveScope,
      maxAgeSeconds: 600
    }),
    (error) => error?.code === "live_codex_metrics_failed"
  );
  assert.throws(
    () => selectLatestLiveCodexMainRecordForExpectedScope({
      recent_requests: [independentLiveRecord],
      recent_failed_requests: []
    }, expectedLiveScope),
    (error) => error?.code === "live_codex_metrics_expected_scope_missing"
  );
  const staleExpectedScope = selectLatestLiveCodexMainRecordForExpectedScope({
    recent_requests: [expectedLiveRecord({ at: liveAt(-601_000) }), independentLiveRecord],
    recent_failed_requests: []
  }, expectedLiveScope);
  assert.throws(
    () => validateLiveCodexMetricsScopeRecord({
      checkpoint: "self_test_stale_expected_scope",
      latest: staleExpectedScope,
      ...expectedLiveScope,
      maxAgeSeconds: 600
    }),
    (error) => error?.code === "live_codex_metrics_stale"
  );
  assert.doesNotThrow(() => validateLiveCodexMetricsScopeRecord({
    checkpoint: "self_test_horizon_stale_matching_scope",
    latest: staleExpectedScope,
    ...expectedLiveScope,
    maxAgeSeconds: 600,
    requireFresh: false,
    selectionMode: "latest-main"
  }), "horizon verification may retain an old matching record only after checking its complete scope");
  const newestScopeChangedRecord = selectLatestLiveCodexMainRecord({
    recent_requests: [expectedLiveRecord({ at: liveAt(-601_000) }), independentLiveRecord],
    recent_failed_requests: []
  });
  assert.throws(
    () => validateLiveCodexMetricsScopeRecord({
      checkpoint: "self_test_horizon_latest_scope_changed",
      latest: newestScopeChangedRecord,
      ...expectedLiveScope,
      maxAgeSeconds: 600,
      requireFresh: false,
      selectionMode: "latest-main"
    }),
    (error) => error?.code === "live_codex_metrics_scope_changed",
    "the horizon gate must inspect the newest Codex main record rather than selecting an old matching scope"
  );
  const incompleteExpectedScope = selectLatestLiveCodexMainRecordForExpectedScope({
    recent_requests: [expectedLiveRecord({ at: liveAt(-250), sse_completed_event_seen: false }), independentLiveRecord],
    recent_failed_requests: []
  }, expectedLiveScope);
  assert.throws(
    () => validateLiveCodexMetricsScopeRecord({
      checkpoint: "self_test_incomplete_expected_scope",
      latest: incompleteExpectedScope,
      ...expectedLiveScope,
      maxAgeSeconds: 600
    }),
    (error) => error?.code === "live_codex_metrics_incomplete"
  );
  const incompleteLiveGateEvidence = liveCodexGateEvidence({
    checkpoint: "after_pair_0",
    latest: {
      source: "recent_failed_requests",
      observedAtMs: Date.now(),
      record: {
        status: 200,
        client_channel: "responses",
        upstream_channel: "responses",
        upstream_call_kind: "stream",
        sse_completed_event_seen: false,
        sse_done_marker_seen: false,
        sse_end_reason: "upstream_sse_error",
        cache_status: "error",
        provider_id: "provider-a",
        model: "model-a",
        shadow_affinity_realm_id: liveRealm,
        request_body_bytes: 1_024,
        sent_body_bytes: 512,
        gzip_attempted: true,
        sse_chunks: 4,
        upstream_error_message: "Bearer must-not-be-retained",
        raw_request_material: "must-not-be-retained"
      }
    },
    expectedProviderId: "provider-a",
    expectedModel: "model-a",
    expectedRealm: liveRealm,
    maxAgeSeconds: 600
  });
  assert.equal(incompleteLiveGateEvidence.record_available, true);
  assert.equal(incompleteLiveGateEvidence.record_source, "recent_failed_requests");
  assert.equal(incompleteLiveGateEvidence.http_status, 200);
  assert.equal(incompleteLiveGateEvidence.terminal_sse.completed_event_seen, false);
  assert.equal(incompleteLiveGateEvidence.scope.key_realm_matches_expected, true);
  assert.equal(JSON.stringify(incompleteLiveGateEvidence).includes("Bearer"), false);
  assert.equal(JSON.stringify(incompleteLiveGateEvidence).includes("must-not-be-retained"), false);
  assert.deepEqual(
    liveCodexGateEvidence({ checkpoint: "after_pair_0", maxAgeSeconds: 600 }),
    {
      schema: "atoapi-live-codex-gate-evidence-v1",
      checkpoint: "after_pair_0",
      record_available: false
    },
    "a metrics-read failure must not fabricate a record projection"
  );
  assert.throws(
    () => selectLatestLiveCodexMainRecord({
      recent_requests: [{
        agent_id: "codex",
        upstream_call_source: "main",
        status: 200
      }],
      recent_failed_requests: []
    }),
    (error) => error?.code === "live_codex_metrics_timestamp_missing"
  );
  const boundCodexRoute = codexAgentRoute([
    '[[agent_injections]]',
    'id = "codex"',
    'enabled = true',
    'provider_id = "selected-provider"',
    'model_id = "selected-model"'
  ].join("\n"));
  assert.deepEqual(boundCodexRoute, {
    provider_id: "selected-provider",
    model_id: "selected-model",
    enabled: true,
    target_path: ""
  });
  assert.doesNotThrow(() => assertCodexRouteModelScope(boundCodexRoute, "selected-model"));
  assert.throws(
    () => assertCodexRouteModelScope(boundCodexRoute, "different-model"),
    (error) => error?.code === "model_scope_mismatch",
    "a bound Codex model must not be silently replaced for live verification"
  );
  assert.doesNotThrow(
    () => assertCodexRouteModelScope({ ...boundCodexRoute, model_id: "" }, "request-model"),
    "an unpinned Codex binding deliberately takes its model from the actual request"
  );
  assert.equal(booleanArg(parseArgs(["--reuse-runtime-per-arm"])["reuse-runtime-per-arm"]), true);
  assert.equal(
    booleanArg(
      parseArgs(["--exercise-local-previous-response-id-full-replay"])[
        "exercise-local-previous-response-id-full-replay"
      ]
    ),
    true,
    "the unchanged local previous_response_id FullReplay fixture flag must parse explicitly"
  );
  assert.equal(
    normalizeCacheControlSymmetryPolicy({
      exerciseLocalPreviousResponseIdFullReplay: true,
      expectedLocalPreviousResponseIdFullReplayRequests: 2,
      toolProtocol: "function"
    }).exercise_local_previous_response_id_full_replay,
    true
  );
  assert.equal(
    booleanArg(parseArgs(["--candidate-cache-options-24h"])["candidate-cache-options-24h"]),
    true,
    "the isolated 24h cache-options probe flag must parse explicitly"
  );
  assert.equal(
    booleanArg(
      parseArgs(["--candidate-thread-stable-pck-bridge"])[
        "candidate-thread-stable-pck-bridge"
      ]
    ),
    true,
    "the isolated thread-stable prompt-cache-key bridge flag must parse explicitly"
  );
  assert.equal(
    isolatedRuntimeExperimentEnvironment({
      threadStablePromptCacheKeyBridgeTestEnabled: true
    }).ATOAPI_EXPERIMENTAL_THREAD_STABLE_PCK_BRIDGE,
    "1",
    "the bridge flag must be injected only when an isolated candidate runtime asks for it"
  );
  assert.equal(
    isolatedRuntimeExperimentEnvironment().ATOAPI_EXPERIMENTAL_THREAD_STABLE_PCK_BRIDGE,
    "0",
    "the champion/default isolated runtime must never inherit the bridge treatment"
  );
  const bridgeWire = {
    input_full: "input-full",
    input_prefixes: ["input-prefixes"],
    instructions: "instructions",
    tools_schema: "tools",
    pre_input_wire: "pre-input",
    cache_metadata: "metadata"
  };
  const bridgeRun = (arm, keyFingerprint) => ({
    arm,
    pair: 0,
    scenario: "dynamic-tail-mix",
    requests: [{
      phase: "seed",
      request_kind: "turn",
      sse_completed: true,
      input_fingerprint: "same-client-input",
      input_tokens: 20_000,
      fixture_identity_phase: "base",
      fixture_identity_thread_stable: true,
      provider_prefix_fingerprint: "provider-prefix",
      provider_prefix_key_fingerprint: keyFingerprint,
      outbound_prefix_fingerprints: bridgeWire
    }, {
      phase: "dynamic-tail-1-natural",
      request_kind: "turn",
      sse_completed: true,
      input_fingerprint: "same-dynamic-input",
      input_tokens: 20_128,
      fixture_identity_phase: "rotated",
      fixture_identity_thread_stable: true,
      provider_prefix_fingerprint: "provider-prefix",
      provider_prefix_key_fingerprint: keyFingerprint,
      outbound_prefix_fingerprints: bridgeWire
    }]
  });
  const bridgeWitness = threadStablePromptCacheKeyBridgeWireWitness(
    { runs: [bridgeRun("champion", "placement-a")] },
    { runs: [bridgeRun("candidate", "placement-b")] },
    true,
    128,
    true
  );
  assert.equal(bridgeWitness.pass, true, JSON.stringify(bridgeWitness));
  assert.equal(bridgeWitness.identity_churn_observed, true);
  const bridgeWithWireDrift = {
    runs: [bridgeRun("candidate", "placement-b")]
  };
  bridgeWithWireDrift.runs[0].requests[1].outbound_prefix_fingerprints = {
    ...bridgeWire,
    instructions: "changed-instructions"
  };
  assert.equal(
    threadStablePromptCacheKeyBridgeWireWitness(
      { runs: [bridgeRun("champion", "placement-a")] },
      bridgeWithWireDrift,
      true,
      128,
      true
    ).pass,
    false,
    "the thread-stable bridge witness must reject any non-PCK final-wire difference"
  );
  assert.equal(
    booleanArg(
      parseArgs(["--require-candidate-options24h-sibling-settle"])[
        "require-candidate-options24h-sibling-settle"
      ]
    ),
    true,
    "the options-24h sibling-settle evidence gate must parse explicitly"
  );
  assert.deepEqual(
    cacheControlFieldEvidence({
      upstream_pool_diagnostic: "probe:cache-options-24h-injected"
    }),
    ["prompt-cache-options"],
    "the 24h diagnostic must still prove its parent cache-control field"
  );
  assert.equal(
    cacheOptions24hEvidence({
      upstream_pool_diagnostic: "probe:cache-options-24h-injected"
    }),
    true,
    "the exact 24h variant must have its own final-wire witness"
  );
  assert.equal(
    cacheOptions24hEvidence({
      upstream_pool_diagnostic: "probe:cache-options-injected"
    }),
    false,
    "the ordinary implicit/30m options probe must not satisfy the 24h witness"
  );
  assert.deepEqual(persistentRuntimeStartOrder("champion"), ["champion", "candidate"]);
  assert.deepEqual(persistentRuntimeStartOrder("candidate"), ["candidate", "champion"]);
  assert.throws(
    () => normalizePersistentRuntimeStartOrder("lane-a"),
    (error) => error instanceof FailClosedError && error.code === "invalid_persistent_runtime_start_order"
  );
  assert.equal(optionalBoolean("true", "--force-use-system-proxy"), true);
  assert.equal(optionalBoolean("false", "--force-use-system-proxy"), false);
  assert.throws(
    () => optionalBoolean("maybe", "--force-use-system-proxy"),
    (error) => error?.code === "invalid_boolean_parameter"
  );
  assert.equal(resolveIncludeToolSchema({}), true);
  const opaquePlacementKey = "release-fixture-placement-secret";
  const opaquePlacementKeyFingerprint = opaquePlacementFingerprint(opaquePlacementKey);
  assert.match(opaquePlacementKeyFingerprint, /^[a-f0-9]{32}$/u);
  assert.notEqual(opaquePlacementKeyFingerprint, opaquePlacementKey);
  const placementDiagnostic = projectDiagnosticRequest({
    provider_prefix_key_present: true,
    provider_prefix_key_fingerprint: opaquePlacementKeyFingerprint
  });
  assert.equal(
    placementDiagnostic.provider_prefix_key_fingerprint,
    opaquePlacementKeyFingerprint,
    "release diagnostics must retain only the one-way native placement fingerprint"
  );
  assert.equal(
    JSON.stringify(placementDiagnostic).includes(opaquePlacementKey),
    false,
    "release diagnostics must never retain a raw prompt-cache placement key"
  );
  assert.equal(
    resolveIncludeToolSchema(parseArgs(["--include-tool-schema"])),
    true,
    "the historical tool-history wire must retain its schema by default"
  );
  assert.equal(
    resolveIncludeToolSchema(parseArgs(["--include-tool-schema=false"])),
    false
  );
  assert.throws(
    () => resolveIncludeToolSchema({ "include-tool-schema": "maybe" }),
    (error) => error?.code === "invalid_boolean_parameter"
  );
  assert.deepEqual(
    evaluateUpstreamUserAgentParity({
      championUpstreamUserAgent: "Atoapi/test",
      candidateUpstreamUserAgent: "Atoapi/test",
      sourceCustomUserAgent: "",
      championExecutableSha256: "a".repeat(64),
      candidateExecutableSha256: "b".repeat(64)
    }),
    { ok: true, mode: "explicit-common" },
    "cross-binary comparisons must accept an explicitly shared User-Agent"
  );
  assert.equal(
    evaluateUpstreamUserAgentParity({
      championUpstreamUserAgent: null,
      candidateUpstreamUserAgent: null,
      sourceCustomUserAgent: "Provider-Compatible/2.0",
      championExecutableSha256: "a".repeat(64),
      candidateExecutableSha256: "b".repeat(64)
    }).mode,
    "source-config-common",
    "a configured Provider User-Agent is shared by both isolated arms"
  );
  assert.equal(
    evaluateUpstreamUserAgentParity({
      championUpstreamUserAgent: null,
      candidateUpstreamUserAgent: null,
      sourceCustomUserAgent: "",
      championExecutableSha256: "a".repeat(64),
      candidateExecutableSha256: "a".repeat(64)
    }).mode,
    "same-binary-default"
  );
  assert.deepEqual(
    evaluateUpstreamUserAgentParity({
      championUpstreamUserAgent: null,
      candidateUpstreamUserAgent: null,
      sourceCustomUserAgent: "",
      championExecutableSha256: "a".repeat(64),
      candidateExecutableSha256: "b".repeat(64)
    }),
    {
      ok: false,
      code: "cross_binary_upstream_user_agent_unproven",
      message: "cross-binary live comparison requires a common --upstream-user-agent when the selected Provider has no custom_user_agent"
    },
    "a cross-binary comparison without a common User-Agent must fail closed"
  );
  assert.deepEqual(
    evaluateUpstreamUserAgentParity({
      championUpstreamUserAgent: "Atoapi/1.4.33",
      candidateUpstreamUserAgent: "Atoapi/1.4.37",
      sourceCustomUserAgent: "",
      championExecutableSha256: "a".repeat(64),
      candidateExecutableSha256: "b".repeat(64)
    }),
    {
      ok: false,
      code: "upstream_user_agent_mismatch",
      message: "champion and candidate must use the same explicit upstream User-Agent"
    },
    "different explicit User-Agents must fail closed"
  );
  assert.deepEqual(
    evaluateUpstreamUserAgentParity({
      championUpstreamUserAgent: null,
      candidateUpstreamUserAgent: "Atoapi-ReleaseChampion-1",
      sourceCustomUserAgent: "",
      championExecutableSha256: "a".repeat(64),
      candidateExecutableSha256: "a".repeat(64),
      diagnosticUserAgentSplit: true
    }),
    { ok: true, mode: "same-binary-diagnostic-split", promotion_eligible: false },
    "same-binary User-Agent split is available only as a non-promotable diagnosis"
  );
  assert.deepEqual(
    evaluateUpstreamUserAgentParity({
      championUpstreamUserAgent: null,
      candidateUpstreamUserAgent: "Atoapi-ReleaseChampion-1",
      sourceCustomUserAgent: "",
      championExecutableSha256: "a".repeat(64),
      candidateExecutableSha256: "b".repeat(64),
      diagnosticUserAgentSplit: true
    }),
    {
      ok: false,
      code: "diagnostic_user_agent_split_requires_same_binary",
      message: "--diagnostic-user-agent-split requires identical champion and candidate executable hashes"
    },
    "a User-Agent split must never turn into a cross-binary release comparison"
  );
  assert.equal(
    extractTomlBoolean(
      replaceProviderTomlBoolean(
        '[[providers]]\nid = "provider-a"\nuse_system_proxy = true\n',
        "provider-a",
        "use_system_proxy",
        false
      ),
      "use_system_proxy"
    ),
    false,
    "a transport diagnostic may rewrite only the copied provider record"
  );
  assert.equal(boundedInteger("60000", "--pair-delay-ms", 0, 60_000), 60_000);
  assert.equal(boundedInteger("1500", "--inter-arm-delay-ms", 0, 5_000), 1_500);
  assert.equal(boundedInteger("1", "--warmup-pairs", 0, 1), 1);
  assert.throws(
    () => boundedInteger("2", "--warmup-pairs", 0, 1),
    (error) => error?.code === "invalid_parameter",
    "warm-up pairs must leave at least one scored pair"
  );
  assert.deepEqual(scheduledPairIds(2), [0, 1]);
  assert.deepEqual(
    partitionRunsByWarmup([{ pair: 0 }, { pair: 1 }, { pair: 2 }], 1),
    { warmup: [{ pair: 0 }], scored: [{ pair: 1 }, { pair: 2 }] },
    "warm-up evidence must stay out of the scored aggregate"
  );
  assert.deepEqual(
    pairedRunIds([{ pair: 2 }, { pair: 1 }], [{ pair: 1 }, { pair: 2 }]),
    [1, 2],
    "scored pair identifiers must contain only complete two-arm pairs"
  );
  assert.equal(
    boundedInteger("2500000", "--seed-context-chars", 0, 2_500_000),
    2_500_000,
    "long-context verification must permit a 500k-token-class seed estimate"
  );
  assert.equal(normalizeFirstArm("candidate"), "candidate");
  assert.throws(
    () => normalizeFirstArm("other"),
    (error) => error?.code === "invalid_first_arm"
  );
  assert.equal(resolveFreshFixturePerPair({}), true);
  assert.equal(resolveFreshFixturePerPair({ "reuse-fixture-across-pairs": true }), false);
  assert.throws(
    () => resolveFreshFixturePerPair({
      "fresh-fixture-per-pair": true,
      "reuse-fixture-across-pairs": true
    }),
    /cannot be used together/u
  );
  assert.equal(cacheableInputTokens128(1_023), 0);
  assert.equal(cacheableInputTokens128(1_024), 1_024);
  assert.equal(cacheableInputTokens128(1_151), 1_024);
  assert.equal(cacheableInputTokens128(1_152), 1_152);
  assert.equal(normalizeScenario("tool_burst"), "tool-burst");
  assert.equal(normalizeScenario("dynamic_tail_mix"), "dynamic-tail-mix");
  assert.equal(normalizeScenario("tool_tail_maturity"), "tool-tail-maturity");
  assert.equal(normalizeScenario("compaction_root"), "compaction-root");
  assert.equal(
    responseErrorKind(
      '{"error":{"type":"atoapi_error","message":"upstream request failed: proxy connect timeout"}}'
    ),
    "upstream_transport",
    "generic Atoapi failures must retain only the safe upstream-transport category"
  );
  assert.equal(
    responseErrorKind(
      '{"error":{"type":"atoapi_error","message":"failed to select provider key: DPAPI unavailable"}}'
    ),
    "provider_key_selection",
    "fresh-runtime key-selection failures must remain distinguishable without retaining their message"
  );
  assert.equal(
    responseErrorKind(
      'event: response.failed\ndata: {"type":"response.failed","response":{"error":{"code":"upstream_sse_error","message":"provider overloaded"}}}\n\n'
    ),
    "upstream_sse_error:capacity",
    "a relayed upstream SSE failure must retain only an allow-listed cause category"
  );
  assert.equal(
    responseErrorKind(
      'event: response.failed\ndata: {"type":"response.failed","response":{"error":{"code":"upstream_sse_error","message":"input token limit exceeded"}}}\n\n'
    ),
    "upstream_sse_error:payload_limit",
    "payload categories must require a real request/context limit signal"
  );
  assert.equal(
    responseErrorKind(
      'event: response.failed\ndata: {"type":"response.failed","response":{"error":{"code":"upstream_sse_error","message":"access token expired"}}}\n\n'
    ),
    "upstream_sse_error:authentication",
    "a bearer-token failure must not be misclassified as a payload limit"
  );
  assert.deepEqual(
    buildResponsesRequestBody({
      cohort: { model: "request-model" },
      maxOutputTokens: 16,
      instructions: "stable",
      input: [],
      reasoningEffort: "max",
      promptCacheKey: "fixture-key"
    }),
    {
      model: "request-model",
      stream: true,
      store: false,
      max_output_tokens: 16,
      instructions: "stable",
      input: [],
      reasoning: { effort: "max" },
      prompt_cache_key: "fixture-key"
    },
    "live champion fixtures must retain Codex's store=false wire contract and an explicit symmetric reasoning effort"
  );
  assert.equal(
    buildResponsesRequestBody({
      cohort: { model: "request-model" },
      maxOutputTokens: 16,
      instructions: "stable",
      input: [],
      previousResponseId: "resp_ephemeral_only"
    }).previous_response_id,
    "resp_ephemeral_only",
    "the local previous response id belongs only on the explicit rebind request body"
  );
  assert.equal(optionalReasoningEffort("MAX"), "max");
  assert.equal(optionalReasoningEffort(""), null);
  assert.throws(
    () => optionalReasoningEffort("turbo"),
    (error) => error instanceof FailClosedError && error.code === "invalid_reasoning_effort"
  );
  const dynamicFixtureTools = releaseFixtureToolsForScenario(
    "dynamic-tail-mix",
    "tool",
    true
  );
  assert.equal(dynamicFixtureTools.length, 1);
  assert.equal(dynamicFixtureTools[0].name, "read_release_fixture");
  assert.deepEqual(
    releaseFixtureToolsForScenario("dynamic-tail-mix"),
    dynamicFixtureTools,
    "tool-history fixtures must retain their historical schema by default"
  );
  assert.deepEqual(releaseFixtureToolsForScenario("full-replay"), []);
  assert.deepEqual(releaseFixtureToolsForScenario("dynamic-tail-mix", "text", true), []);
  assert.deepEqual(
    buildResponsesRequestBody({
      cohort: { model: "request-model" },
      maxOutputTokens: 16,
      instructions: "stable",
      input: [],
      tools: dynamicFixtureTools,
      toolChoice: "none",
      promptCacheKey: "fixture-key"
    }),
    {
      model: "request-model",
      stream: true,
      store: false,
      max_output_tokens: 16,
      instructions: "stable",
      input: [],
      tools: dynamicFixtureTools,
      tool_choice: "none",
      prompt_cache_key: "fixture-key"
    },
    "an explicit tool-schema probe must declare the replayed tool and prevent a new tool call"
  );
  assert.equal(normalizeToolProtocol("CUSTOM"), "custom");
  assert.equal(normalizeToolProtocol("function"), "function");
  assert.equal(normalizeToolProtocol("other"), null);
  const customFixtureTools = releaseFixtureToolsForScenario(
    "dynamic-tail-mix",
    "tool",
    true,
    "custom"
  );
  assert.deepEqual(customFixtureTools, [{
    type: "custom",
    name: "read_release_fixture",
    description: "Read a deterministic release-validation fixture."
  }]);
  assert.deepEqual(
    releaseFixtureToolsForScenario("dynamic-tail-mix", "text", true, "custom"),
    [],
    "custom tool schema is not emitted for text-only tails"
  );
  const customFixtureItems = buildToolFixtureItems({
    pair: 0,
    fixtureFamily: "custom-fixture",
    targetChars: 2048,
    shape: "natural",
    calls: 2,
    toolProtocol: "custom"
  });
  assert.equal(customFixtureItems.length, 4);
  assert.deepEqual(
    customFixtureItems.map((item) => item.type),
    ["custom_tool_call", "custom_tool_call_output", "custom_tool_call", "custom_tool_call_output"]
  );
  assert.equal(typeof customFixtureItems[0].input, "string");
  assert.equal("arguments" in customFixtureItems[0], false);
  assert.equal(
    customFixtureItems[0].call_id,
    customFixtureItems[1].call_id,
    "custom call/output pairs must share their call id"
  );
  assert.equal(
    customFixtureItems[2].call_id,
    customFixtureItems[3].call_id,
    "every custom call/output pair must share its call id"
  );
  const customRebindInput = [
    message("seed"),
    ...customFixtureItems.slice(0, 2),
    message("tail")
  ];
  const customRebound = regenerateClosedToolPairCallIds(customRebindInput, {
    pair: 0,
    fixtureFamily: "custom-fixture",
    turn: 2,
    toolProtocol: "custom"
  });
  assert.equal(customRebound.regeneratedToolPairCount, 1);
  assert.equal(customRebound.input[1].type, "custom_tool_call");
  assert.equal(customRebound.input[2].type, "custom_tool_call_output");
  assert.equal(customRebound.input[1].call_id, customRebound.input[2].call_id);
  assert.equal(customRebound.input[1].input, customRebindInput[1].input);
  assert.equal(customRebound.input[2].output, customRebindInput[2].output);
  assert.notEqual(customRebound.input[1].call_id, customRebindInput[1].call_id);
  assert.equal(
    customRebindInput[1].call_id,
    customFixtureItems[0].call_id,
    "custom rebind must not mutate the caller input"
  );
  assert.throws(
    () => regenerateClosedToolPairCallIds(
      [message("seed"), {
        type: "function_call",
        call_id: "same-id",
        name: "read_release_fixture",
        arguments: "{}"
      }, {
        type: "custom_tool_call_output",
        call_id: "same-id",
        output: "x"
      }],
      { pair: 0, fixtureFamily: "mixed", turn: 1, toolProtocol: "custom" }
    ),
    (error) => error?.code === "local_previous_response_id_tool_protocol_mismatch"
  );
  const rebindSeedItems = buildLocalPreviousResponseIdRebindSeedItems({
    pair: 0,
    fixtureFamily: "fixture"
  });
  assert.equal(localPreviousResponseIdRebindTargetRequestCount(2), 0);
  assert.equal(localPreviousResponseIdRebindTargetRequestCount(3), 1);
  assert.equal(rebindSeedItems.length, 2);
  const rebindFixtureInput = [
    message("seed"),
    ...rebindSeedItems,
    message("tail")
  ];
  const regeneratedFixture = regenerateClosedToolPairCallIds(rebindFixtureInput, {
    pair: 0,
    fixtureFamily: "fixture",
    turn: 1
  });
  assert.equal(regeneratedFixture.regeneratedToolPairCount, 1);
  assert.equal(regeneratedFixture.input[1].name, rebindFixtureInput[1].name);
  assert.equal(regeneratedFixture.input[1].arguments, rebindFixtureInput[1].arguments);
  assert.equal(regeneratedFixture.input[2].output, rebindFixtureInput[2].output);
  assert.equal(regeneratedFixture.input[1].call_id, regeneratedFixture.input[2].call_id);
  assert.notEqual(regeneratedFixture.input[1].call_id, rebindFixtureInput[1].call_id);
  const preservedFixture = preserveClosedToolPairCallIds(rebindFixtureInput);
  assert.equal(preservedFixture.regeneratedToolPairCount, 0);
  assert.notEqual(preservedFixture.input, rebindFixtureInput);
  assert.equal(preservedFixture.input[1].call_id, rebindFixtureInput[1].call_id);
  assert.equal(preservedFixture.input[2].call_id, rebindFixtureInput[2].call_id);
  assert.deepEqual(
    preservedFixture.input,
    rebindFixtureInput,
    "the unchanged FullReplay fixture must clone input without rewriting call ids"
  );
  assert.throws(
    () => regenerateClosedToolPairCallIds(
      [message("seed"), rebindFixtureInput[1]],
      { pair: 0, fixtureFamily: "fixture", turn: 1 }
    ),
    (error) => error?.code === "local_previous_response_id_tool_pair_unclosed"
  );
  assert.equal(
    extractCompletedResponseId(
      'event: response.completed\ndata: {"type":"response.completed","response":{"id":"resp_completed_1"}}\n\n'
    ),
    "resp_completed_1"
  );
  assert.equal(
    extractCompletedResponseId(
      'event: response.created\ndata: {"type":"response.created","response":{"id":"resp_created_1"}}\n\n' +
      'event: response.failed\ndata: {"type":"response.failed","response":{"id":"resp_failed_1"}}\n\n'
    ),
    null,
    "only response.completed may provide the ephemeral local response id"
  );
  assert.equal(
    extractCompletedResponseId(
      'event: response.completed\ndata: {"type":"response.completed","response":{"id":"bad id"}}\n\n'
    ),
    null,
    "malformed response ids must not enter cursor memory"
  );
  const makeRebindRequest = ({ inputFull, inputPrefixes, target }) => ({
    phase: target ? "followup-1" : "seed",
    request_kind: "turn",
    sse_completed: true,
    input_fingerprint: "same-client-input",
    input_tokens: 100,
    local_rebind_target: target,
    local_previous_response_id_sent: target,
    local_response_id_present: true,
    regenerated_tool_pair_count: target ? 1 : 0,
    fixture_tool_protocol: "function",
    outbound_prefix_fingerprints: {
      input_full: inputFull,
      input_prefixes: inputPrefixes,
      instructions: "instructions-fingerprint",
      tools_schema: "tools-fingerprint",
      pre_input_wire: "pre-input-wire-fingerprint"
    }
  });
  const championRebindRuns = [{
    pair: 0,
    requests: [
      makeRebindRequest({ inputFull: "seed-full", inputPrefixes: [], target: false }),
      makeRebindRequest({ inputFull: "champion-target", inputPrefixes: ["other-prefix"], target: true })
    ]
  }];
  const candidateRebindRuns = [{
    pair: 0,
    requests: [
      makeRebindRequest({ inputFull: "seed-full", inputPrefixes: [], target: false }),
      makeRebindRequest({ inputFull: "candidate-target", inputPrefixes: ["seed-full"], target: true })
    ]
  }];
  const rebindSymmetry = pairedRunInputSymmetry(
    championRebindRuns,
    candidateRebindRuns,
    128,
    true,
    {
      exerciseLocalPreviousResponseIdRebind: true,
      expectedLocalPreviousResponseIdRebindRequests: 1
    }
  );
  assert.equal(rebindSymmetry.pass, true);
  assert.equal(rebindSymmetry.local_previous_response_id_rebind.pass, true);
  assert.equal(
    pairedRunInputSymmetry(
      championRebindRuns,
      candidateRebindRuns,
      128,
      true,
      null
    ).pass,
    false,
    "ordinary comparisons must still reject an input_full difference"
  );
  const missingCandidatePrefix = structuredClone(candidateRebindRuns);
  missingCandidatePrefix[0].requests[1].outbound_prefix_fingerprints.input_prefixes = [];
  assert.equal(
    pairedRunInputSymmetry(
      championRebindRuns,
      missingCandidatePrefix,
      128,
      true,
      {
        exerciseLocalPreviousResponseIdRebind: true,
        expectedLocalPreviousResponseIdRebindRequests: 1
      }
    ).local_previous_response_id_rebind.pass,
    false,
    "a rebind witness must fail when the candidate predecessor prefix is absent"
  );
  const makeFullReplayRequest = ({ phase, target = false, material = false, wait = false }) => ({
    phase,
    request_kind: "turn",
    sse_completed: true,
    input_fingerprint: "same-client-input",
    input_tokens: material ? 20_000 : 20_128,
    local_previous_response_id_sent: target,
    local_response_id_present: target || phase === "seed" || phase === "dynamic-tail-1-natural",
    local_rebind_target: false,
    local_full_replay_target: target,
    regenerated_tool_pair_count: 0,
    fixture_tool_protocol: "function",
    tail_source: material ? "tool_output" : null,
    tail_tool_output_chars: material ? 8_192 : 0,
    tail_largest_tool_output_chars: material ? 8_192 : 0,
    prefix_guard_wait_ms: wait ? 250 : 0,
    prefix_guard_wait_reason: wait ? "responses_material_tool_tail_maturity_pending" : null,
    prefix_guard_wait_source: wait ? "exact" : null,
    outbound_prefix_fingerprints: {
      input_full: target ? "full-target" : material ? "full-material" : "full-seed",
      input_prefixes: target ? ["full-material"] : [],
      instructions: "instructions-fingerprint",
      tools_schema: "tools-fingerprint",
      pre_input_wire: "pre-input-wire-fingerprint"
    }
  });
  const fullReplayRequests = [
    makeFullReplayRequest({ phase: "seed" }),
    makeFullReplayRequest({ phase: "dynamic-tail-1-natural", material: true }),
    makeFullReplayRequest({ phase: "followup-2", target: true, wait: true })
  ];
  const fullReplayRuns = [{ pair: 0, requests: fullReplayRequests }];
  const fullReplayWitness = localPreviousResponseIdFullReplayWitness(
    fullReplayRuns,
    structuredClone(fullReplayRuns),
    {
      exerciseLocalPreviousResponseIdFullReplay: true,
      expectedLocalPreviousResponseIdFullReplayRequests: 1,
      expectedToolProtocol: "function"
    }
  );
  assert.equal(fullReplayWitness.pass, true, JSON.stringify(fullReplayWitness));
  assert.equal(fullReplayWitness.regenerated_tool_pairs_absent, true);
  assert.equal(fullReplayWitness.champion_maturity_wait_observed, true);
  assert.equal(fullReplayWitness.candidate_maturity_wait_observed, true);
  const missingFullReplayWait = structuredClone(fullReplayRuns);
  missingFullReplayWait[0].requests[2].prefix_guard_wait_reason = null;
  missingFullReplayWait[0].requests[2].prefix_guard_wait_source = null;
  missingFullReplayWait[0].requests[2].prefix_guard_wait_ms = 0;
  assert.equal(
    localPreviousResponseIdFullReplayWitness(
      fullReplayRuns,
      missingFullReplayWait,
      {
        exerciseLocalPreviousResponseIdFullReplay: true,
        expectedLocalPreviousResponseIdFullReplayRequests: 1,
        expectedToolProtocol: "function"
      }
    ).pass,
    false,
    "an unchanged FullReplay witness must fail without the dedicated material-tail wait"
  );
  const regeneratedFullReplay = structuredClone(fullReplayRuns);
  regeneratedFullReplay[0].requests[2].regenerated_tool_pair_count = 1;
  assert.equal(
    localPreviousResponseIdFullReplayWitness(
      fullReplayRuns,
      regeneratedFullReplay,
      {
        exerciseLocalPreviousResponseIdFullReplay: true,
        expectedLocalPreviousResponseIdFullReplayRequests: 1,
        expectedToolProtocol: "function"
      }
    ).pass,
    false,
    "the unchanged FullReplay witness must reject regenerated call ids"
  );
  assert.deepEqual(
    [1, 3, 5, 7, 9].map((turn) => dynamicTailProfileForTurn(turn, 16_384, 1).shape),
    ["natural", "structured", "noisy", "flat", "natural"]
  );
  assert.equal(dynamicTailProfileForTurn(2, 16_384, 1), null);
  assert.equal(
    dynamicTailProfileForTurn(3, 16_384, 1, "mixed", true),
    null,
    "the late shallow rollback fixture reserves turn three for its quiet delayed direct child"
  );
  assert.deepEqual(
    crossoverPhaseLabels("dynamic-tail-mix", 3, true),
    ["late-shallow-delayed-direct-child"],
    "the late shallow fixture must not label its delayed direct child as a second dynamic tail"
  );
  assert.equal(
    releaseFixtureCallId(1, "fixture", 0, 2, 3),
    "call_release_fixture_fixture_event_3_pair_1_part_1"
  );
  assert.equal(normalizeToolOutputShape("structured"), "structured");
  assert.equal(normalizeToolOutputShape("noisy"), "noisy");
  assert.equal(normalizeToolOutputShape("natural"), "natural");
  assert.equal(normalizeToolOutputShape("natural_dense"), "natural-dense");
  assert.equal(normalizeDynamicTailProfile("natural_dense"), "natural-dense");
  assert.equal(normalizeDynamicTailProfile("unknown"), null);
  assert.equal(normalizeDynamicTailMode("text"), "text");
  assert.equal(normalizeDynamicTailMode("unknown"), null);
  assert.equal(
    buildDynamicTextTail({
      targetChars: 1024,
      shape: "natural-dense",
      fixtureFamily: "fixture",
      eventOrdinal: 1
    }).length,
    1024,
    "text-tail mode must preserve the configured dynamic-tail size"
  );
  assert.equal(normalizeFixtureProfile("natural"), "natural");
  assert.equal(normalizeFixtureProfile("natural_dense"), "natural-dense");
  assert.equal(normalizeFixtureProfile("legacy_repeated"), "legacy-repeated");
  assert.equal(normalizeFixtureProfile("unknown"), null);
  assert.deepEqual(
    transportEvidence({
      upstream_http_version: "HTTP/2.0",
      upstream_network_path: "system-proxy",
      upstream_header_wait_class: "system-proxy:large_body_upload_header_wait",
      request_body_bytes: 1024,
      sent_body_bytes: 512,
      request_body_encode_ms: 1,
      gzip_encode_ms: 2,
      gzip_attempted: true,
      gzip_fallback_used: false,
      upstream_attempt_headers_ms: 3,
      stream_upstream_wait_ms: 4,
      stream_client_backpressure_ms: 5,
      downstream_disconnected: false,
      sse_chunks: 6
    }),
    {
      upstream_http_version: "HTTP/2.0",
      upstream_network_path: "system-proxy",
      upstream_header_wait_class: "system-proxy:large_body_upload_header_wait",
      request_body_bytes: 1024,
      sent_body_bytes: 512,
      request_body_encode_ms: 1,
      gzip_encode_ms: 2,
      gzip_attempted: true,
      gzip_fallback_used: false,
      upstream_attempt_headers_ms: 3,
      stream_upstream_wait_ms: 4,
      stream_client_backpressure_ms: 5,
      downstream_disconnected: false,
      sse_chunks: 6
    },
    "transport evidence must retain only bounded operational metadata"
  );
  assert.equal(
    transportEvidence({ upstream_network_path: "https://proxy.example.invalid/?secret=no" })
      .upstream_network_path,
    null,
    "transport evidence must not retain URLs or query text"
  );
  assert.deepEqual(
    [0, 1, 2, 3].map((turn) => interleavedTurnOrder(0, turn)),
    [
      ["champion", "candidate"],
      ["candidate", "champion"],
      ["champion", "candidate"],
      ["candidate", "champion"]
    ],
    "persistent live comparisons must rotate the first sender every matching turn"
  );
  assert.deepEqual(
    [0, 1, 2, 3, 4, 5, 6].map((turn) =>
      interleavedTurnOrder(0, turn, 0, "champion", "dynamic-tail-mix")
    ),
    [
      ["champion", "candidate"],
      ["champion", "candidate"],
      ["candidate", "champion"],
      ["candidate", "champion"],
      ["champion", "candidate"],
      ["champion", "candidate"],
      ["candidate", "champion"]
    ],
    "dynamic changed tails and their direct follow-ups must not permanently favor opposite arms"
  );
  assert.deepEqual(
    [0, 1, 2, 3, 4, 5, 6].map((turn) =>
      interleavedTurnOrder(1, turn, 0, "champion", "dynamic-tail-mix")
    ),
    [
      ["candidate", "champion"],
      ["candidate", "champion"],
      ["champion", "candidate"],
      ["champion", "candidate"],
      ["candidate", "champion"],
      ["candidate", "champion"],
      ["champion", "candidate"]
    ],
    "the next dynamic pair must reverse the phase-balanced order"
  );
  assert.deepEqual(
    interleavedTurnOrder(1, 0),
    ["candidate", "champion"],
    "the next pair must start from the opposite sender"
  );
  assert.deepEqual(
    interleavedTurnOrder(0, 0, 0, "candidate"),
    ["candidate", "champion"],
    "--first-arm candidate must make the candidate send first"
  );
  assert.deepEqual(
    interleavedTurnOrder(0, 0, 1, "champion"),
    ["candidate", "champion"],
    "--pair-offset 1 must make pair zero start from the candidate"
  );
  assert.deepEqual(
    [0, 1].map((pair) => interleavedTurnOrder(pair, 0, 1, "candidate")),
    [
      ["champion", "candidate"],
      ["candidate", "champion"]
    ],
    "pair offset and first-arm polarity must compose deterministically"
  );
  assert.equal(
    comparisonPairInvalid({ champion: { pass: true }, candidate: { pass: true } }),
    false,
    "a fully valid pair may proceed to the next fresh fixture"
  );
  assert.equal(
    comparisonPairInvalid({ champion: { pass: true }, candidate: { pass: false } }),
    true,
    "one invalid arm must stop a live comparison before it creates more test traffic"
  );
  const failedInboundMetric = { inbound_request_id: "failed-inbound" };
  assert.equal(
    requestLogRows({
      recent_requests: [{ inbound_request_id: "completed-inbound" }],
      recent_failed_requests: [failedInboundMetric]
    }).length,
    2,
    "release evidence must keep terminally failed inbounds visible"
  );
  assert.equal(
    selectNewRequestLog(
      { recent_requests: [], recent_failed_requests: [failedInboundMetric] },
      new Set()
    ),
    failedInboundMetric,
    "a failed inbound must be selected from the failed-request ledger"
  );
  assert.equal(
    responseErrorCode('event: response.failed\ndata: {"code":"upstream_waf_blocked"}\n\n'),
    "upstream_waf_blocked",
    "release evidence must retain only a bounded failure category"
  );
  assert.equal(
    responseErrorCode('event: response.created\ndata: {"type":"response.created"}\n\n'),
    null,
    "ordinary Responses event types must never be misclassified as failures"
  );
  assert.equal(
    responseErrorKind('{"error":{"message":"request payload exceeds the context token limit"}}'),
    "payload_limit",
    "live evidence must classify a payload ceiling without retaining the message"
  );
  assert.equal(
    responseErrorKind('{"error":{"type":"atoapi_error","message":"failed to select provider key for agent-codex-aidns-20"}}'),
    "provider_key_selection",
    "a local provider-key selection failure must not be mislabeled as upstream transport"
  );
  assert.equal(
    responseErrorKind('{"error":{"code":"unsupported_parameter"}}'),
    "code:unsupported_parameter",
    "an opaque upstream code is retained without the error body"
  );
  assert.equal(
    inboundFailureReason({
      transportError: null,
      responseDeadlineExceeded: true,
      responseStatus: 0,
      responseFailureCode: null,
      responseFailed: false,
      checks: { terminal_response_completed: false }
    }),
    "client_deadline_exceeded",
    "a verifier client deadline must remain distinct from a generic transport failure"
  );
  assert.equal(
    classifyDownstreamTransportError(Object.assign(new Error("This operation was aborted"), {
      name: "AbortError"
    })),
    "aborted",
    "an externally classified abort must retain a bounded transport category"
  );
  assert.equal(
    inboundFailureReason({
      transportError: null,
      responseDeadlineExceeded: false,
      responseStatus: 503,
      responseFailureCode: "new_api_error",
      responseFailed: true,
      checks: { terminal_response_completed: false }
    }),
    "http_status_503:new_api_error",
    "an HTTP rejection must not be mislabeled as a missing Responses terminal"
  );
  assert.equal(
    inboundFailureReason({
      transportError: null,
      responseStatus: 200,
      responseFailureCode: "upstream_waf_blocked",
      responseFailed: true,
      checks: { terminal_response_completed: false }
    }),
    "response_failed:upstream_waf_blocked",
    "a native failed terminal must retain its bounded failure category"
  );
  assert.equal(
    inboundFailureReason({
      transportError: null,
      responseStatus: 200,
      responseFailureCode: null,
      responseFailed: false,
      checks: { terminal_response_completed: true }
    }),
    null,
    "a completed Responses stream must remain accepted"
  );
  assert.equal(
    terminalUsageShape(
      'event: response.completed\ndata: {"type":"response.completed","response":{"usage":{"input_tokens":2,"output_tokens":1}}}\n\n'
    ),
    "present",
    "a standard completed usage payload must be recognized"
  );
  assert.equal(
    terminalUsageShape(
      'event: response.completed\ndata: {"type":"response.completed","response":{}}\n\n'
    ),
    "absent",
    "a completed event without usage must be distinguished from a parsed usage"
  );
  assert.equal(
    terminalUsageShape(
      'event: response.completed\ndata: {"type":"response.completed","response":{"usage":{"input_tokens":"2"}}}\n\n'
    ),
    "unrecognized",
    "a usage object with no numeric token fields must be marked unrecognized"
  );
  assert.equal(
    terminalUsageShape('event: response.failed\ndata: {"type":"response.failed"}\n\n'),
    "not_seen",
    "a failed stream must not be reported as a completed usage shape"
  );
  assert.equal(buildSeedContext(128).length, 128);
  const pairFixtureA = "fixture-pair-a";
  const pairFixtureB = "fixture-pair-b";
  const denseSeedA = buildSeedContext(4096, pairFixtureA, "natural-dense");
  const denseSeedB = buildSeedContext(4096, pairFixtureB, "natural-dense");
  assert.notEqual(denseSeedA, denseSeedB, "dense seed fixtures must split fresh pairs");
  const denseSeedStats = fixtureLineStats(denseSeedA);
  assert.equal(
    denseSeedStats.line_count,
    denseSeedStats.unique_line_count,
    "dense seed fixtures must not repeat a complete line"
  );
  assert.equal(denseSeedStats.max_repeated_line_count, 1);
  const denseToolA = buildToolOutput(4096, "natural-dense", pairFixtureA);
  const denseToolB = buildToolOutput(4096, "natural-dense", pairFixtureB);
  assert.notEqual(denseToolA, denseToolB, "dense fixtures must split fresh pairs");
  const denseStats = fixtureLineStats(denseToolA);
  assert.equal(
    denseStats.line_count,
    denseStats.unique_line_count,
    "dense fixtures must not repeat a complete tool-output line"
  );
  assert.equal(denseStats.max_repeated_line_count, 1);
  assert.equal(
    dynamicTailProfileForTurn(1, 16_384, 1, "natural-dense").shape,
    "natural-dense"
  );
  assert.equal(
    dynamicTailProfileForTurn(2, 16_384, 1, "natural-dense"),
    null
  );
  const naturalFixture = buildStableInstructions(32_768, pairFixtureA, "natural");
  const naturalFixtureStats = fixtureLineStats(naturalFixture);
  assert.equal(naturalFixture.length, 32_768);
  assert.equal(
    naturalFixtureStats.line_count,
    naturalFixtureStats.unique_line_count,
    "the default fixture must not contain repeated full lines"
  );
  assert.equal(
    naturalFixtureStats.max_repeated_line_count,
    1,
    "the default fixture must avoid a repeated-line WAF signature"
  );
  assert.notEqual(
    buildStableInstructions(4096, pairFixtureA, "natural"),
    buildStableInstructions(4096, pairFixtureB, "natural"),
    "fresh fixtures must remain pair-specific in the natural profile"
  );
  assert.equal(
    buildStableInstructions(4096, pairFixtureA, "natural"),
    buildStableInstructions(4096, pairFixtureA, "natural"),
    "the natural fixture must remain byte-stable within a pair"
  );
  for (const shape of ["natural", "flat", "structured", "noisy"]) {
    assert.equal(buildToolOutput(4096, shape).length, 4096);
  }
  const naturalToolStats = fixtureLineStats(buildToolOutput(16_384, "natural", pairFixtureA));
  assert.equal(
    naturalToolStats.line_count,
    naturalToolStats.unique_line_count,
    "the default tool fixture must not contain repeated full lines"
  );
  assert.equal(releaseFixtureCallId(2), "call_release_fixture_pair_2");
  assert.equal(
    buildSeedContext(128, pairFixtureA),
    buildSeedContext(128, pairFixtureA),
    "both arms in one pair must receive identical seed context"
  );
  assert.notEqual(
    buildSeedContext(128, pairFixtureA),
    buildSeedContext(128, pairFixtureB),
    "fresh fixtures must split seed context across pairs"
  );
  assert.equal(
    buildStableInstructions(1024, pairFixtureA),
    buildStableInstructions(1024, pairFixtureA),
    "both arms in one pair must receive identical stable instructions"
  );
  assert.notEqual(
    buildStableInstructions(1024, pairFixtureA),
    buildStableInstructions(1024, pairFixtureB),
    "fresh fixtures must split stable instructions across pairs"
  );
  for (const shape of ["natural", "flat", "structured", "noisy"]) {
    assert.equal(
      buildToolOutput(4096, shape, pairFixtureA),
      buildToolOutput(4096, shape, pairFixtureA),
      `both arms in one pair must receive identical ${shape} tool output`
    );
    assert.notEqual(
      buildToolOutput(4096, shape, pairFixtureA),
      buildToolOutput(4096, shape, pairFixtureB),
      `fresh fixtures must split ${shape} tool output across pairs`
    );
  }
  assert.equal(
    releaseFixtureCallId(2, pairFixtureA),
    releaseFixtureCallId(2, pairFixtureA),
    "both arms in one pair must use the same tool call id"
  );
  assert.notEqual(
    releaseFixtureCallId(2, pairFixtureA),
    releaseFixtureCallId(3, pairFixtureB),
    "fresh fixtures must split tool call ids across pairs"
  );
  const multiToolItems = buildToolFixtureItems({
    pair: 2,
    fixtureFamily: pairFixtureA,
    targetChars: 4096,
    shape: "noisy",
    calls: 4
  });
  assert.equal(multiToolItems.length, 8);
  const multiToolOutputs = multiToolItems
    .filter((item) => item.type === "function_call_output")
    .map((item) => item.output);
  assert.equal(multiToolOutputs.reduce((sum, output) => sum + output.length, 0), 4096);
  assert.equal(new Set(multiToolItems.map((item) => item.call_id)).size, 4);
  const multiCustomToolItems = buildToolFixtureItems({
    pair: 2,
    fixtureFamily: pairFixtureA,
    targetChars: 4096,
    shape: "noisy",
    calls: 4,
    toolProtocol: "custom"
  });
  assert.equal(multiCustomToolItems.length, 8);
  assert.equal(
    multiCustomToolItems.filter((item) => item.type === "custom_tool_call").length,
    4
  );
  assert.equal(
    multiCustomToolItems.filter((item) => item.type === "custom_tool_call_output").length,
    4
  );
  assert.equal(
    multiCustomToolItems
      .filter((item) => item.type === "custom_tool_call_output")
      .reduce((sum, item) => sum + item.output.length, 0),
    4096
  );
  assert.equal(
    multiCustomToolItems.some((item) => Object.hasOwn(item, "arguments")),
    false,
    "custom fixture items must never carry function-call arguments"
  );
  assert.equal(effectiveReuseRuntimePerArm(true, false), true);
  assert.equal(effectiveReuseRuntimePerArm(true, true), false);
  assert.equal(effectiveReuseRuntimePerArm(false, true), false);
  assert.equal(isolationLaneForPair(0, "champion"), "lane-a");
  assert.equal(isolationLaneForPair(0, "candidate"), "lane-b");
  assert.equal(isolationLaneForPair(1, "champion"), "lane-b");
  assert.equal(isolationLaneForPair(1, "candidate"), "lane-a");
  const laneAOnChampionPair0 = releaseCachePlacementLane({
    runId: "self-test-run",
    keyRealmHash: "opaque-realm-1234",
    requestFamily: "codex-responses-full-replay",
    pair: 0,
    arm: "champion",
    isolationLane: isolationLaneForPair(0, "champion"),
    isolateUpstreamCache: true
  });
  const laneAOnCandidatePair1 = releaseCachePlacementLane({
    runId: "self-test-run",
    keyRealmHash: "opaque-realm-1234",
    requestFamily: "codex-responses-full-replay",
    pair: 1,
    arm: "candidate",
    isolationLane: isolationLaneForPair(1, "candidate"),
    isolateUpstreamCache: true
  });
  const laneBOnCandidatePair0 = releaseCachePlacementLane({
    runId: "self-test-run",
    keyRealmHash: "opaque-realm-1234",
    requestFamily: "codex-responses-full-replay",
    pair: 0,
    arm: "candidate",
    isolationLane: isolationLaneForPair(0, "candidate"),
    isolateUpstreamCache: true
  });
  assert.equal(laneAOnChampionPair0, laneAOnCandidatePair1);
  assert.equal(
    generatedPromptCacheKey("self-test", laneAOnChampionPair0),
    generatedPromptCacheKey("self-test", laneAOnCandidatePair1),
    "the crossed-over arm must retain the same isolated lane placement key after warm-up"
  );
  assert.notEqual(
    generatedPromptCacheKey("self-test", laneAOnChampionPair0),
    generatedPromptCacheKey("self-test", laneBOnCandidatePair0),
    "the two arms in one pair must retain distinct placement lanes"
  );
  const sharedCrossoverChampion = releaseCachePlacementLane({
    runId: "self-test-run",
    keyRealmHash: "opaque-realm-1234",
    requestFamily: "codex-responses-full-replay",
    pair: 0,
    arm: "champion",
    isolationLane: null,
    isolateUpstreamCache: false,
    sharedCacheCrossover: true
  });
  const sharedCrossoverCandidate = releaseCachePlacementLane({
    runId: "self-test-run",
    keyRealmHash: "opaque-realm-1234",
    requestFamily: "codex-responses-full-replay",
    pair: 0,
    arm: "candidate",
    isolationLane: null,
    isolateUpstreamCache: false,
    sharedCacheCrossover: true
  });
  assert.equal(
    sharedCrossoverChampion,
    sharedCrossoverCandidate,
    "shared-cache crossover must give both arms one cache placement key per pair"
  );
  assert.deepEqual(
    releaseFixtureConversationIdentity(2, pairFixtureA),
    releaseFixtureConversationIdentity(2, pairFixtureA),
    "both arms in one pair must replay the same logical conversation identity"
  );
  assert.notDeepEqual(
    releaseFixtureConversationIdentity(2, pairFixtureA),
    releaseFixtureConversationIdentity(3, pairFixtureB),
    "fresh fixtures must isolate conversation identities across pairs"
  );
  assert.notDeepEqual(
    releaseFixtureConversationIdentity(2, pairFixtureA, "champion"),
    releaseFixtureConversationIdentity(2, pairFixtureA, "candidate"),
    "isolated cache lanes must not share a generated placement identity"
  );
  const churnBaseIdentity = releaseFixtureTurnIdentity({
    pair: 2,
    fixtureFamily: pairFixtureA,
    isolationLane: "lane-a",
    turn: 0,
    scenario: "dynamic-tail-mix",
    identityChurn: true
  });
  const churnRotatedIdentity = releaseFixtureTurnIdentity({
    pair: 2,
    fixtureFamily: pairFixtureA,
    isolationLane: "lane-a",
    turn: 1,
    scenario: "dynamic-tail-mix",
    identityChurn: true
  });
  assert.equal(churnBaseIdentity.phase, "base");
  assert.equal(churnRotatedIdentity.phase, "rotated");
  assert.equal(churnBaseIdentity.thread_id, churnRotatedIdentity.thread_id);
  assert.notEqual(churnBaseIdentity.session_id, churnRotatedIdentity.session_id);
  assert.notEqual(churnBaseIdentity.conversation_id, churnRotatedIdentity.conversation_id);
  const noChurnIdentity = releaseFixtureTurnIdentity({
    pair: 2,
    fixtureFamily: pairFixtureA,
    isolationLane: "lane-a",
    turn: 1,
    scenario: "dynamic-tail-mix",
    identityChurn: false
  });
  assert.equal(noChurnIdentity.phase, "base");
  assert.equal(noChurnIdentity.session_id, churnBaseIdentity.session_id);
  assert.equal(noChurnIdentity.thread_id, churnBaseIdentity.thread_id);
  assert.equal(noChurnIdentity.conversation_id, null);
  const stableWire = {
    cache_metadata: "sha256-128:cache-a",
    input_full: "sha256-128:input-a",
    instructions: "sha256-128:instructions-a",
    tools_schema: "sha256-128:tools-a",
    pre_input_wire: "sha256-128:pre-a"
  };
  const changedEpochWire = {
    cache_metadata: "sha256-128:cache-b",
    instructions: "sha256-128:instructions-b",
    tools_schema: "sha256-128:tools-a",
    pre_input_wire: "sha256-128:pre-b"
  };
  assert.equal(
    staticWireContinuity([
      { phase: "seed", request_kind: "turn", outbound_prefix_fingerprints: stableWire },
      { phase: "followup-1", request_kind: "turn", outbound_prefix_fingerprints: stableWire }
    ]).pass,
    true,
    "ordinary full replay must keep its static wire stable"
  );
  assert.equal(
    staticWireContinuity([
      { phase: "seed", request_kind: "turn", outbound_prefix_fingerprints: stableWire },
      { phase: "compaction", request_kind: "compaction", outbound_prefix_fingerprints: changedEpochWire },
      { phase: "followup-1", request_kind: "turn", outbound_prefix_fingerprints: changedEpochWire }
    ]).pass,
    true,
    "compaction is a legal static-wire epoch boundary"
  );
  assert.equal(
    staticWireContinuity([
      { phase: "seed", request_kind: "turn", outbound_prefix_fingerprints: stableWire },
      {
        phase: "followup-1",
        request_kind: "turn",
        outbound_prefix_fingerprints: { ...stableWire, tools_schema: "sha256-128:tools-drift" }
      }
    ]).pass,
    false,
    "ordinary static-wire drift must fail closed"
  );
  const keyPoolToml = [
    '[[provider_key_pools]]',
    'provider_id = "provider-a"',
    'enabled = true',
    '[[provider_key_pools.keys]]',
    'id = "key-a"',
    'key_encrypted = "encrypted-a"',
    'enabled = true',
    '[[provider_key_pools.keys]]',
    'id = "key-b"',
    'key_encrypted = "encrypted-b"',
    'enabled = false',
    'disabled_until = 2099-01-01T00:00:00Z',
    ''
  ].join("\n");
  assert.throws(
    () => validatePinnedKeyConfiguration(keyPoolToml, "provider-a", null),
    (error) => error?.code === "key_pin_required_for_pool"
  );
  assert.throws(
    () => pinProviderKeyInToml(keyPoolToml, "provider-a", "key-b"),
    (error) => error?.code === "pinned_key_unavailable",
    "an isolated verifier must never revive a disabled/cooling Key"
  );
  const pinnedKeyPoolToml = pinProviderKeyInToml(keyPoolToml, "provider-a", "key-a");
  const pinnedContext = providerKeyPoolContext(pinnedKeyPoolToml, "provider-a");
  assert.equal(extractTomlBoolean(pinnedContext.keys[0].body, "enabled"), true);
  assert.equal(extractTomlBoolean(pinnedContext.keys[1].body, "enabled"), false);
  assert.equal(extractTomlValuePresent(pinnedContext.keys[1].body, "disabled_until"), true);
  assert.equal(requestFamilyForScenario("full-replay"), "codex-responses-full-replay");
  const cohort = {
    provider_id: "provider-a",
    model: "gpt-test",
    key_realm_hash: "opaque-realm-1234",
    request_family: "codex-responses-full-replay"
  };
  const valid = (arm, raw) => ({
    schema: SCHEMA,
    kind: "dynamic-arm-aggregate",
    pass: true,
    arm,
    cohort,
    executable: { path: `${arm}.exe`, sha256: "a".repeat(64) },
    runs: [{
      schema: SCHEMA,
      kind: "dynamic-run",
      pass: true,
      arm,
      cohort,
      scenario: "full-replay",
      pair: 0,
      requests: [{
        phase: "seed",
        request_kind: "turn",
        sse_completed: true,
        input_fingerprint: "self-test-valid-seed",
        input_tokens: 1024,
        cache_read_tokens: 0,
        cache_read_tokens_observed: true,
        outbound_prefix_fingerprints: stableWire
      }]
    }],
    metrics: {
      requests: 4,
      successful_sse_requests: 4,
      input_tokens: 4096,
      warm_input_tokens: 3072,
      seed_input_tokens: 1024,
      seed_cache_read_tokens: 0,
      seed_request_count: 1,
      cold_seed_request_count: 1,
      cache_read_tokens: raw * 4096,
      raw_token_hit_rate: raw,
      warm_cache_read_tokens: raw * 3072,
      warm_raw_token_hit_rate: raw,
      cacheable_tokens_128: 4096,
      cacheable_read_tokens_128: raw * 4096,
      cache_128_hit_rate: raw,
      warm_cacheable_tokens_128: 3072,
      warm_cacheable_read_tokens_128: raw * 3072,
      warm_cache_128_hit_rate: raw,
      warm_stable_prefix_tokens_128: 3072,
      warm_stable_prefix_cached_tokens_128: raw * 3072,
      warm_stable_prefix_hit_rate: raw,
      full_bucket_requests: raw === 1 ? 4 : 3,
      full_bucket_rate: raw === 1 ? 1 : 0.75,
      warm_full_bucket_requests: raw === 1 ? 3 : 2,
      warm_full_bucket_rate: raw === 1 ? 1 : 2 / 3,
      warm_full_bucket_denominator: 3,
      cacheable_request_count: 4,
      full_bucket_denominator: 4,
      avoidable_gap_tokens: 0,
      new_tail_gap_tokens: 0,
      provider_unstable_gap_tokens: 0,
      shortfall_tokens: 0,
      local_pre_upstream_overhead_p95_ms: 0,
      local_proxy_overhead_p95_ms: 0,
      upstream_ttft_p95_ms: 100,
      ttft_p95_ms: 100,
      usage_coverage: 1,
      observed_realm_ids: ["observed-realm"]
    },
    checks: {
      every_inbound_one_attempt_one_main_post: true,
      static_wire_continuity: true
    }
  });
  const withNativePlacement = (aggregate, fingerprints) => ({
    ...aggregate,
    runs: aggregate.runs.map((run) => ({
      ...run,
      requests: [
        {
          ...run.requests[0],
          provider_prefix_key_present: true,
          provider_prefix_key_fingerprint: fingerprints[0]
        },
        {
          ...run.requests[0],
          phase: "followup-2",
          input_fingerprint: "self-test-valid-followup",
          provider_prefix_key_present: true,
          provider_prefix_key_fingerprint: fingerprints[1] ?? fingerprints[0]
        }
      ]
    }))
  });
  const siblingSettleAggregate = aggregateArm(
    "candidate",
    cohort,
    valid("candidate", 0.95).executable,
    [{
      ...valid("candidate", 0.95).runs[0],
      metrics: {
        ...valid("candidate", 0.95).metrics,
        candidate_options24h_sibling_settle_requests: 1
      },
      checks: { ...valid("candidate", 0.95).checks }
    }]
  );
  assert.equal(
    siblingSettleAggregate.metrics.candidate_options24h_sibling_settle_requests,
    1,
    "candidate arm aggregation must retain the observed options-24h sibling settle"
  );
  const unInjectedTreatmentCandidate = {
    ...valid("candidate", 0.95),
    checks: {
      ...valid("candidate", 0.95).checks,
      candidate_cache_control_field_injected: false,
      candidate_cache_options_24h_injected: false
    }
  };
  const unInjectedTreatmentVerdict = compareArmResults(
    valid("champion", 0.9),
    unInjectedTreatmentCandidate,
    0,
    0,
    0,
    false,
    128,
    false,
    {
      candidate_cache_control_field: "prompt-cache-options",
      candidate_cache_options_24h: true
    }
  );
  assert.equal(unInjectedTreatmentVerdict.effect_evaluable, false);
  assert.equal(unInjectedTreatmentVerdict.effect_evaluation_reason, "candidate_treatment_not_injected");
  assert.equal(unInjectedTreatmentVerdict.positive_cache_evidence, false);
  assert.equal(unInjectedTreatmentVerdict.deltas.warm_cache_128_hit_rate, null);
  const noOpPromptCacheKeyCandidate = {
    ...valid("candidate", 0.95),
    runs: valid("candidate", 0.95).runs.map((run) => ({
      ...run,
      requests: run.requests.map((request) => ({
        ...request,
        // The treatment receipt says "injected", but the final wire is
        // intentionally identical to the champion's wire. This must fail
        // closed as a no-op rather than pass as PCK evidence.
        cache_control_fields: ["prompt-cache-key"],
        outbound_prefix_fingerprints: stableWire
      }))
    })),
    checks: {
      ...valid("candidate", 0.95).checks,
      candidate_cache_control_field_injected: true
    }
  };
  const noOpPromptCacheKeyVerdict = compareArmResults(
    valid("champion", 0.9),
    noOpPromptCacheKeyCandidate,
    0,
    0,
    0,
    false,
    128,
    false,
    { candidate_cache_control_field: "prompt-cache-key" }
  );
  assert.equal(
    noOpPromptCacheKeyVerdict.actual_outbound_input_symmetry
      .declared_cache_control_difference.pass,
    false,
    "a declared PCK treatment with no final-wire difference must fail closed"
  );
  const pckWireDifferenceChampion = {
    ...valid("champion", 0.9),
    runs: valid("champion", 0.9).runs.map((run) => ({
      ...run,
      requests: run.requests.map((request) => ({
        ...request,
        provider_prefix_key_fingerprint: "key-a"
      }))
    }))
  };
  const pckWireDifferenceCandidate = {
    ...noOpPromptCacheKeyCandidate,
    runs: noOpPromptCacheKeyCandidate.runs.map((run) => ({
      ...run,
      requests: run.requests.map((request) => ({
        ...request,
        provider_prefix_key_fingerprint: "key-b"
      }))
    }))
  };
  const pckWireDifferenceVerdict = compareArmResults(
    pckWireDifferenceChampion,
    pckWireDifferenceCandidate,
    0,
    0,
    0,
    false,
    128,
    false,
    { candidate_cache_control_field: "prompt-cache-key" }
  );
  assert.equal(
    pckWireDifferenceVerdict.actual_outbound_input_symmetry
      .declared_cache_control_difference.pass,
    true,
    "a PCK-only provider-prefix difference must count as attributable treatment evidence"
  );
  const missingExactMediumWaitCandidate = {
    ...valid("candidate", 0.95),
    checks: {
      ...valid("candidate", 0.95).checks,
      candidate_exact_medium_tool_tail_maturity_wait_observed: false
    }
  };
  const missingExactMediumWaitVerdict = compareArmResults(
    valid("champion", 0.9),
    missingExactMediumWaitCandidate,
    0,
    0,
    0,
    false,
    128,
    false,
    { require_candidate_exact_medium_tool_tail_maturity_wait: true }
  );
  assert.equal(
    missingExactMediumWaitVerdict.effect_evaluation_reason,
    "candidate_exact_medium_tool_tail_maturity_wait_not_observed"
  );
  assert.equal(
    missingExactMediumWaitVerdict.checks.candidate_exact_medium_tool_tail_maturity_wait_observed,
    false
  );
  assert.equal(missingExactMediumWaitVerdict.baseline_pass, false);
  const missingExactLargeTailLagCandidate = {
    ...valid("candidate", 0.95),
    checks: {
      ...valid("candidate", 0.95).checks,
      candidate_exact_large_message_tail_lag_observed: false
    }
  };
  const missingExactLargeTailLagVerdict = compareArmResults(
    valid("champion", 0.9),
    missingExactLargeTailLagCandidate,
    0,
    0,
    0,
    false,
    128,
    false,
    { require_candidate_exact_large_message_tail_lag: true }
  );
  assert.equal(
    missingExactLargeTailLagVerdict.effect_evaluation_reason,
    "candidate_exact_large_message_tail_lag_not_observed"
  );
  assert.equal(
    missingExactLargeTailLagVerdict.checks.candidate_exact_large_message_tail_lag_observed,
    false
  );
  assert.equal(missingExactLargeTailLagVerdict.baseline_pass, false);
  const observedExactLargeTailLagCandidate = {
    ...valid("candidate", 0.95),
    checks: {
      ...valid("candidate", 0.95).checks,
      candidate_exact_large_message_tail_lag_observed: true
    }
  };
  const observedExactLargeTailLagVerdict = compareArmResults(
    valid("champion", 0.9),
    observedExactLargeTailLagCandidate,
    0,
    0,
    0,
    false,
    128,
    false,
    { require_candidate_exact_large_message_tail_lag: true }
  );
  assert.equal(observedExactLargeTailLagVerdict.effect_evaluable, true);
  assert.equal(
    observedExactLargeTailLagVerdict.checks.candidate_exact_large_message_tail_lag_observed,
    true
  );
  const missingLateShallowWaterlineCandidate = {
    ...valid("candidate", 0.95),
    checks: {
      ...valid("candidate", 0.95).checks,
      candidate_late_shallow_provider_waterline_rollback_wait_observed: false
    }
  };
  const missingLateShallowWaterlineVerdict = compareArmResults(
    valid("champion", 0.9),
    missingLateShallowWaterlineCandidate,
    0,
    0,
    0,
    false,
    128,
    false,
    { require_candidate_late_shallow_provider_waterline_rollback_wait: true }
  );
  assert.equal(
    missingLateShallowWaterlineVerdict.effect_evaluation_reason,
    "candidate_late_shallow_provider_waterline_rollback_wait_not_observed"
  );
  assert.equal(
    missingLateShallowWaterlineVerdict.checks
      .candidate_late_shallow_provider_waterline_rollback_wait_observed,
    false
  );
  assert.equal(missingLateShallowWaterlineVerdict.baseline_pass, false);
  const observedLateShallowWaterlineCandidate = {
    ...valid("candidate", 0.95),
    checks: {
      ...valid("candidate", 0.95).checks,
      candidate_late_shallow_provider_waterline_rollback_wait_observed: true
    }
  };
  const observedLateShallowWaterlineVerdict = compareArmResults(
    valid("champion", 0.9),
    observedLateShallowWaterlineCandidate,
    0,
    0,
    0,
    false,
    128,
    false,
    { require_candidate_late_shallow_provider_waterline_rollback_wait: true }
  );
  assert.equal(observedLateShallowWaterlineVerdict.effect_evaluable, true);
  assert.equal(
    observedLateShallowWaterlineVerdict.checks
      .candidate_late_shallow_provider_waterline_rollback_wait_observed,
    true
  );
  const missingOptions24hSiblingSettleVerdict = compareArmResults(
    valid("champion", 0.9),
    valid("candidate", 0.95),
    0,
    0,
    0,
    false,
    128,
    false,
    { require_candidate_options24h_sibling_settle: true }
  );
  assert.equal(
    missingOptions24hSiblingSettleVerdict.effect_evaluation_reason,
    "candidate_options24h_sibling_settle_not_observed"
  );
  assert.equal(
    missingOptions24hSiblingSettleVerdict.checks.candidate_options24h_sibling_settle_observed,
    false
  );
  const observedOptions24hSiblingSettleCandidate = {
    ...valid("candidate", 0.95),
    metrics: {
      ...valid("candidate", 0.95).metrics,
      candidate_options24h_sibling_settle_requests: 1
    }
  };
  const observedOptions24hSiblingSettleVerdict = compareArmResults(
    valid("champion", 0.9),
    observedOptions24hSiblingSettleCandidate,
    0,
    0,
    0,
    false,
    128,
    false,
    { require_candidate_options24h_sibling_settle: true }
  );
  assert.equal(observedOptions24hSiblingSettleVerdict.effect_evaluable, true);
  assert.equal(
    observedOptions24hSiblingSettleVerdict.checks.candidate_options24h_sibling_settle_observed,
    true
  );
  const nativePlacementChampion = withNativePlacement(
    valid("champion", 0.9),
    ["a".repeat(32), "a".repeat(32)]
  );
  const nativePlacementCandidate = withNativePlacement(
    valid("candidate", 0.9),
    ["b".repeat(32), "b".repeat(32)]
  );
  const nativePlacementVerdict = compareArmResults(
    nativePlacementChampion,
    nativePlacementCandidate,
    0,
    0,
    0,
    true,
    128,
    true
  );
  assert.equal(nativePlacementVerdict.baseline_pass, true);
  assert.equal(nativePlacementVerdict.checks.native_placement_isolation, true);
  assert.equal(nativePlacementVerdict.native_placement_isolation.required, true);
  assert.equal(nativePlacementVerdict.native_placement_isolation.pair_count, 1);
  assert.equal(nativePlacementVerdict.native_placement_isolation.fingerprints_present, true);
  assert.equal(nativePlacementVerdict.native_placement_isolation.fingerprints_stable, true);
  assert.equal(nativePlacementVerdict.native_placement_isolation.arms_differ, true);
  const missingNativePlacementCandidate = {
    ...nativePlacementCandidate,
    runs: nativePlacementCandidate.runs.map((run) => ({
      ...run,
      requests: run.requests.map(({ provider_prefix_key_present, provider_prefix_key_fingerprint, ...request }) => request)
    }))
  };
  const missingNativePlacementVerdict = compareArmResults(
    nativePlacementChampion,
    missingNativePlacementCandidate,
    0,
    0,
    0,
    true,
    128,
    true
  );
  assert.equal(missingNativePlacementVerdict.checks.native_placement_isolation, false);
  assert.equal(missingNativePlacementVerdict.baseline_pass, false);
  const unstableNativePlacementCandidate = withNativePlacement(
    valid("candidate", 0.9),
    ["b".repeat(32), "c".repeat(32)]
  );
  const unstableNativePlacementVerdict = compareArmResults(
    nativePlacementChampion,
    unstableNativePlacementCandidate,
    0,
    0,
    0,
    true,
    128,
    true
  );
  assert.equal(unstableNativePlacementVerdict.checks.native_placement_isolation, false);
  assert.equal(unstableNativePlacementVerdict.native_placement_isolation.fingerprints_stable, false);
  const sharedNativePlacementCandidate = withNativePlacement(
    valid("candidate", 0.9),
    ["a".repeat(32), "a".repeat(32)]
  );
  const sharedNativePlacementVerdict = compareArmResults(
    nativePlacementChampion,
    sharedNativePlacementCandidate,
    0,
    0,
    0,
    true,
    128,
    true
  );
  assert.equal(sharedNativePlacementVerdict.checks.native_placement_isolation, false);
  assert.equal(sharedNativePlacementVerdict.native_placement_isolation.arms_differ, false);
  const offlineNativePlacementCompatibility = compareArmResults(
    valid("champion", 0.9),
    valid("candidate", 0.9),
    0
  );
  assert.equal(offlineNativePlacementCompatibility.native_placement_isolation.required, false);
  assert.equal(offlineNativePlacementCompatibility.checks.native_placement_isolation, true);
  const isolatedPlacementPromotionVerdict = compareArmResults(
    valid("champion", 0.9),
    valid("candidate", 0.9),
    0,
    0,
    0,
    true,
    128,
    false,
    {
      require_shared_upstream_placement_crossover: true,
      shared_upstream_placement_crossover_observed: false
    }
  );
  assert.equal(isolatedPlacementPromotionVerdict.baseline_pass, false);
  assert.equal(isolatedPlacementPromotionVerdict.checks.upstream_placement_crossover, false);
  assert.equal(
    isolatedPlacementPromotionVerdict.upstream_placement_crossover.reason,
    "shared_turn_crossover_required_for_live_promotion"
  );
  const sharedPlacementPromotionVerdict = compareArmResults(
    valid("champion", 0.9),
    valid("candidate", 0.9),
    0,
    0,
    0,
    true,
    128,
    false,
    {
      require_shared_upstream_placement_crossover: true,
      shared_upstream_placement_crossover_observed: true
    }
  );
  assert.equal(sharedPlacementPromotionVerdict.baseline_pass, true);
  assert.equal(sharedPlacementPromotionVerdict.checks.upstream_placement_crossover, true);
  const onePairSharedCrossoverPromotionVerdict = compareArmResults(
    valid("champion", 0.9),
    valid("candidate", 0.9),
    0,
    0,
    0,
    true,
    128,
    false,
    {
      require_shared_upstream_placement_crossover: true,
      shared_upstream_placement_crossover_observed: true,
      shared_turn_crossover: {
        required: true,
        observed: true,
        scenario: "dynamic-tail-mix",
        turns: 3,
        scored_pair_ids: [0],
        turn_orders: [
          [["champion", "candidate"], ["champion", "candidate"], ["candidate", "champion"]]
        ],
        champion_runs: valid("champion", 0.9).runs,
        candidate_runs: valid("candidate", 0.9).runs
      }
    }
  );
  assert.equal(onePairSharedCrossoverPromotionVerdict.promotion_eligible, false);
  assert.equal(onePairSharedCrossoverPromotionVerdict.baseline_pass, false);
  assert.equal(
    onePairSharedCrossoverPromotionVerdict.shared_turn_crossover.reason,
    "requires_at_least_two_complete_scored_pairs"
  );
  const twoPairCrossoverRuns = (arm) => [0, 1].map((pair) => ({
    ...valid(arm, 0.9).runs[0],
    pair
  }));
  const balancedSharedCrossoverPromotionVerdict = compareArmResults(
    { ...valid("champion", 0.9), runs: twoPairCrossoverRuns("champion") },
    { ...valid("candidate", 0.9), runs: twoPairCrossoverRuns("candidate") },
    0,
    0,
    0,
    true,
    128,
    false,
    {
      require_shared_upstream_placement_crossover: true,
      shared_upstream_placement_crossover_observed: true,
      shared_turn_crossover: {
        required: true,
        observed: true,
        scenario: "dynamic-tail-mix",
        turns: 3,
        scored_pair_ids: [0, 1],
        turn_orders: [
          [["champion", "candidate"], ["champion", "candidate"], ["candidate", "champion"]],
          [["candidate", "champion"], ["candidate", "champion"], ["champion", "candidate"]]
        ],
        champion_runs: twoPairCrossoverRuns("champion"),
        candidate_runs: twoPairCrossoverRuns("candidate")
      }
    }
  );
  assert.equal(balancedSharedCrossoverPromotionVerdict.promotion_eligible, true);
  assert.equal(balancedSharedCrossoverPromotionVerdict.checks.shared_turn_crossover_balanced, true);
  const failedZeroUsageRealmRun = (arm) => buildDynamicRun({
    arm,
    pair: 0,
    cohort,
    executable: valid(arm, 0.9).executable,
    scenario: "full-replay",
    promptCacheKeyUsed: false,
    dynamicTailEvents: [],
    minimumGuardedRequests: 0,
    minimumSeedInputTokens: 0,
    minimumPeakInputTokens: 0,
    maximumPeakInputTokens: 0,
    requests: [{
      phase: "seed",
      request_kind: "turn",
      sse_completed: false,
      input_fingerprint: "self-test-zero-usage-failure",
      input_tokens: 0,
      cache_read_tokens: 0,
      cache_read_tokens_observed: true,
      observed_realm_id: cohort.key_realm_hash,
      outbound_prefix_fingerprints: stableWire,
      checks: {
        per_inbound_one_attempt_one_post: true,
        exact_counter_delta: true,
        provider_matches_cohort: true,
        model_matches_cohort: true,
        observed_key_realm_matches_cohort: true
      }
    }],
    fatal: "upstream_sse_error",
    compactionSeen: false
  });
  const zeroUsageRealmRun = failedZeroUsageRealmRun("champion");
  assert.deepEqual(
    zeroUsageRealmRun.metrics.observed_realm_ids,
    [cohort.key_realm_hash],
    "a failed zero-usage request must retain its observed realm as scope evidence"
  );
  assert.equal(zeroUsageRealmRun.checks.one_observed_key_realm, true);
  assert.equal(zeroUsageRealmRun.pass, false, "the failed run must remain invalid");
  const aggregateFailedZeroUsageRealm = (arm) => {
    const run = failedZeroUsageRealmRun(arm);
    return aggregateArm(arm, cohort, valid(arm, 0.9).executable, [{
      ...run,
      // Simulate an already-materialized run aggregate that lost the realm
      // because its only request had no usage. Retained request evidence must
      // still restore the scope observation without legitimizing the failure.
      metrics: { ...run.metrics, observed_realm_ids: [] }
    }]);
  };
  const failedZeroUsageChampion = aggregateFailedZeroUsageRealm("champion");
  const failedZeroUsageCandidate = aggregateFailedZeroUsageRealm("candidate");
  assert.deepEqual(failedZeroUsageChampion.metrics.observed_realm_ids, [cohort.key_realm_hash]);
  assert.equal(failedZeroUsageChampion.checks.one_observed_key_realm, true);
  assert.equal(failedZeroUsageChampion.pass, false, "the failed aggregate must remain invalid");
  const failedZeroUsageVerdict = compareArmResults(
    failedZeroUsageChampion,
    failedZeroUsageCandidate,
    0
  );
  assert.equal(
    failedZeroUsageVerdict.checks.observed_key_realm_matches,
    true,
    "matching zero-usage realm evidence must not be reported as scope drift"
  );
  assert.equal(failedZeroUsageVerdict.checks.champion_valid, false);
  assert.equal(failedZeroUsageVerdict.checks.candidate_valid, false);
  assert.equal(failedZeroUsageVerdict.baseline_pass, false);
  assert.equal(failedZeroUsageVerdict.pass, false, "failed evidence must never qualify for promotion");
  const providerOnlyRun = {
    ...valid("champion", 0.9),
    pass: false,
    metrics: {
      ...valid("champion", 0.9).metrics,
      provider_unstable_gap_tokens: 256
    },
    checks: {
      ...valid("champion", 0.9).checks,
      cohort_bound_on_every_request: true,
      one_observed_key_realm: true
    },
    requests: [{
      sse_completed: true,
      checks: {
        per_inbound_one_attempt_one_post: true,
        exact_counter_delta: true
      }
    }]
  };
  assert.equal(
    providerInstabilityOnlyRun(providerOnlyRun),
    true,
    "a complete exact-scope provider instability may proceed to the crossover pair"
  );
  assert.equal(
    providerInstabilityOnlyRun({
      ...providerOnlyRun,
      requests: [{ sse_completed: false, checks: { per_inbound_one_attempt_one_post: true } }]
    }),
    false,
    "an incomplete upstream stream must still stop the comparison"
  );
  const equalBaseline = compareArmResults(valid("champion", 0.9), valid("candidate", 0.9), 0);
  assert.equal(equalBaseline.pass, false);
  assert.equal(equalBaseline.baseline_pass, true);
  assert.equal(equalBaseline.positive_cache_evidence, false);
  assert.equal(equalBaseline.cold_start_accounting.excluded_from_hit_comparison, true);
  assert.equal(equalBaseline.cold_start_accounting.symmetric, true);
  assert.equal(equalBaseline.cold_start_accounting.candidate_no_extra_cold_start, true);
  assert.equal(
    equalBaseline.dynamic_tail_warm_attribution.applicable,
    false,
    "a non-dynamic comparison must expose no dynamic-tail attribution and keep its verdict unchanged"
  );
  const coldExcludedCandidate = valid("candidate", 0.9);
  coldExcludedCandidate.metrics.raw_token_hit_rate = 0.1;
  coldExcludedCandidate.metrics.cache_128_hit_rate = 0.1;
  const coldExcludedVerdict = compareArmResults(valid("champion", 0.9), coldExcludedCandidate, 0);
  assert.equal(coldExcludedVerdict.checks.candidate_raw_token_hit_not_lower, true);
  assert.equal(coldExcludedVerdict.checks.candidate_cache_128_hit_not_lower, true);
  assert.equal(coldExcludedVerdict.deltas.raw_token_hit_rate < 0, true);
  assert.equal(coldExcludedVerdict.deltas.warm_cache_128_hit_rate, 0);
  const coldSeedAccountingCandidate = valid("candidate", 0.9);
  coldSeedAccountingCandidate.runs = coldSeedAccountingCandidate.runs.map((run) => ({
    ...run,
    requests: run.requests.map((request) => ({
      ...request,
      input_tokens: number(request.input_tokens) + 7_144
    }))
  }));
  const coldSeedAccountingVerdict = compareArmResults(
    valid("champion", 0.9),
    coldSeedAccountingCandidate,
    0,
    0,
    0,
    true,
    128
  );
  assert.equal(coldSeedAccountingVerdict.checks.actual_outbound_input_symmetry, true);
  assert.equal(coldSeedAccountingVerdict.baseline_pass, true);
  assert.equal(
    coldSeedAccountingVerdict.actual_outbound_input_symmetry.max_cold_seed_input_token_delta,
    7_144
  );
  assert.equal(compareArmResults(valid("champion", 0.9), valid("candidate", 0.89), 0).pass, false);
  const cacheWinsButRemoteTtftVaries = valid("candidate", 0.9);
  cacheWinsButRemoteTtftVaries.metrics.ttft_p95_ms = 125;
  const splitVerdict = compareArmResults(
    valid("champion", 0.9),
    cacheWinsButRemoteTtftVaries,
    0
  );
  assert.equal(splitVerdict.pass, false);
  assert.equal(splitVerdict.cache_pass, true);
  assert.equal(splitVerdict.latency_pass, false);
  const saturatedChampion = valid("champion", 0.9);
  saturatedChampion.metrics.warm_stable_prefix_hit_rate = 0.99;
  saturatedChampion.metrics.shortfall_tokens = 512;
  saturatedChampion.metrics.new_tail_gap_tokens = 512;
  const saturatedCandidate = valid("candidate", 0.91);
  saturatedCandidate.metrics.warm_stable_prefix_hit_rate = 0.99;
  saturatedCandidate.metrics.shortfall_tokens = 128;
  saturatedCandidate.metrics.new_tail_gap_tokens = 128;
  const saturatedPrefixVerdict = compareArmResults(saturatedChampion, saturatedCandidate, 0);
  assert.equal(
    saturatedPrefixVerdict.pass,
    true,
    "a saturated warm prefix may tie while raw/cacheable hit and real tail loss improve"
  );
  const positiveCacheWithRemoteTtftVariance = valid("candidate", 0.91);
  positiveCacheWithRemoteTtftVariance.metrics.warm_stable_prefix_hit_rate = 0.99;
  positiveCacheWithRemoteTtftVariance.metrics.shortfall_tokens = 128;
  positiveCacheWithRemoteTtftVariance.metrics.new_tail_gap_tokens = 128;
  positiveCacheWithRemoteTtftVariance.metrics.ttft_p95_ms = 125;
  const remoteTtftVarianceVerdict = compareArmResults(
    saturatedChampion,
    positiveCacheWithRemoteTtftVariance,
    0
  );
  assert.equal(
    remoteTtftVarianceVerdict.pass,
    false,
    "promotion evidence must reject an end-to-end TTFT regression by default"
  );
  assert.equal(remoteTtftVarianceVerdict.latency_pass, false);
  assert.equal(remoteTtftVarianceVerdict.ttft_no_regression_required, true);
  const strictRemoteTtftVerdict = compareArmResults(
    saturatedChampion,
    positiveCacheWithRemoteTtftVariance,
    0,
    0,
    0,
    true
  );
  assert.equal(
    strictRemoteTtftVerdict.pass,
    false,
    "an explicit strict TTFT gate must reject a remote latency regression"
  );
  assert.equal(strictRemoteTtftVerdict.baseline_pass, false);
  assert.equal(strictRemoteTtftVerdict.ttft_no_regression_required, true);
  const legacyRelaxedTtftVerdict = compareArmResults(
    saturatedChampion,
    positiveCacheWithRemoteTtftVariance,
    0,
    0,
    0,
    false
  );
  assert.equal(
    legacyRelaxedTtftVerdict.baseline_pass,
    true,
    "an explicit relaxed comparison may ignore remote TTFT when local timing is unchanged"
  );
  assert.equal(legacyRelaxedTtftVerdict.ttft_no_regression_required, false);
  assert.equal(legacyRelaxedTtftVerdict.end_to_end_ttft_regression_exempted, true);
  const localRegressionUnderRelaxedPolicy = valid("candidate", 0.91);
  localRegressionUnderRelaxedPolicy.metrics.local_pre_upstream_overhead_p95_ms = 1;
  const localRegressionVerdict = compareArmResults(
    valid("champion", 0.9),
    localRegressionUnderRelaxedPolicy,
    0,
    0,
    0,
    false
  );
  assert.equal(localRegressionVerdict.baseline_pass, false);
  assert.equal(localRegressionVerdict.checks.candidate_local_pre_upstream_overhead_p95_not_regressed, false);
  const dynamicAttributionRun = (pair, newTailGapTokens, providerUnstableGapTokens = 0, explicit = true) => ({
    scenario: "dynamic-tail-mix",
    pair,
    requests: [
      {
        phase: "seed",
        input_tokens: 1_024,
        cache_read_tokens: 0,
        cache_new_tail_gap_tokens: 0,
        cache_provider_unstable_gap_tokens: 0
      },
      {
        phase: "followup",
        input_tokens: 8_192,
        cache_read_tokens: 4_096,
        cache_new_tail_gap_tokens: newTailGapTokens,
        cache_provider_unstable_gap_tokens: providerUnstableGapTokens,
        ...(explicit ? {
          cache_new_tail_gap_tokens_observed: true,
          cache_provider_unstable_gap_tokens_observed: true
        } : {})
      }
    ]
  });
  const cleanTailAttribution = pairedDynamicTailAttribution(
    { runs: [dynamicAttributionRun(0, 8_192), dynamicAttributionRun(1, 8_192)] },
    { runs: [dynamicAttributionRun(0, 0), dynamicAttributionRun(1, 8_192)] }
  );
  assert.equal(cleanTailAttribution.complete, true);
  assert.equal(cleanTailAttribution.counter_observations_explicit, true);
  assert.equal(cleanTailAttribution.provider_instability_state, "clean");
  assert.equal(cleanTailAttribution.candidate_new_tail_direction, "candidate_lower");
  assert.equal(cleanTailAttribution.hypothesis_only, false);
  assert.equal(cleanTailAttribution.candidate_new_tail_confirmed_improvement, true);
  const unstableTailAttribution = pairedDynamicTailAttribution(
    { runs: [dynamicAttributionRun(0, 8_192)] },
    { runs: [dynamicAttributionRun(0, 0, 8_192)] }
  );
  assert.equal(unstableTailAttribution.complete, true);
  assert.equal(unstableTailAttribution.provider_instability_state, "unstable");
  assert.equal(unstableTailAttribution.hypothesis_only, true);
  assert.equal(unstableTailAttribution.candidate_new_tail_improvement_hypothesis, true);
  assert.equal(unstableTailAttribution.candidate_new_tail_confirmed_improvement, false);
  const mixedTailAttribution = pairedDynamicTailAttribution(
    { runs: [dynamicAttributionRun(0, 8_192), dynamicAttributionRun(1, 8_192)] },
    { runs: [dynamicAttributionRun(0, 0), dynamicAttributionRun(1, 16_384)] }
  );
  assert.equal(mixedTailAttribution.provider_instability_state, "clean");
  assert.equal(mixedTailAttribution.candidate_new_tail_direction, "mixed");
  assert.equal(mixedTailAttribution.hypothesis_only, true);
  const missingCounterSource = dynamicAttributionRun(0, 8_192);
  const { cache_new_tail_gap_tokens, cache_provider_unstable_gap_tokens, ...missingCounterRequest } =
    missingCounterSource.requests[1];
  const missingCounterAttribution = pairedDynamicTailAttribution(
    { runs: [missingCounterSource] },
    {
      runs: [{
        ...dynamicAttributionRun(0, 0),
        requests: [dynamicAttributionRun(0, 0).requests[0], missingCounterRequest]
      }]
    }
  );
  assert.equal(missingCounterAttribution.complete, false);
  assert.equal(missingCounterAttribution.provider_instability_state, "incomplete");
  assert.equal(missingCounterAttribution.hypothesis_only, true);
  const dynamicInputRuns = (fingerprint, inputTokens, outboundFamily = fingerprint) => [{
    scenario: "dynamic-tail-mix",
    pair: 0,
    requests: [
      ["seed", inputTokens],
      ["dynamic-tail-1-natural-dense", inputTokens + 128],
      ["followup-2", inputTokens + 256]
    ].map(([phase, tokens]) => ({
      phase,
      request_kind: "turn",
      sse_completed: true,
      input_fingerprint: phase === "seed" ? fingerprint : `${fingerprint}-${phase}`,
      input_tokens: tokens,
      outbound_prefix_fingerprints: {
        input_full: `input:${outboundFamily}:${phase}`,
        instructions: `instructions:${outboundFamily}`,
        tools_schema: `tools:${outboundFamily}`,
        pre_input_wire: `pre-input:${outboundFamily}`
      }
    }))
  }];
  const symmetricDynamicInputs = pairedDynamicInputSymmetry(
    { runs: dynamicInputRuns("same", 450_000) },
    { runs: dynamicInputRuns("same", 450_064) },
    128
  );
  assert.equal(symmetricDynamicInputs.pass, true);
  assert.equal(symmetricDynamicInputs.max_input_token_delta, 64);
  assert.equal(symmetricDynamicInputs.max_warm_input_token_delta, 64);
  const symmetricAllScenarioInputs = pairedInputSymmetry(
    { runs: dynamicInputRuns("same", 450_000) },
    { runs: dynamicInputRuns("same", 450_064) },
    128
  );
  assert.equal(symmetricAllScenarioInputs.pass, true);
  assert.equal(symmetricAllScenarioInputs.client_input_fingerprints_match, true);
  assert.equal(symmetricAllScenarioInputs.actual_outbound_semantic_fingerprints_match, true);
  const coldSeedAccountingVariance = pairedInputSymmetry(
    { runs: dynamicInputRuns("same", 450_000) },
    {
      runs: dynamicInputRuns("same", 450_000).map((run) => ({
        ...run,
        requests: run.requests.map((request, index) => index === 0
          ? { ...request, input_tokens: request.input_tokens + 7_144 }
          : request)
      }))
    },
    128
  );
  assert.equal(coldSeedAccountingVariance.pass, true);
  assert.equal(coldSeedAccountingVariance.max_input_token_delta, 7_144);
  assert.equal(coldSeedAccountingVariance.max_warm_input_token_delta, 0);
  assert.equal(coldSeedAccountingVariance.max_cold_seed_input_token_delta, 7_144);
  const warmInputDelta = pairedInputSymmetry(
    { runs: dynamicInputRuns("same", 450_000) },
    {
      runs: dynamicInputRuns("same", 450_000).map((run) => ({
        ...run,
        requests: run.requests.map((request, index) => index === 1
          ? { ...request, input_tokens: request.input_tokens + 7_144 }
          : request)
      }))
    },
    128
  );
  assert.equal(warmInputDelta.pass, false);
  assert.equal(warmInputDelta.max_warm_input_token_delta, 7_144);
  const allScenarioOutboundMismatch = pairedInputSymmetry(
    { runs: dynamicInputRuns("same", 450_000, "left") },
    { runs: dynamicInputRuns("same", 450_000, "right") },
    128
  );
  assert.equal(allScenarioOutboundMismatch.pass, false);
  assert.equal(allScenarioOutboundMismatch.actual_outbound_semantic_fingerprints_match, false);
  const cacheControlBaselineRuns = dynamicInputRuns("same", 450_000).map((run) => ({
    ...run,
    requests: run.requests.map((request) => ({
      ...request,
      outbound_prefix_fingerprints: {
        ...request.outbound_prefix_fingerprints,
        cache_metadata: "cache:baseline"
      }
    }))
  }));
  const cacheControlCandidateRuns = cacheControlBaselineRuns.map((run) => ({
    ...run,
    requests: run.requests.map((request) => ({
      ...request,
      cache_control_fields: ["prompt-cache-options"],
      cache_options_24h: true,
      outbound_prefix_fingerprints: {
        ...request.outbound_prefix_fingerprints,
        cache_metadata: "cache:candidate-options-24h",
        pre_input_wire: `${request.outbound_prefix_fingerprints.pre_input_wire}:candidate-options-24h`
      }
    }))
  }));
  const declaredCacheControlDifference = pairedInputSymmetry(
    { runs: cacheControlBaselineRuns },
    { runs: cacheControlCandidateRuns },
    128,
    {
      candidateCacheControlField: "prompt-cache-options",
      candidateCacheOptions24h: true
    }
  );
  assert.equal(
    declaredCacheControlDifference.pass,
    true,
    "a declared 24h options metadata difference must preserve semantic input symmetry"
  );
  assert.equal(declaredCacheControlDifference.pre_input_wire_fingerprints_match, false);
  assert.equal(
    declaredCacheControlDifference.declared_cache_control_difference.attributed_differences,
    3
  );
  assert.equal(
    declaredCacheControlDifference.declared_cache_control_difference.unattributed_differences,
    0
  );
  const unprovenCacheControlDifference = pairedInputSymmetry(
    { runs: cacheControlBaselineRuns },
    {
      runs: cacheControlCandidateRuns.map((run) => ({
        ...run,
        requests: run.requests.map(({ cache_control_fields, ...request }) => request)
      }))
    },
    128,
    {
      candidateCacheControlField: "prompt-cache-options",
      candidateCacheOptions24h: true
    }
  );
  assert.equal(
    unprovenCacheControlDifference.pass,
    false,
    "an unproven pre-input wire difference must remain fail-closed"
  );
  assert.equal(
    unprovenCacheControlDifference.declared_cache_control_difference.unattributed_differences,
    3
  );
  const phaseMismatchedInput = pairedInputSymmetry(
    { runs: dynamicInputRuns("same", 450_000) },
    {
      runs: [{
        ...dynamicInputRuns("same", 450_000)[0],
        requests: dynamicInputRuns("same", 450_000)[0].requests.map((request, index) =>
          index === 1 ? { ...request, phase: "unexpected-phase" } : request
        )
      }]
    },
    128
  );
  assert.equal(phaseMismatchedInput.pass, false);
  assert.equal(phaseMismatchedInput.phases_match, false);
  const compactionInputRuns = (fingerprint) => [{
    scenario: "compaction-root",
    pair: 0,
    requests: [{
      phase: "compaction",
      request_kind: "compaction",
      sse_completed: true,
      input_fingerprint: fingerprint,
      input_tokens: 450_000,
      outbound_prefix_fingerprints: {
        input_full: "input:compaction",
        instructions: "instructions:compaction",
        tools_schema: "tools:compaction",
        pre_input_wire: "pre-input:compaction"
      }
    }]
  }];
  assert.equal(
    pairedInputSymmetry(
      { runs: compactionInputRuns("compaction-input") },
      { runs: compactionInputRuns("compaction-input") },
      128
    ).pass,
    true,
    "all-scenario input symmetry must validate compaction requests without requiring a turn kind"
  );
  assert.equal(
    pairedInputSymmetry({ runs: [] }, { runs: [] }, 128).pass,
    false,
    "promotion evidence must fail closed when no scored pair has actual outbound input proof"
  );
  const emptyDynamicPair = pairedDynamicInputSymmetry(
    {
      runs: [
        ...dynamicInputRuns("same", 450_000),
        { ...dynamicInputRuns("same", 450_000)[0], pair: 1, requests: [] }
      ]
    },
    {
      runs: [
        ...dynamicInputRuns("same", 450_000),
        { ...dynamicInputRuns("same", 450_000)[0], pair: 1, requests: [] }
      ]
    },
    128
  );
  assert.equal(emptyDynamicPair.pass, false);
  assert.equal(emptyDynamicPair.all_pairs_have_requests, false);
  const duplicateDynamicPair = pairedDynamicInputSymmetry(
    {
      runs: [
        ...dynamicInputRuns("same", 450_000),
        { ...dynamicInputRuns("same", 450_000)[0], pair: 0 }
      ]
    },
    {
      runs: [
        ...dynamicInputRuns("same", 450_000),
        { ...dynamicInputRuns("same", 450_000)[0], pair: 0 }
      ]
    },
    128
  );
  assert.equal(duplicateDynamicPair.pass, false);
  assert.equal(duplicateDynamicPair.pair_ids_unique, false);
  const invalidDynamicPair = pairedDynamicInputSymmetry(
    { runs: [{ ...dynamicInputRuns("same", 450_000)[0], pair: null }] },
    { runs: [{ ...dynamicInputRuns("same", 450_000)[0], pair: null }] },
    128
  );
  assert.equal(invalidDynamicPair.pass, false);
  assert.equal(invalidDynamicPair.pair_ids_valid, false);
  const asymmetricDynamicInputs = pairedDynamicInputSymmetry(
    { runs: dynamicInputRuns("same", 450_000) },
    { runs: dynamicInputRuns("other", 450_129) },
    128
  );
  assert.equal(asymmetricDynamicInputs.pass, false);
  assert.equal(asymmetricDynamicInputs.input_fingerprints_match, false);
  const upstreamSemanticMismatch = pairedDynamicInputSymmetry(
    { runs: dynamicInputRuns("same", 450_000, "left") },
    { runs: dynamicInputRuns("same", 450_000, "right") },
    128
  );
  assert.equal(upstreamSemanticMismatch.pass, false);
  assert.equal(upstreamSemanticMismatch.outbound_semantic_fingerprints_match, false);
  assert.equal(
    pairedDynamicInputSymmetry(
      { runs: [] },
      { runs: [] },
      128
    ).pass,
    true,
    "non-dynamic comparisons do not need a dynamic input symmetry proof"
  );
  const terminalDynamicFollowup = dynamicTailTerminalFollowup(
    [{ phase: "dynamic-tail-1-natural-dense", turn: 1 }],
    dynamicInputRuns("same", 450_000)[0].requests
  );
  assert.equal(terminalDynamicFollowup.input_tokens, 450_256);
  assert.equal(terminalDynamicFollowup.present, true);
  assert.equal(
    dynamicTailTerminalFollowup(
      [{ phase: "dynamic-tail-1-natural-dense" }],
      dynamicInputRuns("same", 450_000)[0].requests
    ).present,
    false,
    "a terminal tail without a recorded turn must not infer an arbitrary next request"
  );
  assert.equal(
    dynamicTailTerminalFollowup(
      [{ turn: 1 }],
      [
        { ...dynamicInputRuns("same", 450_000)[0].requests[0], phase: undefined },
        { ...dynamicInputRuns("same", 450_000)[0].requests[2] }
      ]
    ).present,
    false,
    "a terminal tail without a recorded phase must not match an undefined request phase"
  );
  assert.equal(
    dynamicTailTerminalFollowup(
      [{ phase: "dynamic-tail-1-natural-dense", turn: 1 }],
      [
        dynamicInputRuns("same", 450_000)[0].requests[0],
        dynamicInputRuns("same", 450_000)[0].requests[1],
        { ...dynamicInputRuns("same", 450_000)[0].requests[2], phase: "wrong-followup" }
      ]
    ).present,
    false,
    "a tail must be followed by its expected turn before it can satisfy the peak gate"
  );
  const repeatedTailPhaseRequests = [
    dynamicInputRuns("same", 450_000)[0].requests[0],
    dynamicInputRuns("same", 450_000)[0].requests[1],
    { ...dynamicInputRuns("same", 450_000)[0].requests[1] },
    dynamicInputRuns("same", 450_000)[0].requests[2]
  ];
  assert.equal(
    dynamicTailTerminalFollowup(
      [{ phase: "dynamic-tail-1-natural-dense", turn: 1 }],
      repeatedTailPhaseRequests
    ).present,
    false,
    "a duplicated terminal tail phase must not select an arbitrary follow-up"
  );
  const dynamicPeakRequest = (phase, inputTokens, fingerprint) => ({
    phase,
    request_kind: "turn",
    input_fingerprint: fingerprint,
    input_tokens: inputTokens,
    cache_read_tokens: inputTokens,
    cache_avoidable_gap_tokens: 0,
    cache_new_tail_gap_tokens: 0,
    cache_provider_unstable_gap_tokens: 0,
    cache_shortfall_tokens: 0,
    sse_completed: true,
    observed_realm_id: cohort.key_realm_hash,
    outbound_prefix_fingerprints: stableWire,
    prefix_guard_wait_ms: 0,
    local_prepare_ms: 1,
    upstream_ttft_ms: 80,
    ttft_ms: 80,
    checks: {
      per_inbound_one_attempt_one_post: true,
      exact_counter_delta: true,
      provider_matches_cohort: true,
      model_matches_cohort: true,
      observed_key_realm_matches_cohort: true,
      timing_present: true
    }
  });
  const seedPeakButTailFollowupLow = buildDynamicRun({
    arm: "champion",
    pair: 0,
    cohort,
    executable: valid("champion", 0.9).executable,
    scenario: "dynamic-tail-mix",
    promptCacheKeyUsed: true,
    dynamicTailEvents: [{ phase: "dynamic-tail-1-natural-dense", shape: "natural-dense", target_chars: 1 }],
    minimumGuardedRequests: 0,
    minimumSeedInputTokens: 0,
    minimumPeakInputTokens: 450_000,
    maximumPeakInputTokens: 500_000,
    requests: [
      dynamicPeakRequest("seed", 460_000, "seed"),
      dynamicPeakRequest("dynamic-tail-1-natural-dense", 460_128, "tail"),
      dynamicPeakRequest("followup-2", 420_000, "followup")
    ],
    fatal: null,
    compactionSeen: false
  });
  assert.equal(seedPeakButTailFollowupLow.checks.required_peak_input_tokens, true);
  assert.equal(seedPeakButTailFollowupLow.checks.dynamic_tail_terminal_followup_peak_in_range, false);
  assert.equal(seedPeakButTailFollowupLow.pass, false);
  const buildLargeTailWitnessRun = (waitReason) => {
    const directSuccessor = {
      ...dynamicPeakRequest("followup-1", 270_000, `large-tail-${waitReason}`),
      prefix_guard_wait_ms: 500,
      prefix_guard_wait_reason: waitReason,
      prefix_guard_wait_source: "exact"
    };
    return buildDynamicRun({
      arm: "candidate",
      pair: 0,
      cohort,
      executable: valid("candidate", 0.95).executable,
      scenario: "full-replay",
      promptCacheKeyUsed: false,
      dynamicTailEvents: [],
      minimumGuardedRequests: 0,
      minimumSeedInputTokens: 0,
      minimumPeakInputTokens: 0,
      maximumPeakInputTokens: 0,
      requests: [
        dynamicPeakRequest("seed", 265_000, "large-tail-seed"),
        directSuccessor
      ],
      fatal: null,
      compactionSeen: false
    });
  };
  const exactLargeTailWitnessAggregate = aggregateArm(
    "candidate",
    cohort,
    valid("candidate", 0.95).executable,
    [buildLargeTailWitnessRun("responses_exact_large_message_tail_lag")],
    0,
    0,
    0,
    false,
    true
  );
  assert.equal(
    exactLargeTailWitnessAggregate.metrics.candidate_exact_large_message_tail_lag_requests,
    1,
    "the exact large-tail reason must survive raw request to arm aggregation"
  );
  assert.equal(
    exactLargeTailWitnessAggregate.checks.candidate_exact_large_message_tail_lag_observed,
    true
  );
  const genericLargeTailWaitAggregate = aggregateArm(
    "candidate",
    cohort,
    valid("candidate", 0.95).executable,
    [buildLargeTailWitnessRun("responses_fresh_exact_prefix_settle")],
    0,
    0,
    0,
    false,
    true
  );
  assert.equal(genericLargeTailWaitAggregate.metrics.guarded_requests, 1);
  assert.equal(genericLargeTailWaitAggregate.metrics.candidate_exact_large_message_tail_lag_requests, 0);
  assert.equal(
    genericLargeTailWaitAggregate.checks.candidate_exact_large_message_tail_lag_observed,
    false,
    "a generic prefix wait must never qualify the exact large-tail witness"
  );
  const oneFullBucketBehind = valid("candidate", 0.9);
  oneFullBucketBehind.metrics.full_bucket_requests = 2;
  oneFullBucketBehind.metrics.full_bucket_rate = 0.5;
  oneFullBucketBehind.metrics.warm_full_bucket_requests = 1;
  oneFullBucketBehind.metrics.warm_full_bucket_rate = 1 / 3;
  assert.equal(
    compareArmResults(valid("champion", 0.9), oneFullBucketBehind, 0).pass,
    false
  );
  assert.equal(
    compareArmResults(valid("champion", 0.9), oneFullBucketBehind, 0, 0, 1).baseline_pass,
    true
  );
  const boundedLocalWait = valid("candidate", 0.9);
  boundedLocalWait.metrics.local_proxy_overhead_p95_ms = 500;
  assert.equal(
    compareArmResults(valid("champion", 0.9), boundedLocalWait, 0, 500).baseline_pass,
    true,
    "an explicitly allowed bounded foreground wait must not be treated as an unbounded local regression"
  );
  assert.equal(
    compareArmResults(valid("champion", 0.9), boundedLocalWait, 0).pass,
    false,
    "the default release gate remains strict unless the bounded wait is explicitly allowed"
  );
  const tokenSuperiorChampion = valid("champion", 0.9);
  tokenSuperiorChampion.metrics.shortfall_tokens = 512;
  tokenSuperiorChampion.metrics.new_tail_gap_tokens = 512;
  const tokenSuperiorButOneFullBucketBehind = valid("candidate", 0.91);
  tokenSuperiorButOneFullBucketBehind.metrics.shortfall_tokens = 128;
  tokenSuperiorButOneFullBucketBehind.metrics.new_tail_gap_tokens = 128;
  tokenSuperiorButOneFullBucketBehind.metrics.full_bucket_requests = 2;
  tokenSuperiorButOneFullBucketBehind.metrics.full_bucket_rate = 0.5;
  tokenSuperiorButOneFullBucketBehind.metrics.warm_full_bucket_requests = 1;
  tokenSuperiorButOneFullBucketBehind.metrics.warm_full_bucket_rate = 1 / 3;
  const tokenSuperiorVerdict = compareArmResults(
    tokenSuperiorChampion,
    tokenSuperiorButOneFullBucketBehind,
    0
  );
  assert.equal(tokenSuperiorVerdict.pass, true);
  assert.equal(tokenSuperiorVerdict.cache_pass, true);
  assert.equal(tokenSuperiorVerdict.checks.candidate_full_bucket_rate_not_lower, false);
  assert.equal(tokenSuperiorVerdict.checks.candidate_full_bucket_loss_explained_by_token_gain, true);
  const upstreamConfoundedChampion = valid("champion", 0.9);
  upstreamConfoundedChampion.metrics.provider_unstable_gap_tokens = 29_184;
  const upstreamConfoundedCandidate = valid("candidate", 0.91);
  const upstreamConfoundedVerdict = compareArmResults(
    upstreamConfoundedChampion,
    upstreamConfoundedCandidate,
    0
  );
  assert.equal(upstreamConfoundedVerdict.pass, false);
  assert.equal(upstreamConfoundedVerdict.baseline_pass, true);
  assert.equal(upstreamConfoundedVerdict.evidence_confounded_by_provider_instability, true);
  const asymmetricSeedChampion = valid("champion", 0.9);
  const asymmetricSeedCandidate = valid("candidate", 0.91);
  asymmetricSeedCandidate.metrics.seed_cache_read_tokens = 128;
  asymmetricSeedCandidate.runs = asymmetricSeedCandidate.runs.map((run) => ({
    ...run,
    requests: run.requests.map((request) => ({
      ...request,
      cache_read_tokens: request.phase === "seed" ? 128 : request.cache_read_tokens
    }))
  }));
  const asymmetricSeedVerdict = compareArmResults(
    asymmetricSeedChampion,
    asymmetricSeedCandidate,
    0
  );
  assert.equal(asymmetricSeedVerdict.checks.seed_cache_read_evidence_complete, true);
  assert.equal(asymmetricSeedVerdict.checks.seed_cache_read_symmetry, false);
  assert.equal(
    asymmetricSeedVerdict.baseline_pass,
    true,
    "a candidate warm seed is excluded from cache scoring when it adds no cold start"
  );
  assert.equal(asymmetricSeedVerdict.positive_cache_evidence, false);
  const seedRun = (arm, pair, seedCacheReadTokens) => ({
    ...valid(arm, 0.9),
    kind: "dynamic-run",
    pair,
    scenario: "full-replay",
    metrics: {
      ...valid(arm, 0.9).metrics,
      seed_input_tokens: 156_416,
      seed_cache_read_tokens: seedCacheReadTokens
    },
    requests: [{
      phase: "seed",
      request_kind: "turn",
      sse_completed: true,
      input_fingerprint: `shared-crossover-seed-${pair}`,
      input_tokens: 156_416,
      cache_read_tokens: seedCacheReadTokens,
      cache_read_tokens_observed: true,
      outbound_prefix_fingerprints: stableWire,
      observed_realm_id: "observed-realm",
      prefix_guard_wait_ms: 0,
      local_prepare_ms: 1,
      upstream_ttft_ms: 80,
      ttft_ms: 80,
      checks: {
        per_inbound_one_attempt_one_post: true,
        exact_counter_delta: true,
        timing_present: true,
        static_wire_continuity: true
      }
      }]
  });
  const unsafeDiagnosticRun = (arm) => {
    const run = seedRun(arm, 0, 0);
    return {
      ...run,
      metrics: {
        ...run.metrics,
        candidate_exact_large_message_tail_lag_requests: 1
      },
      fatal: "Bearer secret https://example.invalid/private/fatal",
      executable: { path: "C:/private/path/secret.exe", sha256: "b".repeat(64) },
      requests: run.requests.map((request) => ({
        ...request,
        compacted_input: [{ type: "input_text", text: "secret request body" }],
        raw_request_body: "Bearer secret request body",
        failure: "fatal https://example.invalid/private/fatal",
        prefix_guard_wait_reason: "responses_exact_large_message_tail_lag",
        prefix_guard_wait_source: "exact",
        prefix_guard_skip_reason: "not_applicable",
        transport: {
          ...request.transport,
          upstream_network_path: "https://example.invalid/private/path",
          request_body_bytes: 1_024,
          sent_body_bytes: 512
        }
      }))
    };
  };
  const postPairGateDiagnostic = buildAfterPairLiveGateFailureReport({
    mode: "live-isolated",
    cohort,
    settings: {
      minimum_peak_input_tokens: 0,
      maximum_peak_input_tokens: 0
    },
    pairOrder: [["champion", "candidate"]],
    turnOrder: null,
    abortedAfterPair: 0,
    warmupPairs: 0,
    rawArmRuns: {
      champion: [unsafeDiagnosticRun("champion")],
      candidate: [unsafeDiagnosticRun("candidate")]
    },
    artifacts: {
      champion: valid("champion", 0.9).executable,
      candidate: valid("candidate", 0.9).executable
    },
    liveGateFailure: {
      code: "live_codex_metrics_incomplete",
      evidence: incompleteLiveGateEvidence
    }
  });
  assert.equal(postPairGateDiagnostic.pass, false);
  assert.equal(postPairGateDiagnostic.fail_closed, true);
  assert.equal(postPairGateDiagnostic.promotion_eligible, false);
  assert.equal(postPairGateDiagnostic.diagnostic_only, true);
  assert.equal(postPairGateDiagnostic.aborted_after_pair, 0);
  assert.deepEqual(postPairGateDiagnostic.scored_pair_ids, []);
  assert.deepEqual(postPairGateDiagnostic.completed_isolated_pair_ids, [0]);
  assert.equal(postPairGateDiagnostic.completed_isolated_arm_runs.champion.length, 1);
  assert.equal(postPairGateDiagnostic.completed_isolated_arm_runs.candidate.length, 1);
  const retainedDiagnosticRequest = postPairGateDiagnostic
    .completed_isolated_arm_runs.candidate[0].requests[0];
  assert.equal(
    retainedDiagnosticRequest.prefix_guard_wait_reason,
    "responses_exact_large_message_tail_lag",
    "a fail-closed diagnostic must retain the bounded candidate wait reason"
  );
  assert.equal(retainedDiagnosticRequest.prefix_guard_wait_source, "exact");
  assert.equal(retainedDiagnosticRequest.prefix_guard_skip_reason, "not_applicable");
  assert.equal(postPairGateDiagnostic.diagnostic_arm_aggregates.champion.completed_run_count, 1);
  assert.equal(postPairGateDiagnostic.diagnostic_arm_aggregates.candidate.completed_run_count, 1);
  assert.equal(
    postPairGateDiagnostic.diagnostic_arm_aggregates.candidate.metrics
      .candidate_exact_large_message_tail_lag_requests,
    1,
    "fail-closed diagnostics must retain the bounded exact large-tail witness count"
  );
  assert.equal(
    postPairGateDiagnostic.diagnostic_arm_aggregates.candidate.checks
      .candidate_exact_large_message_tail_lag_observed,
    true,
    "fail-closed diagnostics must retain the exact large-tail witness check"
  );
  assert.equal(postPairGateDiagnostic.live_codex_gate.evidence.http_status, 200);
  assert.equal(Object.hasOwn(postPairGateDiagnostic, "cohort"), false);
  assert.equal(Object.hasOwn(postPairGateDiagnostic, "settings"), false);
  assert.equal(Object.hasOwn(postPairGateDiagnostic.diagnostic_arm_aggregates.champion, "runs"), false);
  assert.equal(Object.hasOwn(postPairGateDiagnostic.diagnostic_arm_aggregates.champion, "executable"), false);
  const postPairDiagnosticText = JSON.stringify(postPairGateDiagnostic).toLowerCase();
  for (const forbidden of [
    "https://",
    "bearer",
    "secret",
    "body",
    "path",
    "fatal",
    "compacted"
  ]) {
    assert.equal(
      postPairDiagnosticText.includes(forbidden),
      false,
      "diagnostic evidence must not retain " + forbidden
    );
  }
  assert.throws(
    () => validateAggregate(postPairGateDiagnostic.diagnostic_arm_aggregates.champion, "champion"),
    (error) => error?.code === "invalid_offline_artifact_schema",
    "diagnostic aggregates must never be usable as offline promotion evidence"
  );
  assert.throws(
    () => extractArmArtifact(postPairGateDiagnostic, "champion"),
    (error) => error?.code === "invalid_offline_artifact",
    "a fail-closed post-pair report must not expose a promotion-shaped top-level arm"
  );
  const crossedSeedChampion = aggregateArm(
    "champion",
    cohort,
    valid("champion", 0.9).executable,
    [seedRun("champion", 0, 0), seedRun("champion", 1, 156_416)]
  );
  const crossedSeedCandidate = aggregateArm(
    "candidate",
    cohort,
    valid("candidate", 0.9).executable,
    [seedRun("candidate", 0, 156_416), seedRun("candidate", 1, 0)]
  );
  assert.equal(crossedSeedChampion.metrics.seed_cache_read_tokens, 156_416);
  assert.equal(crossedSeedCandidate.metrics.seed_cache_read_tokens, 156_416);
  const crossedSeedVerdict = compareArmResults(crossedSeedChampion, crossedSeedCandidate, 0);
  assert.equal(crossedSeedVerdict.checks.seed_cache_read_evidence_complete, true);
  assert.equal(
    crossedSeedVerdict.checks.seed_cache_read_symmetry,
    true,
    "a two-pair shared-cache cold/warm crossover must aggregate to symmetric seed evidence"
  );
  assert.equal(crossedSeedVerdict.deltas.seed_cache_read_tokens, 0);
  assert.equal(crossedSeedVerdict.baseline_pass, true);
  assert.equal(crossedSeedVerdict.cold_start_accounting.symmetric, true);
  const extraColdChampion = aggregateArm(
    "champion",
    cohort,
    valid("champion", 0.9).executable,
    [seedRun("champion", 0, 128), seedRun("champion", 1, 128)]
  );
  const extraColdCandidate = aggregateArm(
    "candidate",
    cohort,
    valid("candidate", 0.9).executable,
    [seedRun("candidate", 0, 0), seedRun("candidate", 1, 256)]
  );
  const extraColdVerdict = compareArmResults(extraColdChampion, extraColdCandidate, 0);
  assert.equal(extraColdVerdict.checks.seed_cache_read_symmetry, true);
  assert.equal(extraColdVerdict.checks.cold_seed_evidence_complete, true);
  assert.equal(extraColdVerdict.checks.cold_seed_request_symmetry, true);
  assert.equal(extraColdVerdict.checks.cold_seed_symmetry, false);
  assert.equal(extraColdVerdict.checks.candidate_no_extra_cold_start, false);
  assert.equal(extraColdVerdict.baseline_pass, false);
  const legacyFirstPairSeedChampion = {
    ...crossedSeedChampion,
    metrics: {
      ...crossedSeedChampion.metrics,
      // Older reports retained the full per-pair evidence but accidentally
      // surfaced only the first pair's cold seed in the aggregate metric.
      seed_cache_read_tokens: 0
    }
  };
  const legacyFirstPairSeedCandidate = {
    ...crossedSeedCandidate,
    metrics: {
      ...crossedSeedCandidate.metrics,
      seed_cache_read_tokens: 156_416
    }
  };
  const legacyFirstPairSeedVerdict = compareArmResults(
    legacyFirstPairSeedChampion,
    legacyFirstPairSeedCandidate,
    0
  );
  assert.equal(
    legacyFirstPairSeedVerdict.checks.seed_cache_read_evidence_complete,
    true
  );
  assert.equal(
    legacyFirstPairSeedVerdict.checks.seed_cache_read_symmetry,
    true,
    "retained per-pair seed evidence must override legacy first-pair aggregate fields"
  );
  assert.equal(legacyFirstPairSeedVerdict.deltas.seed_cache_read_tokens, 0);
  assert.equal(legacyFirstPairSeedVerdict.baseline_pass, true);
  const unbalancedSeedCandidate = aggregateArm(
    "candidate",
    cohort,
    valid("candidate", 0.9).executable,
    [seedRun("candidate", 0, 156_416), seedRun("candidate", 1, 156_416)]
  );
  const unbalancedSeedVerdict = compareArmResults(
    crossedSeedChampion,
    unbalancedSeedCandidate,
    0
  );
  assert.equal(
    unbalancedSeedVerdict.checks.seed_cache_read_evidence_complete,
    true
  );
  assert.equal(
    unbalancedSeedVerdict.checks.seed_cache_read_symmetry,
    false,
    "the diagnostic must preserve an unbalanced shared-cache seed allocation"
  );
  assert.equal(
    unbalancedSeedVerdict.baseline_pass,
    true,
    "fewer candidate cold seeds are allowed because cold starts are excluded from hit scoring"
  );
  assert.equal(unbalancedSeedVerdict.pass, false);
  const missingSeedEvidenceCandidate = {
    ...crossedSeedCandidate,
    runs: crossedSeedCandidate.runs.map((run, index) => index === 1
      ? {
        ...run,
        requests: run.requests.map(({ cache_read_tokens, ...request }) => request)
      }
      : run)
  };
  const missingSeedEvidenceVerdict = compareArmResults(
    crossedSeedChampion,
    missingSeedEvidenceCandidate,
    0
  );
  assert.equal(missingSeedEvidenceVerdict.checks.seed_cache_read_evidence_complete, false);
  assert.equal(missingSeedEvidenceVerdict.checks.seed_cache_read_symmetry, false);
  assert.equal(missingSeedEvidenceVerdict.baseline_pass, false);
  assert.equal(missingSeedEvidenceVerdict.pass, false);
  assert.equal(missingSeedEvidenceVerdict.deltas.seed_cache_read_tokens, null);
  const missingSeedEvidenceBothArms = (aggregate) => ({
    ...aggregate,
    // Keep the legacy aggregate metric populated: raw evidence, not this
    // fallback field, must decide whether a crossover is comparable.
    metrics: { ...aggregate.metrics, seed_cache_read_tokens: 156_416 },
    runs: aggregate.runs.map((run) => ({
      ...run,
      requests: run.requests.map(({ cache_read_tokens, cache_read_tokens_observed, ...request }) => request)
    }))
  });
  const missingBothSeedVerdict = compareArmResults(
    missingSeedEvidenceBothArms(crossedSeedChampion),
    missingSeedEvidenceBothArms(crossedSeedCandidate),
    0
  );
  assert.equal(missingBothSeedVerdict.checks.seed_cache_read_evidence_complete, false);
  assert.equal(missingBothSeedVerdict.checks.seed_cache_read_symmetry, false);
  assert.equal(missingBothSeedVerdict.baseline_pass, false);
  assert.equal(missingBothSeedVerdict.deltas.seed_cache_read_tokens, null);
  const staleSeedMetricRuns = [
    seedRun("champion", 0, 0),
    seedRun("champion", 1, 156_416)
  ].map((run) => ({
    ...run,
    metrics: { ...run.metrics, seed_cache_read_tokens: 0 }
  }));
  const rawSeedAggregate = aggregateArm(
    "champion",
    cohort,
    valid("champion", 0.9).executable,
    staleSeedMetricRuns
  );
  assert.equal(
    rawSeedAggregate.metrics.seed_cache_read_tokens,
    156_416,
    "aggregateArm must retain every scored run's raw seed total"
  );
  const mismatched = valid("candidate", 0.9);
  mismatched.cohort = { ...cohort, model: "other" };
  assert.equal(compareArmResults(valid("champion", 0.9), mismatched, 0).pass, false);
  const aggregate = aggregateArm("champion", cohort, valid("champion", 0.9).executable, [
    {
      ...valid("champion", 0.9),
      kind: "dynamic-run",
      pair: 0,
      scenario: "full-replay",
      requests: [
        {
          phase: "seed",
          input_tokens: 1024,
          cache_read_tokens: 900,
          outbound_prefix_fingerprints: stableWire,
          sse_completed: true,
          observed_realm_id: "observed-realm",
          prefix_guard_wait_ms: 0,
          local_prepare_ms: 1,
          upstream_ttft_ms: 80,
          ttft_ms: 80,
          checks: {
            per_inbound_one_attempt_one_post: true,
            exact_counter_delta: true,
            timing_present: true,
            static_wire_continuity: true
          }
        },
        {
          phase: "followup-1",
          input_tokens: 1152,
          cache_read_tokens: 1152,
          outbound_prefix_fingerprints: stableWire,
          sse_completed: true,
          observed_realm_id: "observed-realm",
          prefix_guard_wait_ms: 0,
          local_prepare_ms: 1,
          upstream_ttft_ms: 90,
          ttft_ms: 90,
          checks: {
            per_inbound_one_attempt_one_post: true,
            exact_counter_delta: true,
            timing_present: true,
            static_wire_continuity: true
          }
        }
      ]
    }
  ]);
  assert.equal(aggregate.pass, true);
  const guardedRuns = [0, 1].map((pair) => ({
    ...valid("candidate", 0.9),
    metrics: {
      ...valid("candidate", 0.9).metrics,
      guarded_requests: 1
    },
    kind: "dynamic-run",
    pair,
    scenario: "tool-tail-maturity",
    requests: [{
      phase: `followup-${pair}`,
      input_tokens: 1152,
      cache_read_tokens: 1152,
      outbound_prefix_fingerprints: stableWire,
      sse_completed: true,
      observed_realm_id: "observed-realm",
      prefix_guard_wait_ms: 250,
      local_prepare_ms: 1,
      upstream_ttft_ms: 80,
      ttft_ms: 80,
      checks: {
        per_inbound_one_attempt_one_post: true,
        exact_counter_delta: true,
        timing_present: true,
        static_wire_continuity: true
      }
    }]
  }));
  const twoPairGuard = aggregateArm(
    "candidate",
    cohort,
    valid("candidate", 0.9).executable,
    guardedRuns,
    2
  );
  assert.equal(twoPairGuard.pass, true, JSON.stringify(twoPairGuard.checks));
  assert.equal(twoPairGuard.checks.required_guarded_requests, true);
  const overRequiredGuard = aggregateArm(
    "candidate",
    cohort,
    valid("candidate", 0.9).executable,
    guardedRuns,
    3
  );
  assert.equal(overRequiredGuard.pass, false);
  assert.equal(overRequiredGuard.checks.required_guarded_requests, false);
  assert.equal(generatedPromptCacheKey("test", "lane").startsWith("atoapi-"), true);
  const scopedConfig = [
    'local_key = "before"',
    "[[agent_injections]]",
    'id = "codex"',
    'provider_id = "provider-a"',
    'enabled = true',
    "[[providers]]",
    'id = "provider-a"',
    'base_url = "https://example.test/v1"',
    'channel = "responses"',
    'api_key_encrypted = "encrypted-a"',
    'enabled = true',
    ""
  ].join("\n");
  const scopedFingerprint = await currentLiveSelectionScopeFingerprint(
    tmpdir(),
    scopedConfig
  );
  assert.equal(
    scopedFingerprint,
    await currentLiveSelectionScopeFingerprint(tmpdir(), scopedConfig),
    "the selected route fingerprint must be deterministic"
  );
  const targetScopeRoot = await mkdtemp(join(tmpdir(), "atoapi-release-champion-target-scope-"));
  try {
    const targetConfigPath = join(targetScopeRoot, "codex.toml");
    await writeFile(targetConfigPath, "unrelated_runtime_marker = 1\n", "utf8");
    const targetScopedConfig = scopedConfig.replace(
      'enabled = true',
      `enabled = true\ntarget_path = ${JSON.stringify(targetConfigPath)}`
    );
    const targetScopedFingerprint = await currentLiveSelectionScopeFingerprint(
      tmpdir(),
      targetScopedConfig
    );
    await writeFile(targetConfigPath, "unrelated_runtime_marker = 2\n", "utf8");
    assert.equal(
      targetScopedFingerprint,
      await currentLiveSelectionScopeFingerprint(tmpdir(), targetScopedConfig),
      "unrelated Codex client config churn must not invalidate an isolated upstream selection"
    );
  } finally {
    await rm(targetScopeRoot, { recursive: true, force: true });
  }
  assert.equal(normalizeProviderScope("active_provider"), "active-provider");
  assert.throws(
    () => normalizeProviderScope("unknown-scope"),
    (error) => error?.code === "invalid_provider_scope"
  );
  assert.notEqual(
    scopedFingerprint,
    await currentLiveSelectionScopeFingerprint(
      tmpdir(),
      scopedConfig.replace('channel = "responses"', 'channel = "chat"')
    ),
    "a selected upstream channel change must invalidate live evidence"
  );
  assert.equal(
    scopedFingerprint,
    await currentLiveSelectionScopeFingerprint(
      tmpdir(),
      scopedConfig.replace('local_key = "before"', 'local_key = "before"\nactive_provider_id = "unrelated-provider"')
    ),
    "an unrelated active_provider_id must not alter codex-agent scope"
  );
  const pinnedScopeConfig = [
    'local_key = "before"',
    "[[agent_injections]]",
    'id = "codex"',
    'provider_id = "provider-a"',
    'enabled = true',
    "[[providers]]",
    'id = "provider-a"',
    'base_url = "https://example.test/v1"',
    'channel = "responses"',
    'api_key_encrypted = "encrypted-a"',
    'enabled = true',
    "[[provider_key_pools]]",
    'provider_id = "provider-a"',
    'enabled = true',
    'strategy = "sequential"',
    "[[provider_key_pools.keys]]",
    'id = "key-a"',
    'key_encrypted = "encrypted-key-a"',
    'enabled = true',
    "[[provider_key_pools.keys]]",
    'id = "key-b"',
    'key_encrypted = "encrypted-key-b"',
    'enabled = true',
    ''
  ].join("\n");
  const pinnedScopeFingerprint = await currentLiveSelectionScopeFingerprint(
    tmpdir(),
    pinnedScopeConfig,
    "codex-agent",
    "key-a"
  );
  assert.equal(
    pinnedScopeFingerprint,
    await currentLiveSelectionScopeFingerprint(
      tmpdir(),
      pinnedScopeConfig.replace(
        'id = "key-b"\nkey_encrypted = "encrypted-key-b"\nenabled = true',
        'id = "key-b"\nkey_encrypted = "rotated-sibling-material"\nenabled = false\ndisabled_until = 2099-01-01T00:00:00Z'
      ),
      "codex-agent",
      "key-a"
    ),
    "a pinned live scope must ignore sibling Key health and material changes"
  );
  assert.notEqual(
    pinnedScopeFingerprint,
    await currentLiveSelectionScopeFingerprint(
      tmpdir(),
      pinnedScopeConfig.replace('id = "key-a"\nkey_encrypted = "encrypted-key-a"\nenabled = true', 'id = "key-a"\nkey_encrypted = "encrypted-key-a"\nenabled = false'),
      "codex-agent",
      "key-a"
    ),
    "a pinned live scope must fail closed when the selected Key becomes unavailable"
  );
  assert.notEqual(
    pinnedScopeFingerprint,
    await currentLiveSelectionScopeFingerprint(
      tmpdir(),
      pinnedScopeConfig.replace('key_encrypted = "encrypted-key-a"', 'key_encrypted = "rotated-key-a-material"'),
      "codex-agent",
      "key-a"
    ),
    "a pinned live scope must fail closed when the selected Key material changes"
  );
  const activeProviderConfig = [
    'local_key = "before"',
    'active_provider_id = "provider-b"',
    "[[agent_injections]]",
    'id = "codex"',
    'provider_id = "provider-a"',
    'model_id = "stale-model"',
    'enabled = false',
    "[[providers]]",
    'id = "provider-a"',
    'base_url = "https://a.example.test/v1"',
    'channel = "responses"',
    'api_key_encrypted = "encrypted-a"',
    'enabled = true',
    "[[providers]]",
    'id = "provider-b"',
    'base_url = "https://b.example.test/v1"',
    'channel = "responses"',
    'api_key_encrypted = "encrypted-b"',
    'enabled = true',
    ""
  ].join("\n");
  const activeFingerprint = await currentLiveSelectionScopeFingerprint(
    tmpdir(),
    activeProviderConfig,
    "active-provider"
  );
  assert.equal(
    activeFingerprint,
    await currentLiveSelectionScopeFingerprint(tmpdir(), activeProviderConfig, "active_provider"),
    "active-provider scope fingerprint must be deterministic and accept underscore alias"
  );
  assert.equal(
    activeFingerprint,
    await currentLiveSelectionScopeFingerprint(
      tmpdir(),
      activeProviderConfig.replace('channel = "responses"', 'channel = "chat"'),
      "active-provider"
    ),
    "a stale Codex injection Provider change must not alter active-provider scope"
  );
  assert.equal(
    activeFingerprint,
    await currentLiveSelectionScopeFingerprint(
      tmpdir(),
      activeProviderConfig.replace('provider_id = "provider-a"', 'provider_id = "other-stale-provider"'),
      "active-provider"
    ),
    "a stale Codex injection selection must not alter active-provider scope"
  );
  assert.notEqual(
    activeFingerprint,
    await currentLiveSelectionScopeFingerprint(
      tmpdir(),
      activeProviderConfig.replace(
        'id = "provider-b"\nbase_url = "https://b.example.test/v1"\nchannel = "responses"',
        'id = "provider-b"\nbase_url = "https://b.example.test/v1"\nchannel = "chat"'
      ),
      "active-provider"
    ),
    "the selected active Provider change must invalidate live evidence"
  );
  const sourceRoot = await mkdtemp(join(tmpdir(), "atoapi-release-champion-self-test-"));
  let snapshot = null;
  try {
    await writeFile(join(sourceRoot, "config.toml"), 'local_key = "before"\n', "utf8");
    await writeFile(join(sourceRoot, "cache-key.dpapi"), "before-key", "utf8");
    snapshot = await snapshotLiveConfig(sourceRoot);
    await writeFile(join(sourceRoot, "config.toml"), 'local_key = "after"\n', "utf8");
    await writeFile(join(sourceRoot, "cache-key.dpapi"), "after-key", "utf8");
    assert.equal(
      await readRequiredText(join(snapshot.configDir, "config.toml"), "snapshotted self-test config"),
      'local_key = "before"\n'
    );
    assert.equal(
      await readRequiredText(join(snapshot.configDir, "cache-key.dpapi"), "snapshotted self-test key"),
      "before-key"
    );
  } finally {
    if (snapshot) await rm(snapshot.root, { recursive: true, force: true });
    await rm(sourceRoot, { recursive: true, force: true });
  }
  console.log(JSON.stringify({ schema: SCHEMA, self_test: "passed" }));
}
