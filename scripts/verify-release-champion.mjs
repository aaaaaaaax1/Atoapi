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
  if (scenario === "dynamic-tail-mix" && (turns < 3 || turns % 2 === 0)) {
    throw new FailClosedError(
      "dynamic_tail_mix_turn_count",
      "--scenario dynamic-tail-mix requires an odd turn count of at least 3; 11 remains the full five-tail default"
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
  // Historical tool-history validation declares the synthetic fixture schema
  // from the seed onward. Keep that behavior by default: omitting the flag
  // must never silently change a previously valid comparison wire.
  // `--include-tool-schema=false` is available only for an explicit protocol
  // compatibility probe and has a distinct fixture identity.
  const includeToolSchema = resolveIncludeToolSchema(options);
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
  const keepRunDir = booleanArg(options["keep-run-dir"]);
  const isolateUpstreamCache = booleanArg(options["isolate-upstream-cache"]);
  // A live isolated comparison is only meaningful when the native upstream
  // placement telemetry proves both arms stayed on distinct, stable lanes.
  // Offline artifacts predate this proof in some cases, so their explicit
  // compatibility path keeps the requirement disabled.
  const nativePlacementIsolationRequired = isolateUpstreamCache;
  const sharedCacheCrossover = booleanArg(options["shared-cache-crossover"]);
  // An opaque prompt-cache placement value is local evidence that the two
  // arms chose different values; it does not prove that a selected upstream
  // honors the field as an isolation boundary. The live v1.4.39 replay
  // demonstrated cross-arm cache transfer despite distinct fingerprints, so
  // promotion must use turn-by-turn shared placement crossover until a future
  // upstream-specific isolation proof exists.
  const promotionRequiresSharedUpstreamPlacementCrossover = true;
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
  const sourceSnapshot = await snapshotLiveConfig(sourceConfigDir);
  try {
    const configText = await readRequiredText(
      join(sourceSnapshot.configDir, "config.toml"),
      "snapshotted source config.toml"
    );
    if (!extractTomlString(configText, "local_key")) {
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

    const cohort = {
      provider_id: providerId,
      model,
      key_realm_hash: keyRealmHash,
      request_family: requestFamily
    };
    const assertLiveCodexScope = async (checkpoint) => {
      if (!liveCodexMetricsUrl) return;
      await assertLiveCodexMetricsScopeUnchanged({
        metricsUrl: liveCodexMetricsUrl,
        expectedProviderId: cohort.provider_id,
        expectedModel: cohort.model,
        expectedRealm: cohort.key_realm_hash,
        maxAgeSeconds: liveCodexMaxAgeSeconds,
        checkpoint
      });
    };
    await assertLiveCodexScope("before_isolated_runtime_start");
    const settings = {
      scenario,
      pairs,
      warmup_pairs: warmupPairs,
      pair_offset: pairOffset,
      first_arm: firstArm,
      turns,
      max_output_tokens: maxOutputTokens,
      stable_instruction_chars: stableInstructionChars,
      seed_context_chars: seedContextChars,
      minimum_seed_input_tokens: minimumSeedInputTokens,
      minimum_peak_input_tokens: minimumPeakInputTokens,
      maximum_peak_input_tokens: maximumPeakInputTokens,
      tool_chars: toolChars,
      tool_calls: toolCalls,
      tool_output_shape: toolOutputShape,
      include_tool_schema: includeToolSchema,
      dynamic_tail_profile: dynamicTailProfile,
      dynamic_tail_mode: dynamicTailMode,
      fixture_profile: fixtureProfile,
      fresh_fixture_per_pair: freshFixturePerPair,
      turn_delay_ms: turnDelayMs,
      inter_arm_delay_ms: interArmDelayMs,
      pair_delay_ms: pairDelayMs,
      require_candidate_guarded_requests: requireCandidateGuardedRequests,
      client_prompt_cache_key: Boolean(options["prompt-cache-key-prefix"]),
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
    const upstreamUserAgentParity = evaluateUpstreamUserAgentParity({
      championUpstreamUserAgent,
      candidateUpstreamUserAgent,
      sourceCustomUserAgent: extractTomlString(providerBlock, "custom_user_agent").trim(),
      championExecutableSha256: artifacts.champion.sha256,
      candidateExecutableSha256: artifacts.candidate.sha256
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
          requestedPort,
          championUpstreamUserAgent,
          candidateUpstreamUserAgent,
          pinnedKeyId,
          forceUseSystemProxy,
          startOrder: persistentRuntimeStartOrder,
          keepRunDir
        });
      }

      for (let pair = 0; pair < pairs; pair += 1) {
        await assertLiveCodexScope("before_pair_" + pair);
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
            cohort,
            settings,
            requestedPort,
            runId,
            pair,
            lane,
            isolationLane,
            fixtureFamily,
            promptCacheKeyPrefix: options["prompt-cache-key-prefix"],
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
              runtime: persistentArmRuntimes.champion.runtime
            },
            candidate: {
              ...armSpecFor("candidate"),
              runtime: persistentArmRuntimes.candidate.runtime
            }
          });
          orderedPairs.push(
            result.turn_order[0] ?? interleavedTurnOrder(pair, 0, pairOffset, firstArm)
          );
          interleavedTurnOrders.push(result.turn_order);
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
          await assertLiveCodexScope("after_pair_" + pair);
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
      maximumPeakInputTokens
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
        shared_upstream_placement_crossover_observed: sharedCacheCrossover
      }
    );
    return {
      schema: SCHEMA,
      kind: "release-champion-comparison",
      mode: reuseRuntimePerArm ? "live-isolated-reused-runtime" : "live-isolated",
      pass: comparison.pass,
      run_id: runId,
      cohort,
      settings,
      pair_order: orderedPairs,
      turn_order: reuseRuntimePerArm ? interleavedTurnOrders : null,
      aborted_after_pair: abortedAfterPair,
      warmup_pair_ids: scheduledPairIds(warmupPairs),
      scored_pair_ids: scoredPairIds,
      warmup_raw_runs: warmupRawRuns,
      champion,
      candidate,
      comparison
    };
  } finally {
    await rm(sourceSnapshot.root, { recursive: true, force: true });
  }
}

async function runIsolatedDynamicArm(spec) {
  let workspace = null;
  try {
    workspace = await startIsolatedRuntimeWorkspace(spec);
    return await runDynamicArmOnRuntime({ ...spec, runtime: workspace.runtime });
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
        upstreamUserAgent: arm === "champion"
          ? spec.championUpstreamUserAgent
          : spec.candidateUpstreamUserAgent,
        pinnedKeyId: spec.pinnedKeyId,
        forceUseSystemProxy: spec.forceUseSystemProxy,
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
      requestedPort: spec.requestedPort
    });
    return { tempRoot, runtime };
  } catch (error) {
    if (runtime) await stopChild(runtime.child, `${spec.arm} isolated startup`);
    await rm(tempRoot, { recursive: true, force: true });
    throw error;
  }
}

async function disposeIsolatedRuntimeWorkspace(workspace, label, keepRunDir) {
  if (!workspace) return;
  if (workspace.runtime) await stopChild(workspace.runtime.child, label);
  if (!keepRunDir) await rm(workspace.tempRoot, { recursive: true, force: true });
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
  const fixtureFamily = spec.fixtureFamily ?? null;
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
  return {
    spec,
    fixtureFamily,
    state: {
      input: [message(buildSeedContext(
        spec.settings.seed_context_chars,
        fixtureFamily,
        spec.settings.fixture_profile
      ))],
      compactionSeen: false
    },
    sessionId: conversationIdentity.session_id,
    threadId: conversationIdentity.thread_id,
    stableInstructions,
    requests: [],
    dynamicTailEvents: [],
    fatal: null
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
        spec.settings.dynamic_tail_profile
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
            eventOrdinal
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
          calls: spec.settings.tool_calls
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
      spec.settings.include_tool_schema
    );
    const record = await sendOneInbound({
      runtime: spec.runtime,
      sessionId: cursor.sessionId,
      threadId: cursor.threadId,
      cohort: spec.cohort,
      input: state.input,
      instructions: cursor.stableInstructions,
      maxOutputTokens: spec.settings.max_output_tokens,
      tools: fixtureTools,
      toolChoice: fixtureTools.length > 0 ? "none" : null,
      requestKind,
      phase,
      promptCacheKey: spec.promptCacheKey
    });
    cursor.requests.push(record);
    if (!record.pass) {
      cursor.fatal = record.failure ?? "inbound verification failed";
      return false;
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
    dynamicTailEvents: cursor.dynamicTailEvents,
    // A required guarded count applies to the candidate aggregate, not to
    // each fresh pair independently. Per-pair enforcement would abort a
    // healthy crossover after its first valid guarded successor and prevent
    // the order-balanced second pair from running.
    minimumGuardedRequests: 0,
    minimumSeedInputTokens: spec.settings.minimum_seed_input_tokens,
    minimumPeakInputTokens: spec.settings.minimum_peak_input_tokens,
    maximumPeakInputTokens: spec.settings.maximum_peak_input_tokens,
    requests,
    fatal,
    compactionSeen: state.compactionSeen
  });
}

function interleavedTurnOrder(pair, turn, pairOffset = 0, firstArm = "champion") {
  const candidateFirst = firstArm === "candidate";
  return (pair + turn + pairOffset + Number(candidateFirst)) % 2 === 0
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
  const turnOrder = [];
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
        champion.settings.first_arm
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
        turn_order: turnOrder
      };
    }
  }
  return {
    champion: finalizeScenarioCursor(cursors.champion),
    candidate: finalizeScenarioCursor(cursors.candidate),
    turn_order: turnOrder
  };
}

async function sendOneInbound(spec) {
  const before = await getJson(`${spec.runtime.baseUrl}/admin/metrics`, 10_000);
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
  try {
    const response = await fetch(`${spec.runtime.baseUrl}/codex/v1/responses`, {
      method: "POST",
      headers: {
        authorization: `Bearer ${spec.runtime.localKey}`,
        "content-type": "application/json",
        accept: "text/event-stream",
        "x-codex-turn-metadata": JSON.stringify({
          session_id: spec.sessionId,
          thread_id: spec.threadId,
          request_kind: spec.requestKind
        })
      },
      body: serializedBody,
      signal: AbortSignal.timeout(180_000)
    });
    responseStatus = response.status;
    responseText = await response.text();
  } catch (error) {
    transportError = safeErrorMessage(error);
  }
  const after = await waitForSettledInbound({
    baseUrl: spec.runtime.baseUrl,
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
  const responseFailureCode = responseErrorCode(responseText);
  const responseFailed = responseHasNativeFailure(responseText);
  const responseFailureKind = (
    !(responseStatus >= 200 && responseStatus < 300) || responseFailed
  ) ? responseErrorKind(responseText, responseFailureCode) : null;
  const terminal = responseStatus >= 200 && responseStatus < 300 &&
    /\bresponse\.completed\b/u.test(responseText) &&
    !responseFailed;
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
    timing_present: timingPresent
  };
  const failure = inboundFailureReason({
    transportError,
    responseStatus,
    responseFailureCode,
    responseFailed,
    checks
  });
  return {
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
  includeToolSchema = true
) {
  if (!includeToolSchema) return [];
  if (scenario === "dynamic-tail-mix" && dynamicTailMode === "text") return [];
  if (!new Set(["dynamic-tail-mix", "tool-burst", "tool-tail-maturity"]).has(scenario)) {
    return [];
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

async function waitForSettledInbound({ baseUrl, beforeCounters, knownRawInboundIds }) {
  const deadline = Date.now() + 15_000;
  let latest = null;
  do {
    latest = await getJson(`${baseUrl}/admin/metrics`, 5_000);
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
    if (/(?:upstream request failed|transport|connect|connection|dns|tls|proxy|timeout)/u.test(normalized)) {
      return "upstream_transport";
    }
    if (/failed to select provider key/u.test(normalized)) {
      return "provider_key_selection";
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
  responseStatus,
  responseFailureCode,
  responseFailed,
  checks
}) {
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
  maximumPeakInputTokens = 0
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
      "guarded_requests"
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
  const fields = ["input_full", "instructions", "tools_schema", "pre_input_wire"];
  if (!source || typeof source !== "object") return null;
  const result = {};
  for (const field of fields) {
    const value = typeof source[field] === "string" ? source[field] : "";
    if (!value) return null;
    result[field] = value;
  }
  return result;
}

// Compare the request evidence that actually left the proxy, not merely the
// nominal scenario fixture. The same client input can be serialized or
// transformed differently by the two versions; that is not valid A/B evidence
// even when a provider happens to return a cache hit.
function pairedInputSymmetry(champion, candidate, maxInputTokenDelta = 128) {
  return pairedRunInputSymmetry(
    array(champion?.runs),
    array(candidate?.runs),
    maxInputTokenDelta,
    true
  );
}

// Keep the dynamic-tail-specific diagnostic because its follow-up evidence is
// useful when tuning a growing context. Promotion itself uses the all-scenario
// proof above, so a full-replay or compaction comparison cannot bypass it.
function pairedDynamicInputSymmetry(champion, candidate, maxInputTokenDelta = 128) {
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
    dynamicExpected
  );
}

function pairedRunInputSymmetry(
  championRuns,
  candidateRuns,
  maxInputTokenDelta,
  required
) {
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
      request_kinds_match: true,
      terminal_sse_complete: true,
      pair_ids_valid: true,
      pair_ids_unique: true,
      all_pairs_have_requests: true,
      input_tokens_present: true,
      runs_present: false,
      scored_pair_ids: []
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
      if (!baselineOutbound || !contenderOutbound ||
        Object.keys(baselineOutbound).some((field) => baselineOutbound[field] !== contenderOutbound[field])) {
        outboundSemanticFingerprintsMatch = false;
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
  return {
    applicable: true,
    pass: runsPresent && pairsAligned && allPairsHaveRequests && requestPairCount > 0 && phasesMatch &&
      inputFingerprintsMatch && outboundSemanticFingerprintsMatch && requestKindsMatch &&
      terminalSseComplete && inputTokensPresent && maxWarmInputTokenDelta <= maxInputTokenDelta,
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
    request_kinds_match: requestKindsMatch,
    terminal_sse_complete: terminalSseComplete,
    pair_ids_valid: pairIdsValid,
    pair_ids_unique: championPairIdsUnique && candidatePairIdsUnique,
    all_pairs_have_requests: allPairsHaveRequests,
    input_tokens_present: inputTokensPresent,
    runs_present: runsPresent,
    scored_pair_ids: scoredPairIds
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
  const dynamicInputSymmetry = pairedDynamicInputSymmetry(
    champion,
    candidate,
    maxInputTokenDelta
  );
  const actualOutboundInputSymmetry = pairedInputSymmetry(
    champion,
    candidate,
    maxInputTokenDelta
  );
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
  const positiveCacheEvidence =
    aggregateTokenHitStrictlyImproves && providerInstabilityFree && seedCacheReadSymmetry &&
    coldSeedSymmetry && candidateNoExtraColdStart;
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
    actual_outbound_input_symmetry: actualOutboundInputSymmetry.pass,
    native_placement_isolation: nativePlacementIsolation.pass,
    upstream_placement_crossover: upstreamPlacementCrossover.pass,
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
    "seed_cache_read_symmetry",
    "cold_seed_evidence_complete",
    "cold_seed_request_symmetry",
    "cold_seed_symmetry",
    "candidate_no_extra_cold_start",
    "actual_outbound_input_symmetry",
    "native_placement_isolation",
    "upstream_placement_crossover",
    "candidate_raw_token_hit_not_lower",
    "candidate_cache_128_hit_not_lower",
    "candidate_warm_stable_prefix_hit_not_lower",
    "candidate_full_bucket_gate",
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
  return {
    // A passing baseline only means "not measurably worse". The user's
    // release rule is stronger: promotion additionally needs a strict,
    // provider-unconfounded cache gain.
    pass: baselinePass && positiveCacheEvidence,
    baseline_pass: baselinePass,
    cache_pass: cachePass,
    positive_cache_evidence: positiveCacheEvidence,
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
    native_placement_isolation: nativePlacementIsolation,
    upstream_placement_crossover: upstreamPlacementCrossover,
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
      raw_token_hit_rate: candidate.metrics.raw_token_hit_rate - champion.metrics.raw_token_hit_rate,
      cache_128_hit_rate: candidate.metrics.cache_128_hit_rate - champion.metrics.cache_128_hit_rate,
      warm_raw_token_hit_rate:
        candidate.metrics.warm_raw_token_hit_rate - champion.metrics.warm_raw_token_hit_rate,
      warm_cache_128_hit_rate:
        candidate.metrics.warm_cache_128_hit_rate - champion.metrics.warm_cache_128_hit_rate,
      warm_stable_prefix_hit_rate:
        candidate.metrics.warm_stable_prefix_hit_rate - champion.metrics.warm_stable_prefix_hit_rate,
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
    await rm(root, { recursive: true, force: true });
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
  const targetPath = normalizedProviderScope === "codex-agent"
    ? route?.target_path ?? ""
    : "";
  const targetConfigDigest = targetPath
    ? await readFile(targetPath)
      .then((contents) => createHash("sha256").update(contents).digest("hex"))
      .catch(() => "missing")
    : "none";
  return sha256Parts([
    "atoapi-release-champion-live-selection-scope-v2",
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
          target_path_digest: sha256Text(targetPath)
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
      key_pool: keyPoolMaterial,
      target_config_digest: targetConfigDigest
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
      `the hand-selected Codex route, model, Key realm, or target config changed at ${checkpoint}; live comparison stopped before more traffic was sent (expected=${expectedFingerprint.slice(0, 16)}, current=${currentFingerprint.slice(0, 16)})`
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
  requestedPort
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
  const child = spawn(executable, [], {
    cwd: repoRoot,
    windowsHide: true,
    stdio: "ignore",
    env: {
      ...process.env,
      ATOAPI_CONFIG_DIR: configDir,
      ATOAPI_ISOLATED_TEST_INSTANCE: "1",
      ATOAPI_TEST_LISTEN_PORT: String(port),
      ATOAPI_PREFIX_DIAGNOSTICS: "1",
      ATOAPI_AUTOMATIC_CACHE_CANARY: "0"
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

function buildToolFixtureItems({ pair, fixtureFamily = null, targetChars, shape, calls, eventOrdinal = 0 }) {
  const items = [];
  let remainingChars = targetChars;
  for (let index = 0; index < calls; index += 1) {
    const remainingCalls = calls - index;
    const chars = Math.floor(remainingChars / remainingCalls);
    remainingChars -= chars;
    const callId = releaseFixtureCallId(pair, fixtureFamily, index, calls, eventOrdinal);
    items.push(
      {
        type: "function_call",
        call_id: callId,
        name: "read_release_fixture",
        arguments: calls > 1 ? JSON.stringify({ part: index + 1, total_parts: calls }) : "{}"
      },
      {
        type: "function_call_output",
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

function dynamicTailProfileForTurn(turn, baseChars, baseCalls, tailProfile = "mixed") {
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
    thread_id: `release-champion-thread-${identityFamily}`
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
  maxAgeSeconds
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
  maxAgeSeconds
}) {
  const record = latest.record;
  const ageMs = Date.now() - latest.observedAtMs;
  const evidence = liveCodexGateEvidence({
    checkpoint,
    latest,
    expectedProviderId,
    expectedModel,
    expectedRealm,
    maxAgeSeconds
  });
  if (ageMs > maxAgeSeconds * 1_000 || ageMs < -60_000) {
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
  expectedProviderId,
  expectedModel,
  expectedRealm,
  maxAgeSeconds,
  checkpoint
}) {
  let metrics;
  try {
    metrics = await getJson(metricsUrl, 5_000);
  } catch (error) {
    const code = error instanceof FailClosedError
      ? error.code
      : "live_codex_metrics_unavailable";
    throw liveCodexGateError(
      code,
      "the live Codex metrics gate could not be evaluated at " + checkpoint,
      liveCodexGateEvidence({ checkpoint, maxAgeSeconds })
    );
  }
  let latest;
  try {
    latest = selectLatestLiveCodexMainRecordForExpectedScope(metrics, {
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
      liveCodexGateEvidence({ checkpoint, maxAgeSeconds })
    );
  }
  return validateLiveCodexMetricsScopeRecord({
    checkpoint,
    latest,
    expectedProviderId,
    expectedModel,
    expectedRealm,
    maxAgeSeconds
  });
}

async function getJson(url, timeoutMs) {
  const response = await fetch(url, { signal: AbortSignal.timeout(timeoutMs) });
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
  candidateExecutableSha256
}) {
  const champion = String(championUpstreamUserAgent ?? "").trim();
  const candidate = String(candidateUpstreamUserAgent ?? "").trim();
  const source = String(sourceCustomUserAgent ?? "").trim();

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
    [--scenario full-replay|tool-burst|dynamic-tail-mix|tool-tail-maturity|compacted-anchor|compaction-root] [--pairs 2] [--warmup-pairs 0] [--turns 6] \\
    [--pair-offset 0|1] [--first-arm champion|candidate] \\
    [--seed-context-chars <0-2500000>] [--minimum-seed-input-tokens <0-1000000>] \\
    [--minimum-peak-input-tokens <0-1000000>] [--maximum-peak-input-tokens <0-1000000>] \\
    [--max-input-token-delta <0-10000>] \\
    [--fixture-profile natural|natural-dense|legacy-repeated] [--dynamic-tail-profile mixed|natural-dense] \\
    [--tool-calls <1-8>] [--tool-output-shape natural|natural-dense|flat|structured|noisy] \\
    [--include-tool-schema true|false] \\
    [--turn-delay-ms <0-5000>] [--inter-arm-delay-ms <0-5000>] [--pair-delay-ms <0-60000>] \\
    [--reuse-fixture-across-pairs] \\
    [--isolate-upstream-cache] \\
    [--reuse-runtime-per-arm] [--shared-cache-crossover] \\
    [--max-local-proxy-overhead-regression-ms <0-500>] \\
    [--require-ttft-no-regression] \\
    [--max-full-bucket-regression-requests <calibrated-count>] \\
    [--upstream-user-agent <test-only-stable-value>] \\
    [--champion-upstream-user-agent <value>] [--candidate-upstream-user-agent <value>] \\
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
  --fixture-profile natural is the default: an equal-length, deterministic,
  non-repeated synthetic context. legacy-repeated exists only to reproduce
  older fixture behavior; neither profile uses user context or changes Atoapi.
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
      promptCacheKey: "fixture-key"
    }),
    {
      model: "request-model",
      stream: true,
      store: false,
      max_output_tokens: 16,
      instructions: "stable",
      input: [],
      prompt_cache_key: "fixture-key"
    },
    "live champion fixtures must retain Codex's store=false wire contract"
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
  assert.deepEqual(
    [1, 3, 5, 7, 9].map((turn) => dynamicTailProfileForTurn(turn, 16_384, 1).shape),
    ["natural", "structured", "noisy", "flat", "natural"]
  );
  assert.equal(dynamicTailProfileForTurn(2, 16_384, 1), null);
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
    responseErrorKind('{"error":{"code":"unsupported_parameter"}}'),
    "code:unsupported_parameter",
    "an opaque upstream code is retained without the error body"
  );
  assert.equal(
    inboundFailureReason({
      transportError: null,
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
  assert.equal(asymmetricSeedVerdict.baseline_pass, false);
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
      fatal: "Bearer secret https://example.invalid/private/fatal",
      executable: { path: "C:/private/path/secret.exe", sha256: "b".repeat(64) },
      requests: run.requests.map((request) => ({
        ...request,
        compacted_input: [{ type: "input_text", text: "secret request body" }],
        raw_request_body: "Bearer secret request body",
        failure: "fatal https://example.invalid/private/fatal",
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
  assert.equal(postPairGateDiagnostic.diagnostic_arm_aggregates.champion.completed_run_count, 1);
  assert.equal(postPairGateDiagnostic.diagnostic_arm_aggregates.candidate.completed_run_count, 1);
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
    "a genuinely unbalanced shared-cache seed allocation must still fail closed"
  );
  assert.equal(unbalancedSeedVerdict.baseline_pass, false);
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
