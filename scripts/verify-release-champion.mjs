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
  if (scenario === "dynamic-tail-mix" && (turns < 11 || turns % 2 === 0)) {
    throw new FailClosedError(
      "dynamic_tail_mix_turn_count",
      "--scenario dynamic-tail-mix requires an odd turn count of at least 11 so every tail profile gets a follow-up"
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
  const toolChars = boundedInteger(options["tool-chars"] ?? 32_768, "--tool-chars", 1_024, 512_000);
  const toolCalls = boundedInteger(options["tool-calls"] ?? 1, "--tool-calls", 1, 8);
  const toolOutputShape = normalizeToolOutputShape(options["tool-output-shape"] ?? "natural");
  const fixtureProfile = normalizeFixtureProfile(options["fixture-profile"] ?? "natural");
  if (!fixtureProfile) {
    throw new FailClosedError(
      "invalid_fixture_profile",
      "--fixture-profile must be natural or legacy-repeated"
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
  // A bounded foreground prefix-maturity wait is an intentional cache
  // optimization, not an unbounded local regression. Keep the default strict
  // for ordinary comparisons, but let a live verification explicitly model
  // the user-approved 500ms ceiling when it is exercising that path.
  const maxLocalProxyOverheadRegressionMs = boundedInteger(
    options["max-local-proxy-overhead-regression-ms"] ?? 0,
    "--max-local-proxy-overhead-regression-ms",
    0,
    500
  );
  const maxFullBucketRegressionRequests = boundedInteger(
    options["max-full-bucket-regression-requests"] ?? 0,
    "--max-full-bucket-regression-requests",
    0,
    pairs * turns
  );
  const sharedUpstreamUserAgent = optionalUpstreamUserAgent(options["upstream-user-agent"]);
  const championUpstreamUserAgent = optionalUpstreamUserAgent(
    options["champion-upstream-user-agent"]
  ) ?? sharedUpstreamUserAgent;
  const candidateUpstreamUserAgent = optionalUpstreamUserAgent(
    options["candidate-upstream-user-agent"]
  ) ?? sharedUpstreamUserAgent;
  const keepRunDir = booleanArg(options["keep-run-dir"]);
  const isolateUpstreamCache = booleanArg(options["isolate-upstream-cache"]);
  const requestedReuseRuntimePerArm = booleanArg(options["reuse-runtime-per-arm"]);
  // An isolated-cache arm must not keep a process-owned upstream connection
  // pool across pairs. Otherwise an upstream placement can remain permanently
  // attached to the champion/candidate role even after the metadata lane is
  // crossed over, making a same-binary control look like a product regression.
  const reuseRuntimePerArm = effectiveReuseRuntimePerArm(
    requestedReuseRuntimePerArm,
    isolateUpstreamCache
  );
  const pinnedKeyId = optionalOpaqueIdentifier(options["key-id"], "--key-id");
  const forceUseSystemProxy = optionalBoolean(
    options["force-use-system-proxy"],
    "--force-use-system-proxy"
  );
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
    const configProviderId = codexRoute?.provider_id ?? "";
    if (!configProviderId) {
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
    const providerId = String(options["provider-id"] ?? configProviderId).trim();
    if (!providerId || providerId !== configProviderId) {
      throw new FailClosedError(
        "provider_scope_mismatch",
        "--provider-id must match the Codex provider_id in the snapshotted source config"
      );
    }
    if (!model) {
      throw new FailClosedError("invalid_model", "--model must not be empty");
    }
    // A Codex binding may leave model_id empty, meaning the Codex client
    // chooses its model per request. If it is explicitly bound, never let a
    // release comparison silently test a different model on the same Provider.
    assertCodexRouteModelScope(codexRoute, model);
    validatePinnedKeyConfiguration(configText, providerId, pinnedKeyId);
    const liveSelectionScopeFingerprint = await currentLiveSelectionScopeFingerprint(
      sourceConfigDir,
      configText
    );
    await assertLiveSelectionScopeUnchanged(
      sourceConfigDir,
      liveSelectionScopeFingerprint,
      "before_isolated_runtime_start"
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
    const settings = {
      scenario,
      pairs,
      pair_offset: pairOffset,
      first_arm: firstArm,
      turns,
      max_output_tokens: maxOutputTokens,
      stable_instruction_chars: stableInstructionChars,
      seed_context_chars: seedContextChars,
      minimum_seed_input_tokens: minimumSeedInputTokens,
      tool_chars: toolChars,
      tool_calls: toolCalls,
      tool_output_shape: toolOutputShape,
      fixture_profile: fixtureProfile,
      fresh_fixture_per_pair: freshFixturePerPair,
      turn_delay_ms: turnDelayMs,
      pair_delay_ms: pairDelayMs,
      require_candidate_guarded_requests: requireCandidateGuardedRequests,
      client_prompt_cache_key: Boolean(options["prompt-cache-key-prefix"]),
      max_ttft_regression_ms: maxTtftRegressionMs,
      max_local_proxy_overhead_regression_ms: maxLocalProxyOverheadRegressionMs,
      max_full_bucket_regression_requests: maxFullBucketRegressionRequests,
      champion_upstream_user_agent: championUpstreamUserAgent,
      candidate_upstream_user_agent: candidateUpstreamUserAgent,
      forced_use_system_proxy: forceUseSystemProxy,
      isolate_upstream_cache: isolateUpstreamCache,
      isolation_lane_strategy: isolateUpstreamCache ? "pair-crossover-v1" : "shared-v1",
      reuse_runtime_per_arm: reuseRuntimePerArm,
      reuse_runtime_per_arm_requested: requestedReuseRuntimePerArm,
      runtime_reuse_disabled_for_isolation:
        requestedReuseRuntimePerArm && isolateUpstreamCache,
      key_id_pinned: Boolean(pinnedKeyId),
      hand_selected_model_bound: Boolean(codexRoute.model_id),
      live_selection_scope_guard: "fail_closed"
    };
    const artifacts = {
      champion: await executableArtifact(championExe),
      candidate: await executableArtifact(candidateExe)
    };
    const armRuns = { champion: [], candidate: [] };
    const orderedPairs = [];
    const interleavedTurnOrders = [];
    let abortedAfterPair = null;
    let persistentArmRuntimes = null;
    try {
      if (reuseRuntimePerArm) {
        persistentArmRuntimes = await startPersistentIsolatedArmRuntimes({
          championExe,
          candidateExe,
          sourceConfigDir: sourceSnapshot.configDir,
          configProviderId,
          requestedPort,
          championUpstreamUserAgent,
          candidateUpstreamUserAgent,
          pinnedKeyId,
          keepRunDir
        });
      }

      for (let pair = 0; pair < pairs; pair += 1) {
        await assertLiveSelectionScopeUnchanged(
          sourceConfigDir,
          liveSelectionScopeFingerprint,
          `before_pair_${pair}`
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
          const lane = sha256Parts([
            "release-champion-lane-v2",
            runId,
            keyRealmHash,
            requestFamily,
            pair,
            isolationLane ?? arm
          ]);
          return {
            arm,
            executable,
            sourceConfigDir: sourceSnapshot.configDir,
            configProviderId,
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
          armRuns.champion.push(result.champion);
          armRuns.candidate.push(result.candidate);
          pairResult = result;
        } else {
          // The one-runtime-per-arm fallback retains the previous pair-level
          // alternation, because the two isolated processes do not coexist.
          const order = interleavedTurnOrder(pair, 0, pairOffset, firstArm);
          orderedPairs.push(order);
          const results = {};
          for (const arm of order) {
            await assertLiveSelectionScopeUnchanged(
              sourceConfigDir,
              liveSelectionScopeFingerprint,
              `before_${arm}_pair_${pair}`
            );
            const armSpec = armSpecFor(arm);
            const result = await runIsolatedDynamicArm(armSpec);
            armRuns[arm].push(result);
            results[arm] = result;
          }
          pairResult = results;
        }
        await assertLiveSelectionScopeUnchanged(
          sourceConfigDir,
          liveSelectionScopeFingerprint,
          `after_pair_${pair}`
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

    const champion = aggregateArm("champion", cohort, artifacts.champion, armRuns.champion);
    const candidate = aggregateArm(
      "candidate",
      cohort,
      artifacts.candidate,
      armRuns.candidate,
      requireCandidateGuardedRequests
    );
    const comparison = compareArmResults(
      champion,
      candidate,
      maxTtftRegressionMs,
      maxLocalProxyOverheadRegressionMs,
      maxFullBucketRegressionRequests
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
    for (const arm of ["champion", "candidate"]) {
      workspaces[arm] = await startIsolatedRuntimeWorkspace({
        arm,
        executable: arm === "champion" ? spec.championExe : spec.candidateExe,
        sourceConfigDir: spec.sourceConfigDir,
        configProviderId: spec.configProviderId,
        upstreamUserAgent: arm === "champion"
          ? spec.championUpstreamUserAgent
          : spec.candidateUpstreamUserAgent,
        pinnedKeyId: spec.pinnedKeyId,
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
      ? dynamicTailProfileForTurn(turn, spec.settings.tool_chars, spec.settings.tool_calls)
      : null;
    if (dynamicTail) {
      const eventOrdinal = dynamicTail.ordinal;
      const scopedFixtureFamily = fixtureFamily
        ? `${fixtureFamily}-tail-${eventOrdinal}`
        : `tail-${eventOrdinal}`;
      state.input.push(
        ...buildToolFixtureItems({
          pair: spec.pair,
          fixtureFamily: scopedFixtureFamily,
          targetChars: dynamicTail.targetChars,
          shape: dynamicTail.shape,
          calls: dynamicTail.calls,
          eventOrdinal
        }),
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

    const record = await sendOneInbound({
      runtime: spec.runtime,
      sessionId: cursor.sessionId,
      threadId: cursor.threadId,
      cohort: spec.cohort,
      input: state.input,
      instructions: cursor.stableInstructions,
      maxOutputTokens: spec.settings.max_output_tokens,
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
      for (const arm of order) {
        const advanced = await advanceScenarioCursor(cursors[arm], turn);
        if (!advanced) {
          terminalFailure = true;
          break;
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
  const startedAt = Date.now();
  let responseStatus = 0;
  let responseText = "";
  let transportError = null;
  try {
    const body = buildResponsesRequestBody(spec);
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
      body: JSON.stringify(body),
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
  const responseFailureCode = responseErrorCode(responseText);
  const responseFailed = responseHasNativeFailure(responseText);
  const responseFailureKind = (
    !(responseStatus >= 200 && responseStatus < 300) || responseFailed
  ) ? responseErrorKind(responseText, responseFailureCode) : null;
  const terminal = responseStatus >= 200 && responseStatus < 300 &&
    /\bresponse\.completed\b/u.test(responseText) &&
    !responseFailed;
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
  const completedInput = spec.requestKind === "compaction"
    ? extractCompactionItems(responseText)
    : [];
  const timing = {
    prefix_guard_wait_ms: finiteNonNegativeNumber(metric?.prefix_guard_wait_ms),
    local_prepare_ms: finiteNonNegativeNumber(metric?.local_prepare_ms),
    upstream_headers_ms: finiteNonNegativeNumber(metric?.upstream_headers_ms),
    upstream_first_chunk_ms: finiteNonNegativeNumber(metric?.upstream_first_chunk_ms),
    upstream_ttft_ms: finiteNonNegativeNumber(metric?.upstream_ttft_ms),
    ttft_ms: finiteNonNegativeNumber(metric?.ttft_ms)
  };
  const timingPresent = !terminal || [
    timing.prefix_guard_wait_ms,
    timing.local_prepare_ms,
    timing.upstream_ttft_ms,
    timing.ttft_ms
  ].every((value) => value !== null);
  const checks = {
    terminal_response_completed: terminal,
    exact_counter_delta: exactCounterDelta,
    per_inbound_one_attempt_one_post: perInboundSingleAttempt,
    aggregate_no_multi_attempt: aggregateSingleAttempt,
    metric_present: Boolean(metric),
    provider_matches_cohort: providerMatches,
    model_matches_cohort: modelMatches,
    observed_key_realm_present: observedRealmPresent,
    observed_key_realm_matches_cohort: observedKeyRealmMatches,
    usage_present: number(metric?.input_tokens) > 0,
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
    elapsed_ms: Date.now() - startedAt,
    sse_completed: terminal,
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
    outbound_prefix_fingerprints: metric?.outbound_prefix_fingerprints ?? null,
    prefix_lag_classification: metric?.prefix_lag_classification ?? null,
    prefix_lag_input_delta_tokens: number(metric?.prefix_lag_input_delta_tokens),
    prefix_lag_cache_delta_tokens: number(metric?.prefix_lag_cache_delta_tokens),
    prefix_lag_previous_gap_tokens: number(metric?.prefix_lag_previous_gap_tokens),
    prefix_cache_instability_score: number(metric?.prefix_cache_instability_score),
    prefix_state_cache_read_tokens: number(metric?.prefix_state_cache_read_tokens),
    input_tokens: number(metric?.input_tokens),
    cache_read_tokens: number(metric?.cache_read_tokens),
    cache_avoidable_gap_tokens: number(metric?.cache_avoidable_gap_tokens),
    cache_new_tail_gap_tokens: number(metric?.cache_new_tail_gap_tokens),
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
    // Keep the local and upstream portions separate in the release evidence.
    // This is diagnostic only: the release gate below still evaluates total
    // TTFT, so an upstream regression cannot be silently reclassified away.
    prefix_guard_wait_ms: timing.prefix_guard_wait_ms,
    prefix_guard_wait_reason: metric?.prefix_guard_wait_reason ?? null,
    prefix_guard_wait_source: metric?.prefix_guard_wait_source ?? null,
    prefix_guard_skip_reason: metric?.prefix_guard_skip_reason ?? null,
    static_wire_drift_late_mutation_categories:
      metric?.static_wire_drift_late_mutation_categories ?? null,
    local_prepare_ms: timing.local_prepare_ms,
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
  if (spec.promptCacheKey) body.prompt_cache_key = spec.promptCacheKey;
  return body;
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
  const warm = cacheable.filter((item) => item.phase !== "seed");
  const inputTokens = sum(comparable, "input_tokens");
  const cacheReadTokens = sum(comparable, "cache_read_tokens");
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
  const observedRealms = unique(
    comparable.map((item) => item.observed_realm_id).filter(Boolean)
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
  const usageCoverage = requests.length === 0 ? 0 : comparable.length / requests.length;
  const timing = timingSummary(comparable);
  const staticWire = staticWireContinuity(requests);
  const seedRequest = requests.find((item) => item.phase === "seed");
  const seedInputTokens = number(seedRequest?.input_tokens);
  const seedCacheReadTokens = number(seedRequest?.cache_read_tokens);
  const dynamicTail = dynamicTailRecoverySummary(input.dynamicTailEvents, requests);
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
    seed_input_tokens: seedInputTokens,
    seed_cache_read_tokens: seedCacheReadTokens,
    cache_read_tokens: cacheReadTokens,
    raw_token_hit_rate: ratio(cacheReadTokens, inputTokens),
    cacheable_tokens_128: cacheableTokens,
    cacheable_read_tokens_128: cacheableReadTokens,
    cache_128_hit_rate: ratio(cacheableReadTokens, cacheableTokens),
    warm_stable_prefix_tokens_128: warmCacheableTokens,
    warm_stable_prefix_cached_tokens_128: warmCacheableReadTokens,
    warm_stable_prefix_hit_rate: ratio(warmCacheableReadTokens, warmCacheableTokens),
    full_bucket_requests: fullBuckets,
    full_bucket_rate: ratio(fullBuckets, cacheable.length),
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
    complete_timing_coverage: metrics.timing_complete_requests === comparable.length,
    input_usage_present: inputTokens > 0,
    required_seed_input_tokens:
      seedInputTokens >= number(input.minimumSeedInputTokens),
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

function aggregateArm(arm, cohort, executable, runs, minimumGuardedRequests = 0) {
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
      "cache_read_tokens",
      "cacheable_tokens_128",
      "cacheable_read_tokens_128",
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
  metrics.cache_128_hit_rate = ratio(
    metrics.cacheable_read_tokens_128,
    metrics.cacheable_tokens_128
  );
  metrics.warm_stable_prefix_hit_rate = ratio(
    metrics.warm_stable_prefix_cached_tokens_128,
    metrics.warm_stable_prefix_tokens_128
  );
  // Per-run full bucket rates may have different denominators.  The raw run
  // object does not expose a separate count in older artifacts, so derive it
  // from the retained request evidence when available and otherwise fail closed.
  const retainedRequests = normalized.flatMap((run) => array(run.requests));
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
  Object.assign(metrics, timingSummary(comparableRows));
  metrics.usage_coverage = normalized.length > 0 && normalized.every(
    (run) => number(run.metrics?.usage_coverage) === 1
  ) ? 1 : 0;
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

function aggregateSeedCacheReadTokens(aggregate) {
  const explicit = Number(aggregate?.metrics?.seed_cache_read_tokens);
  if (Number.isFinite(explicit)) return explicit;
  for (const run of aggregate?.runs ?? []) {
    const seed = (run.requests ?? []).find((item) => item.phase === "seed");
    if (seed) return number(seed.cache_read_tokens);
  }
  return 0;
}

function compareArmResults(
  champion,
  candidate,
  maxTtftRegressionMs,
  maxLocalProxyOverheadRegressionMs = 0,
  maxFullBucketRegressionRequests = 0
) {
  const cohortMatches = sameCohort(champion.cohort, candidate.cohort);
  const observedRealmMatches = champion.metrics.observed_realm_ids.length === 1 &&
    candidate.metrics.observed_realm_ids.length === 1 &&
    champion.metrics.observed_realm_ids[0] === candidate.metrics.observed_realm_ids[0];
  const championSeedCacheReadTokens = aggregateSeedCacheReadTokens(champion);
  const candidateSeedCacheReadTokens = aggregateSeedCacheReadTokens(candidate);
  const seedCacheReadSymmetry = championSeedCacheReadTokens === candidateSeedCacheReadTokens;
  const fullBucketRequestDelta =
    candidate.metrics.full_bucket_requests - champion.metrics.full_bucket_requests;
  const fullBucketDenominatorsMatch =
    candidate.metrics.full_bucket_denominator === champion.metrics.full_bucket_denominator;
  const fullBucketCountWithinTolerance =
    fullBucketDenominatorsMatch && fullBucketRequestDelta >= -maxFullBucketRegressionRequests;
  // A full-bucket request is a useful discrete signal, but it must not veto a
  // demonstrably better aggregate cache result merely because one request
  // crossed a 128-token boundary differently. A stable-prefix ratio can also
  // already be saturated: equal is the best possible result there, so require
  // it not to regress while raw/cacheable hit and a real shortfall measure
  // strictly improve.
  const aggregateTokenHitStrictlyImproves =
    candidate.metrics.raw_token_hit_rate > champion.metrics.raw_token_hit_rate &&
    candidate.metrics.cache_128_hit_rate > champion.metrics.cache_128_hit_rate &&
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
    aggregateTokenHitStrictlyImproves && providerInstabilityFree && seedCacheReadSymmetry;
  const checks = {
    champion_valid: champion.pass,
    candidate_valid: candidate.pass,
    cohort_matches: cohortMatches,
    observed_key_realm_matches: observedRealmMatches,
    seed_cache_read_symmetry: seedCacheReadSymmetry,
    candidate_raw_token_hit_not_lower:
      candidate.metrics.raw_token_hit_rate >= champion.metrics.raw_token_hit_rate,
    candidate_cache_128_hit_not_lower:
      candidate.metrics.cache_128_hit_rate >= champion.metrics.cache_128_hit_rate,
    candidate_warm_stable_prefix_hit_not_lower:
      candidate.metrics.warm_stable_prefix_hit_rate >= champion.metrics.warm_stable_prefix_hit_rate,
    candidate_full_bucket_rate_not_lower:
      candidate.metrics.full_bucket_rate >= champion.metrics.full_bucket_rate,
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
    candidate_local_proxy_overhead_p95_not_regressed:
      candidate.metrics.local_proxy_overhead_p95_ms <=
      champion.metrics.local_proxy_overhead_p95_ms + maxLocalProxyOverheadRegressionMs,
    candidate_ttft_p95_not_regressed:
      candidate.metrics.ttft_p95_ms <= champion.metrics.ttft_p95_ms + maxTtftRegressionMs
  };
  const gatingChecks = { ...checks };
  delete gatingChecks.candidate_full_bucket_rate_not_lower;
  delete gatingChecks.candidate_full_bucket_count_within_tolerance;
  delete gatingChecks.candidate_full_bucket_loss_explained_by_token_gain;
  delete gatingChecks.provider_instability_free;
  delete gatingChecks.seed_cache_read_symmetry;
  delete gatingChecks.candidate_cache_strictly_improves;
  delete gatingChecks.candidate_positive_cache_evidence;
  // Remote TTFT belongs to the hand-selected upstream and has its own visible
  // verdict below. A cache-preserving local candidate must not be rejected
  // solely because this pair saw provider-side timing variance.
  delete gatingChecks.candidate_ttft_p95_not_regressed;
  const cacheCheckNames = [
    "champion_valid",
    "candidate_valid",
    "cohort_matches",
    "observed_key_realm_matches",
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
  const localLatencyPass = checks.candidate_local_proxy_overhead_p95_not_regressed;
  const latencyPass = checks.candidate_ttft_p95_not_regressed;
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
    checks,
    deltas: {
      raw_token_hit_rate: candidate.metrics.raw_token_hit_rate - champion.metrics.raw_token_hit_rate,
      cache_128_hit_rate: candidate.metrics.cache_128_hit_rate - champion.metrics.cache_128_hit_rate,
      warm_stable_prefix_hit_rate:
        candidate.metrics.warm_stable_prefix_hit_rate - champion.metrics.warm_stable_prefix_hit_rate,
      full_bucket_rate: candidate.metrics.full_bucket_rate - champion.metrics.full_bucket_rate,
      full_bucket_requests: fullBucketRequestDelta,
      avoidable_gap_tokens:
        candidate.metrics.avoidable_gap_tokens - champion.metrics.avoidable_gap_tokens,
      new_tail_gap_tokens:
        candidate.metrics.new_tail_gap_tokens - champion.metrics.new_tail_gap_tokens,
      provider_unstable_gap_tokens:
        candidate.metrics.provider_unstable_gap_tokens - champion.metrics.provider_unstable_gap_tokens,
      seed_cache_read_tokens:
        candidateSeedCacheReadTokens - championSeedCacheReadTokens,
      local_proxy_overhead_p95_ms:
        candidate.metrics.local_proxy_overhead_p95_ms - champion.metrics.local_proxy_overhead_p95_ms,
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
  const maxLocalProxyOverheadRegressionMs = boundedInteger(
    options["max-local-proxy-overhead-regression-ms"] ?? 0,
    "--max-local-proxy-overhead-regression-ms",
    0,
    500
  );
  const maxFullBucketRegressionRequests = boundedInteger(
    options["max-full-bucket-regression-requests"] ?? 0,
    "--max-full-bucket-regression-requests",
    0,
    Math.max(champion.metrics.full_bucket_denominator, candidate.metrics.full_bucket_denominator)
  );
  const comparison = compareArmResults(
    champion,
    candidate,
    maxTtftRegressionMs,
    maxLocalProxyOverheadRegressionMs,
    maxFullBucketRegressionRequests
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
      max_full_bucket_regression_requests: maxFullBucketRegressionRequests
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
  if (value.arm !== expectedArm) {
    throw new FailClosedError(
      "invalid_offline_artifact_arm",
      `expected ${expectedArm} result but received ${String(value.arm)}`
    );
  }
  if (!isCompleteCohort(value.cohort)) {
    throw new FailClosedError("invalid_offline_cohort", "result JSON has no complete comparison cohort");
  }
  const metrics = value.metrics ?? {};
  for (const field of [
    "input_tokens",
    "cache_read_tokens",
    "raw_token_hit_rate",
    "cache_128_hit_rate",
    "warm_stable_prefix_hit_rate",
    "full_bucket_rate",
    "avoidable_gap_tokens",
    "ttft_p95_ms"
  ]) {
    if (!Number.isFinite(Number(metrics[field]))) {
      throw new FailClosedError("invalid_offline_metrics", `result JSON metric ${field} is missing`);
    }
  }
  return value;
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

async function currentLiveSelectionScopeFingerprint(sourceConfigDir, configText = null) {
  const text = configText ?? await readRequiredText(
    join(sourceConfigDir, "config.toml"),
    "live source config.toml"
  );
  const route = codexAgentRoute(text);
  if (!route?.enabled || !route.provider_id) {
    throw new FailClosedError(
      "live_selection_scope_unavailable",
      "the live Codex route is not enabled or has no selected Provider"
    );
  }
  const providerBlock = tomlArrayBlocks(text, "providers")
    .map((item) => item.body)
    .find((item) => extractTomlString(item, "id") === route.provider_id);
  if (!providerBlock) {
    throw new FailClosedError(
      "live_selection_scope_unavailable",
      "the live Codex Provider is not present in the source config"
    );
  }
  const keyPool = providerKeyPoolContext(text, route.provider_id);
  const keyPoolMaterial = keyPool
    ? {
      enabled: extractTomlBoolean(keyPool.pool.body, "enabled"),
      strategy: extractTomlString(keyPool.pool.body, "strategy"),
      keys: keyPool.keys.map((key) => ({
        id: extractTomlString(key.body, "id"),
        enabled: extractTomlBoolean(key.body, "enabled"),
        disabled_until: extractTomlRawValue(key.body, "disabled_until"),
        material_digest: sha256Text(extractTomlString(key.body, "key_encrypted"))
      }))
    }
    : null;
  const targetPath = route.target_path;
  const targetConfigDigest = targetPath
    ? await readFile(targetPath)
      .then((contents) => createHash("sha256").update(contents).digest("hex"))
      .catch(() => "missing")
    : "none";
  return sha256Parts([
    "atoapi-release-champion-live-selection-scope-v1",
    JSON.stringify({
      route: {
        provider_id: route.provider_id,
        model_id: route.model_id,
        enabled: route.enabled,
        target_path_digest: sha256Text(targetPath)
      },
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

async function assertLiveSelectionScopeUnchanged(sourceConfigDir, expectedFingerprint, checkpoint) {
  const currentFingerprint = await currentLiveSelectionScopeFingerprint(sourceConfigDir);
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

function timingSummary(rows) {
  const comparable = array(rows);
  const compaction = comparable.filter((item) => item.phase === "compaction");
  const p95 = (items, project, empty = 0) => {
    const values = items.map(project).filter((value) => Number.isFinite(value) && value >= 0);
    return values.length === items.length && values.length > 0 ? percentile(values, 95) : empty;
  };
  return {
    timing_complete_requests: comparable.filter((item) => item.checks?.timing_present).length,
    local_proxy_overhead_p95_ms: p95(
      comparable,
      (item) => number(item.prefix_guard_wait_ms) + number(item.local_prepare_ms)
    ),
    upstream_ttft_p95_ms: p95(comparable, (item) => item.upstream_ttft_ms),
    ttft_p95_ms: p95(comparable, (item) => item.ttft_ms),
    compaction_request_count: compaction.length,
    compaction_local_proxy_overhead_p95_ms: p95(
      compaction,
      (item) => number(item.prefix_guard_wait_ms) + number(item.local_prepare_ms),
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
    cache_read_tokens: 0,
    raw_token_hit_rate: 0,
    cacheable_tokens_128: 0,
    cacheable_read_tokens_128: 0,
    cache_128_hit_rate: 0,
    warm_stable_prefix_tokens_128: 0,
    warm_stable_prefix_cached_tokens_128: 0,
    warm_stable_prefix_hit_rate: 0,
    full_bucket_requests: 0,
    full_bucket_rate: 0,
    cacheable_request_count: 0,
    full_bucket_denominator: 0,
    avoidable_gap_tokens: 0,
    new_tail_gap_tokens: 0,
    provider_unstable_gap_tokens: 0,
    shortfall_tokens: 0,
    guarded_requests: 0,
    timing_complete_requests: 0,
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

function buildToolOutput(targetChars, shape, fixtureFamily = null, partIndex = 0, partCount = 1) {
  const partLabel = partCount > 1 ? ` tool_part=${partIndex + 1}/${partCount}` : "";
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

function dynamicTailProfileForTurn(turn, baseChars, baseCalls) {
  if (turn <= 0 || turn % 2 === 0) return null;
  const profiles = [
    { shape: "natural", scale: 0.25, calls_delta: 0 },
    { shape: "structured", scale: 0.5, calls_delta: 1 },
    { shape: "noisy", scale: 0.75, calls_delta: 2 },
    { shape: "flat", scale: 1, calls_delta: 3 },
    { shape: "natural", scale: 0.35, calls_delta: 1 }
  ];
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

function normalizeToolOutputShape(value) {
  const normalized = String(value).trim().toLowerCase().replace(/_/gu, "-");
  if (new Set(["natural", "flat", "structured", "noisy"]).has(normalized)) return normalized;
  throw new FailClosedError(
    "invalid_tool_output_shape",
    "--tool-output-shape must be natural, flat, structured, or noisy"
  );
}

function normalizeFixtureProfile(value) {
  const normalized = String(value).trim().toLowerCase().replace(/_/gu, "-");
  return new Set(["natural", "legacy-repeated"]).has(normalized)
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

function ratio(numerator, denominator) {
  return denominator > 0 ? numerator / denominator : 0;
}

function finiteNonNegativeNumber(value) {
  const parsed = Number(value);
  return Number.isFinite(parsed) && parsed >= 0 ? parsed : null;
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
    --key-realm-hash <opaque-hash> [--provider-id <id>] \\
    [--scenario full-replay|tool-burst|dynamic-tail-mix|tool-tail-maturity|compacted-anchor|compaction-root] [--pairs 2] [--turns 6] \\
    [--pair-offset 0|1] [--first-arm champion|candidate] \\
    [--seed-context-chars <0-2500000>] [--minimum-seed-input-tokens <0-1000000>] [--fixture-profile natural|legacy-repeated] \\
    [--tool-calls <1-8>] [--tool-output-shape natural|flat|structured|noisy] \\
    [--turn-delay-ms <0-5000>] [--pair-delay-ms <0-60000>] \\
    [--reuse-fixture-across-pairs] \\
    [--isolate-upstream-cache] \\
    [--reuse-runtime-per-arm] \\
    [--max-local-proxy-overhead-regression-ms <0-500>] \\
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
  --fixture-profile natural is the default: an equal-length, deterministic,
  non-repeated synthetic context. legacy-repeated exists only to reproduce
  older fixture behavior; neither profile uses user context or changes Atoapi.
  --pair-delay-ms is test-only pacing between fresh pairs; it never changes a
  request body, retries an inbound, or touches the running service. --first-arm
  and --pair-offset only choose which isolated arm sends first; they never alter
  the selected Provider, Key, proxy, request body, or upstream call count.`);
}

async function runSelfTest() {
  assert.deepEqual(parseArgs(["--pairs=2", "--live", "--model", "m"]), {
    pairs: "2",
    live: true,
    model: "m"
  });
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
  assert.equal(optionalBoolean("true", "--force-use-system-proxy"), true);
  assert.equal(optionalBoolean("false", "--force-use-system-proxy"), false);
  assert.throws(
    () => optionalBoolean("maybe", "--force-use-system-proxy"),
    (error) => error?.code === "invalid_boolean_parameter"
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
  assert.equal(normalizeFixtureProfile("natural"), "natural");
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
  assert.equal(buildSeedContext(128).length, 128);
  const pairFixtureA = "fixture-pair-a";
  const pairFixtureB = "fixture-pair-b";
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
    runs: [],
    metrics: {
      requests: 4,
      successful_sse_requests: 4,
      input_tokens: 4096,
      seed_input_tokens: 1024,
      seed_cache_read_tokens: 0,
      cache_read_tokens: raw * 4096,
      raw_token_hit_rate: raw,
      cacheable_tokens_128: 4096,
      cacheable_read_tokens_128: raw * 4096,
      cache_128_hit_rate: raw,
      warm_stable_prefix_tokens_128: 3072,
      warm_stable_prefix_cached_tokens_128: raw * 3072,
      warm_stable_prefix_hit_rate: raw,
      full_bucket_requests: raw === 1 ? 4 : 3,
      full_bucket_rate: raw === 1 ? 1 : 0.75,
      cacheable_request_count: 4,
      full_bucket_denominator: 4,
      avoidable_gap_tokens: 0,
      new_tail_gap_tokens: 0,
      provider_unstable_gap_tokens: 0,
      shortfall_tokens: 0,
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
    true,
    "remote TTFT variance must not veto otherwise unconfounded cache evidence"
  );
  assert.equal(remoteTtftVarianceVerdict.latency_pass, false);
  const oneFullBucketBehind = valid("candidate", 0.9);
  oneFullBucketBehind.metrics.full_bucket_requests = 2;
  oneFullBucketBehind.metrics.full_bucket_rate = 0.5;
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
  const asymmetricSeedVerdict = compareArmResults(
    asymmetricSeedChampion,
    asymmetricSeedCandidate,
    0
  );
  assert.equal(asymmetricSeedVerdict.checks.seed_cache_read_symmetry, false);
  assert.equal(asymmetricSeedVerdict.positive_cache_evidence, false);
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
  assert.notEqual(
    scopedFingerprint,
    await currentLiveSelectionScopeFingerprint(
      tmpdir(),
      scopedConfig.replace('channel = "responses"', 'channel = "chat"')
    ),
    "a selected upstream channel change must invalidate live evidence"
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
