import { createHash, randomUUID } from "node:crypto";
import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import {
  copyFile,
  mkdtemp,
  mkdir,
  readFile,
  rm,
  stat
} from "node:fs/promises";
import { homedir, tmpdir } from "node:os";
import { basename, dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { createConnection } from "node:net";

const args = parseArgs(process.argv.slice(2));
const runId = String(args["run-id"] ?? randomUUID()).trim();
const model = String(args.model ?? process.env.ATOAPI_TEST_MODEL ?? "gpt-5.6-luna").trim();
const mode = String(args.mode ?? "full").trim().toLowerCase();
const lane = normalizeLane(args.lane ?? "tool_burst_quarantine");
const evidencePerWindow = lane === "compacted_anchor" ? 4 : 3;
const targetSuccessfulPerArm = boundedNumber(
  args["target-successes"] ?? 50,
  9,
  500
);
const targetInputTokensPerArm = boundedNumber(
  args["target-input-tokens"] ?? 5_000_000,
  100_000,
  50_000_000
);
const maxProbes = boundedNumber(args["max-probes"] ?? 120, 2, 2_000);
const maxProbeFailures = boundedNumber(args["max-probe-failures"] ?? 5, 0, 50);
const maxExperimentFailures = boundedNumber(args["max-experiment-failures"] ?? 3, 0, 20);
const toolChars = boundedNumber(args["tool-chars"] ?? 280_000, 80_000, 600_000);
const stableInstructionChars = boundedNumber(
  args["stable-instruction-chars"] ?? 24_000,
  4_096,
  120_000
);
const compactionHistoryChars = boundedNumber(
  args["compaction-history-chars"] ?? 300_000,
  80_000,
  600_000
);
const compactionSummaryChars = boundedNumber(
  args["compaction-summary-chars"] ?? 220_000,
  40_000,
  500_000
);
const maxInputTokens = boundedNumber(
  args["max-input-tokens"] ?? targetInputTokensPerArm * (lane === "compacted_anchor" ? 5 : 4),
  targetInputTokensPerArm * 2,
  100_000_000
);
const requestedPort = boundedNumber(args.port ?? 18_885, 1_024, 65_533);
const keepRunDir = booleanArg(args["keep-run-dir"]);
const forceCanary = booleanArg(args["force-canary"]);
const fixedWindows = args["fixed-windows"] === undefined
  ? null
  : boundedNumber(args["fixed-windows"], 1, 20);

if (!runId) failUsage("--run-id must not be empty.");
if (!model) failUsage("Set ATOAPI_TEST_MODEL or pass --model.");
if (!new Set(["observe", "full"]).has(mode)) {
  failUsage("--mode must be observe or full.");
}
if (fixedWindows !== null && mode !== "observe") {
  failUsage("--fixed-windows is diagnostic-only and requires --mode observe.");
}
if (forceCanary && mode !== "full") {
  failUsage("--force-canary requires --mode full.");
}
if (!lane) failUsage("--lane must be tool_burst_quarantine or compacted_anchor.");
if (booleanArg(args["self-test"])) {
  runSelfTest();
  process.exit(0);
}

const repoRoot = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const responses = [];
const probes = [];
const selectedSessions = { baseline: [], candidate: [] };
const selected = { baseline: 0, candidate: 0 };
let requestCount = 0;
let probeFailures = 0;
let experimentFailures = 0;
let runtime = null;
let beforeAffinity = null;
let observeWindows = 0;
let canaryWindows = 0;
let phase = "observe";
let runError = null;
let cohortPredictor = null;
const stableInstructions = buildStableInstructions(stableInstructionChars);

try {
  runtime = args.url
    ? await useExternalRuntime()
    : await createIsolatedRuntime(false);
  beforeAffinity = await getJson(`${runtime.baseUrl}/admin/cache-affinity`);

  await findCohortSessions();
  const observeResult = await collectShadowEvidence();
  let finalAffinity = observeResult.affinity;

  if (
    mode === "full" &&
    (observeResult.readiness?.status === "ready_for_canary" ||
      (forceCanary &&
        observeResult.readiness?.status === "canary_healthy" &&
        observeResult.readiness?.reason === "no_addressable_post_burst_gap"))
  ) {
    if (!runtime.managed) {
      throw new Error(
        "full mode requires a managed isolated instance so canary admission cannot affect a live proxy"
      );
    }
    await restartIsolatedRuntimeWithCanary();
    phase = "canary";
    finalAffinity = await collectCanaryEvidence();
  }

  const result = buildResult(finalAffinity);
  console.log(JSON.stringify(result, null, 2));
  if (!result.pass) process.exitCode = 1;
} catch (error) {
  runError = error;
  throw error;
} finally {
  try {
    await cleanupRuntime();
  } catch (cleanupError) {
    if (!runError) throw cleanupError;
    console.error(
      `cleanup also failed: ${cleanupError instanceof Error ? cleanupError.message : cleanupError}`
    );
  }
}

async function useExternalRuntime() {
  const baseUrl = String(args.url).replace(/\/+$/u, "");
  const localKey = String(args.key ?? process.env.ATOAPI_LOCAL_KEY ?? "").trim();
  if (!localKey) {
    failUsage("External mode requires ATOAPI_LOCAL_KEY or --key.");
  }
  await getJson(`${baseUrl}/health`);
  return {
    baseUrl,
    localKey,
    managed: false,
    child: null,
    configDir: null,
    port: null,
    executable: null
  };
}

async function createIsolatedRuntime(canaryEnabled, existing = null) {
  const sourceConfigDir = configuredSourceConfigDir();
  const configDir = existing?.configDir ??
    await mkdtemp(join(tmpdir(), `atoapi-accept-${safeRunId(runId)}-`));
  if (!existing) {
    await copyRuntimeConfig(sourceConfigDir, configDir);
  }
  const configText = await readFile(join(configDir, "config.toml"), "utf8");
  const localKey = extractTomlString(configText, "local_key");
  if (!localKey) {
    throw new Error(`isolated config ${join(configDir, "config.toml")} has no local_key`);
  }
  const port = existing?.port ?? await findAvailablePort(requestedPort);
  const baseUrl = `http://127.0.0.1:${port}`;
  const executable = existing?.executable ?? await resolveExecutable();
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
      ATOAPI_AUTOMATIC_CACHE_CANARY: canaryEnabled ? "1" : "0",
      ATOAPI_FORCE_CACHE_CANARY: canaryEnabled && forceCanary ? "1" : "0"
    }
  });
  await waitForHealth(baseUrl, child);
  return { baseUrl, localKey, managed: true, child, configDir, port, executable };
}

async function restartIsolatedRuntimeWithCanary() {
  const previous = runtime;
  await stopChild(previous.child);
  runtime = await createIsolatedRuntime(true, previous);
}

async function cleanupRuntime() {
  if (!runtime?.managed) return;
  await stopChild(runtime.child);
  if (!keepRunDir && runtime.configDir) {
    await rm(runtime.configDir, { recursive: true, force: true });
  }
}

async function findCohortSessions() {
  const configText = await readFile(
    join(runtime.configDir ?? configuredSourceConfigDir(), "config.toml"),
    "utf8"
  );
  const workspaceFingerprint = extractTomlString(configText, "workspace_fingerprint");
  const providerId = codexProviderId(configText);
  if (!workspaceFingerprint || !providerId) {
    throw new Error("could not derive the isolated Codex cohort identity from config.toml");
  }

  let calibration;
  let seed = null;
  while (!seed) {
    if (probes.length >= maxProbes) {
      throw new Error(`calibration seed requests exceeded the ${maxProbes} limit`);
    }
    calibration = newSession("calibration");
    seed = await verifySessionSeed(calibration, null);
  }
  const effectiveModel = String(seed.metric?.model ?? model).trim();
  cohortPredictor = createCohortPredictor({
    workspaceFingerprint,
    providerId,
    effectiveModel
  });
  const arm = String(seed.metric?.shadow_affinity_arm ?? "");
  if (cohortPredictor(calibration.threadId) !== arm) {
    throw new Error(
      `offline cohort predictor disagreed with the proxy: ` +
      `predicted=${cohortPredictor(calibration.threadId)}, actual=${arm}`
    );
  }
  selectSession(calibration, arm, true);
  for (const requiredArm of ["baseline", "candidate"]) {
    if (selectedSessions[requiredArm].length === 0) {
      selectedSessions[requiredArm].push(await createVerifiedSession(requiredArm));
    }
  }
}

async function collectShadowEvidence() {
  const maximumWindows = fixedWindows ?? Math.max(
    Math.ceil(targetSuccessfulPerArm / evidencePerWindow) + 12,
    24
  );
  let affinity = await getJson(`${runtime.baseUrl}/admin/cache-affinity`);
  let readiness = targetReadiness(affinity);
  for (let index = 0; index < maximumWindows; index += 1) {
    if (fixedWindows === null && shadowTargetsReached(readiness)) break;
    const baseline = await takeVerifiedSession("baseline");
    const candidate = await takeVerifiedSession("candidate");
    const pair = index % 2 === 0
      ? [baseline, candidate]
      : [candidate, baseline];
    for (const session of pair) {
      await runAffinityWindow(session, index + 1);
    }
    observeWindows += 1;
    affinity = await getJson(`${runtime.baseUrl}/admin/cache-affinity`);
    readiness = targetReadiness(affinity);
    printProgress("observe", readiness);
  }
  if (fixedWindows === null && !shadowTargetsReached(readiness)) {
    throw new Error(
      `shadow evidence did not reach ${targetSuccessfulPerArm} successes and ` +
      `${targetInputTokensPerArm} input tokens per arm within ${maximumWindows} windows`
    );
  }
  return { affinity, readiness };
}

async function collectCanaryEvidence() {
  const maximumWindows = 8;
  let affinity = await getJson(`${runtime.baseUrl}/admin/cache-affinity`);
  for (let index = 0; index < maximumWindows; index += 1) {
    const readiness = targetReadiness(affinity);
    if (canaryReachedTerminalGate(readiness)) break;
    const candidate = await createVerifiedSession("candidate");
    const baseline = await createVerifiedSession("baseline");
    for (const session of [candidate, baseline]) {
      await runAffinityWindow(session, observeWindows + index + 1);
    }
    canaryWindows += 1;
    affinity = await getJson(`${runtime.baseUrl}/admin/cache-affinity`);
    printProgress("canary", targetReadiness(affinity));
  }
  return affinity;
}

async function takeVerifiedSession(arm) {
  return selectedSessions[arm].shift() ?? createVerifiedSession(arm);
}

async function createVerifiedSession(desiredArm) {
  if (!cohortPredictor) throw new Error("cohort predictor is not initialized");
  for (;;) {
    if (probes.length >= maxProbes) {
      throw new Error(`verified seed requests exceeded the ${maxProbes} limit`);
    }
    let session;
    do {
      session = newSession(desiredArm);
    } while (cohortPredictor(session.threadId) !== desiredArm);
    const seed = await verifySessionSeed(session, desiredArm);
    if (!seed) continue;
    selectSession(session, desiredArm, false);
    return session;
  }
}

function newSession(label) {
  const sessionRunId = randomUUID();
  const seedInput = [
    message(`Acceptance ${runId}, ${label} session ${sessionRunId}. Reply with OK only.`)
  ];
  return {
    sessionId: `atoapi-accept-${runId}-${sessionRunId}`,
    threadId: `atoapi-accept-${runId}-${sessionRunId}`,
    sessionRunId,
    seedInput,
    input: seedInput
  };
}

async function verifySessionSeed(session, expectedArm) {
  const seed = await sendRequest(session, "seed", true);
  const arm = String(seed.metric?.shadow_affinity_arm ?? "");
  const hasUsage = number(seed.metric?.input_tokens) > 0;
  const validArm = new Set(["baseline", "candidate"]).has(arm);
  const selectable = seed.completed && hasUsage && validArm &&
    (expectedArm === null || arm === expectedArm);
  probes.push({
    index: probes.length + 1,
    arm: validArm ? arm : null,
    status: seed.status,
    completed: seed.completed,
    hasUsage,
    selected: selectable,
    error: seed.error
  });
  if (selectable) {
    session.arm = arm;
    return seed;
  }
  probeFailures += 1;
  if (probeFailures > maxProbeFailures) {
    const diagnosticMetrics = await getJson(`${runtime.baseUrl}/admin/metrics`);
    const latestError = diagnosticMetrics.recent_errors?.[0];
    throw new Error(
      `probe failures or missing usage exceeded ${maxProbeFailures}; ` +
      `last status=${seed.status}, arm=${arm || "missing"}, usage=${hasUsage}, ` +
      `proxy_error=${latestError ? `${latestError.scope}:${latestError.message}` : "missing"}`
    );
  }
  return null;
}

function selectSession(session, arm, enqueue) {
  session.arm = arm;
  selected[arm] += 1;
  if (enqueue && !selectedSessions[arm].includes(session)) {
    selectedSessions[arm].push(session);
  }
}

async function runAffinityWindow(session, windowIndex) {
  if (lane === "compacted_anchor") {
    return runCompactionWindow(session, windowIndex);
  }
  return runPostBurstWindow(session, windowIndex);
}

async function runPostBurstWindow(session, windowIndex) {
  const suffix = `${phase}-${windowIndex}-${session.arm}-${randomUUID().replaceAll("-", "")}`;
  const toolCallId = `call_${suffix}`;
  session.input = [
    ...session.seedInput,
    { type: "function_call", call_id: toolCallId, name: "read_test_log", arguments: "{}" },
    {
      type: "function_call_output",
      call_id: toolCallId,
      output: buildToolOutput(toolChars, suffix)
    },
    message("Use the completed tool result and reply with OK only.")
  ];
  await sendExperimentRequest(session, `${phase}_${session.arm}_giant_tail`);
  for (let followup = 1; followup <= 3; followup += 1) {
    session.input.push(message(`Follow-up ${followup}: reply with OK only.`));
    await sendExperimentRequest(
      session,
      `${phase}_${session.arm}_followup_${followup}`
    );
  }
}

async function runCompactionWindow(session, windowIndex) {
  const suffix = `${phase}-${windowIndex}-${session.arm}-${randomUUID().replaceAll("-", "")}`;
  session.input = [
    message(buildCompactionHistory(compactionHistoryChars, suffix)),
    message("Continue from this history and reply with OK only.")
  ];
  await sendExperimentRequest(session, `${phase}_${session.arm}_pre_compaction`);

  session.input.push(
    message("Summarize the conversation state for the next turn, then reply with OK only.")
  );
  const compacted = await sendExperimentRequest(
    session,
    `${phase}_${session.arm}_compaction`,
    "compaction"
  );
  if (compacted.completed && compacted.metric?.cache_status !== "compact") {
    throw new Error(
      `trusted compaction was not recorded as compact for ${session.arm}: ` +
      `${compacted.metric?.cache_status ?? "missing"}`
    );
  }

  session.input = [message(buildCompactedSummary(compactionSummaryChars, suffix))];
  for (let followup = 1; followup <= evidencePerWindow; followup += 1) {
    session.input.push(message(`Post-compaction follow-up ${followup}: reply with OK only.`));
    await sendExperimentRequest(
      session,
      `${phase}_${session.arm}_post_compaction_${followup}`
    );
  }
}

async function sendExperimentRequest(session, requestPhase, requestKind = "turn") {
  const result = await sendRequest(session, requestPhase, true, requestKind);
  if (!result.completed) {
    experimentFailures += 1;
    if (experimentFailures > maxExperimentFailures) {
      throw new Error(
        `experiment failures exceeded ${maxExperimentFailures}; ` +
        `last phase=${requestPhase}, status=${result.status}`
      );
    }
  }
  return result;
}

async function sendRequest(
  session,
  requestPhase,
  allowFailure = false,
  requestKind = "turn"
) {
  enforceTokenBudget();
  const before = await getJson(`${runtime.baseUrl}/admin/metrics`);
  const beforeCounters = requestCounters(before);
  const previousInboundId = String(before.recent_requests?.[0]?.inbound_request_id ?? "");
  const startedAt = Date.now();
  requestCount += 1;
  let responseStatus = 0;
  let responseBody = "";
  let caughtError = null;
  try {
    const response = await fetch(`${runtime.baseUrl}/codex/v1/responses`, {
      method: "POST",
      headers: {
        authorization: `Bearer ${runtime.localKey}`,
        "content-type": "application/json",
        accept: "text/event-stream",
        "x-codex-turn-metadata": JSON.stringify({
          session_id: session.sessionId,
          thread_id: session.threadId,
          request_kind: requestKind
        })
      },
      body: JSON.stringify({
        model,
        stream: true,
        max_output_tokens: 16,
        instructions: stableInstructions,
        input: session.input
      }),
      signal: AbortSignal.timeout(180_000)
    });
    responseStatus = response.status;
    responseBody = await response.text();
  } catch (error) {
    caughtError = error;
  }

  const after = await waitForRequestFinalization(beforeCounters);
  const counters = subtractCounters(requestCounters(after), beforeCounters);
  const latest = after.recent_requests?.[0] ?? null;
  const metric = latest &&
    String(latest.inbound_request_id ?? "") !== previousInboundId
      ? latest
      : null;
  const completed = !caughtError && responseStatus >= 200 && responseStatus < 300 &&
    (responseBody.includes("response.completed") || responseBody.includes("[DONE]"));
  const result = {
    phase: requestPhase,
    arm: session.arm ?? null,
    session_id: session.sessionId,
    status: responseStatus,
    elapsedMs: Date.now() - startedAt,
    completed,
    counters,
    metric: compactMetric(metric),
    error: caughtError
      ? caughtError instanceof Error ? caughtError.message : String(caughtError)
      : completed ? null : responseBody.slice(0, 240)
  };
  responses.push(result);
  if (!completed && !allowFailure) {
    throw new Error(
      `${requestPhase} failed: HTTP ${responseStatus}; ` +
      `terminal=${completed}; body=${responseBody.slice(0, 240)}`
    );
  }
  return result;
}

async function waitForRequestFinalization(before) {
  const deadline = Date.now() + 5_000;
  let current = null;
  do {
    current = await getJson(`${runtime.baseUrl}/admin/metrics`);
    const counters = requestCounters(current);
    if (
      counters.inboundRequests >= before.inboundRequests + 1 &&
      counters.generationAttempts >= before.generationAttempts + 1 &&
      counters.upstreamRequests >= before.upstreamRequests + 1
    ) {
      return current;
    }
    await delay(50);
  } while (Date.now() < deadline);
  return current;
}

function buildResult(finalAffinity) {
  const diagnosticMode = fixedWindows !== null;
  const readiness = targetReadiness(finalAffinity);
  const exact = responses.reduce(
    (sum, item) => ({
      inboundRequests: sum.inboundRequests + item.counters.inboundRequests,
      generationAttempts: sum.generationAttempts + item.counters.generationAttempts,
      upstreamRequests: sum.upstreamRequests + item.counters.upstreamRequests,
      inputTokens: sum.inputTokens + number(item.metric?.input_tokens),
      automaticApplications: sum.automaticApplications +
        Number(item.metric?.shadow_affinity_decision === "automatic_candidate_applied")
    }),
    {
      inboundRequests: 0,
      generationAttempts: 0,
      upstreamRequests: 0,
      inputTokens: 0,
      automaticApplications: 0
    }
  );
  const baseline = readiness?.baseline ?? {};
  const candidate = readiness?.candidate_shadow ?? {};
  const targetChecks = {
    baseline: armReachedTarget(baseline),
    candidate: armReachedTarget(candidate)
  };
  const selectedSeeds = probes.filter((item) => item.selected);
  const compactionRequests = responses.filter((item) =>
    isCompactionBoundaryPhase(item.phase)
  );
  const expectedEvidence =
    observeWindows * 2 * evidencePerWindow + canaryWindows * 2 * evidencePerWindow;
  const status = String(readiness?.status ?? "missing");
  const modeOutcome = mode === "observe"
    ? new Set(["ready_for_canary", "canary_healthy"]).has(status)
    : new Set(["canary_healthy", "ready_for_promotion"]).has(status);
  const checks = {
    managedIsolatedRun: runtime.managed,
    foundBothArms: selected.baseline > 0 && selected.candidate > 0,
    selectedSeedsHaveUsage: selectedSeeds.length >= 2 &&
      selectedSeeds.every((item) => item.hasUsage),
    probeFailuresWithinBudget: probeFailures <= maxProbeFailures,
    experimentFailuresWithinBudget: experimentFailures <= maxExperimentFailures,
    everySuccessfulExperimentCompleted: responses
      .filter((item) => item.phase !== "seed")
      .every((item) => item.completed),
    oneInboundPerRequest: exact.inboundRequests === requestCount,
    oneAttemptPerInbound: exact.generationAttempts === requestCount,
    oneUpstreamPerInbound: exact.upstreamRequests === requestCount,
    capturedOnlyThisRun: number(beforeAffinity?.evidence_records) === 0,
    capturedEveryFollowup:
      number(finalAffinity.evidence_records) === expectedEvidence,
    compactionBoundariesConfirmed: lane !== "compacted_anchor" ||
      (compactionRequests.length === (observeWindows + canaryWindows) * 2 &&
        compactionRequests.every(
          (item) => item.completed && item.metric?.cache_status === "compact"
        )),
    baselineTargetReached: diagnosticMode || targetChecks.baseline,
    candidateTargetReached: diagnosticMode || targetChecks.candidate,
    usageCoverageReached: diagnosticMode ||
      (number(baseline.usage_coverage_bps) >= 8_000 &&
        number(candidate.usage_coverage_bps) >= 8_000),
    stayedWithinTokenBudget: exact.inputTokens <= maxInputTokens,
    noOpenEvidenceWindows: number(finalAffinity.open_windows) === 0,
    modeResult: diagnosticMode || modeOutcome
  };
  const pass = Object.values(checks).every(Boolean);
  return {
    runId,
    isolated: runtime.managed,
    port: runtime.port,
    executable: runtime.executable,
    model,
    mode,
    lane,
    limits: {
      targetSuccessfulPerArm,
      targetInputTokensPerArm,
      maxInputTokens,
      maxProbes,
      maxProbeFailures,
      maxExperimentFailures,
      toolChars,
      stableInstructionChars,
      compactionHistoryChars,
      compactionSummaryChars,
      fixedWindows
    },
    selected,
    probeSummary: {
      total: probes.length,
      baseline: probes.filter((item) => item.arm === "baseline").length,
      candidate: probes.filter((item) => item.arm === "candidate").length,
      failuresOrMissingUsage: probeFailures,
      selectedAt: selectedSeeds.map((item) => ({ index: item.index, arm: item.arm }))
    },
    windows: { observe: observeWindows, canary: canaryWindows },
    requestSummary: {
      total: requestCount,
      completed: responses.filter((item) => item.completed).length,
      failed: responses.filter((item) => !item.completed).map((item) => ({
        phase: item.phase,
        status: item.status,
        error: item.error
      })),
      ...exact
    },
    timingSummary: summarizeTiming(responses),
    compactionProfile: classifyCompactionPrefixDrift(
      responses
        .filter((item) => compactionPhase(item.phase) !== null)
        .map(compactionProfileEntry)
    ),
    affinity: {
      evidenceRecords: number(finalAffinity.evidence_records),
      readiness: readiness ?? null,
      rollbackKeys: finalAffinity.rollback_keys ?? []
    },
    promotionEligible: status === "ready_for_promotion",
    decision: diagnosticMode
      ? "diagnostic_only"
      : status === "ready_for_promotion"
        ? "candidate_can_be_promoted"
        : "candidate_remains_disabled",
    checks,
    pass
  };
}

function summarizeTiming(entries) {
  const groups = new Map();
  for (const entry of entries) {
    const metric = entry.metric;
    if (!metric || entry.phase === "seed") continue;
    const key = `${entry.arm ?? "unknown"}:${metric.shadow_affinity_decision ?? "unknown"}`;
    const bucket = groups.get(key) ?? [];
    bucket.push({
      ttft: number(metric.ttft_ms),
      elapsed: number(entry.elapsedMs),
      headers: number(metric.upstream_headers_ms),
      processing: number(metric.upstream_reported_processing_ms),
      nonProcessing: number(metric.upstream_non_processing_ms),
      firstChunk: number(metric.upstream_first_chunk_ms)
    });
    groups.set(key, bucket);
  }
  return Object.fromEntries(
    [...groups.entries()].map(([key, samples]) => [key, {
      observations: samples.length,
      ttft_p50_ms: percentile(samples.map((sample) => sample.ttft), 50),
      ttft_p95_ms: percentile(samples.map((sample) => sample.ttft), 95),
      elapsed_p50_ms: percentile(samples.map((sample) => sample.elapsed), 50),
      elapsed_p95_ms: percentile(samples.map((sample) => sample.elapsed), 95),
      upstream_headers_p95_ms: percentile(samples.map((sample) => sample.headers), 95),
      upstream_processing_p95_ms: percentile(samples.map((sample) => sample.processing), 95),
      upstream_non_processing_p95_ms: percentile(samples.map((sample) => sample.nonProcessing), 95),
      upstream_first_chunk_p95_ms: percentile(samples.map((sample) => sample.firstChunk), 95)
    }])
  );
}

function percentile(values, percentileValue) {
  const sorted = values.filter((value) => value > 0).sort((left, right) => left - right);
  if (sorted.length === 0) return 0;
  const index = Math.min(
    sorted.length - 1,
    Math.max(0, Math.ceil(sorted.length * percentileValue / 100) - 1)
  );
  return sorted[index];
}

function shadowTargetsReached(readiness) {
  return armReachedTarget(readiness?.baseline) &&
    armReachedTarget(readiness?.candidate_shadow);
}

function armReachedTarget(summary) {
  if (!summary) return false;
  return number(summary.successful_observations) >= targetSuccessfulPerArm &&
    armInputTokens(summary) >= targetInputTokensPerArm;
}

function armInputTokens(summary) {
  return number(summary.average_input_tokens) * number(summary.observations);
}

function canaryReachedTerminalGate(readiness) {
  const status = String(readiness?.status ?? "");
  if (
    forceCanary &&
    status === "canary_healthy" &&
    number(readiness?.candidate_applied?.observations) < 18
  ) {
    return false;
  }
  return new Set([
    "canary_healthy",
    "ready_for_promotion",
    "rollback_required"
  ]).has(status);
}

function targetReadiness(affinity) {
  return (affinity.readiness ?? []).find(
    (item) => item.lane === lane
  ) ?? null;
}

function printProgress(label, readiness) {
  if (!readiness) return;
  const baseline = readiness.baseline ?? {};
  const candidate = readiness.candidate_shadow ?? {};
  console.error(
    `[${label}] baseline=${number(baseline.successful_observations)}/` +
    `${armInputTokens(baseline)} candidate=${number(candidate.successful_observations)}/` +
    `${armInputTokens(candidate)} status=${readiness.status}:${readiness.reason}`
  );
}

function requestCounters(metrics) {
  return {
    inboundRequests: number(metrics.agent_generation?.inbound_requests),
    generationAttempts: number(metrics.agent_generation?.generation_attempts),
    upstreamRequests: number(metrics.upstream_requests)
  };
}

function subtractCounters(after, before) {
  return {
    inboundRequests: after.inboundRequests - before.inboundRequests,
    generationAttempts: after.generationAttempts - before.generationAttempts,
    upstreamRequests: after.upstreamRequests - before.upstreamRequests
  };
}

function compactMetric(metric) {
  if (!metric) return null;
  return {
    inbound_request_id: metric.inbound_request_id ?? null,
    model: metric.model ?? null,
    status: number(metric.status),
    input_tokens: number(metric.input_tokens),
    cache_read_tokens: number(metric.cache_read_tokens),
    ttft_ms: number(metric.ttft_ms),
    upstream_attempts: number(metric.upstream_attempts),
    shadow_affinity_arm: metric.shadow_affinity_arm ?? null,
    shadow_affinity_lane: metric.shadow_affinity_lane ?? null,
    shadow_affinity_decision: metric.shadow_affinity_decision ?? null,
    cache_status: metric.cache_status ?? null,
    upstream_call_source: metric.upstream_call_source ?? null,
    shadow_affinity_realm_id: metric.shadow_affinity_realm_id ?? null,
    upstream_http_version: metric.upstream_http_version ?? null,
    upstream_network_path: metric.upstream_network_path ?? null,
    upstream_remote_addr: metric.upstream_remote_addr ?? null,
    upstream_pool_diagnostic: metric.upstream_pool_diagnostic ?? null,
    upstream_trace_source: metric.upstream_trace_source ?? null,
    upstream_server_timing: metric.upstream_server_timing ?? null,
    upstream_timing_source: metric.upstream_timing_source ?? null,
    upstream_reported_processing_ms: number(metric.upstream_reported_processing_ms),
    upstream_non_processing_ms: number(metric.upstream_non_processing_ms),
    upstream_headers_ms: number(metric.upstream_headers_ms),
    upstream_first_chunk_ms: number(metric.upstream_first_chunk_ms),
    outbound_prefix_fingerprints: compactPrefixFingerprints(
      metric.outbound_prefix_fingerprints
    )
  };
}

function compactPrefixFingerprints(fingerprints) {
  if (!fingerprints || typeof fingerprints !== "object") return null;
  return {
    version: number(fingerprints.version),
    cache_metadata: String(fingerprints.cache_metadata ?? ""),
    instructions: String(fingerprints.instructions ?? ""),
    tools_schema: String(fingerprints.tools_schema ?? ""),
    input_history: String(fingerprints.input_history ?? ""),
    input_full: String(fingerprints.input_full ?? ""),
    input_item_count: number(fingerprints.input_item_count),
    input_prefixes: Array.isArray(fingerprints.input_prefixes)
      ? fingerprints.input_prefixes.slice(-32).map(String)
      : [],
    pre_input_wire: String(fingerprints.pre_input_wire ?? "")
  };
}

function compactionProfileEntry(item) {
  const inputTokens = number(item.metric?.input_tokens);
  const cacheReadTokens = number(item.metric?.cache_read_tokens);
  return {
    phase: item.phase,
    phase_kind: compactionPhase(item.phase),
    arm: item.arm,
    session_id: item.session_id,
    completed: item.completed,
    input_tokens: inputTokens,
    cache_read_tokens: cacheReadTokens,
    cache_ratio_bps: inputTokens > 0
      ? Math.round(cacheReadTokens * 10_000 / inputTokens)
      : 0,
    ttft_ms: number(item.metric?.ttft_ms),
    upstream_attempts: number(item.metric?.upstream_attempts),
    cache_status: item.metric?.cache_status ?? null,
    upstream_call_source: item.metric?.upstream_call_source ?? null,
    shadow_affinity_realm_id: item.metric?.shadow_affinity_realm_id ?? null,
    upstream_http_version: item.metric?.upstream_http_version ?? null,
    upstream_network_path: item.metric?.upstream_network_path ?? null,
    upstream_remote_addr: item.metric?.upstream_remote_addr ?? null,
    upstream_pool_diagnostic: item.metric?.upstream_pool_diagnostic ?? null,
    upstream_trace_source: item.metric?.upstream_trace_source ?? null,
    upstream_server_timing: item.metric?.upstream_server_timing ?? null,
    upstream_timing_source: item.metric?.upstream_timing_source ?? null,
    upstream_reported_processing_ms: number(
      item.metric?.upstream_reported_processing_ms
    ),
    upstream_non_processing_ms: number(item.metric?.upstream_non_processing_ms),
    upstream_headers_ms: number(item.metric?.upstream_headers_ms),
    outbound_prefix_fingerprints: item.metric?.outbound_prefix_fingerprints ?? null
  };
}

function classifyCompactionPrefixDrift(entries) {
  const lastBySession = new Map();
  return entries.map((entry) => {
    const previous = lastBySession.get(entry.session_id);
    const prefix_drift = classifyPrefixDrift(previous, entry);
    lastBySession.set(entry.session_id, entry);
    return { ...entry, prefix_drift };
  });
}

function classifyPrefixDrift(previous, current) {
  const currentHashes = current.outbound_prefix_fingerprints;
  if (!currentHashes || !previous) {
    return currentHashes ? "initial_phase" : "prefix_diagnostics_unavailable";
  }
  const previousHashes = previous.outbound_prefix_fingerprints;
  if (!previousHashes || previousHashes.version !== currentHashes.version) {
    return "prefix_diagnostics_unavailable";
  }
  if (previousHashes.cache_metadata !== currentHashes.cache_metadata) {
    return "cache_key_changed";
  }
  if (previousHashes.instructions !== currentHashes.instructions) {
    return "instructions_changed";
  }
  if (previousHashes.tools_schema !== currentHashes.tools_schema) {
    return "tools_changed";
  }
  const preservedInputPrefix = currentHashes.input_prefixes.includes(
    previousHashes.input_full
  ) || previousHashes.input_full === currentHashes.input_history;
  if (!preservedInputPrefix) {
    return "history_prefix_changed";
  }
  if (previousHashes.pre_input_wire !== currentHashes.pre_input_wire) {
    return "pre_input_wire_changed";
  }
  return "stable_prefix_preserved";
}

function compactionPhase(value) {
  const phaseValue = String(value);
  if (/_pre_compaction$/u.test(phaseValue)) return "pre_compaction";
  if (/_compaction$/u.test(phaseValue)) return "compaction";
  if (/_post_compaction_\d+$/u.test(phaseValue)) return "post_compaction";
  return null;
}

function enforceTokenBudget() {
  const used = responses.reduce(
    (sum, item) => sum + number(item.metric?.input_tokens),
    0
  );
  if (used >= maxInputTokens) {
    throw new Error(`acceptance run reached its ${maxInputTokens} input-token hard limit`);
  }
}

async function copyRuntimeConfig(sourceDir, targetDir) {
  await mkdir(targetDir, { recursive: true });
  await copyFile(join(sourceDir, "config.toml"), join(targetDir, "config.toml"));
  const keyFile = join(sourceDir, "cache-key.dpapi");
  if (await exists(keyFile)) {
    await copyFile(keyFile, join(targetDir, basename(keyFile)));
  }
}

function codexProviderId(configText) {
  return tomlArrayBlocks(configText, "agent_injections")
    .map((block) => block.body)
    .find((block) => extractTomlString(block, "id") === "codex")
    ?.match(/^provider_id\s*=\s*"([^"]+)"/mu)?.[1] ?? "";
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
    const nextSection = text.indexOf("\n[[", start + marker.length);
    const end = nextSection >= 0 ? nextSection + 1 : text.length;
    return { body: text.slice(start, end) };
  });
}

function createCohortPredictor({ workspaceFingerprint, providerId, effectiveModel }) {
  return (threadId) => {
    const conversationId = hashParts([
      "trusted-conversation-v1",
      workspaceFingerprint,
      "codex",
      providerId,
      effectiveModel,
      "thread-id",
      threadId
    ]);
    const digest = createHash("sha256").update(conversationId).digest();
    return digest.readUInt32LE(0) % 100 < 5 ? "candidate" : "baseline";
  };
}

function hashParts(parts) {
  const hasher = createHash("sha256");
  for (const part of parts) {
    hasher.update(String(part));
    hasher.update(Buffer.from([0]));
  }
  return hasher.digest("hex");
}

function runSelfTest() {
  const config = [
    'workspace_fingerprint = "default-workspace"',
    "",
    "[[agent_injections]]",
    'id = "other"',
    'provider_id = "provider-other"',
    "",
    "[[agent_injections]]",
    'id = "codex"',
    'provider_id = "agent-codex-vcsub"'
  ].join("\n");
  assert.equal(extractTomlString(config, "workspace_fingerprint"), "default-workspace");
  assert.equal(codexProviderId(config), "agent-codex-vcsub");
  const predict = createCohortPredictor({
    workspaceFingerprint: "default-workspace",
    providerId: "agent-codex-vcsub",
    effectiveModel: "gpt-5.6-luna"
  });
  assert.equal(predict("thread-0"), "baseline");
  assert.equal(predict("thread-7"), "candidate");
  assert.equal(normalizeLane("tool-burst"), "tool_burst_quarantine");
  assert.equal(normalizeLane("compacted-anchor"), "compacted_anchor");
  assert.equal(normalizeLane("unknown"), null);
  assert.equal(isCompactionBoundaryPhase("observe_baseline_compaction"), true);
  assert.equal(isCompactionBoundaryPhase("canary_candidate_compaction"), true);
  assert.equal(isCompactionBoundaryPhase("observe_baseline_pre_compaction"), false);
  assert.equal(compactionPhase("observe_baseline_pre_compaction"), "pre_compaction");
  assert.equal(compactionPhase("observe_baseline_compaction"), "compaction");
  assert.equal(compactionPhase("observe_baseline_post_compaction_1"), "post_compaction");
  assert.equal(compactionPhase("seed"), null);
  const fingerprints = {
    version: 2,
    cache_metadata: "cache",
    instructions: "instructions",
    tools_schema: "tools",
    input_history: "history-0",
    input_full: "input-1",
    input_item_count: 1,
    input_prefixes: ["history-0"],
    pre_input_wire: "wire"
  };
  const prefixProfile = classifyCompactionPrefixDrift([
    { session_id: "session-a", outbound_prefix_fingerprints: fingerprints },
    {
      session_id: "session-a",
      outbound_prefix_fingerprints: {
        ...fingerprints,
        input_history: "input-1",
        input_full: "input-2",
        input_item_count: 2,
        input_prefixes: ["history-0", "input-1"]
      }
    },
    {
      session_id: "session-a",
      outbound_prefix_fingerprints: {
        ...fingerprints,
        input_history: "new-history",
        input_full: "input-3",
        input_item_count: 1,
        input_prefixes: ["new-history"]
      }
    }
  ]);
  assert.equal(prefixProfile[0].prefix_drift, "initial_phase");
  assert.equal(prefixProfile[1].prefix_drift, "stable_prefix_preserved");
  assert.equal(prefixProfile[2].prefix_drift, "history_prefix_changed");
  console.log(JSON.stringify({ selfTest: "passed" }));
}

async function resolveExecutable() {
  const extension = process.platform === "win32" ? ".exe" : "";
  const explicitCandidates = [
    args.exe,
    process.env.ATOAPI_TEST_EXE
  ].filter(Boolean).map((item) => resolve(String(item)));
  for (const candidate of explicitCandidates) {
    if (await exists(candidate)) return candidate;
  }

  const discoveredCandidates = [
    join(repoRoot, "src-tauri", "target", "debug", `atoapi${extension}`),
    join(repoRoot, "src-tauri", "target", "release", `atoapi${extension}`),
  ].map((item) => resolve(String(item)));
  const existingCandidates = [];
  for (const candidate of discoveredCandidates) {
    if (!await exists(candidate)) continue;
    existingCandidates.push({ candidate, modifiedAt: (await stat(candidate)).mtimeMs });
  }
  existingCandidates.sort((left, right) => right.modifiedAt - left.modifiedAt);
  if (existingCandidates.length > 0) return existingCandidates[0].candidate;

  const checked = [...explicitCandidates, ...discoveredCandidates];
  throw new Error(`could not find an Atoapi executable; checked ${checked.join(", ")}`);
}

async function findAvailablePort(start) {
  for (let port = start; port <= Math.min(start + 32, 65_533); port += 1) {
    if (await portIsAvailable(port)) return port;
  }
  throw new Error(`could not find a free isolated port starting at ${start}`);
}

function portIsAvailable(port) {
  return new Promise((resolvePort) => {
    const socket = createConnection({ host: "127.0.0.1", port });
    socket.setTimeout(300);
    socket.once("connect", () => {
      socket.destroy();
      resolvePort(false);
    });
    socket.once("timeout", () => {
      socket.destroy();
      resolvePort(true);
    });
    socket.once("error", () => resolvePort(true));
  });
}

async function waitForHealth(baseUrl, child) {
  const deadline = Date.now() + 30_000;
  while (Date.now() < deadline) {
    if (child.exitCode !== null) {
      throw new Error(`isolated Atoapi exited during startup with code ${child.exitCode}`);
    }
    try {
      const health = await getJson(`${baseUrl}/health`, 1_000);
      if (health.ok) return;
    } catch {
      // The isolated proxy is still starting.
    }
    await delay(100);
  }
  throw new Error(`isolated Atoapi did not become healthy at ${baseUrl}`);
}

async function stopChild(child) {
  if (!child || !(await processExists(child.pid))) return;
  child.kill();
  const deadline = Date.now() + 10_000;
  while (await processExists(child.pid) && Date.now() < deadline) {
    await delay(50);
  }
  if (await processExists(child.pid)) {
    throw new Error(`isolated Atoapi process ${child.pid} did not exit`);
  }
}

function processExists(pid) {
  if (!pid) return Promise.resolve(false);
  try {
    process.kill(pid, 0);
    return Promise.resolve(true);
  } catch {
    return Promise.resolve(false);
  }
}

function defaultConfigDir() {
  if (process.platform === "win32" && process.env.APPDATA) {
    return join(process.env.APPDATA, "Atoapi");
  }
  return join(process.env.XDG_CONFIG_HOME ?? join(homedir(), ".config"), "Atoapi");
}

function configuredSourceConfigDir() {
  return resolve(
    String(
      args["source-config-dir"] ??
        process.env.ATOAPI_SOURCE_CONFIG_DIR ??
        defaultConfigDir()
    )
  );
}

function extractTomlString(text, key) {
  const escaped = escapeRegExp(key);
  return text.match(new RegExp(`^${escaped}\\s*=\\s*"([^"]*)"`, "mu"))?.[1] ?? "";
}

function escapeRegExp(value) {
  return value.replace(/[.*+?^${}()|[\]\\]/gu, "\\$&");
}

function safeRunId(value) {
  return value.replace(/[^a-zA-Z0-9_-]+/gu, "-").slice(0, 48) || "run";
}

function message(text) {
  return {
    type: "message",
    role: "user",
    content: [{ type: "input_text", text }]
  };
}

function parseArgs(items) {
  const parsed = {};
  for (let index = 0; index < items.length; index += 1) {
    const item = items[index];
    if (!item.startsWith("--")) continue;
    const key = item.slice(2);
    const next = items[index + 1];
    if (next && !next.startsWith("--")) {
      parsed[key] = next;
      index += 1;
    } else {
      parsed[key] = true;
    }
  }
  return parsed;
}

function boundedNumber(value, minimum, maximum) {
  const parsed = Number(value);
  if (!Number.isFinite(parsed)) return minimum;
  return Math.max(minimum, Math.min(maximum, Math.trunc(parsed)));
}

function booleanArg(value) {
  return value === true || new Set(["1", "true", "on", "yes"]).has(String(value).toLowerCase());
}

function normalizeLane(value) {
  const normalized = String(value).trim().toLowerCase().replaceAll("-", "_");
  if (new Set(["tool_burst", "tool_burst_quarantine", "post_burst"]).has(normalized)) {
    return "tool_burst_quarantine";
  }
  if (new Set(["compaction", "compacted", "compacted_anchor"]).has(normalized)) {
    return "compacted_anchor";
  }
  return null;
}

function isCompactionBoundaryPhase(value) {
  return /^(?:observe|canary)_(?:baseline|candidate)_compaction$/u.test(String(value));
}

function buildToolOutput(targetChars, suffix) {
  const line = `2026-07-15T00:00:00Z ${suffix} G:\\Atoapi\\src\\proxy\\cache_affinity.rs cache evidence line\n`;
  return line.repeat(Math.ceil(targetChars / line.length)).slice(0, targetChars);
}

function buildStableInstructions(targetChars) {
  const line = "Atoapi stable system context: preserve tools, rules, and project state.\n";
  const prefix = "Reply with OK only after following this stable context.\n";
  return (prefix + line.repeat(Math.ceil(targetChars / line.length))).slice(0, targetChars);
}

function buildCompactionHistory(targetChars, suffix) {
  const line = `Historical conversation ${suffix}: verified project decision and tool result remain available.\n`;
  return line.repeat(Math.ceil(targetChars / line.length)).slice(0, targetChars);
}

function buildCompactedSummary(targetChars, suffix) {
  const line = `Compacted state ${suffix}: preserve the accepted plan, current files, and pending verification.\n`;
  return line.repeat(Math.ceil(targetChars / line.length)).slice(0, targetChars);
}

async function getJson(url, timeout = 15_000) {
  const response = await fetch(url, { signal: AbortSignal.timeout(timeout) });
  if (!response.ok) throw new Error(`GET ${url} failed: HTTP ${response.status}`);
  return response.json();
}

async function exists(path) {
  try {
    await stat(path);
    return true;
  } catch {
    return false;
  }
}

function delay(ms) {
  return new Promise((resolveDelay) => setTimeout(resolveDelay, ms));
}

function number(value) {
  const parsed = Number(value ?? 0);
  return Number.isFinite(parsed) ? parsed : 0;
}

function failUsage(message) {
  console.error(message);
  process.exit(2);
}
