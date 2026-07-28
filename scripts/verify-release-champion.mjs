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
const repoRoot = resolve(dirname(fileURLToPath(import.meta.url)), "..");
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
  runSelfTest();
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
  const configPath = join(sourceConfigDir, "config.toml");
  const model = String(options.model).trim();
  const keyRealmHash = validateOpaqueHash(options["key-realm-hash"], "--key-realm-hash");
  const scenario = normalizeScenario(options.scenario ?? "full-replay");
  if (!scenario) {
    throw new FailClosedError(
      "invalid_scenario",
      "--scenario must be full-replay, tool-burst, or compacted-anchor"
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
  const turns = boundedInteger(options.turns ?? 6, "--turns", 4, 60);
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
    160_000
  );
  const toolChars = boundedInteger(options["tool-chars"] ?? 32_768, "--tool-chars", 1_024, 512_000);
  const maxTtftRegressionMs = boundedInteger(
    options["max-ttft-regression-ms"] ?? 0,
    "--max-ttft-regression-ms",
    0,
    120_000
  );
  const keepRunDir = booleanArg(options["keep-run-dir"]);
  const configText = await readRequiredText(configPath, "source config.toml");
  if (!extractTomlString(configText, "local_key")) {
    throw new FailClosedError(
      "missing_local_key",
      "source config.toml has no local_key; live verification cannot authenticate safely"
    );
  }
  const configProviderId = codexProviderId(configText);
  if (!configProviderId) {
    throw new FailClosedError(
      "missing_codex_provider",
      "source config has no Codex agent injection provider_id"
    );
  }
  const providerId = String(options["provider-id"] ?? configProviderId).trim();
  if (!providerId || providerId !== configProviderId) {
    throw new FailClosedError(
      "provider_scope_mismatch",
      "--provider-id must match the Codex provider_id in the copied source config"
    );
  }
  if (!model) {
    throw new FailClosedError("invalid_model", "--model must not be empty");
  }
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
    turns,
    max_output_tokens: maxOutputTokens,
    stable_instruction_chars: stableInstructionChars,
    tool_chars: toolChars,
    client_prompt_cache_key: Boolean(options["prompt-cache-key-prefix"]),
    max_ttft_regression_ms: maxTtftRegressionMs
  };
  const artifacts = {
    champion: await executableArtifact(championExe),
    candidate: await executableArtifact(candidateExe)
  };
  const armRuns = { champion: [], candidate: [] };
  const orderedPairs = [];

  for (let pair = 0; pair < pairs; pair += 1) {
    // Alternating order removes the deterministic "first lane warmed later"
    // bias without ever sharing a lane between old and new executables.
    const order = pair % 2 === 0 ? ["champion", "candidate"] : ["candidate", "champion"];
    orderedPairs.push(order);
    for (const arm of order) {
      const executable = arm === "champion" ? championExe : candidateExe;
      const lane = sha256Parts([
        "release-champion-lane-v1",
        runId,
        keyRealmHash,
        requestFamily,
        pair,
        arm
      ]);
      const result = await runIsolatedDynamicArm({
        arm,
        executable,
        sourceConfigDir,
        configProviderId,
        cohort,
        settings,
        requestedPort,
        runId,
        pair,
        lane,
        promptCacheKeyPrefix: options["prompt-cache-key-prefix"],
        keepRunDir
      });
      armRuns[arm].push(result);
    }
  }

  const champion = aggregateArm("champion", cohort, artifacts.champion, armRuns.champion);
  const candidate = aggregateArm("candidate", cohort, artifacts.candidate, armRuns.candidate);
  const comparison = compareArmResults(champion, candidate, maxTtftRegressionMs);
  return {
    schema: SCHEMA,
    kind: "release-champion-comparison",
    mode: "live-isolated",
    pass: comparison.pass,
    run_id: runId,
    cohort,
    settings,
    pair_order: orderedPairs,
    champion,
    candidate,
    comparison
  };
}

async function runIsolatedDynamicArm(spec) {
  const tempRoot = await mkdtemp(join(tmpdir(), `atoapi-release-champion-${safeSegment(spec.arm)}-`));
  const configDir = join(tempRoot, "config");
  let runtime = null;
  try {
    await copyIsolatedConfig(spec.sourceConfigDir, configDir);
    runtime = await startIsolatedRuntime({
      executable: spec.executable,
      configDir,
      requestedPort: spec.requestedPort
    });
    const executable = await executableArtifact(spec.executable);
    const promptCacheKey = spec.promptCacheKeyPrefix
      ? generatedPromptCacheKey(spec.promptCacheKeyPrefix, spec.lane)
      : null;
    const run = await exerciseScenario({
      runtime,
      arm: spec.arm,
      pair: spec.pair,
      runId: spec.runId,
      lane: spec.lane,
      cohort: spec.cohort,
      settings: spec.settings,
      expectedProviderId: spec.configProviderId,
      promptCacheKey,
      executable
    });
    return run;
  } catch (error) {
    return failedDynamicRun({
      arm: spec.arm,
      pair: spec.pair,
      cohort: spec.cohort,
      executable: await executableArtifact(spec.executable),
      reason: safeErrorMessage(error)
    });
  } finally {
    if (runtime) await stopChild(runtime.child, `${spec.arm} isolated runtime`);
    if (!spec.keepRunDir) await rm(tempRoot, { recursive: true, force: true });
  }
}

async function exerciseScenario(spec) {
  const state = {
    input: [message("Release champion seed. Reply with OK only.")],
    compactionSeen: false
  };
  const sessionId = `release-champion-${spec.arm}-${spec.pair}-${spec.lane.slice(0, 12)}`;
  const threadId = `release-champion-thread-${spec.pair}-${spec.lane.slice(12, 24)}`;
  const stableInstructions = buildStableInstructions(spec.settings.stable_instruction_chars, spec.lane);
  const requests = [];
  let fatal = null;

  for (let turn = 0; turn < spec.settings.turns; turn += 1) {
    let requestKind = "turn";
    let phase = turn === 0 ? "seed" : `followup-${turn}`;
    if (turn > 0 && spec.settings.scenario === "tool-burst" && turn === 1) {
      const callId = `call_${spec.lane.slice(0, 20)}`;
      state.input.push(
        { type: "function_call", call_id: callId, name: "read_release_fixture", arguments: "{}" },
        {
          type: "function_call_output",
          call_id: callId,
          output: buildToolOutput(spec.settings.tool_chars, spec.lane)
        },
        message("Use the completed tool output. Reply with OK only.")
      );
      phase = "tool-burst";
    } else if (turn > 0 && spec.settings.scenario === "compacted-anchor" && turn === 1) {
      state.input.push({ type: "compaction_trigger" });
      requestKind = "compaction";
      phase = "compaction";
    } else if (turn > 0) {
      state.input.push(message(`Stable follow-up ${turn}. Reply with OK only.`));
    }

    const record = await sendOneInbound({
      runtime: spec.runtime,
      sessionId,
      threadId,
      cohort: spec.cohort,
      input: state.input,
      instructions: stableInstructions,
      maxOutputTokens: spec.settings.max_output_tokens,
      requestKind,
      phase,
      promptCacheKey: spec.promptCacheKey
    });
    requests.push(record);
    if (!record.pass) {
      fatal = record.failure ?? "inbound verification failed";
      break;
    }
    if (requestKind === "compaction") {
      const compacted = record.compacted_input;
      if (!Array.isArray(compacted) || compacted.length === 0) {
        fatal = "compaction response did not contain a reusable compaction item";
        break;
      }
      state.input = compacted;
      state.compactionSeen = true;
    }
  }

  const run = buildDynamicRun({
    arm: spec.arm,
    pair: spec.pair,
    cohort: spec.cohort,
    executable: spec.executable,
    scenario: spec.settings.scenario,
    promptCacheKeyUsed: Boolean(spec.promptCacheKey),
    requests,
    fatal,
    compactionSeen: state.compactionSeen
  });
  return run;
}

async function sendOneInbound(spec) {
  const before = await getJson(`${spec.runtime.baseUrl}/admin/metrics`, 10_000);
  const beforeCounters = requestCounters(before);
  const knownRawInboundIds = new Set(
    array(before.recent_requests)
      .map((item) => String(item?.inbound_request_id ?? ""))
      .filter(Boolean)
  );
  const startedAt = Date.now();
  let responseStatus = 0;
  let responseText = "";
  let transportError = null;
  try {
    const body = {
      model: spec.cohort.model,
      stream: true,
      max_output_tokens: spec.maxOutputTokens,
      instructions: spec.instructions,
      input: spec.input
    };
    if (spec.promptCacheKey) body.prompt_cache_key = spec.promptCacheKey;
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
  const terminal = responseStatus >= 200 && responseStatus < 300 &&
    /\bresponse\.completed\b/u.test(responseText) &&
    !/\bresponse\.failed\b/u.test(responseText);
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
  const completedInput = spec.requestKind === "compaction"
    ? extractCompactionItems(responseText)
    : [];
  const checks = {
    terminal_response_completed: terminal,
    exact_counter_delta: exactCounterDelta,
    per_inbound_one_attempt_one_post: perInboundSingleAttempt,
    aggregate_no_multi_attempt: aggregateSingleAttempt,
    metric_present: Boolean(metric),
    provider_matches_cohort: providerMatches,
    model_matches_cohort: modelMatches,
    observed_key_realm_present: observedRealmPresent,
    usage_present: number(metric?.input_tokens) > 0
  };
  const failure = transportError
    ? "downstream transport failed"
    : Object.entries(checks).find(([, passed]) => !passed)?.[0] ?? null;
  return {
    phase: spec.phase,
    request_kind: spec.requestKind,
    pass: !failure,
    failure,
    http_status: responseStatus || null,
    elapsed_ms: Date.now() - startedAt,
    sse_completed: terminal,
    counters,
    inbound_id_hash: inboundId ? sha256Text(inboundId).slice(0, 24) : null,
    observed_realm_id: observedRealmId || null,
    provider_id: metric?.provider_id ?? null,
    model: metric?.model ?? null,
    input_tokens: number(metric?.input_tokens),
    cache_read_tokens: number(metric?.cache_read_tokens),
    cache_avoidable_gap_tokens: number(metric?.cache_avoidable_gap_tokens),
    cache_new_tail_gap_tokens: number(metric?.cache_new_tail_gap_tokens),
    cache_shortfall_tokens: number(metric?.cache_shortfall_tokens),
    ttft_ms: number(metric?.ttft_ms),
    upstream_attempt_index: metric?.upstream_attempt_index ?? null,
    upstream_attempt_total: metric?.upstream_attempt_total ?? null,
    outcome_attempt_count: outcome?.attempt_count ?? null,
    outcome_attempt_budget: outcome?.attempt_budget ?? null,
    matched_attempts: attempts.length,
    compacted_input: completedInput,
    checks
  };
}

async function waitForSettledInbound({ baseUrl, beforeCounters, knownRawInboundIds }) {
  const deadline = Date.now() + 15_000;
  let latest = null;
  do {
    latest = await getJson(`${baseUrl}/admin/metrics`, 5_000);
    const counters = requestCounters(latest);
    const hasNewRequest = array(latest.recent_requests).some((item) => {
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
  return array(metrics?.recent_requests).find((item) => {
    const id = String(item?.inbound_request_id ?? "");
    return id && !knownRawInboundIds.has(id);
  }) ?? null;
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
    (item) => item.checks?.provider_matches_cohort && item.checks?.model_matches_cohort
  );
  const usageCoverage = requests.length === 0 ? 0 : comparable.length / requests.length;
  const metrics = {
    requests: requests.length,
    successful_sse_requests: requests.filter((item) => item.sse_completed).length,
    input_tokens: inputTokens,
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
    shortfall_tokens: sum(comparable, "cache_shortfall_tokens"),
    ttft_p95_ms: percentile(comparable.map((item) => item.ttft_ms), 95),
    usage_coverage: usageCoverage,
    observed_realm_ids: observedRealms
  };
  const checks = {
    no_runtime_failure: !input.fatal,
    every_sse_completed: allTerminal,
    every_inbound_one_attempt_one_main_post: allSingle,
    cohort_bound_on_every_request: allCohortBound,
    complete_usage_coverage: usageCoverage === 1,
    input_usage_present: inputTokens > 0,
    cacheable_128_evidence_present: cacheableTokens > 0,
    warm_stable_prefix_evidence_present: warmCacheableTokens > 0,
    one_observed_key_realm: observedRealms.length === 1,
    avoidable_gap_zero: metrics.avoidable_gap_tokens === 0,
    compaction_observed: input.scenario !== "compacted-anchor" || input.compactionSeen === true
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
      cacheable_128_evidence_present: false,
      warm_stable_prefix_evidence_present: false,
      one_observed_key_realm: false,
      avoidable_gap_zero: false,
      compaction_observed: false
    },
    requests: []
  };
}

function aggregateArm(arm, cohort, executable, runs) {
  const normalized = runs.map((run) => validateDynamicRun(run, arm));
  const metrics = emptyMetrics();
  const observedRealms = [];
  const ttftSamples = [];
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
      "shortfall_tokens"
    ]) {
      metrics[key] += number(source[key]);
    }
    ttftSamples.push(number(source.ttft_p95_ms));
    metrics.cacheable_request_count += number(source.cacheable_request_count);
    metrics.full_bucket_denominator += number(source.full_bucket_denominator);
    observedRealms.push(...array(source.observed_realm_ids));
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
  metrics.ttft_p95_ms = percentile(ttftSamples, 95);
  metrics.usage_coverage = normalized.length > 0 && normalized.every(
    (run) => number(run.metrics?.usage_coverage) === 1
  ) ? 1 : 0;
  metrics.observed_realm_ids = unique(observedRealms);
  const checks = {
    every_run_passed: normalized.length > 0 && normalized.every((run) => run.pass),
    cohort_consistent: normalized.length > 0 && normalized.every((run) => sameCohort(run.cohort, cohort)),
    one_observed_key_realm: metrics.observed_realm_ids.length === 1,
    every_sse_completed: metrics.successful_sse_requests === metrics.requests && metrics.requests > 0,
    every_inbound_one_attempt_one_main_post: normalized.every(
      (run) => run.checks?.every_inbound_one_attempt_one_main_post === true
    ),
    avoidable_gap_zero: metrics.avoidable_gap_tokens === 0,
    input_usage_present: metrics.input_tokens > 0,
    cacheable_128_evidence_present: metrics.cacheable_tokens_128 > 0,
    warm_stable_prefix_evidence_present: metrics.warm_stable_prefix_tokens_128 > 0,
    full_bucket_denominator_present: metrics.full_bucket_denominator > 0
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

function compareArmResults(champion, candidate, maxTtftRegressionMs) {
  const cohortMatches = sameCohort(champion.cohort, candidate.cohort);
  const observedRealmMatches = champion.metrics.observed_realm_ids.length === 1 &&
    candidate.metrics.observed_realm_ids.length === 1 &&
    champion.metrics.observed_realm_ids[0] === candidate.metrics.observed_realm_ids[0];
  const checks = {
    champion_valid: champion.pass,
    candidate_valid: candidate.pass,
    cohort_matches: cohortMatches,
    observed_key_realm_matches: observedRealmMatches,
    candidate_raw_token_hit_not_lower:
      candidate.metrics.raw_token_hit_rate >= champion.metrics.raw_token_hit_rate,
    candidate_cache_128_hit_not_lower:
      candidate.metrics.cache_128_hit_rate >= champion.metrics.cache_128_hit_rate,
    candidate_warm_stable_prefix_hit_not_lower:
      candidate.metrics.warm_stable_prefix_hit_rate >= champion.metrics.warm_stable_prefix_hit_rate,
    candidate_full_bucket_rate_not_lower:
      candidate.metrics.full_bucket_rate >= champion.metrics.full_bucket_rate,
    candidate_avoidable_gap_zero: candidate.metrics.avoidable_gap_tokens === 0,
    candidate_all_sse_completed:
      candidate.metrics.successful_sse_requests === candidate.metrics.requests && candidate.metrics.requests > 0,
    candidate_one_attempt_one_main_post:
      candidate.checks.every_inbound_one_attempt_one_main_post === true,
    candidate_ttft_p95_not_regressed:
      candidate.metrics.ttft_p95_ms <= champion.metrics.ttft_p95_ms + maxTtftRegressionMs
  };
  return {
    pass: Object.values(checks).every(Boolean),
    checks,
    deltas: {
      raw_token_hit_rate: candidate.metrics.raw_token_hit_rate - champion.metrics.raw_token_hit_rate,
      cache_128_hit_rate: candidate.metrics.cache_128_hit_rate - champion.metrics.cache_128_hit_rate,
      warm_stable_prefix_hit_rate:
        candidate.metrics.warm_stable_prefix_hit_rate - champion.metrics.warm_stable_prefix_hit_rate,
      full_bucket_rate: candidate.metrics.full_bucket_rate - champion.metrics.full_bucket_rate,
      avoidable_gap_tokens:
        candidate.metrics.avoidable_gap_tokens - champion.metrics.avoidable_gap_tokens,
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
  const comparison = compareArmResults(champion, candidate, maxTtftRegressionMs);
  return {
    schema: SCHEMA,
    kind: "release-champion-comparison",
    mode: "offline-artifacts",
    pass: comparison.pass,
    cohort: champion.cohort,
    champion,
    candidate,
    comparison
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

async function copyIsolatedConfig(sourceConfigDir, targetConfigDir) {
  const sourceConfig = join(sourceConfigDir, "config.toml");
  await assertFile(sourceConfig, "source config.toml");
  await mkdir(targetConfigDir, { recursive: true });
  await copyFile(sourceConfig, join(targetConfigDir, "config.toml"));
  const sourceKey = join(sourceConfigDir, "cache-key.dpapi");
  if (await fileExists(sourceKey)) {
    await copyFile(sourceKey, join(targetConfigDir, basename(sourceKey)));
  }
}

async function startIsolatedRuntime({ executable, configDir, requestedPort }) {
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
    shortfall_tokens: 0,
    ttft_p95_ms: 0,
    usage_coverage: 0,
    observed_realm_ids: []
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

function buildStableInstructions(targetChars, lane) {
  const prefix = `Release-cache validation lane ${lane.slice(0, 24)}. Preserve the supplied history. `;
  const unit = "Follow the existing instructions exactly; reply with OK only when asked. ";
  return (prefix + unit.repeat(Math.ceil(Math.max(0, targetChars - prefix.length) / unit.length)))
    .slice(0, targetChars);
}

function buildToolOutput(targetChars, lane) {
  const unit = `tool-output:${lane.slice(0, 16)}: stable release validation data; `;
  return unit.repeat(Math.ceil(targetChars / unit.length)).slice(0, targetChars);
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
  for (const block of String(responseText).split(/\r?\n\r?\n/u)) {
    const payload = block
      .split(/\r?\n/u)
      .filter((line) => line.startsWith("data:"))
      .map((line) => line.slice(5).trim())
      .join("\n");
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
    "compacted-anchor": "codex-responses-compacted-anchor"
  }[scenario];
}

function normalizeScenario(value) {
  const normalized = String(value).trim().toLowerCase().replace(/_/gu, "-");
  return new Set(["full-replay", "tool-burst", "compacted-anchor"]).has(normalized)
    ? normalized
    : null;
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
    const next = text.indexOf("\n[[", start + marker.length);
    const end = next < 0 ? text.length : next + 1;
    return { body: text.slice(start, end) };
  });
}

function extractTomlString(text, key) {
  const escaped = escapeRegExp(key);
  return text.match(new RegExp(`^${escaped}\\s*=\\s*"([^"]*)"`, "mu"))?.[1] ?? "";
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

function ratio(numerator, denominator) {
  return denominator > 0 ? numerator / denominator : 0;
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
    [--scenario full-replay|tool-burst|compacted-anchor] [--pairs 2] [--turns 6]

Offline comparison (does not start any process):
  node scripts/verify-release-champion.mjs \\
    --champion-result <champion-arm.json> --candidate-result <candidate-arm.json>

Safety:
  --live is required before an upstream-backed isolated run.  Missing config,
  usage, terminal SSE, key-realm evidence, or one-attempt/one-POST evidence
  fails closed.  The script only starts temporary isolated ports; it never
  sends signals to the existing 18883 process.`);
}

function runSelfTest() {
  assert.deepEqual(parseArgs(["--pairs=2", "--live", "--model", "m"]), {
    pairs: "2",
    live: true,
    model: "m"
  });
  assert.equal(cacheableInputTokens128(1_023), 0);
  assert.equal(cacheableInputTokens128(1_024), 1_024);
  assert.equal(cacheableInputTokens128(1_151), 1_024);
  assert.equal(cacheableInputTokens128(1_152), 1_152);
  assert.equal(normalizeScenario("tool_burst"), "tool-burst");
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
      shortfall_tokens: 0,
      ttft_p95_ms: 100,
      usage_coverage: 1,
      observed_realm_ids: ["observed-realm"]
    },
    checks: {
      every_inbound_one_attempt_one_main_post: true
    }
  });
  assert.equal(compareArmResults(valid("champion", 0.9), valid("candidate", 0.9), 0).pass, true);
  assert.equal(compareArmResults(valid("champion", 0.9), valid("candidate", 0.89), 0).pass, false);
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
          sse_completed: true,
          observed_realm_id: "observed-realm",
          checks: { per_inbound_one_attempt_one_post: true, exact_counter_delta: true }
        },
        {
          phase: "followup-1",
          input_tokens: 1152,
          cache_read_tokens: 1152,
          sse_completed: true,
          observed_realm_id: "observed-realm",
          checks: { per_inbound_one_attempt_one_post: true, exact_counter_delta: true }
        }
      ]
    }
  ]);
  assert.equal(aggregate.pass, true);
  assert.equal(generatedPromptCacheKey("test", "lane").startsWith("atoapi-"), true);
  console.log(JSON.stringify({ schema: SCHEMA, self_test: "passed" }));
}
