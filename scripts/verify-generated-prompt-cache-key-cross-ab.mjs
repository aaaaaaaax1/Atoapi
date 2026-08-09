import { createHash, randomUUID } from "node:crypto";
import assert from "node:assert/strict";
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
import { createServer } from "node:net";
import { homedir, tmpdir } from "node:os";
import { basename, dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";

// Live A/B verifier for generated native Responses cache controls and the
// exact binaries that carry them. It deliberately runs two isolated proxy
// processes. It never uses the live 18883 process, never changes its config,
// and sends exactly one POST for each generated inbound.
const repoRoot = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const args = parseArgs(process.argv.slice(2));
const extension = process.platform === "win32" ? ".exe" : "";
// `cache_metadata` is a final-wire fingerprint.  Its implementation redacts
// prompt_cache_key values before hashing, but preserves the ordered presence
// and shape of the cache-control fields.  Keep only known Atoapi projections;
// an unknown value is fail-closed rather than being guessed from a prefix key.
const FINAL_WIRE_CACHE_METADATA_WITNESSES = buildFinalWireCacheMetadataWitnesses();

if (booleanArg(args.help) || booleanArg(args.h)) {
  printUsage();
  process.exit(0);
}
if (booleanArg(args["self-test"])) {
  runSelfTest();
  process.exit(0);
}

const sourceConfigDir = resolve(String(args["source-config-dir"] ?? defaultConfigDir()));
const defaultExecutable = resolve(String(
  args.exe ?? join(repoRoot, "src-tauri", "target", "debug", `atoapi${extension}`)
));
const baselineExecutable = resolve(String(args["baseline-exe"] ?? defaultExecutable));
const candidateExecutable = resolve(String(args["candidate-exe"] ?? defaultExecutable));
const providerId = requiredString(args["provider-id"], "--provider-id");
const model = requiredString(args.model, "--model");
const controlField = normalizeControlField(args.field ?? "prompt-cache-key");
const preservedControlField = optionalControlField(args["preserve-control-field"]);
if (preservedControlField === controlField) {
  throw new Error("--preserve-control-field must differ from --field");
}
const baselineCapabilityMode = normalizeCapabilityMode(args["baseline-capability"] ?? "unsupported");
const candidateCapabilityMode = normalizeCapabilityMode(args["candidate-capability"] ?? "verified");
const serialArms = booleanArg(args.serial);
const minimumTurns = serialArms ? 3 : 20;
const turns = boundedInteger(args.turns ?? 20, "--turns", minimumTurns, 60);
const betweenArmsDelayMs = boundedInteger(
  args["between-arms-delay-ms"] ?? 0,
  "--between-arms-delay-ms",
  0,
  60_000
);
// The input-budget gate, rather than a hard-coded prefix length, proves that
// each arm exercised enough context. Some valid upstreams reject the former
// 2,200-line default by request shape before any cache evidence is produced.
const fixtureLines = boundedInteger(args.lines ?? 1000, "--lines", 10, 10_000);
const portStart = boundedInteger(args.port ?? 64_940, "--port", 1_024, 65_400);
const maxOutputTokens = boundedInteger(args["max-output-tokens"] ?? 16, "--max-output-tokens", 1, 256);
const targetInputTokens = boundedInteger(args["target-input-tokens"] ?? 500_000, "--target-input-tokens", 100_000, 10_000_000);
const maxTtftRegressionMs = boundedInteger(
  args["max-ttft-regression-ms"] ?? 0,
  "--max-ttft-regression-ms",
  0,
  60_000
);
const allowUpstreamTtftRegression = booleanArg(args["allow-upstream-ttft-regression"]);
const maxLocalTtftOverheadRegressionMs = boundedInteger(
  args["max-local-ttft-overhead-regression-ms"] ?? 500,
  "--max-local-ttft-overhead-regression-ms",
  0,
  500
);
const maxInputTokenDelta = boundedInteger(
  args["max-input-token-delta"] ?? 128,
  "--max-input-token-delta",
  0,
  10_000
);
const firstArm = normalizeFirstArm(args["first-arm"] ?? "baseline");
const expectedRealmHash = optionalRealmHash(args["key-realm-hash"]);
const pinnedKeyId = optionalOpaqueIdentifier(args["key-id"], "--key-id");
const keepRunDir = booleanArg(args["keep-run-dir"]);
const runId = String(args["run-id"] ?? randomUUID()).trim();
const outputPath = args.output ? resolve(String(args.output)) : null;

let root = null;
let baselineRuntime = null;
let candidateRuntime = null;
let baselineArm = null;
let candidateArm = null;
let runFailure = null;

class InvariantError extends Error {
  constructor(message, diagnostics) {
    super(message);
    this.name = "InvariantError";
    this.diagnostics = diagnostics;
  }
}

try {
  await assertFile(baselineExecutable, "baseline Atoapi executable");
  await assertFile(candidateExecutable, "candidate Atoapi executable");
  const source = await snapshotSourceConfig(sourceConfigDir);
  root = await mkdtemp(join(tmpdir(), "atoapi-generated-key-cross-ab-"));
  const baselineConfigDir = join(root, "baseline");
  const candidateConfigDir = join(root, "candidate");
  await materializeConfig(source, baselineConfigDir, baselineCapabilityMode);
  await materializeConfig(source, candidateConfigDir, candidateCapabilityMode);

  const fixtureFamily = stableGuid("fixture-family", runId);
  const baselinePrefix = buildStablePrefix(fixtureFamily, 0);
  const candidatePrefix = buildStablePrefix(fixtureFamily, 1);
  if (baselinePrefix.length !== candidatePrefix.length) {
    throw new Error("fixture construction produced unequal prefix lengths");
  }

  if (serialArms) {
    const serialOrder = serialArmOrder(firstArm);
    for (const [serialIndex, armName] of serialOrder.entries()) {
      const isBaseline = armName === "baseline";
      const runtime = await startRuntime(
        armName,
        isBaseline ? baselineConfigDir : candidateConfigDir,
        isBaseline ? portStart : portStart + 1,
        source.localKey,
        isBaseline ? baselineExecutable : candidateExecutable
      );
      if (isBaseline) baselineRuntime = runtime;
      else candidateRuntime = runtime;
      const arm = createArm(
        armName,
        runtime,
        stableGuid(armName, runId),
        fixtureFamily,
        isBaseline ? 0 : 1,
        isBaseline ? baselinePrefix : candidatePrefix
      );
      if (isBaseline) baselineArm = arm;
      else candidateArm = arm;
      try {
        for (let index = 0; index < turns; index += 1) {
          await sendTurn(arm, index + 1);
        }
      } finally {
        // Capacity-sensitive upstreams must never see two local arm processes
        // at the same time. Stop the completed arm before even starting the
        // next one; the copied configs remain independent.
        await stopRuntime(runtime);
      }
      if (serialIndex + 1 < serialOrder.length && betweenArmsDelayMs > 0) {
        await delay(betweenArmsDelayMs);
      }
    }
  } else {
    baselineRuntime = await startRuntime(
      "baseline",
      baselineConfigDir,
      portStart,
      source.localKey,
      baselineExecutable
    );
    candidateRuntime = await startRuntime(
      "candidate",
      candidateConfigDir,
      portStart + 1,
      source.localKey,
      candidateExecutable
    );
    baselineArm = createArm(
      "baseline",
      baselineRuntime,
      stableGuid("baseline", runId),
      fixtureFamily,
      0,
      baselinePrefix
    );
    candidateArm = createArm(
      "candidate",
      candidateRuntime,
      stableGuid("candidate", runId),
      fixtureFamily,
      1,
      candidatePrefix
    );
    for (let index = 0; index < turns; index += 1) {
      for (const armName of interleavedArmOrder(index, firstArm)) {
        await sendTurn(armName === "baseline" ? baselineArm : candidateArm, index + 1);
      }
    }
  }

  const baselineSummary = summarizeArm(baselineArm);
  const candidateSummary = summarizeArm(candidateArm);
  const tokenSymmetry = inputTokenSymmetry(baselineArm.records, candidateArm.records);
  const checks = buildChecks({
    baseline: baselineArm,
    candidate: candidateArm,
    baselineSummary,
    candidateSummary,
    tokenSymmetry
  });
  const result = {
    schema: "atoapi-generated-prompt-cache-key-cross-ab-v1",
    run_id_hash: shortHash(runId),
    isolated: true,
    comparison: {
      provider_id: providerId,
      model,
      control_field: controlField,
      preserved_control_field: preservedControlField,
      request_family: "responses-full-replay",
      turns_per_arm: turns,
      stable_prefix_lines: fixtureLines,
      max_ttft_regression_ms: maxTtftRegressionMs,
      allow_upstream_ttft_regression: allowUpstreamTtftRegression,
      max_local_ttft_overhead_regression_ms: maxLocalTtftOverheadRegressionMs,
      max_input_token_delta: maxInputTokenDelta,
      expected_realm_hash_prefix: expectedRealmHash ? expectedRealmHash.slice(0, 12) : null,
      baseline_executable: await executableReceipt(baselineExecutable),
      candidate_executable: await executableReceipt(candidateExecutable),
      same_binary: baselineExecutable === candidateExecutable,
      baseline_capability_mode: baselineCapabilityMode,
      candidate_capability_mode: candidateCapabilityMode,
      control_field_isolation: cacheControlFieldIsolation(
        baselineArm.records,
        candidateArm.records,
        controlField,
        baselineCapabilityMode,
        candidateCapabilityMode,
        preservedControlField
      ),
      same_source_config_snapshot: true,
      key_id_pinned: Boolean(pinnedKeyId),
      pinned_key_id: pinnedKeyId,
      interleaved_order: !serialArms,
      serial_arms: serialArms,
      serial_order: serialArms ? serialArmOrder(firstArm) : null,
      serial_processes_do_not_overlap: serialArms,
      between_arms_delay_ms: serialArms ? betweenArmsDelayMs : null,
      first_arm: firstArm,
      same_fixture_except_equal_width_first_line_marker: true
    },
    baseline: baselineSummary,
    candidate: candidateSummary,
    input_token_symmetry: tokenSymmetry,
    timing_samples: {
      baseline: timingSamples(baselineArm.records),
      candidate: timingSamples(candidateArm.records)
    },
    delta: compareSummaries(baselineSummary, candidateSummary, tokenSymmetry),
    checks,
    comparable: checksPassUnderLatencyPolicy(checks),
    outcome: describeOutcome(
      baselineSummary,
      candidateSummary,
      checks,
      checksPassUnderLatencyPolicy(checks)
    )
  };
  await emitResult(result, false);
  if (!result.comparable) process.exitCode = 1;
} catch (error) {
  runFailure = error;
  const failure = {
    schema: "atoapi-generated-prompt-cache-key-cross-ab-v1",
    comparable: false,
    error: safeError(error),
    failure_diagnostics: safeInvariantDiagnostics(error),
    partial: {
      baseline: partialArmResult(baselineArm),
      candidate: partialArmResult(candidateArm)
    },
    // A retained isolated directory is useful for a local-only follow-up, but
    // never expose it on ordinary runs.  The directory contains a copied
    // configuration, so callers must opt in deliberately with --keep-run-dir.
    retained_run_dir: keepRunDir && root ? root : null
  };
  try {
    await emitResult(failure, true);
  } catch (writeError) {
    console.error(JSON.stringify({
      schema: "atoapi-generated-prompt-cache-key-cross-ab-output-write-failure-v1",
      error: safeError(writeError)
    }, null, 2));
    console.error(JSON.stringify(failure, null, 2));
  }
  process.exitCode = 2;
} finally {
  try {
    await stopRuntime(candidateRuntime);
    await stopRuntime(baselineRuntime);
    if (root && !keepRunDir) await rm(root, { recursive: true, force: true });
  } catch (cleanupError) {
    if (!runFailure) throw cleanupError;
    console.error(JSON.stringify({ cleanup_error: safeError(cleanupError) }));
  }
}

async function snapshotSourceConfig(directory) {
  const configPath = join(directory, "config.toml");
  const configText = await readRequiredText(configPath, "source config.toml");
  const localKey = extractTomlString(configText, "local_key");
  if (!localKey) throw new Error("source config.toml has no local_key");
  const route = codexAgentRoute(configText);
  if (!route?.enabled || route.provider_id !== providerId) {
    throw new Error("source config Codex route does not match the hand-selected Provider");
  }
  if (route.model_id && route.model_id !== model) {
    throw new Error("source config Codex model does not match the requested model");
  }
  // A multi-Key pool must never be selected implicitly for a live isolated
  // comparison. Validate the requested pin once against the source snapshot;
  // each materialized arm then receives the same deterministic pool state.
  if (pinnedKeyId) validatePinnedKeyConfiguration(configText, providerId, pinnedKeyId);
  const preserveSourceConfig = baselineCapabilityMode === "source" &&
    candidateCapabilityMode === "source";
  if (preserveSourceConfig) {
    const keyPath = join(directory, "cache-key.dpapi");
    return {
      configText,
      localKey,
      keyPath: await fileExists(keyPath) ? keyPath : null
    };
  }
  const blocks = tomlArrayBlocks(configText, "provider_cache_capabilities");
  const matches = blocks.filter((block) => capabilityMatches(block.body));
  const activeMatches = pinnedKeyId
    ? matches.filter((block) => extractTomlString(block.body, "key_id") === pinnedKeyId)
    : matches.filter((block) => !extractTomlString(block.body, "key_id"));
  if (activeMatches.length !== 1) {
    const scope = pinnedKeyId ? `Key ${pinnedKeyId}` : "generic scope";
    throw new Error(`expected exactly one ${scope} matching ${controlField} capability record`);
  }
  if (extractTomlString(activeMatches[0].body, "status") !== "verified") {
    throw new Error("the active matching candidate capability must be verified before a cross A/B run");
  }
  if (preservedControlField) {
    const preservedMatches = blocks.filter((block) =>
      capabilityScopeMatches(block.body) &&
      extractTomlString(block.body, "field") === preservedControlField &&
      (pinnedKeyId
        ? extractTomlString(block.body, "key_id") === pinnedKeyId
        : !extractTomlString(block.body, "key_id"))
    );
    if (preservedMatches.length !== 1 ||
      extractTomlString(preservedMatches[0].body, "status") !== "verified") {
      throw new Error(`the preserved ${preservedControlField} capability must be verified in the active scope`);
    }
  }
  const keyPath = join(directory, "cache-key.dpapi");
  return {
    configText,
    localKey,
    keyPath: await fileExists(keyPath) ? keyPath : null
  };
}

async function materializeConfig(source, directory, capabilityMode) {
  await mkdir(directory, { recursive: true });
  let configText = capabilityMode === "source"
    ? source.configText
    : rewriteCapabilityScope(source.configText, capabilityMode);
  if (pinnedKeyId) configText = pinProviderKeyInToml(configText, providerId, pinnedKeyId);
  await writeFile(join(directory, "config.toml"), configText, "utf8");
  if (source.keyPath) {
    await copyFile(source.keyPath, join(directory, basename(source.keyPath)));
  }
}

function rewriteCapabilityScope(configText, targetStatus) {
  return rewriteCapabilityScopeFor(
    configText,
    targetStatus,
    { providerId, model, keyId: pinnedKeyId },
    controlField,
    preservedControlField
  );
}

function rewriteCapabilityScopeFor(configText, targetStatus, scope, field, preservedField = null) {
  const blocks = tomlArrayBlocks(configText, "provider_cache_capabilities");
  const scoped = blocks.filter((block) => capabilityScopeMatchesFor(block.body, scope));
  const targets = scoped.filter(
    (block) => extractTomlString(block.body, "field") === field
  );
  if (targets.length === 0) throw new Error("matching capability record changed while creating an isolated probe");
  let rewrittenConfig = configText;
  for (const block of [...scoped].sort((left, right) => right.start - left.start)) {
    const blockField = extractTomlString(block.body, "field");
    if (blockField === preservedField) continue;
    const status = blockField === field ? targetStatus : "unsupported";
    let body = replaceCapabilityString(block.body, "status", status);
    body = replaceCapabilityString(body, "effect_status", "unverified");
    rewrittenConfig = `${rewrittenConfig.slice(0, block.start)}${body}${rewrittenConfig.slice(block.end)}`;
  }
  return rewrittenConfig;
}

function capabilityScopeMatches(body) {
  return capabilityScopeMatchesFor(body, { providerId, model });
}

function capabilityScopeMatchesFor(body, scope) {
  const matchesRoute = extractTomlString(body, "provider_id") === scope.providerId &&
    extractTomlString(body, "model_id") === scope.model &&
    extractTomlString(body, "channel") === "responses";
  if (!matchesRoute) return false;
  return !scope.keyId || extractTomlString(body, "key_id") === scope.keyId;
}

function capabilityMatches(body) {
  return capabilityScopeMatches(body) && extractTomlString(body, "field") === controlField;
}

function replaceCapabilityString(block, key, value) {
  const escapedKey = key.replace(/[.*+?^${}()|[\[\]\\]/gu, "\\$&");
  const field = new RegExp(`^${escapedKey}\\s*=\\s*"[^"]*"\\s*$`, "mu");
  const replacement = `${key} = "${value}"`;
  return field.test(block) ? block.replace(field, replacement) : `${block.trimEnd()}\n${replacement}\n`;
}

async function startRuntime(label, configDir, requestedPort, localKey, executable) {
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
    await waitForHealth(baseUrl, child, label);
  } catch (error) {
    await stopChild(child, label);
    throw error;
  }
  return { label, child, baseUrl, localKey, port };
}

async function executableReceipt(path) {
  const bytes = await readFile(path);
  return {
    file: basename(path),
    sha256: createHash("sha256").update(bytes).digest("hex").toUpperCase()
  };
}

function createArm(name, runtime, guid, fixtureFamily, fixtureRotation, preparedPrefix = null) {
  const prefix = preparedPrefix ?? buildStablePrefix(fixtureFamily, fixtureRotation);
  return {
    name,
    runtime,
    guid,
    fixtureFamily,
    prefix,
    input: [message(prefix)],
    sessionId: `cross-ab-session-${guid}`,
    threadId: `cross-ab-thread-${guid}`,
    records: []
  };
}

function buildStablePrefix(fixtureFamily, fixtureRotation, lineCount = fixtureLines) {
  const shared = "immutable-schema=alpha|stable-field=cache-validation|payload=abcdefghijklmno";
  // Keep both arms byte/shape symmetric.  Only a single one-character marker
  // in the first record separates the arms; rotating the complete record set
  // changes BPE merge boundaries and caused hundreds of tokens of artificial
  // skew even though the line multiset was identical.
  const armMarker = String(((Number(fixtureRotation) % 10) + 10) % 10);
  const records = Array.from({ length: lineCount }, (_, index) => {
    const ordinal = String(index + 1).padStart(4, "0");
    const mirror = String(lineCount - index).padStart(4, "0");
    const arm = index === 0 ? `|arm=${armMarker}` : "";
    return `record=${ordinal}|mirror=${mirror}|fixture=${fixtureFamily}${arm}|${shared}\n`;
  });
  return records.join("");
}

function buildEqualLengthTail(turn) {
  const ordinal = String(turn).padStart(4, "0");
  return `new-turn=${ordinal}|tail=constant-length-deterministic-follow-up|ack=required`;
}

async function sendTurn(arm, turn) {
  arm.input.push(message(buildEqualLengthTail(turn)));
  const before = await getJson(`${arm.runtime.baseUrl}/admin/metrics`, 5_000);
  const countersBefore = requestCounters(before);
  const knownIds = new Set(requestLogRows(before)
    .map((item) => String(item?.inbound_request_id ?? ""))
    .filter(Boolean));
  const requestPayload = {
    model,
    stream: true,
    max_output_tokens: maxOutputTokens,
    instructions: "Return exactly ACK. This is a deterministic cache validation fixture.",
    input: arm.input
  };
  const requestBody = JSON.stringify(requestPayload);
  const requestBodyBytes = Buffer.byteLength(requestBody, "utf8");
  const response = await fetch(`${arm.runtime.baseUrl}/codex/v1/responses`, {
    method: "POST",
    headers: {
      authorization: `Bearer ${arm.runtime.localKey}`,
      "content-type": "application/json",
      accept: "text/event-stream",
      "x-codex-turn-metadata": JSON.stringify({
        session_id: arm.sessionId,
        thread_id: arm.threadId,
        request_kind: "turn"
      })
    },
    body: requestBody,
    signal: AbortSignal.timeout(180_000)
  });
  const sse = await response.text();
  const responseFailed = responseHasNativeFailure(sse);
  const responseFailureCode = responseErrorCode(sse);
  const failureClass = safeFailureClass(response.status, sse, responseFailed);
  const failureReceipt = safeFailureReceipt(sse);
  const after = await waitForFinalization(arm.runtime.baseUrl, countersBefore, knownIds);
  const metric = selectNewRequestLog(after, knownIds);
  const counters = subtractCounters(requestCounters(after), countersBefore);
  const completed = response.ok && /\bresponse\.completed\b/u.test(sse) && !responseFailed;
  const singleAttempt = counters.inbound_requests === 1 &&
    counters.generation_attempts === 1 &&
    counters.upstream_requests === 1 &&
    Number(metric?.upstream_attempt_index) === 1 &&
    Number(metric?.upstream_attempt_total) === 1 &&
    Number(metric?.upstream_attempts) === 1;
  const inputTokens = number(metric?.input_tokens);
  const cacheReadTokens = number(metric?.cache_read_tokens);
  const fullReplay = String(metric?.response_context_plan ?? "") === "full_replay";
  const responseContextPlan = safeMetricLabel(metric?.response_context_plan);
  const providerMatches = String(metric?.provider_id ?? "") === providerId && String(metric?.model ?? "") === model;
  const realm = String(metric?.shadow_affinity_realm_id ?? "");
  const ttftMs = optionalNumber(metric?.ttft_ms);
  const upstreamTtftMs = optionalNumber(metric?.upstream_ttft_ms);
  const localTtftOverheadMs = ttftMs !== null && upstreamTtftMs !== null
    ? Math.max(0, ttftMs - upstreamTtftMs)
    : null;
  const prefixGuardWaitMs = optionalNumber(metric?.prefix_guard_wait_ms) ?? 0;
  const prefixGuardWaitReason = safeMetricLabel(metric?.prefix_guard_wait_reason);
  const prefixGuardWaitSource = safeMetricLabel(metric?.prefix_guard_wait_source);
  const prefixGuardSkipReason = safeMetricLabel(metric?.prefix_guard_skip_reason);
  const localPrepareMs = optionalNumber(metric?.local_prepare_ms) ?? 0;
  // `provider_prefix_key` is a compatibility/recovery diagnostic and may be
  // populated even when the requested native control was not on the final
  // wire.  The final-wire cache metadata fingerprint intentionally redacts the
  // opaque key value while preserving field presence, so it is the safe wire
  // witness for this controlled prompt_cache_key probe.
  const finalWireCacheControls = finalWireCacheControlEvidence(metric);
  const cacheMetadata = finalWireCacheControls.cache_metadata;
  const cacheMetadataPresent = finalWireCacheControls.observed;
  const cacheMetadataRecognized = finalWireCacheControls.recognized;
  const promptCacheKeyWirePresent = finalWireCacheControls.fields.includes("prompt_cache_key");
  const promptCacheKeyWireAbsent = cacheMetadataRecognized && !promptCacheKeyWirePresent;
  const validationCandidateApplied = metric?.shadow_affinity_decision === "validation_candidate_applied";
  // The generated prompt-cache-key compatibility path can apply the field
  // without opening the shadow validation controller. The shadow decision is
  // retained as a separate diagnostic only.
  const candidateControlFieldWirePresent = finalWireCacheControls.fields.includes(
    controlFieldJsonName(controlField)
  );
  const record = {
    completed,
    single_attempt: singleAttempt,
    input_tokens: inputTokens,
    cache_read_tokens: cacheReadTokens,
    request_body_bytes: requestBodyBytes,
    cache_avoidable_gap_tokens: number(metric?.cache_avoidable_gap_tokens),
    cache_provider_unstable_gap_tokens: number(metric?.cache_provider_unstable_gap_tokens),
    cache_new_tail_gap_tokens: number(metric?.cache_new_tail_gap_tokens),
    full_replay: fullReplay,
    response_context_plan: responseContextPlan,
    provider_matches: providerMatches,
    realm_id: realm,
    ttft_ms: ttftMs,
    upstream_ttft_ms: upstreamTtftMs,
    local_ttft_overhead_ms: localTtftOverheadMs,
    prefix_guard_wait_ms: prefixGuardWaitMs,
    prefix_guard_wait_reason: prefixGuardWaitReason,
    prefix_guard_wait_source: prefixGuardWaitSource,
    prefix_guard_skip_reason: prefixGuardSkipReason,
    local_prepare_ms: localPrepareMs,
    timing_observed: ttftMs !== null,
    // Retain the legacy provider-prefix signal for diagnostics, but never use
    // it as a control-field gate.
    provider_prefix_key_present: typeof metric?.provider_prefix_key === "string" && metric.provider_prefix_key.length > 0,
    cache_metadata: cacheMetadata,
    cache_metadata_present: cacheMetadataPresent,
    cache_metadata_recognized: cacheMetadataRecognized,
    final_wire_cache_control_fields: finalWireCacheControls.fields,
    prompt_cache_key_wire_present: promptCacheKeyWirePresent,
    prompt_cache_key_wire_absent: promptCacheKeyWireAbsent,
    validation_candidate_applied: validationCandidateApplied,
    candidate_control_field_wire_present: candidateControlFieldWirePresent,
    downstream_disconnected: Boolean(metric?.downstream_disconnected),
    status: Number(metric?.status),
    failure_class: failureClass,
    failure_receipt: failureReceipt,
    response_failed: responseFailed,
    response_failure_code: responseFailureCode,
    sse_end_reason: safeMetricLabel(metric?.sse_end_reason)
  };
  arm.records.push(record);
  if (!completed || !singleAttempt || inputTokens <= 0 || !fullReplay || !providerMatches || !realm || !record.timing_observed || record.downstream_disconnected) {
    // Preserve just enough evidence to distinguish an upstream terminal failure,
    // delayed metrics, and a verifier assumption.  Never print the request,
    // response body, credentials, or any upstream headers.
    throw new InvariantError(`${arm.name} turn ${turn} did not reach the required terminal, metric, or one-POST invariant`, {
      arm: arm.name,
      turn,
      http_status: response.status,
      failure_class: failureClass,
      failure_receipt: failureReceipt,
      response_failed: responseFailed,
      response_failure_code: responseFailureCode,
      sse_end_reason: record.sse_end_reason,
      metric_observed: Boolean(metric),
      completed,
      single_attempt: singleAttempt,
      input_tokens: inputTokens,
      full_replay: fullReplay,
      provider_matches: providerMatches,
      affinity_realm_present: Boolean(realm),
      timing_observed: record.timing_observed,
      downstream_disconnected: record.downstream_disconnected,
      counters
    });
  }
}

function safeFailureClass(status, body, responseFailed = responseHasNativeFailure(body)) {
  if (responseFailed) return "response_failed";
  if (status >= 200 && status < 300) return "none";
  const normalized = String(body ?? "").toLowerCase();
  if (normalized.includes("selected upstream is cooling down")) return "local_upstream_cooldown";
  if (normalized.includes("request blocked")) return "upstream_request_blocked";
  if (normalized.includes("new_api_error")) return "upstream_new_api_error";
  if (normalized.includes("transport_error")) return "upstream_transport_error";
  if (normalized.includes("response.failed")) return "response_failed_other";
  return `http_${status}`;
}

function responseHasNativeFailure(body) {
  return /\bresponse\.failed\b/u.test(String(body ?? ""));
}

function responseErrorCode(body) {
  const text = String(body ?? "");
  const responseFailed = text.indexOf("response.failed");
  const payload = responseFailed >= 0
    ? text.slice(responseFailed, responseFailed + 1_024)
    : text.match(/"error"\s*:\s*\{[^}]{0,1024}\}/u)?.[0] ?? "";
  const code = payload.match(/"code"\s*:\s*"([A-Za-z0-9._-]{1,96})"/u);
  if (code?.[1]) return code[1];
  const type = payload.match(/"type"\s*:\s*"([A-Za-z0-9._-]{1,96})"/u);
  return type?.[1] ?? null;
}

// Error bodies can contain upstream-provided text.  Keep diagnostics useful
// without retaining or printing that text: only a known class, safe JSON
// identifiers, and a one-way digest are emitted.
function safeFailureReceipt(body) {
  const raw = String(body ?? "");
  let parsed = null;
  try {
    parsed = JSON.parse(raw);
  } catch {
    // A normal SSE stream is not a JSON error body. Only parse the bounded
    // native failure event; scanning successful stream text can mistake a
    // normal model field for an error class.
    const failedAt = raw.indexOf("response.failed");
    if (failedAt >= 0) {
      const window = raw.slice(failedAt, failedAt + 1_024);
      const data = window.match(/\bdata:\s*(\{[^\r\n]{0,960}\})/u)?.[1];
      if (data) {
        try {
          parsed = JSON.parse(data);
        } catch {
          // The receipt remains a digest plus native-failure classification.
        }
      }
    }
  }
  const error = parsed?.error && typeof parsed.error === "object" ? parsed.error : parsed;
  const message = String(error?.message ?? parsed?.message ?? "").toLowerCase();
  const messageClass = [
    ["failed to decrypt", "credential_decrypt"],
    ["api key", "credential"],
    ["authorization", "authorization"],
    ["no available provider", "no_provider"],
    ["selected upstream", "upstream_selection"],
    ["cooling down", "upstream_cooldown"],
    ["model", "model"],
    ["connection", "transport"],
    ["timeout", "transport_timeout"]
  ].find(([needle]) => message.includes(needle))?.[1] ?? (
    responseHasNativeFailure(raw) ? "response_failed" : "unclassified"
  );
  const stringField = (value) => typeof value === "string" && /^[a-z0-9_.:-]{1,80}$/iu.test(value)
    ? value
    : null;
  return {
    message_class: messageClass,
    error_code: stringField(error?.code),
    error_type: stringField(error?.type),
    body_sha256_prefix: createHash("sha256").update(raw).digest("hex").slice(0, 16)
  };
}

async function waitForFinalization(baseUrl, before, knownIds) {
  const deadline = Date.now() + 15_000;
  let latest = null;
  while (Date.now() < deadline) {
    latest = await getJson(`${baseUrl}/admin/metrics`, 5_000);
    const counters = requestCounters(latest);
    const hasNew = hasNewRequestLog(latest, knownIds);
    if (
      counters.inbound_requests >= before.inbound_requests + 1 &&
      counters.generation_attempts >= before.generation_attempts + 1 &&
      counters.upstream_requests >= before.upstream_requests + 1 &&
      hasNew
    ) return latest;
    await delay(50);
  }
  return latest ?? { agent_generation: {}, recent_requests: [] };
}

function requestLogRows(metrics) {
  return [
    ...array(metrics?.recent_requests),
    ...array(metrics?.recent_failed_requests)
  ];
}

function selectNewRequestLog(metrics, knownIds) {
  return requestLogRows(metrics).find((item) => {
    const id = String(item?.inbound_request_id ?? "");
    return id && !knownIds.has(id);
  }) ?? null;
}

function hasNewRequestLog(metrics, knownIds) {
  return requestLogRows(metrics).some((item) => {
    const id = String(item?.inbound_request_id ?? "");
    return id && !knownIds.has(id);
  });
}

function summarizeArm(arm) {
  const records = arm.records;
  const warm = records.slice(1);
  const totals = aggregate(records);
  const warmTotals = aggregate(warm);
  const realms = [...new Set(records.map((item) => item.realm_id).filter(Boolean))];
  return {
    requests: records.length,
    completed: records.filter((item) => item.completed).length,
    input_tokens: totals.input_tokens,
    cache_read_tokens: totals.cache_read_tokens,
    raw_token_hit_rate: ratio(totals.cache_read_tokens, totals.input_tokens),
    cache_128_hit_rate: ratio(totals.cacheable_read_tokens_128, totals.cacheable_tokens_128),
    warm_requests: warm.length,
    warm_input_tokens: warmTotals.input_tokens,
    warm_raw_token_hit_rate: ratio(warmTotals.cache_read_tokens, warmTotals.input_tokens),
    warm_cache_128_hit_rate: ratio(warmTotals.cacheable_read_tokens_128, warmTotals.cacheable_tokens_128),
    warm_nonzero_cache_requests: warm.filter((item) => item.cache_read_tokens > 0).length,
    ttft_p50_ms: percentile(records.map((item) => item.ttft_ms).filter(Number.isFinite), 0.5),
    ttft_p95_ms: percentile(records.map((item) => item.ttft_ms).filter(Number.isFinite), 0.95),
    upstream_ttft_p95_ms: percentile(records.map((item) => item.upstream_ttft_ms).filter(Number.isFinite), 0.95),
    local_ttft_overhead_p95_ms: percentile(
      records.map((item) => item.local_ttft_overhead_ms).filter(Number.isFinite),
      0.95
    ),
    prefix_guard_wait_p95_ms: percentile(records.map((item) => item.prefix_guard_wait_ms), 0.95),
    prefix_guard_wait_total_ms: sum(records, "prefix_guard_wait_ms"),
    local_prepare_p95_ms: percentile(records.map((item) => item.local_prepare_ms), 0.95),
    max_request_body_bytes: Math.max(0, ...records.map((item) => item.request_body_bytes)),
    full_bucket_requests: records.filter((item) => item.cache_read_tokens >= cacheableInputTokens128(item.input_tokens)).length,
    cache_avoidable_gap_tokens: totals.cache_avoidable_gap_tokens,
    cache_provider_unstable_gap_tokens: totals.cache_provider_unstable_gap_tokens,
    cache_new_tail_gap_tokens: totals.cache_new_tail_gap_tokens,
    all_single_attempt: records.every((item) => item.single_attempt),
    all_full_replay: records.every((item) => item.full_replay),
    all_provider_matches: records.every((item) => item.provider_matches),
    all_completed: records.every((item) => item.completed),
    all_timing_observed: records.every((item) => item.timing_observed),
    all_validation_candidate_applied: records.every((item) => item.validation_candidate_applied),
    all_candidate_control_field_wire_present: records.every(
      (item) => item.candidate_control_field_wire_present
    ),
    all_control_field_final_wire_present: records.every(
      (item) => item.candidate_control_field_wire_present
    ),
    no_validation_candidate_applied: records.every((item) => !item.validation_candidate_applied),
    all_cache_metadata_present: records.every((item) => item.cache_metadata_present),
    all_cache_metadata_recognized: records.every((item) => item.cache_metadata_recognized),
    no_control_field_final_wire_present: records.every(
      (item) => !item.candidate_control_field_wire_present
    ),
    realm_count: realms.length
  };
}

function aggregate(records) {
  return records.reduce((total, item) => {
    const cacheable = cacheableInputTokens128(item.input_tokens);
    total.input_tokens += item.input_tokens;
    total.cache_read_tokens += item.cache_read_tokens;
    total.cacheable_tokens_128 += cacheable;
    total.cacheable_read_tokens_128 += Math.min(cacheable, item.cache_read_tokens);
    total.cache_avoidable_gap_tokens += item.cache_avoidable_gap_tokens;
    total.cache_provider_unstable_gap_tokens += item.cache_provider_unstable_gap_tokens;
    total.cache_new_tail_gap_tokens += item.cache_new_tail_gap_tokens;
    return total;
  }, {
    input_tokens: 0,
    cache_read_tokens: 0,
    cacheable_tokens_128: 0,
    cacheable_read_tokens_128: 0,
    cache_avoidable_gap_tokens: 0,
    cache_provider_unstable_gap_tokens: 0,
    cache_new_tail_gap_tokens: 0
  });
}

function buildChecks({ baseline, candidate, baselineSummary, candidateSummary, tokenSymmetry }) {
  const allRecords = [...baseline.records, ...candidate.records];
  const realmSet = new Set(allRecords.map((item) => item.realm_id).filter(Boolean));
  const cacheMetadataSymmetry = cacheMetadataPairSymmetry(
    baseline.records,
    candidate.records,
    controlField,
    controlFieldExpectedOnWire(baselineCapabilityMode),
    controlFieldExpectedOnWire(candidateCapabilityMode)
  );
  const baselineControlFieldExpected = controlFieldExpectedOnWire(baselineCapabilityMode);
  const candidateControlFieldExpected = controlFieldExpectedOnWire(candidateCapabilityMode);
  const controlFieldIsolation = cacheControlFieldIsolation(
    baseline.records,
    candidate.records,
    controlField,
    baselineCapabilityMode,
    candidateCapabilityMode,
    preservedControlField
  );
  const latencyAcceptance = resolveLatencyAcceptance({
    baselineTtftP95Ms: baselineSummary.ttft_p95_ms,
    candidateTtftP95Ms: candidateSummary.ttft_p95_ms,
    baselineLocalTtftOverheadP95Ms: baselineSummary.local_ttft_overhead_p95_ms,
    candidateLocalTtftOverheadP95Ms: candidateSummary.local_ttft_overhead_p95_ms,
    maxEndToEndRegressionMs: maxTtftRegressionMs,
    maxLocalOverheadRegressionMs: maxLocalTtftOverheadRegressionMs,
    allowUpstreamRegression: allowUpstreamTtftRegression
  });
  return {
    minimum_turns_per_arm: baseline.records.length >= minimumTurns && candidate.records.length >= minimumTurns,
    each_arm_reached_input_budget: baselineSummary.input_tokens >= targetInputTokens && candidateSummary.input_tokens >= targetInputTokens,
    every_sse_completed: baselineSummary.all_completed && candidateSummary.all_completed,
    complete_timing_coverage: baselineSummary.all_timing_observed && candidateSummary.all_timing_observed,
    one_upstream_post_per_inbound: baselineSummary.all_single_attempt && candidateSummary.all_single_attempt,
    all_requests_remained_full_replay: baselineSummary.all_full_replay && candidateSummary.all_full_replay,
    selected_provider_model_unchanged: baselineSummary.all_provider_matches && candidateSummary.all_provider_matches,
    input_token_symmetry: tokenSymmetry.pass,
    one_key_realm_per_arm: baselineSummary.realm_count === 1 && candidateSummary.realm_count === 1,
    same_key_realm_across_arms: realmSet.size === 1,
    selected_realm_pinned: !expectedRealmHash || allRecords.every((item) => item.realm_id === expectedRealmHash),
    final_wire_cache_control_witness_observed:
      baselineSummary.all_cache_metadata_present &&
      candidateSummary.all_cache_metadata_present &&
      baselineSummary.all_cache_metadata_recognized &&
      candidateSummary.all_cache_metadata_recognized,
    baseline_control_field_matches_expected: baselineControlFieldExpected === null ||
      baseline.records.every(
        (item) => item.candidate_control_field_wire_present === baselineControlFieldExpected
      ),
    candidate_control_field_matches_expected: candidateControlFieldExpected === null ||
      candidate.records.every(
        (item) => item.candidate_control_field_wire_present === candidateControlFieldExpected
      ),
    baseline_did_not_apply_control_field: !baselineControlFieldExpected ||
      baselineSummary.no_control_field_final_wire_present,
    candidate_applied_control_field: !candidateControlFieldExpected ||
      candidateSummary.all_control_field_final_wire_present,
    prompt_cache_key_wire_shape: controlField !== "prompt-cache-key" || cacheMetadataSymmetry.pass,
    cache_metadata_final_wire_pair_delta: cacheMetadataSymmetry.pass,
    cache_control_field_isolated: controlFieldIsolation.pass,
    candidate_ttft_p95_not_regressed: latencyAcceptance.end_to_end_pass,
    candidate_local_ttft_overhead_p95_not_regressed: latencyAcceptance.local_overhead_pass,
    candidate_latency_policy_pass: latencyAcceptance.policy_pass,
    no_downstream_disconnect: allRecords.every((item) => !item.downstream_disconnected),
    no_local_avoidable_gap: baselineSummary.cache_avoidable_gap_tokens === 0 && candidateSummary.cache_avoidable_gap_tokens === 0,
    cache_metadata_pair_symmetry: cacheMetadataSymmetry.pass
  };
}

function compareSummaries(baseline, candidate, tokenSymmetry) {
  return {
    raw_token_hit_rate_points: round(candidate.raw_token_hit_rate - baseline.raw_token_hit_rate, 4),
    cache_128_hit_rate_points: round(candidate.cache_128_hit_rate - baseline.cache_128_hit_rate, 4),
    warm_cache_128_hit_rate_points: round(candidate.warm_cache_128_hit_rate - baseline.warm_cache_128_hit_rate, 4),
    ttft_p95_ms: candidate.ttft_p95_ms - baseline.ttft_p95_ms,
    upstream_ttft_p95_ms: candidate.upstream_ttft_p95_ms - baseline.upstream_ttft_p95_ms,
    local_ttft_overhead_p95_ms:
      candidate.local_ttft_overhead_p95_ms - baseline.local_ttft_overhead_p95_ms,
    max_input_token_delta: tokenSymmetry.max_input_token_delta,
    provider_unstable_gap_tokens: candidate.cache_provider_unstable_gap_tokens - baseline.cache_provider_unstable_gap_tokens,
    new_tail_gap_tokens: candidate.cache_new_tail_gap_tokens - baseline.cache_new_tail_gap_tokens
  };
}

function inputTokenSymmetry(baselineRecords, candidateRecords, allowedDelta = maxInputTokenDelta) {
  const baselineRecordCount = baselineRecords.length;
  const candidateRecordCount = candidateRecords.length;
  const expectedPairCount = Math.max(baselineRecordCount, candidateRecordCount);
  const pairCount = Math.min(baselineRecords.length, candidateRecords.length);
  const deltas = Array.from({ length: pairCount }, (_, index) =>
    Math.abs(number(baselineRecords[index]?.input_tokens) - number(candidateRecords[index]?.input_tokens))
  );
  const maxDelta = deltas.length ? Math.max(...deltas) : Number.POSITIVE_INFINITY;
  return {
    pair_count: pairCount,
    expected_pair_count: expectedPairCount,
    baseline_record_count: baselineRecordCount,
    candidate_record_count: candidateRecordCount,
    max_input_token_delta: maxDelta,
    allowed_input_token_delta: allowedDelta,
    pass: baselineRecordCount === candidateRecordCount &&
      pairCount === expectedPairCount &&
      maxDelta <= allowedDelta
  };
}

function controlFieldJsonName(field) {
  if (field === "prompt-cache-key") return "prompt_cache_key";
  if (field === "prompt-cache-retention") return "prompt_cache_retention";
  throw new Error(`unsupported control field: ${field}`);
}

function finalWireCacheControlEvidence(metric) {
  const cacheMetadata = typeof metric?.outbound_prefix_fingerprints?.cache_metadata === "string" &&
    /^sha256-128:[0-9a-f]{32}$/iu.test(metric.outbound_prefix_fingerprints.cache_metadata)
    ? metric.outbound_prefix_fingerprints.cache_metadata
    : null;
  const fields = cacheMetadata
    ? FINAL_WIRE_CACHE_METADATA_WITNESSES.get(cacheMetadata) ?? null
    : null;
  return {
    cache_metadata: cacheMetadata,
    observed: cacheMetadata !== null,
    recognized: Array.isArray(fields),
    fields: Array.isArray(fields) ? [...fields] : []
  };
}

function buildFinalWireCacheMetadataWitnesses() {
  const fieldOrder = [
    "prompt_cache_key",
    "prompt_cache_retention",
    "prompt_cache_options"
  ];
  const witnesses = new Map();
  for (let mask = 0; mask < (1 << fieldOrder.length); mask += 1) {
    const fields = fieldOrder.filter((_, index) => (mask & (1 << index)) !== 0);
    const members = fields.map((field) => {
      if (field === "prompt_cache_key") {
        return '"prompt_cache_key":"[redacted]"';
      }
      if (field === "prompt_cache_retention") {
        return '"prompt_cache_retention":"24h"';
      }
      return '"prompt_cache_options":{"mode":"implicit","ttl":"30m"}';
    });
    witnesses.set(cacheMetadataFingerprint(`{${members.join(",")}}`), fields);
  }
  return witnesses;
}

function cacheMetadataFingerprint(memberProjection) {
  const digest = createHash("sha256")
    .update(`cache_metadata\0${memberProjection}`)
    .digest("hex")
    .slice(0, 32);
  return `sha256-128:${digest}`;
}

// `cache_metadata` is derived from the frozen final-wire cache members with
// the opaque prompt_cache_key value redacted. Pairing the two arms makes this
// a safe field-presence witness; `provider_prefix_key` alone can come from a
// legacy or recovery path and is never used by this probe as proof of wire
// injection.
function cacheMetadataPairSymmetry(
  baselineRecords,
  candidateRecords,
  field,
  baselineFieldExpected = false,
  candidateFieldExpected = true
) {
  const expectedField = controlFieldJsonName(field);
  const pairCount = Math.min(baselineRecords.length, candidateRecords.length);
  const pairs = Array.from({ length: pairCount }, (_, index) => ({
    baseline: baselineRecords[index]?.cache_metadata ?? null,
    candidate: candidateRecords[index]?.cache_metadata ?? null,
    baseline_recognized: baselineRecords[index]?.cache_metadata_recognized === true,
    candidate_recognized: candidateRecords[index]?.cache_metadata_recognized === true,
    baseline_fields: array(baselineRecords[index]?.final_wire_cache_control_fields),
    candidate_fields: array(candidateRecords[index]?.final_wire_cache_control_fields)
  }));
  const allObserved = pairs.length > 0 && pairs.every((pair) =>
    typeof pair.baseline === "string" &&
    typeof pair.candidate === "string" &&
    /^sha256-128:[0-9a-f]{32}$/iu.test(pair.baseline) &&
    /^sha256-128:[0-9a-f]{32}$/iu.test(pair.candidate)
  );
  const allRecognized = pairs.length > 0 && pairs.every(
    (pair) => pair.baseline_recognized && pair.candidate_recognized
  );
  const changedPairCount = pairs.filter((pair) => pair.baseline !== pair.candidate).length;
  const matchingMetadataExpected = baselineFieldExpected === candidateFieldExpected;
  const expectedFieldShape = pairs.length > 0 && pairs.every((pair) => {
    const baselinePresent = pair.baseline_fields.includes(expectedField);
    const candidatePresent = pair.candidate_fields.includes(expectedField);
    if (baselineFieldExpected === null && candidateFieldExpected === null) {
      return baselinePresent === candidatePresent;
    }
    return (baselineFieldExpected === null || baselinePresent === baselineFieldExpected) &&
      (candidateFieldExpected === null || candidatePresent === candidateFieldExpected);
  });
  const metadataShapeMatches = matchingMetadataExpected
    ? changedPairCount === 0
    : changedPairCount === pairCount;
  return {
    pair_count: pairCount,
    baseline_record_count: baselineRecords.length,
    candidate_record_count: candidateRecords.length,
    all_observed: allObserved,
    all_recognized: allRecognized,
    changed_pair_count: changedPairCount,
    expected_field: expectedField,
    baseline_field_expected: baselineFieldExpected,
    candidate_field_expected: candidateFieldExpected,
    metadata_relationship: matchingMetadataExpected ? "equal" : "different",
    expected_field_shape: expectedFieldShape,
    pass: baselineRecords.length === candidateRecords.length &&
      pairCount > 0 &&
      allObserved &&
      allRecognized &&
      expectedFieldShape &&
      metadataShapeMatches
  };
}

// A `source` arm may legitimately retain more than the one field under test
// (for example prompt-cache retention alongside prompt-cache key). That is a
// useful bundle comparison, but it must never be labelled a single-field
// effect. Exact-binary source/source comparisons are unaffected; an isolated
// config effect requires both arms to be rewritten modes and their final-wire
// field sets to match the expected one-field projection exactly.
function cacheControlFieldIsolation(
  baselineRecords,
  candidateRecords,
  field,
  baselineMode,
  candidateMode,
  preservedField = null
) {
  const baselineSource = baselineMode === "source";
  const candidateSource = candidateMode === "source";
  const fieldName = controlFieldJsonName(field);
  if (baselineSource && candidateSource) {
    return {
      applicable: false,
      pass: true,
      reason: "binary_source_comparison"
    };
  }
  if (baselineSource !== candidateSource) {
    return {
      applicable: true,
      pass: false,
      reason: "source_bundle_not_single_field"
    };
  }
  const preservedFieldName = preservedField ? controlFieldJsonName(preservedField) : null;
  const expectedFieldsFor = (mode) => [
    ...(preservedFieldName ? [preservedFieldName] : []),
    ...(controlFieldExpectedOnWire(mode) ? [fieldName] : [])
  ].sort();
  const baselineExpected = expectedFieldsFor(baselineMode);
  const candidateExpected = expectedFieldsFor(candidateMode);
  const hasExactFields = (records, expected) => records.length > 0 && records.every((record) => {
    const actual = [...array(record?.final_wire_cache_control_fields)].sort();
    return actual.length === expected.length && actual.every((value, index) => value === expected[index]);
  });
  return {
    applicable: true,
    pass: hasExactFields(baselineRecords, baselineExpected) &&
      hasExactFields(candidateRecords, candidateExpected),
    reason: "exact_single_field_projection",
    baseline_expected_fields: baselineExpected,
    candidate_expected_fields: candidateExpected
  };
}

function resolveLatencyAcceptance({
  baselineTtftP95Ms,
  candidateTtftP95Ms,
  baselineLocalTtftOverheadP95Ms,
  candidateLocalTtftOverheadP95Ms,
  maxEndToEndRegressionMs,
  maxLocalOverheadRegressionMs,
  allowUpstreamRegression
}) {
  const endToEndPass = Number.isFinite(baselineTtftP95Ms) && Number.isFinite(candidateTtftP95Ms) &&
    candidateTtftP95Ms <= baselineTtftP95Ms + maxEndToEndRegressionMs;
  const localOverheadPass = Number.isFinite(baselineLocalTtftOverheadP95Ms) &&
    Number.isFinite(candidateLocalTtftOverheadP95Ms) &&
    candidateLocalTtftOverheadP95Ms <=
      baselineLocalTtftOverheadP95Ms + maxLocalOverheadRegressionMs;
  return {
    end_to_end_pass: endToEndPass,
    local_overhead_pass: localOverheadPass,
    policy_pass: endToEndPass || (allowUpstreamRegression && localOverheadPass),
    policy: allowUpstreamRegression
      ? "local-overhead-bounded-upstream-ttft-exempt"
      : "strict-end-to-end-ttft"
  };
}

function checksPassUnderLatencyPolicy(checks) {
  return Object.entries(checks).every(([name, value]) =>
    name === "candidate_ttft_p95_not_regressed"
      ? checks.candidate_latency_policy_pass === true
      : name === "candidate_local_ttft_overhead_p95_not_regressed" && !allowUpstreamTtftRegression
        ? true
      : value === true
  );
}

function describeOutcome(baseline, candidate, checks, checksPass = checksPassUnderLatencyPolicy(checks)) {
  if (!checksPass) return "invalid_or_upstream_contaminated";
  if (candidate.warm_cache_128_hit_rate > baseline.warm_cache_128_hit_rate) return "candidate_positive";
  if (candidate.warm_cache_128_hit_rate < baseline.warm_cache_128_hit_rate) return "candidate_negative";
  return "no_measurable_difference";
}

function timingSamples(records) {
  return records.map((item, index) => ({
    turn: index + 1,
    input_tokens: item.input_tokens,
    cache_read_tokens: item.cache_read_tokens,
    response_context_plan: item.response_context_plan,
    cache_avoidable_gap_tokens: item.cache_avoidable_gap_tokens,
    cache_provider_unstable_gap_tokens: item.cache_provider_unstable_gap_tokens,
    cache_new_tail_gap_tokens: item.cache_new_tail_gap_tokens,
    request_body_bytes: item.request_body_bytes,
    ttft_ms: item.ttft_ms,
    upstream_ttft_ms: item.upstream_ttft_ms,
    local_ttft_overhead_ms: item.local_ttft_overhead_ms,
    prefix_guard_wait_ms: item.prefix_guard_wait_ms,
    prefix_guard_wait_reason: item.prefix_guard_wait_reason,
    prefix_guard_wait_source: item.prefix_guard_wait_source,
    prefix_guard_skip_reason: item.prefix_guard_skip_reason,
    local_prepare_ms: item.local_prepare_ms,
    final_wire_cache_metadata_observed: item.cache_metadata_present,
    final_wire_cache_metadata_recognized: item.cache_metadata_recognized,
    final_wire_cache_control_fields: item.final_wire_cache_control_fields,
    control_field_final_wire_present: item.candidate_control_field_wire_present
  }));
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
  return inputTokens < 1_024 ? 0 : 1_024 + Math.floor((inputTokens - 1_024) / 128) * 128;
}

function message(text) {
  return { type: "message", role: "user", content: [{ type: "input_text", text }] };
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
    return { start, end, body: text.slice(start, end) };
  });
}

function extractTomlRawValue(text, key) {
  const escaped = key.replace(/[.*+?^${}()|[\]\\]/gu, "\\$&");
  return text.match(new RegExp(`^${escaped}\\s*=\\s*(.+?)\\s*$`, "mu"))?.[1] ?? "";
}

function extractTomlValuePresent(text, key) {
  return extractTomlRawValue(text, key) !== "";
}

function tomlArrayBlocksWithOffsets(text, section) {
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
    return { start, end, body: text.slice(start, end) };
  });
}

function providerKeyPoolContext(configText, selectedProviderId) {
  const pools = tomlArrayBlocksWithOffsets(configText, "provider_key_pools")
    .filter((block) => extractTomlString(block.body, "provider_id") === selectedProviderId);
  if (pools.length === 0) return null;
  if (pools.length !== 1) {
    throw new Error("isolated key pin requires exactly one provider key pool");
  }
  const pool = pools[0];
  const nextPoolStart = tomlArrayBlocksWithOffsets(configText, "provider_key_pools")
    .map((block) => block.start)
    .filter((start) => start > pool.start)
    .sort((left, right) => left - right)[0] ?? configText.length;
  const keys = tomlArrayBlocksWithOffsets(configText, "provider_key_pools.keys")
    .filter((block) => block.start > pool.start && block.start < nextPoolStart);
  return { pool, keys };
}

function validatePinnedKeyConfiguration(configText, selectedProviderId, selectedKeyId) {
  const context = providerKeyPoolContext(configText, selectedProviderId);
  if (!context) throw new Error("--key-id requires an enabled Provider Key pool for the selected Codex Provider");
  if (extractTomlBoolean(context.pool.body, "enabled") !== true) {
    throw new Error("--key-id requires the selected Provider Key pool to be enabled");
  }
  const target = context.keys.find((block) => extractTomlString(block.body, "id") === selectedKeyId);
  if (!target || !extractTomlValuePresent(target.body, "key_encrypted")) {
    throw new Error("the explicit --key-id is not a saved Key in the selected Provider Key pool");
  }
  assertPinnedKeyUsable(target.body, selectedKeyId);
}

function assertPinnedKeyUsable(keyBody, selectedKeyId) {
  if (extractTomlBoolean(keyBody, "enabled") !== true) {
    throw new Error(`--key-id ${selectedKeyId} is disabled or cooling down; live verification must not revive it`);
  }
  const disabledUntil = extractTomlRawValue(keyBody, "disabled_until");
  if (!disabledUntil) return;
  const timestamp = Date.parse(disabledUntil.replace(/^['"]|['"]$/gu, ""));
  if (!Number.isFinite(timestamp) || timestamp > Date.now()) {
    throw new Error(`--key-id ${selectedKeyId} is disabled or cooling down; live verification must not revive it`);
  }
}

function replaceTomlBooleanField(block, key, value) {
  const escaped = key.replace(/[.*+?^${}()|[\]\\]/gu, "\\$&");
  const field = new RegExp(`^${escaped}[\\t ]*=[\\t ]*(?:true|false)[\\t ]*$`, "mu");
  const replacement = `${key} = ${value ? "true" : "false"}`;
  return field.test(block)
    ? block.replace(field, replacement)
    : `${block.trimEnd()}\n${replacement}\n`;
}

function removeTomlField(block, key) {
  const escaped = key.replace(/[.*+?^${}()|[\]\\]/gu, "\\$&");
  return block.replace(new RegExp(`^${escaped}\\s*=.*(?:\\r?\\n|$)`, "gmu"), "");
}

function pinProviderKeyInToml(configText, selectedProviderId, selectedKeyId) {
  const context = providerKeyPoolContext(configText, selectedProviderId);
  if (!context) throw new Error("cannot pin a Key without the selected Provider Key pool");
  const target = context.keys.find((block) => extractTomlString(block.body, "id") === selectedKeyId);
  if (!target || !extractTomlValuePresent(target.body, "key_encrypted")) {
    throw new Error("the explicit --key-id is not a saved Key in the selected Provider Key pool");
  }
  assertPinnedKeyUsable(target.body, selectedKeyId);
  let rewritten = configText;
  for (const block of [...context.keys].sort((left, right) => right.start - left.start)) {
    const isTarget = extractTomlString(block.body, "id") === selectedKeyId;
    let body = replaceTomlBooleanField(block.body, "enabled", isTarget);
    if (isTarget) body = removeTomlField(body, "disabled_until");
    rewritten = `${rewritten.slice(0, block.start)}${body}${rewritten.slice(block.end)}`;
  }
  return rewritten;
}

function extractTomlString(text, key) {
  const escaped = key.replace(/[.*+?^${}()|[\]\\]/gu, "\\$&");
  return text.match(new RegExp(`^${escaped}\\s*=\\s*"([^"]*)"`, "mu"))?.[1] ?? "";
}

function extractTomlBoolean(text, key) {
  const escaped = key.replace(/[.*+?^${}()|[\]\\]/gu, "\\$&");
  return text.match(new RegExp(`^${escaped}\\s*=\\s*(true|false)`, "mu"))?.[1] === "true";
}

function codexAgentRoute(configText) {
  const block = tomlArrayBlocks(configText, "agent_injections")
    .map((item) => item.body)
    .find((item) => extractTomlString(item, "id") === "codex");
  if (!block) return null;
  return {
    provider_id: extractTomlString(block, "provider_id"),
    model_id: extractTomlString(block, "model_id"),
    enabled: extractTomlBoolean(block, "enabled")
  };
}

async function findAvailablePort(start) {
  for (let port = start; port <= Math.min(start + 40, 65_500); port += 1) {
    if (port === 18_883) continue;
    if (await portIsAvailable(port)) return port;
  }
  throw new Error("no isolated loopback port available");
}

function portIsAvailable(port) {
  return new Promise((resolvePort) => {
    const server = createServer();
    server.once("error", () => resolvePort(false));
    server.listen(port, "127.0.0.1", () => server.close(() => resolvePort(true)));
  });
}

async function waitForHealth(baseUrl, child, label) {
  const deadline = Date.now() + 30_000;
  while (Date.now() < deadline) {
    if (!processIsAlive(child.pid)) throw new Error(`${label} isolated proxy exited during startup`);
    try {
      const health = await getJson(`${baseUrl}/health`, 1_000);
      if (health?.ok === true) return;
    } catch {
      // The child is still starting.
    }
    await delay(100);
  }
  throw new Error(`${label} isolated proxy did not become healthy`);
}

async function stopRuntime(runtime) {
  if (runtime) await stopChild(runtime.child, runtime.label);
}

async function stopChild(child, label) {
  if (!child || !processIsAlive(child.pid)) return;
  child.kill();
  const deadline = Date.now() + 15_000;
  while (processIsAlive(child.pid) && Date.now() < deadline) await delay(50);
  if (processIsAlive(child.pid)) throw new Error(`${label} isolated proxy did not stop cleanly`);
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
  if (!response.ok) throw new Error(`admin endpoint returned HTTP ${response.status}`);
  return response.json();
}

async function assertFile(path, label) {
  const metadata = await stat(path);
  if (!metadata.isFile() || metadata.size <= 0) throw new Error(`${label} is missing`);
}

async function readRequiredText(path, label) {
  try {
    return await readFile(path, "utf8");
  } catch {
    throw new Error(`${label} is not readable`);
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

function defaultConfigDir() {
  if (process.platform === "win32" && process.env.APPDATA) return join(process.env.APPDATA, "Atoapi");
  return join(process.env.XDG_CONFIG_HOME ?? join(homedir(), ".config"), "Atoapi");
}

function stableGuid(arm, id) {
  const hex = createHash("sha256").update(`${arm}\0${id}`).digest("hex");
  return `${hex.slice(0, 8)}-${hex.slice(8, 12)}-${hex.slice(12, 16)}-${hex.slice(16, 20)}-${hex.slice(20, 32)}`;
}

function shortHash(value) {
  return createHash("sha256").update(value).digest("hex").slice(0, 16);
}

function requiredString(value, name) {
  const normalized = String(value ?? "").trim();
  if (!normalized) throw new Error(`${name} is required`);
  return normalized;
}

function optionalRealmHash(value) {
  const normalized = String(value ?? "").trim().toLowerCase();
  if (!normalized) return null;
  if (!/^[0-9a-f]{64}$/u.test(normalized)) {
    throw new Error("--key-realm-hash must be a 64-character hexadecimal realm hash");
  }
  return normalized;
}

function optionalOpaqueIdentifier(value, name) {
  const normalized = String(value ?? "").trim();
  if (!normalized) return null;
  if (!/^[A-Za-z0-9][A-Za-z0-9._:-]{0,159}$/u.test(normalized)) {
    throw new Error(`${name} must be a safe opaque identifier`);
  }
  return normalized;
}

function boundedInteger(value, name, minimum, maximum) {
  const parsed = Number(value);
  if (!Number.isInteger(parsed) || parsed < minimum || parsed > maximum) {
    throw new Error(`${name} must be an integer from ${minimum} through ${maximum}`);
  }
  return parsed;
}

function normalizeFirstArm(value) {
  const normalized = String(value ?? "").trim().toLowerCase();
  if (normalized === "baseline" || normalized === "candidate") return normalized;
  throw new Error("--first-arm must be baseline or candidate");
}

function normalizeCapabilityMode(value) {
  const normalized = String(value ?? "").trim().toLowerCase();
  if (normalized === "source" || normalized === "unsupported" || normalized === "verified") {
    return normalized;
  }
  throw new Error("capability mode must be source, unsupported, or verified");
}

function controlFieldExpectedOnWire(capabilityMode) {
  if (capabilityMode === "source") return null;
  return capabilityMode !== "unsupported";
}

function interleavedArmOrder(turnIndex, initialArm) {
  const first = normalizeFirstArm(initialArm);
  const initialTurn = Number(turnIndex) % 2 === 0;
  const baselineFirst = initialTurn ? first === "baseline" : first !== "baseline";
  return baselineFirst ? ["baseline", "candidate"] : ["candidate", "baseline"];
}

function serialArmOrder(initialArm) {
  const first = normalizeFirstArm(initialArm);
  return first === "baseline" ? ["baseline", "candidate"] : ["candidate", "baseline"];
}

function parseArgs(items) {
  const parsed = {};
  for (let index = 0; index < items.length; index += 1) {
    const item = items[index];
    if (!item.startsWith("--")) continue;
    const [key, inline] = item.slice(2).split("=", 2);
    if (inline !== undefined) {
      parsed[key] = inline;
      continue;
    }
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

function booleanArg(value) {
  return value === true || value === "true" || value === "1";
}

function array(value) {
  return Array.isArray(value) ? value : [];
}

function number(value) {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : 0;
}

function ratio(numerator, denominator) {
  return denominator > 0 ? round((numerator * 100) / denominator, 4) : 0;
}

function sum(records, field) {
  return records.reduce((total, item) => total + number(item?.[field]), 0);
}

function optionalNumber(value) {
  const parsed = Number(value);
  return Number.isFinite(parsed) && parsed >= 0 ? parsed : null;
}

function safeMetricLabel(value) {
  return typeof value === "string" && /^[a-z0-9_:-]{1,120}$/iu.test(value)
    ? value
    : null;
}

function percentile(values, quantile) {
  if (!values.length) return null;
  const sorted = [...values].sort((left, right) => left - right);
  const index = Math.min(sorted.length - 1, Math.max(0, Math.ceil(sorted.length * quantile) - 1));
  return sorted[index];
}

function round(value, digits) {
  const scale = 10 ** digits;
  return Math.round(value * scale) / scale;
}

function delay(ms) {
  return new Promise((resolveDelay) => setTimeout(resolveDelay, ms));
}

function safeError(error) {
  const message = error instanceof Error ? error.message : String(error);
  return message.replace(/[\r\n]+/gu, " ").slice(0, 280);
}

async function emitResult(result, asError) {
  const serialized = `${JSON.stringify(result, null, 2)}\n`;
  if (outputPath) await writeFile(outputPath, serialized, "utf8");
  if (asError) {
    console.error(serialized);
  } else {
    console.log(serialized);
  }
}

// Failure reports intentionally contain only verifier-owned booleans,
// counters, and a one-way upstream receipt.  Requests, response bodies,
// headers, and credentials are never retained in a failed A/B artifact.
function safeInvariantDiagnostics(error) {
  if (!(error instanceof InvariantError) || !error.diagnostics || typeof error.diagnostics !== "object") {
    return null;
  }
  const source = error.diagnostics;
  const counter = source.counters && typeof source.counters === "object" ? source.counters : {};
  const receipt = source.failure_receipt && typeof source.failure_receipt === "object"
    ? source.failure_receipt
    : {};
  return {
    arm: source.arm === "baseline" || source.arm === "candidate" ? source.arm : null,
    turn: boundedSafeInteger(source.turn, 1, 60),
    http_status: boundedSafeInteger(source.http_status, 0, 999),
    failure_class: safeMetricLabel(source.failure_class),
    response_failed: source.response_failed === true,
    response_failure_code: safeMetricLabel(source.response_failure_code),
    sse_end_reason: safeMetricLabel(source.sse_end_reason),
    failure_receipt: {
      message_class: safeMetricLabel(receipt.message_class),
      error_code: safeMetricLabel(receipt.error_code),
      error_type: safeMetricLabel(receipt.error_type),
      body_sha256_prefix: typeof receipt.body_sha256_prefix === "string" &&
        /^[0-9a-f]{16}$/iu.test(receipt.body_sha256_prefix)
        ? receipt.body_sha256_prefix
        : null
    },
    metric_observed: source.metric_observed === true,
    completed: source.completed === true,
    single_attempt: source.single_attempt === true,
    input_tokens: boundedSafeInteger(source.input_tokens, 0, 100_000_000),
    full_replay: source.full_replay === true,
    provider_matches: source.provider_matches === true,
    affinity_realm_present: source.affinity_realm_present === true,
    timing_observed: source.timing_observed === true,
    downstream_disconnected: source.downstream_disconnected === true,
    counters: {
      inbound_requests: boundedSafeInteger(counter.inbound_requests, 0, 1_000_000),
      generation_attempts: boundedSafeInteger(counter.generation_attempts, 0, 1_000_000),
      upstream_requests: boundedSafeInteger(counter.upstream_requests, 0, 1_000_000)
    }
  };
}

function partialArmResult(arm) {
  if (!arm) return null;
  return {
    request_count: arm.records.length,
    summary: summarizeArm(arm),
    timing_samples: timingSamples(arm.records)
  };
}

function boundedSafeInteger(value, minimum, maximum) {
  const parsed = Number(value);
  return Number.isInteger(parsed) && parsed >= minimum && parsed <= maximum ? parsed : null;
}

function printUsage() {
  console.log([
    "Usage:",
    "  node scripts/verify-generated-prompt-cache-key-cross-ab.mjs \\",
    "    --provider-id <id> --model <id> [--field prompt-cache-key|prompt-cache-retention] \\",
    "    [--preserve-control-field prompt-cache-key|prompt-cache-retention] \\",
    "    [--exe <atoapi.exe>] [--baseline-exe <atoapi.exe>] [--candidate-exe <atoapi.exe>] \\",
    "    [--baseline-capability source|unsupported|verified] [--candidate-capability source|unsupported|verified] \\",
    "    [--source-config-dir <dir>] [--lines 1000] [--serial] [--between-arms-delay-ms 0] \\",
    "    [--first-arm baseline|candidate] \\",
    "    [--key-id <opaque-id>] [--key-realm-hash <64-hex>] \\",
    "    [--max-ttft-regression-ms 0] [--allow-upstream-ttft-regression] \\",
    "    [--max-local-ttft-overhead-regression-ms 500] [--max-input-token-delta 128] [--output <report.json>]",
    "Runs isolated cache-control or exact-binary A/B comparisons for one field; --serial stops one arm process before starting the other."
  ].join("\n"));
}

function runSelfTest() {
  const fixtureFamily = stableGuid("fixture-family", "self-test");
  const baselinePrefix = buildStablePrefix(fixtureFamily, 0, 41);
  const candidatePrefix = buildStablePrefix(fixtureFamily, 1, 41);
  const baselineLines = baselinePrefix.trimEnd().split("\n");
  const candidateLines = candidatePrefix.trimEnd().split("\n");
  assert.equal(baselinePrefix.length, candidatePrefix.length);
  assert.notEqual(baselineLines[0], candidateLines[0]);
  assert.equal(baselineLines.length, candidateLines.length);
  assert.deepEqual(baselineLines.slice(1), candidateLines.slice(1));
  assert.equal(baselineLines[0].length, candidateLines[0].length);
  assert.equal(buildEqualLengthTail(1).length, buildEqualLengthTail(9).length);
  assert.equal(buildStablePrefix(fixtureFamily, 0, 10).trimEnd().split("\n").length, 10);
  assert.deepEqual(interleavedArmOrder(0, "baseline"), ["baseline", "candidate"]);
  assert.deepEqual(interleavedArmOrder(1, "baseline"), ["candidate", "baseline"]);
  assert.deepEqual(interleavedArmOrder(0, "candidate"), ["candidate", "baseline"]);
  assert.deepEqual(interleavedArmOrder(1, "candidate"), ["baseline", "candidate"]);
  assert.deepEqual(serialArmOrder("baseline"), ["baseline", "candidate"]);
  assert.deepEqual(serialArmOrder("candidate"), ["candidate", "baseline"]);

  const keyPoolFixture = [
    '[[provider_key_pools]]\nprovider_id = "provider-test"\nenabled = true\n',
    '[[provider_key_pools.keys]]\nid = "key-one"\nkey_encrypted = "dpapi:test-one"\nenabled = true\n',
    '[[provider_key_pools.keys]]\nid = "key-two"\nkey_encrypted = "dpapi:test-two"\nenabled = true\ndisabled_until = "2020-01-01T00:00:00Z"\n',
    '[[provider_key_pools]]\nprovider_id = "other-provider"\nenabled = true\n'
  ].join("\n");
  validatePinnedKeyConfiguration(keyPoolFixture, "provider-test", "key-two");
  const pinnedFixture = pinProviderKeyInToml(keyPoolFixture, "provider-test", "key-two");
  const pinnedKeys = tomlArrayBlocks(pinnedFixture, "provider_key_pools.keys");
  assert.equal(pinnedKeys.length, 2);
  assert.equal(extractTomlBoolean(pinnedKeys[0].body, "enabled"), false);
  assert.equal(extractTomlBoolean(pinnedKeys[1].body, "enabled"), true);
  assert.equal(extractTomlValuePresent(pinnedKeys[1].body, "disabled_until"), false);

  const symmetric = inputTokenSymmetry(
    [{ input_tokens: 100_000 }, { input_tokens: 120_000 }],
    [{ input_tokens: 100_064 }, { input_tokens: 119_936 }],
    128
  );
  assert.equal(symmetric.pass, true);
  const excessDelta = inputTokenSymmetry(
    [{ input_tokens: 100_000 }],
    [{ input_tokens: 100_129 }],
    128
  );
  assert.equal(excessDelta.pass, false);
  const unequalRecordCount = inputTokenSymmetry(
    [{ input_tokens: 100_000 }],
    [{ input_tokens: 100_000 }, { input_tokens: 100_000 }],
    128
  );
  assert.equal(unequalRecordCount.pass, false);
  const strictLatency = resolveLatencyAcceptance({
    baselineTtftP95Ms: 3_000,
    candidateTtftP95Ms: 3_200,
    baselineLocalTtftOverheadP95Ms: 500,
    candidateLocalTtftOverheadP95Ms: 520,
    maxEndToEndRegressionMs: 0,
    maxLocalOverheadRegressionMs: 500,
    allowUpstreamRegression: false
  });
  const upstreamExemptLatency = resolveLatencyAcceptance({
    baselineTtftP95Ms: 3_000,
    candidateTtftP95Ms: 3_200,
    baselineLocalTtftOverheadP95Ms: 500,
    candidateLocalTtftOverheadP95Ms: 520,
    maxEndToEndRegressionMs: 0,
    maxLocalOverheadRegressionMs: 500,
    allowUpstreamRegression: true
  });
  assert.equal(strictLatency.end_to_end_pass, false);
  assert.equal(strictLatency.policy_pass, false);
  assert.equal(upstreamExemptLatency.local_overhead_pass, true);
  assert.equal(upstreamExemptLatency.policy_pass, true);

  // A key-specific record overrides the generic record at dispatch. The
  // isolated baseline must rewrite both, otherwise it silently keeps the
  // production prompt_cache_key despite the generic control being unsupported.
  const capabilityFixture = [
    '[[provider_cache_capabilities]]\nprovider_id = "provider-test"\nmodel_id = "model-test"\nchannel = "responses"\nfield = "prompt-cache-key"\nstatus = "verified"\neffect_status = "unverified"\n',
    '[[provider_cache_capabilities]]\nprovider_id = "provider-test"\nmodel_id = "model-test"\nchannel = "responses"\nkey_id = "key-test"\nfield = "prompt-cache-key"\nstatus = "verified"\neffect_status = "unverified"\n',
    '[[provider_cache_capabilities]]\nprovider_id = "provider-test"\nmodel_id = "model-test"\nchannel = "responses"\nkey_id = "key-test"\nfield = "prompt-cache-retention"\nstatus = "verified"\neffect_status = "unverified"\n',
    '[[provider_cache_capabilities]]\nprovider_id = "other-provider"\nmodel_id = "model-test"\nchannel = "responses"\nfield = "prompt-cache-key"\nstatus = "verified"\neffect_status = "unverified"\n'
  ].join("\n");
  const fixtureScope = { providerId: "provider-test", model: "model-test" };
  const rewrittenBaseline = rewriteCapabilityScopeFor(
    capabilityFixture,
    "unsupported",
    fixtureScope,
    "prompt-cache-key"
  );
  const rewrittenCandidate = rewriteCapabilityScopeFor(
    capabilityFixture,
    "verified",
    fixtureScope,
    "prompt-cache-key"
  );
  const selectedBaseline = tomlArrayBlocks(rewrittenBaseline, "provider_cache_capabilities")
    .filter((block) => capabilityScopeMatchesFor(block.body, fixtureScope));
  const selectedCandidate = tomlArrayBlocks(rewrittenCandidate, "provider_cache_capabilities")
    .filter((block) => capabilityScopeMatchesFor(block.body, fixtureScope));
  assert.equal(selectedBaseline.length, 3);
  assert.equal(selectedBaseline.every(
    (block) => extractTomlString(block.body, "status") === "unsupported"
  ), true);
  assert.equal(selectedCandidate.filter(
    (block) => extractTomlString(block.body, "field") === "prompt-cache-key"
  ).every((block) => extractTomlString(block.body, "status") === "verified"), true);
  assert.equal(selectedCandidate.find(
    (block) => extractTomlString(block.body, "field") === "prompt-cache-retention"
  ) && extractTomlString(selectedCandidate.find(
    (block) => extractTomlString(block.body, "field") === "prompt-cache-retention"
  ).body, "status"), "unsupported");
  const rewrittenPinnedKey = rewriteCapabilityScopeFor(
    capabilityFixture,
    "unsupported",
    { providerId: "provider-test", model: "model-test", keyId: "key-test" },
    "prompt-cache-key"
  );
  const pinnedCapabilityBlocks = tomlArrayBlocks(rewrittenPinnedKey, "provider_cache_capabilities");
  const genericPinnedControl = pinnedCapabilityBlocks.find((block) =>
    capabilityScopeMatchesFor(block.body, fixtureScope) &&
    !extractTomlString(block.body, "key_id") &&
    extractTomlString(block.body, "field") === "prompt-cache-key"
  );
  const scopedPinnedControls = pinnedCapabilityBlocks.filter((block) =>
    capabilityScopeMatchesFor(block.body, { ...fixtureScope, keyId: "key-test" })
  );
  assert.equal(extractTomlString(genericPinnedControl?.body ?? "", "status"), "verified");
  assert.equal(scopedPinnedControls.every(
    (block) => extractTomlString(block.body, "status") === "unsupported"
  ), true);
  const rewrittenRetentionOnly = rewriteCapabilityScopeFor(
    capabilityFixture,
    "unsupported",
    { providerId: "provider-test", model: "model-test", keyId: "key-test" },
    "prompt-cache-retention",
    "prompt-cache-key"
  );
  const retentionOnlyScoped = tomlArrayBlocks(rewrittenRetentionOnly, "provider_cache_capabilities")
    .filter((block) => capabilityScopeMatchesFor(
      block.body,
      { ...fixtureScope, keyId: "key-test" }
    ));
  assert.equal(retentionOnlyScoped.find(
    (block) => extractTomlString(block.body, "field") === "prompt-cache-key"
  ) && extractTomlString(retentionOnlyScoped.find(
    (block) => extractTomlString(block.body, "field") === "prompt-cache-key"
  ).body, "status"), "verified");
  assert.equal(retentionOnlyScoped.find(
    (block) => extractTomlString(block.body, "field") === "prompt-cache-retention"
  ) && extractTomlString(retentionOnlyScoped.find(
    (block) => extractTomlString(block.body, "field") === "prompt-cache-retention"
  ).body, "status"), "unsupported");

  const baselineWire = finalWireCacheControlEvidence({
    // A legacy provider-prefix identity must not turn this zero-control final
    // wire into a positive placement witness.
    provider_prefix_key: "legacy-recovery-key",
    outbound_prefix_fingerprints: {
      cache_metadata: cacheMetadataFingerprint("{}")
    }
  });
  const candidateWire = finalWireCacheControlEvidence({
    outbound_prefix_fingerprints: {
      cache_metadata: cacheMetadataFingerprint('{"prompt_cache_key":"[redacted]"}')
    }
  });
  const retentionWire = finalWireCacheControlEvidence({
    outbound_prefix_fingerprints: {
      cache_metadata: cacheMetadataFingerprint('{"prompt_cache_retention":"24h"}')
    }
  });
  const unknownWire = finalWireCacheControlEvidence({
    outbound_prefix_fingerprints: {
      cache_metadata: "sha256-128:00000000000000000000000000000000"
    }
  });
  assert.equal(baselineWire.observed, true);
  assert.equal(baselineWire.recognized, true);
  assert.deepEqual(baselineWire.fields, []);
  assert.deepEqual(candidateWire.fields, ["prompt_cache_key"]);
  assert.deepEqual(retentionWire.fields, ["prompt_cache_retention"]);
  assert.equal(unknownWire.observed, true);
  assert.equal(unknownWire.recognized, false);

  const wireShape = cacheMetadataPairSymmetry(
    [{
      cache_metadata: baselineWire.cache_metadata,
      cache_metadata_recognized: baselineWire.recognized,
      final_wire_cache_control_fields: baselineWire.fields
    }],
    [{
      cache_metadata: candidateWire.cache_metadata,
      cache_metadata_recognized: candidateWire.recognized,
      final_wire_cache_control_fields: candidateWire.fields
    }],
    "prompt-cache-key"
  );
  assert.equal(wireShape.pass, true);
  const equalWireShape = cacheMetadataPairSymmetry(
    [{
      cache_metadata: candidateWire.cache_metadata,
      cache_metadata_recognized: candidateWire.recognized,
      final_wire_cache_control_fields: candidateWire.fields
    }],
    [{
      cache_metadata: candidateWire.cache_metadata,
      cache_metadata_recognized: candidateWire.recognized,
      final_wire_cache_control_fields: candidateWire.fields
    }],
    "prompt-cache-key",
    true,
    true
  );
  assert.equal(equalWireShape.pass, true);
  const isolatedPck = cacheControlFieldIsolation(
    [{ final_wire_cache_control_fields: baselineWire.fields }],
    [{ final_wire_cache_control_fields: candidateWire.fields }],
    "prompt-cache-key",
    "unsupported",
    "verified"
  );
  const bundledPck = cacheControlFieldIsolation(
    [{ final_wire_cache_control_fields: baselineWire.fields }],
    [{ final_wire_cache_control_fields: ["prompt_cache_key", "prompt_cache_retention"] }],
    "prompt-cache-key",
    "unsupported",
    "source"
  );
  assert.equal(isolatedPck.pass, true);
  assert.equal(bundledPck.pass, false);
  const isolatedRetention = cacheControlFieldIsolation(
    [{ final_wire_cache_control_fields: ["prompt_cache_key"] }],
    [{ final_wire_cache_control_fields: ["prompt_cache_key", "prompt_cache_retention"] }],
    "prompt-cache-retention",
    "unsupported",
    "verified",
    "prompt-cache-key"
  );
  assert.equal(isolatedRetention.pass, true);

  const failedInbound = { inbound_request_id: "failed-inbound" };
  assert.equal(
    selectNewRequestLog({ recent_requests: [], recent_failed_requests: [failedInbound] }, new Set()),
    failedInbound
  );
  assert.equal(
    hasNewRequestLog({ recent_requests: [], recent_failed_requests: [failedInbound] }, new Set()),
    true
  );
  assert.equal(
    responseHasNativeFailure('event: response.failed\ndata: {"code":"upstream_sse_error"}\n\n'),
    true
  );
  assert.equal(
    responseErrorCode('event: response.failed\ndata: {"code":"upstream_sse_error"}\n\n'),
    "upstream_sse_error"
  );
  assert.equal(
    safeFailureReceipt('event: response.output_text.delta\ndata: {"model":"gpt-5.6-terra"}\n\n').message_class,
    "unclassified"
  );
  assert.equal(
    safeFailureReceipt('event: response.failed\ndata: {"code":"upstream_sse_error"}\n\n').message_class,
    "response_failed"
  );
  console.log(JSON.stringify({
    schema: "atoapi-generated-prompt-cache-key-cross-ab-self-test-v1",
    passed: true,
    checks: {
      shared_suffix_and_equal_length_fixture: true,
      distinct_first_prefix_marker: true,
      fixed_width_tail: true,
      interleaved_order_polarity: true,
      serial_arm_order: true,
      explicit_key_pin: true,
      input_token_symmetry_gate: true,
      upstream_only_latency_policy_keeps_local_guard: true,
      key_scoped_capabilities_rewritten: true,
      pinned_key_scope_does_not_rewrite_other_keys: true,
      preserved_control_field_retained_in_both_arms: true,
      final_wire_cache_metadata_witness: true,
      source_bundle_not_misattributed_as_single_field: true,
      provider_prefix_key_not_used_as_witness: true,
      failed_request_ledger_observed: true,
      native_sse_failure_receipt: true,
      normal_sse_not_misclassified_as_model_failure: true,
      equal_verified_wire_shape: true
    }
  }));
}

function normalizeControlField(value) {
  const normalized = String(value).trim().toLowerCase().replace(/_/gu, "-");
  if (normalized === "prompt-cache-key" || normalized === "prompt-cache-retention") return normalized;
  throw new Error("--field must be prompt-cache-key or prompt-cache-retention");
}

function optionalControlField(value) {
  const normalized = String(value ?? "").trim();
  return normalized ? normalizeControlField(normalized) : null;
}
