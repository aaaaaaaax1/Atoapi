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
import { createServer } from "node:net";
import { homedir, tmpdir } from "node:os";
import { basename, dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";

// Live A/B verifier for the generated native Responses prompt_cache_key path.
// It deliberately runs two isolated proxy processes. It never uses the live
// 18883 process, never changes its config, and sends exactly one POST for each
// generated inbound. The only config difference is the exact capability
// record: baseline says `unsupported`, candidate keeps the recorded status.
const repoRoot = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const args = parseArgs(process.argv.slice(2));
const extension = process.platform === "win32" ? ".exe" : "";
const sourceConfigDir = resolve(String(args["source-config-dir"] ?? defaultConfigDir()));
const executable = resolve(String(args.exe ?? join(repoRoot, "src-tauri", "target", "debug", `atoapi${extension}`)));
const providerId = requiredString(args["provider-id"], "--provider-id");
const model = requiredString(args.model, "--model");
const turns = boundedInteger(args.turns ?? 20, "--turns", 20, 60);
const fixtureLines = boundedInteger(args.lines ?? 2200, "--lines", 2200, 10_000);
const portStart = boundedInteger(args.port ?? 64_940, "--port", 1_024, 65_400);
const maxOutputTokens = boundedInteger(args["max-output-tokens"] ?? 16, "--max-output-tokens", 1, 256);
const targetInputTokens = boundedInteger(args["target-input-tokens"] ?? 500_000, "--target-input-tokens", 100_000, 10_000_000);
const keepRunDir = booleanArg(args["keep-run-dir"]);
const runId = String(args["run-id"] ?? randomUUID()).trim();

if (booleanArg(args.help) || booleanArg(args.h)) {
  printUsage();
  process.exit(0);
}

let root = null;
let baselineRuntime = null;
let candidateRuntime = null;
let runFailure = null;

try {
  await assertFile(executable, "Atoapi executable");
  const source = await snapshotSourceConfig(sourceConfigDir);
  root = await mkdtemp(join(tmpdir(), "atoapi-generated-key-cross-ab-"));
  const baselineConfigDir = join(root, "baseline");
  const candidateConfigDir = join(root, "candidate");
  await materializeConfig(source, baselineConfigDir, "unsupported");
  await materializeConfig(source, candidateConfigDir, null);

  baselineRuntime = await startRuntime("baseline", baselineConfigDir, portStart, source.localKey);
  candidateRuntime = await startRuntime("candidate", candidateConfigDir, portStart + 1, source.localKey);

  const baseline = createArm("baseline", baselineRuntime, stableGuid("baseline", runId));
  const candidate = createArm("candidate", candidateRuntime, stableGuid("candidate", runId));
  if (baseline.prefix.length !== candidate.prefix.length) {
    throw new Error("fixture construction produced unequal prefix lengths");
  }

  for (let index = 0; index < turns; index += 1) {
    const order = index % 2 === 0 ? [baseline, candidate] : [candidate, baseline];
    for (const arm of order) {
      await sendTurn(arm, index + 1);
    }
  }

  const baselineSummary = summarizeArm(baseline);
  const candidateSummary = summarizeArm(candidate);
  const checks = buildChecks({ baseline, candidate, baselineSummary, candidateSummary });
  const result = {
    schema: "atoapi-generated-prompt-cache-key-cross-ab-v1",
    run_id_hash: shortHash(runId),
    isolated: true,
    comparison: {
      provider_id: providerId,
      model,
      request_family: "responses-full-replay",
      turns_per_arm: turns,
      stable_prefix_lines: fixtureLines,
      same_binary: true,
      same_source_config_snapshot: true,
      interleaved_order: true,
      separate_equal_length_fixture_guids: true
    },
    baseline: baselineSummary,
    candidate: candidateSummary,
    delta: compareSummaries(baselineSummary, candidateSummary),
    checks,
    comparable: Object.values(checks).every(Boolean),
    outcome: describeOutcome(baselineSummary, candidateSummary, checks)
  };
  console.log(JSON.stringify(result, null, 2));
  if (!result.comparable) process.exitCode = 1;
} catch (error) {
  runFailure = error;
  console.error(JSON.stringify({
    schema: "atoapi-generated-prompt-cache-key-cross-ab-v1",
    comparable: false,
    error: safeError(error)
  }, null, 2));
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
  const blocks = tomlArrayBlocks(configText, "provider_cache_capabilities");
  const matches = blocks.filter((block) => capabilityMatches(block.body));
  if (matches.length !== 1) {
    throw new Error("expected exactly one matching prompt-cache-key capability record");
  }
  if (extractTomlString(matches[0].body, "status") !== "verified") {
    throw new Error("candidate capability must be verified before a cross A/B run");
  }
  const keyPath = join(directory, "cache-key.dpapi");
  return {
    configText,
    localKey,
    keyPath: await fileExists(keyPath) ? keyPath : null
  };
}

async function materializeConfig(source, directory, baselineStatus) {
  await mkdir(directory, { recursive: true });
  const configText = baselineStatus
    ? rewriteCapabilityStatus(source.configText, baselineStatus)
    : source.configText;
  await writeFile(join(directory, "config.toml"), configText, "utf8");
  if (source.keyPath) {
    await copyFile(source.keyPath, join(directory, basename(source.keyPath)));
  }
}

function rewriteCapabilityStatus(configText, status) {
  const blocks = tomlArrayBlocks(configText, "provider_cache_capabilities");
  const target = blocks.filter((block) => capabilityMatches(block.body));
  if (target.length !== 1) throw new Error("matching capability record changed while creating baseline");
  const block = target[0];
  const statusLine = /^status\s*=\s*"[^"]*"\s*$/mu;
  const rewritten = statusLine.test(block.body)
    ? block.body.replace(statusLine, `status = "${status}"`)
    : `${block.body.trimEnd()}\nstatus = "${status}"\n`;
  return `${configText.slice(0, block.start)}${rewritten}${configText.slice(block.end)}`;
}

function capabilityMatches(body) {
  return extractTomlString(body, "provider_id") === providerId &&
    extractTomlString(body, "model_id") === model &&
    extractTomlString(body, "channel") === "responses" &&
    extractTomlString(body, "field") === "prompt-cache-key" &&
    !extractTomlString(body, "key_id");
}

async function startRuntime(label, configDir, requestedPort, localKey) {
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

function createArm(name, runtime, guid) {
  const prefix = buildStablePrefix(guid);
  return {
    name,
    runtime,
    guid,
    prefix,
    input: [message(prefix)],
    sessionId: `cross-ab-session-${guid}`,
    threadId: `cross-ab-thread-${guid}`,
    records: []
  };
}

function buildStablePrefix(guid) {
  const shared = "immutable-schema=alpha|stable-field=cache-validation|payload=abcdefghijklmno";
  return Array.from({ length: fixtureLines }, (_, index) => {
    const ordinal = String(index + 1).padStart(4, "0");
    const mirror = String(fixtureLines - index).padStart(4, "0");
    return `record=${ordinal}|mirror=${mirror}|fixture=${guid}|${shared}\n`;
  }).join("");
}

function buildEqualLengthTail(guid, turn) {
  const ordinal = String(turn).padStart(4, "0");
  return `new-turn=${ordinal}|fixture=${guid}|tail=constant-length-deterministic-follow-up|ack=required`;
}

async function sendTurn(arm, turn) {
  arm.input.push(message(buildEqualLengthTail(arm.guid, turn)));
  const before = await getJson(`${arm.runtime.baseUrl}/admin/metrics`, 5_000);
  const countersBefore = requestCounters(before);
  const knownIds = new Set(array(before.recent_requests)
    .map((item) => String(item?.inbound_request_id ?? ""))
    .filter(Boolean));
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
    body: JSON.stringify({
      model,
      stream: true,
      max_output_tokens: maxOutputTokens,
      instructions: "Return exactly ACK. This is a deterministic cache validation fixture.",
      input: arm.input
    }),
    signal: AbortSignal.timeout(180_000)
  });
  const sse = await response.text();
  const failureClass = safeFailureClass(response.status, sse);
  const after = await waitForFinalization(arm.runtime.baseUrl, countersBefore, knownIds);
  const metric = array(after.recent_requests).find((item) => {
    const id = String(item?.inbound_request_id ?? "");
    return id && !knownIds.has(id);
  });
  const counters = subtractCounters(requestCounters(after), countersBefore);
  const completed = response.ok && /\bresponse\.completed\b/u.test(sse) && !/\bresponse\.failed\b/u.test(sse);
  const singleAttempt = counters.inbound_requests === 1 &&
    counters.generation_attempts === 1 &&
    counters.upstream_requests === 1 &&
    Number(metric?.upstream_attempt_index) === 1 &&
    Number(metric?.upstream_attempt_total) === 1 &&
    Number(metric?.upstream_attempts) === 1;
  const inputTokens = number(metric?.input_tokens);
  const cacheReadTokens = number(metric?.cache_read_tokens);
  const fullReplay = String(metric?.response_context_plan ?? "") === "full_replay";
  const providerMatches = String(metric?.provider_id ?? "") === providerId && String(metric?.model ?? "") === model;
  const realm = String(metric?.shadow_affinity_realm_id ?? "");
  const prefixKeyPresent = typeof metric?.provider_prefix_key === "string" && metric.provider_prefix_key.length > 0;
  const record = {
    completed,
    single_attempt: singleAttempt,
    input_tokens: inputTokens,
    cache_read_tokens: cacheReadTokens,
    cache_avoidable_gap_tokens: number(metric?.cache_avoidable_gap_tokens),
    cache_provider_unstable_gap_tokens: number(metric?.cache_provider_unstable_gap_tokens),
    cache_new_tail_gap_tokens: number(metric?.cache_new_tail_gap_tokens),
    full_replay: fullReplay,
    provider_matches: providerMatches,
    realm_id: realm,
    prefix_key_present: prefixKeyPresent,
    downstream_disconnected: Boolean(metric?.downstream_disconnected),
    status: Number(metric?.status),
    failure_class: failureClass
  };
  arm.records.push(record);
  if (!completed || !singleAttempt || inputTokens <= 0 || !fullReplay || !providerMatches || !realm || record.downstream_disconnected) {
    // Preserve just enough evidence to distinguish an upstream terminal failure,
    // delayed metrics, and a verifier assumption.  Never print the request,
    // response body, credentials, or any upstream headers.
    throw new Error(`${arm.name} turn ${turn} did not reach the required terminal, metric, or one-POST invariant; diagnostics=${JSON.stringify({
      http_status: response.status,
      failure_class: failureClass,
      metric_observed: Boolean(metric),
      completed,
      single_attempt: singleAttempt,
      input_tokens: inputTokens,
      full_replay: fullReplay,
      provider_matches: providerMatches,
      affinity_realm_present: Boolean(realm),
      downstream_disconnected: record.downstream_disconnected,
      counters
    })}`);
  }
}

function safeFailureClass(status, body) {
  if (status >= 200 && status < 300) return "none";
  const normalized = String(body ?? "").toLowerCase();
  if (normalized.includes("selected upstream is cooling down")) return "local_upstream_cooldown";
  if (normalized.includes("request blocked")) return "upstream_request_blocked";
  if (normalized.includes("new_api_error")) return "upstream_new_api_error";
  if (normalized.includes("transport_error")) return "upstream_transport_error";
  if (normalized.includes("response.failed")) return "response_failed_other";
  return `http_${status}`;
}

async function waitForFinalization(baseUrl, before, knownIds) {
  const deadline = Date.now() + 15_000;
  let latest = null;
  while (Date.now() < deadline) {
    latest = await getJson(`${baseUrl}/admin/metrics`, 5_000);
    const counters = requestCounters(latest);
    const hasNew = array(latest.recent_requests).some((item) => {
      const id = String(item?.inbound_request_id ?? "");
      return id && !knownIds.has(id);
    });
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
    full_bucket_requests: records.filter((item) => item.cache_read_tokens >= cacheableInputTokens128(item.input_tokens)).length,
    cache_avoidable_gap_tokens: totals.cache_avoidable_gap_tokens,
    cache_provider_unstable_gap_tokens: totals.cache_provider_unstable_gap_tokens,
    cache_new_tail_gap_tokens: totals.cache_new_tail_gap_tokens,
    all_single_attempt: records.every((item) => item.single_attempt),
    all_full_replay: records.every((item) => item.full_replay),
    all_provider_matches: records.every((item) => item.provider_matches),
    all_completed: records.every((item) => item.completed),
    all_prefix_key_present: records.every((item) => item.prefix_key_present),
    no_prefix_key_present: records.every((item) => !item.prefix_key_present),
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

function buildChecks({ baseline, candidate, baselineSummary, candidateSummary }) {
  const allRecords = [...baseline.records, ...candidate.records];
  const realmSet = new Set(allRecords.map((item) => item.realm_id).filter(Boolean));
  return {
    exactly_twenty_or_more_turns_per_arm: baseline.records.length >= 20 && candidate.records.length >= 20,
    each_arm_reached_input_budget: baselineSummary.input_tokens >= targetInputTokens && candidateSummary.input_tokens >= targetInputTokens,
    every_sse_completed: baselineSummary.all_completed && candidateSummary.all_completed,
    one_upstream_post_per_inbound: baselineSummary.all_single_attempt && candidateSummary.all_single_attempt,
    all_requests_remained_full_replay: baselineSummary.all_full_replay && candidateSummary.all_full_replay,
    selected_provider_model_unchanged: baselineSummary.all_provider_matches && candidateSummary.all_provider_matches,
    one_key_realm_per_arm: baselineSummary.realm_count === 1 && candidateSummary.realm_count === 1,
    same_key_realm_across_arms: realmSet.size === 1,
    baseline_has_no_generated_prompt_cache_key: baselineSummary.no_prefix_key_present,
    candidate_has_generated_prompt_cache_key: candidateSummary.all_prefix_key_present,
    no_downstream_disconnect: allRecords.every((item) => !item.downstream_disconnected),
    no_local_avoidable_gap: baselineSummary.cache_avoidable_gap_tokens === 0 && candidateSummary.cache_avoidable_gap_tokens === 0
  };
}

function compareSummaries(baseline, candidate) {
  return {
    raw_token_hit_rate_points: round(candidate.raw_token_hit_rate - baseline.raw_token_hit_rate, 4),
    cache_128_hit_rate_points: round(candidate.cache_128_hit_rate - baseline.cache_128_hit_rate, 4),
    warm_cache_128_hit_rate_points: round(candidate.warm_cache_128_hit_rate - baseline.warm_cache_128_hit_rate, 4),
    provider_unstable_gap_tokens: candidate.cache_provider_unstable_gap_tokens - baseline.cache_provider_unstable_gap_tokens,
    new_tail_gap_tokens: candidate.cache_new_tail_gap_tokens - baseline.cache_new_tail_gap_tokens
  };
}

function describeOutcome(baseline, candidate, checks) {
  if (!Object.values(checks).every(Boolean)) return "invalid_or_upstream_contaminated";
  if (candidate.warm_cache_128_hit_rate > baseline.warm_cache_128_hit_rate) return "candidate_positive";
  if (candidate.warm_cache_128_hit_rate < baseline.warm_cache_128_hit_rate) return "candidate_negative";
  return "no_measurable_difference";
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

function extractTomlString(text, key) {
  const escaped = key.replace(/[.*+?^${}()|[\]\\]/gu, "\\$&");
  return text.match(new RegExp(`^${escaped}\\s*=\\s*"([^"]*)"`, "mu"))?.[1] ?? "";
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

function boundedInteger(value, name, minimum, maximum) {
  const parsed = Number(value);
  if (!Number.isInteger(parsed) || parsed < minimum || parsed > maximum) {
    throw new Error(`${name} must be an integer from ${minimum} through ${maximum}`);
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

function printUsage() {
  console.log([
    "Usage:",
    "  node scripts/verify-generated-prompt-cache-key-cross-ab.mjs \\",
    "    --provider-id <id> --model <id> [--exe <atoapi.exe>] [--source-config-dir <dir>]",
    "Runs isolated baseline(unsupported) versus candidate(verified) configurations."
  ].join("\n"));
}
