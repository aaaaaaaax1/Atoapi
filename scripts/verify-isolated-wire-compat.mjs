import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { createServer } from "node:http";
import { copyFile, mkdir, mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { createServer as createNetServer } from "node:net";
import { tmpdir } from "node:os";
import { basename, dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const repoRoot = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const args = parseArgs(process.argv.slice(2));
const sourceConfigDir = resolve(
  String(args["source-config-dir"] ?? defaultConfigDir())
);
const oldExecutable = resolve(
  String(
    args["old-exe"] ??
      join(
        repoRoot,
        "releases",
        "v1.3.4-verified-native-delta-lineage-20260721",
        "Atoapi.exe"
      )
  )
);
const newExecutable = resolve(
  String(args["new-exe"] ?? join(repoRoot, "src-tauri", "target", "release", "atoapi.exe"))
);
const model = String(args.model ?? "gpt-5.6-terra").trim();
const concurrency = boundedPositiveInteger(args.concurrency ?? 1, "--concurrency", 32);
const gateHeaders = booleanArg(args["gate-headers"]);

if (!model) throw new Error("--model must not be empty");
if (gateHeaders && concurrency < 2) {
  throw new Error("--gate-headers requires --concurrency of at least 2");
}

let upstream = null;
let upstreamPort = null;
let tempRoot = null;
let headerGate = null;

try {
  const captured = [];
  upstream = createServer(async (request, response) => {
    const body = await readRequestBody(request);
    if (request.method !== "POST" || !request.url?.endsWith("/responses")) {
      response.writeHead(404, { "content-type": "application/json" });
      response.end('{"error":"mock route missing"}');
      return;
    }

    let parsed;
    try {
      parsed = JSON.parse(body);
    } catch {
      response.writeHead(400, { "content-type": "application/json" });
      response.end('{"error":"request body was not JSON"}');
      return;
    }
    // Deliberately never retain or print Authorization. The test is interested
    // only in the final request JSON and request count.
    captured.push({
      body: parsed,
      requestKind: String(request.headers["x-atoapi-request-kind"] ?? ""),
      headers: safeHeaders(request.headers)
    });
    if (headerGate) await headerGate.arrive();
    response.writeHead(200, {
      "cache-control": "no-cache",
      "content-type": "text/event-stream; charset=utf-8"
    });
    response.end([
      "event: response.output_text.delta",
      'data: {"type":"response.output_text.delta","delta":"OK"}',
      "",
      "event: response.completed",
      'data: {"type":"response.completed","response":{"id":"resp_wire_compat","model":"mock","status":"completed","usage":{"input_tokens":4096,"output_tokens":1,"input_tokens_details":{"cached_tokens":3968}}}}',
      "",
      "data: [DONE]",
      ""
    ].join("\n"));
  });
  upstreamPort = await listen(upstream);
  tempRoot = await mkdtemp(join(tmpdir(), "atoapi-wire-compat-"));

  const oldRun = await runIsolatedCapture({
    label: "baseline",
    executable: oldExecutable,
    configDir: join(tempRoot, "baseline"),
    upstreamPort,
    captured,
    model,
    concurrency,
    gateHeaders
  });
  const newRun = await runIsolatedCapture({
    label: "fastrelay",
    executable: newExecutable,
    configDir: join(tempRoot, "fastrelay"),
    upstreamPort,
    captured,
    model,
    concurrency,
    gateHeaders
  });

  const baseline = oldRun.upstreamBody;
  const fastrelay = newRun.upstreamBody;
  const differingPaths = diffPaths(baseline, fastrelay);
  const identity = compareIdentityMarkers(oldRun.identityMarkers, newRun.identityMarkers);
  const baselineComparableHeaders = comparableProtocolHeaders(oldRun.upstreamHeaders);
  const fastrelayComparableHeaders = comparableProtocolHeaders(newRun.upstreamHeaders);
  const report = {
    pass: oldRun.oneInboundOnePost &&
      newRun.oneInboundOnePost &&
      (!gateHeaders || newRun.samePrefixReachedBeforeHeaders),
    model,
    concurrency,
    header_gate: gateHeaders,
    baseline: oldRun.summary,
    fastrelay: newRun.summary,
    wire_equal: differingPaths.length === 0,
    differing_paths: differingPaths,
    differing_controls: summarizeControlDifferences(baseline, fastrelay, differingPaths),
    headers_equal: JSON.stringify(baselineComparableHeaders) === JSON.stringify(fastrelayComparableHeaders),
    baseline_headers: oldRun.upstreamHeaders,
    fastrelay_headers: newRun.upstreamHeaders,
    shadow_identity_equal: identity.equal,
    shadow_identity_differences: identity.differingFields
  };
  console.log(JSON.stringify(report, null, 2));
  assert.equal(
    report.pass,
    true,
    "each isolated inbound must make exactly one upstream POST"
  );
  assert.equal(report.wire_equal, true, "FastRelay must preserve the v1.3.4 upstream wire body");
  assert.equal(
    report.headers_equal,
    true,
    "FastRelay must preserve upstream protocol headers apart from its intentional product-version token"
  );
  assert.equal(
    report.shadow_identity_equal,
    true,
    "FastRelay must preserve v1.3.4 shadow affinity identity for the same request"
  );
  if (gateHeaders) {
    assert.equal(
      newRun.samePrefixReachedBeforeHeaders,
      true,
      "FastRelay same-prefix requests must all reach upstream before any held response headers release"
    );
  }
} finally {
  if (upstream) await closeServer(upstream);
  if (tempRoot) await rm(tempRoot, { recursive: true, force: true });
}

async function runIsolatedCapture({
  label,
  executable,
  configDir,
  upstreamPort,
  captured,
  model,
  concurrency,
  gateHeaders
}) {
  await createIsolatedConfig(configDir, upstreamPort);
  const configText = await readFile(join(configDir, "config.toml"), "utf8");
  const localKey = extractTomlString(configText, "local_key");
  if (!localKey) throw new Error(`${label}: test config has no local_key`);
  const port = await freePort();
  const before = captured.length;
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
  const gate = gateHeaders ? createHeaderGate(concurrency) : null;
  try {
    await waitForHealth(baseUrl, child, label);
    headerGate = gate;
    const downstreamPromise = Promise.all(
      Array.from({ length: concurrency }, async () => {
        const response = await fetch(`${baseUrl}/codex/v1/responses`, {
          method: "POST",
          headers: {
            authorization: `Bearer ${localKey}`,
            "content-type": "application/json",
            accept: "text/event-stream",
            "x-codex-turn-metadata": JSON.stringify({
              // Every parallel inbound deliberately uses the same trusted
              // conversation. This exercises the same-prefix hot path rather
              // than hiding it behind unrelated-session sharding.
              session_id: "wire-compat-session",
              thread_id: "wire-compat-thread",
              request_kind: "turn"
            })
          },
          body: JSON.stringify({
            model,
            stream: true,
            store: false,
            max_output_tokens: 16,
            instructions: "Wire compatibility fixture. Reply with OK only.",
            input: [{
              type: "message",
              role: "user",
              content: [{ type: "input_text", text: "fixture tail" }]
            }]
          }),
          signal: AbortSignal.timeout(30_000)
        });
        const body = await response.text();
        assert.equal(response.status, 200, `${label}: local proxy rejected fixture`);
        assert.match(body, /response\.completed/u, `${label}: terminal event missing`);
        return response.status;
      })
    );
    const gateResult = gate ? await gate.waitForRelease() : null;
    const downstream = await downstreamPromise;
    await waitFor(
      () => captured.length === before + concurrency,
      5_000,
      `${label}: expected ${concurrency} upstream POSTs`
    );
    const metrics = await getJson(`${baseUrl}/admin/metrics`);
    const upstreamRequests = captured.slice(before);
    const upstreamBody = upstreamRequests.at(-1)?.body;
    const upstreamHeaders = upstreamRequests.at(-1)?.headers ?? {};
    assert.ok(upstreamBody, `${label}: mock did not capture an upstream body`);
    assert(
      upstreamRequests.every((request) => JSON.stringify(request.body) === JSON.stringify(upstreamBody)),
      `${label}: parallel inbounds produced different upstream wire bodies`
    );
    assert(
      upstreamRequests.every((request) => JSON.stringify(request.headers) === JSON.stringify(upstreamHeaders)),
      `${label}: parallel inbounds produced different upstream headers`
    );
    const generation = metrics.agent_generation ?? {};
    const oneInboundOnePost = Number(generation.inbound_requests) === concurrency &&
      Number(generation.generation_attempts) === concurrency &&
      Number(metrics.upstream_requests) === concurrency &&
      upstreamRequests.length === concurrency;
    return {
      upstreamBody,
      upstreamHeaders,
      identityMarkers: identityMarkers(metrics.recent_requests?.[0] ?? {}),
      oneInboundOnePost,
      samePrefixReachedBeforeHeaders: !gate || gateResult.arrivalsBeforeRelease === concurrency,
      summary: {
        local_status: downstream[0] ?? null,
        completed_responses: downstream.length,
        concurrency,
        same_prefix_arrivals_before_headers: gateResult?.arrivalsBeforeRelease ?? null,
        same_prefix_header_gate_reason: gateResult?.reason ?? null,
        inbound_requests: Number(generation.inbound_requests),
        generation_attempts: Number(generation.generation_attempts),
        upstream_requests: Number(metrics.upstream_requests),
        request_kind: upstreamRequests.at(-1)?.requestKind || null,
        upstream_controls: summarizeControls(upstreamBody),
        upstream_headers: upstreamHeaders
      }
    };
  } finally {
    if (headerGate === gate) headerGate = null;
    gate?.release("cleanup");
    await stopChild(child, label);
  }
}

function identityMarkers(request) {
  return Object.fromEntries([
    "provider_prefix_key",
    "provider_prefix_fingerprint",
    "shadow_affinity_realm_id",
    "shadow_affinity_cohort_id",
    "shadow_affinity_arm",
    "shadow_affinity_lane",
    "shadow_affinity_shard",
    "shadow_affinity_policy_epoch",
    "shadow_affinity_anchor_epoch",
    "shadow_affinity_trusted_identity",
    "shadow_affinity_decision"
  ].map((field) => [field, request[field] ?? null]));
}

function compareIdentityMarkers(left, right) {
  const differingFields = Object.keys(left).filter(
    (field) => !Object.is(left[field], right[field])
  );
  return { equal: differingFields.length === 0, differingFields };
}

async function createIsolatedConfig(configDir, upstreamPort) {
  await rm(configDir, { recursive: true, force: true });
  await mkdir(configDir, { recursive: true });
  const sourceConfig = join(sourceConfigDir, "config.toml");
  const targetConfig = join(configDir, "config.toml");
  await copyFile(sourceConfig, targetConfig);
  const sourceKey = join(sourceConfigDir, "cache-key.dpapi");
  try {
    await copyFile(sourceKey, join(configDir, basename(sourceKey)));
  } catch {
    // A plaintext/empty cache is sufficient for this loopback-only test.
  }

  const original = await readFile(targetConfig, "utf8");
  const providerId = codexProviderId(original);
  if (!providerId) throw new Error("could not find the enabled Codex provider in config.toml");
  const rewritten = rewriteProviderBlock(original, providerId, (block) => {
    let next = replaceTomlString(block, "base_url", `http://127.0.0.1:${upstreamPort}/v1`);
    next = replaceTomlBoolean(next, "use_system_proxy", false);
    next = replaceTomlBoolean(next, "request_body_gzip_enabled", false);
    return next;
  });
  await writeFile(targetConfig, rewritten, "utf8");
}

function codexProviderId(config) {
  return tomlArrayBlocks(config, "agent_injections")
    .map(({ body }) => body)
    .find((body) => extractTomlString(body, "id") === "codex")
    ?.match(/^provider_id\s*=\s*"([^"]+)"/mu)?.[1] ?? "";
}

function rewriteProviderBlock(config, providerId, transform) {
  const blocks = tomlArrayBlocks(config, "providers");
  for (const block of blocks) {
    if (extractTomlString(block.body, "id") !== providerId) continue;
    return `${config.slice(0, block.start)}${transform(block.body)}${config.slice(block.end)}`;
  }
  throw new Error(`provider ${providerId} was not found in config.toml`);
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

function replaceTomlString(block, key, value) {
  const pattern = new RegExp(`^${escapeRegExp(key)}\\s*=\\s*"[^"]*"`, "mu");
  if (!pattern.test(block)) return `${block.trimEnd()}\n${key} = "${value}"\n`;
  return block.replace(pattern, `${key} = "${value}"`);
}

function replaceTomlBoolean(block, key, value) {
  const pattern = new RegExp(`^${escapeRegExp(key)}\\s*=\\s*(?:true|false)`, "mu");
  if (!pattern.test(block)) return `${block.trimEnd()}\n${key} = ${value}\n`;
  return block.replace(pattern, `${key} = ${value}`);
}

function extractTomlString(text, key) {
  const pattern = new RegExp(`^${escapeRegExp(key)}\\s*=\\s*"([^"]*)"`, "mu");
  return text.match(pattern)?.[1] ?? "";
}

function safeHeaders(headers) {
  const allowed = [
    "accept",
    "content-type",
    "content-encoding",
    "content-length",
    "user-agent",
    "x-atoapi-request-kind"
  ];
  return Object.fromEntries(allowed.map((name) => [name, headers[name] ?? null]));
}

function comparableProtocolHeaders(headers) {
  const userAgent = String(headers["user-agent"] ?? "");
  return {
    ...headers,
    "user-agent": /^Atoapi\/\d+(?:\.\d+){1,3}$/u.test(userAgent)
      ? "Atoapi/<version>"
      : userAgent
  };
}

function summarizeControls(body) {
  const keys = [
    "model",
    "stream",
    "store",
    "max_output_tokens",
    "prompt_cache_key",
    "prompt_cache_retention",
    "prompt_cache_breakpoint",
    "prompt_cache_options",
    "service_tier",
    "truncation"
  ];
  return Object.fromEntries(keys.filter((key) => body[key] !== undefined).map((key) => [
    key,
    key === "prompt_cache_key" ? "present" : body[key]
  ]));
}

function summarizeControlDifferences(left, right, paths) {
  return paths
    .filter((path) => path === "$" || /(?:prompt_cache|reasoning|model|stream|store|max_output_tokens|service_tier|truncation)/u.test(path))
    .map((path) => ({
      path,
      baseline: summarizeValue(valueAt(left, path), path),
      fastrelay: summarizeValue(valueAt(right, path), path)
    }));
}

function summarizeValue(value, path = "") {
  if (value === undefined) return "absent";
  if (path.endsWith(".prompt_cache_key")) return "present";
  if (typeof value === "string") return value.length > 128 ? `string:${value.length}` : value;
  if (Array.isArray(value)) return `array:${value.length}`;
  if (value && typeof value === "object") return `object:${Object.keys(value).sort().join(",")}`;
  return value;
}

function diffPaths(left, right, path = "$") {
  if (Object.is(left, right)) return [];
  if (typeof left !== typeof right || left === null || right === null) return [path];
  if (Array.isArray(left) || Array.isArray(right)) {
    if (!Array.isArray(left) || !Array.isArray(right) || left.length !== right.length) return [path];
    return left.flatMap((item, index) => diffPaths(item, right[index], `${path}[${index}]`));
  }
  if (typeof left !== "object") return [path];
  const keys = new Set([...Object.keys(left), ...Object.keys(right)]);
  return [...keys].sort().flatMap((key) => diffPaths(left[key], right[key], `${path}.${key}`));
}

function valueAt(value, path) {
  if (path === "$") return value;
  const keys = path.slice(2).split(".");
  return keys.reduce((current, key) => current?.[key], value);
}

function defaultConfigDir() {
  return join(process.env.APPDATA ?? process.env.XDG_CONFIG_HOME ?? tmpdir(), "Atoapi");
}

async function listen(server) {
  await new Promise((resolveListen, reject) => {
    server.once("error", reject);
    server.listen(0, "127.0.0.1", resolveListen);
  });
  const address = server.address();
  assert.ok(address && typeof address === "object");
  return address.port;
}

function closeServer(server) {
  return new Promise((resolveClose) => server.close(() => resolveClose()));
}

async function freePort() {
  const server = createNetServer();
  const port = await listen(server);
  await closeServer(server);
  return port;
}

async function readRequestBody(request) {
  const chunks = [];
  for await (const chunk of request) chunks.push(chunk);
  return Buffer.concat(chunks).toString("utf8");
}

async function getJson(url) {
  const response = await fetch(url, { signal: AbortSignal.timeout(5_000) });
  assert.equal(response.ok, true, `${url} returned ${response.status}`);
  return response.json();
}

async function waitForHealth(baseUrl, child, label) {
  await waitFor(async () => {
    if (!processIsAlive(child.pid)) throw new Error(`${label}: Atoapi exited before health`);
    try {
      const health = await getJson(`${baseUrl}/health`);
      return health.ok === true;
    } catch {
      return false;
    }
  }, 30_000, `${label}: local proxy did not become healthy`);
}

async function waitFor(predicate, timeoutMs, message) {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (await predicate()) return;
    await delay(50);
  }
  throw new Error(message);
}

function createHeaderGate(expected, timeoutMs = 750) {
  let arrivals = 0;
  let released = false;
  let resolveRelease;
  let resolveResult;
  const releasePromise = new Promise((resolveReleasePromise) => {
    resolveRelease = resolveReleasePromise;
  });
  const resultPromise = new Promise((resolveResultPromise) => {
    resolveResult = resolveResultPromise;
  });
  const timeout = setTimeout(() => release("timeout"), timeoutMs);

  function release(reason) {
    if (released) return;
    released = true;
    clearTimeout(timeout);
    resolveResult({ arrivalsBeforeRelease: arrivals, reason });
    resolveRelease();
  }

  return {
    async arrive() {
      arrivals += 1;
      if (arrivals >= expected) release("all_arrived");
      await releasePromise;
    },
    waitForRelease() {
      return resultPromise;
    },
    release
  };
}

async function stopChild(child, label) {
  if (!processIsAlive(child.pid)) return;
  child.kill();
  await waitFor(
    () => !processIsAlive(child.pid),
    15_000,
    `${label}: isolated process did not exit`
  );
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

function delay(ms) {
  return new Promise((resolveDelay) => setTimeout(resolveDelay, ms));
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

function boundedPositiveInteger(value, label, maximum) {
  const parsed = Number.parseInt(String(value), 10);
  if (!Number.isSafeInteger(parsed) || parsed < 1 || parsed > maximum) {
    throw new Error(`${label} must be an integer from 1 to ${maximum}`);
  }
  return parsed;
}

function booleanArg(value) {
  return value === true || new Set(["1", "true", "on", "yes"]).has(String(value).toLowerCase());
}

function escapeRegExp(value) {
  return value.replace(/[.*+?^${}()|[\]\\]/gu, "\\$&");
}
