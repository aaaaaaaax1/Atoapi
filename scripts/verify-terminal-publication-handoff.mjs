import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { createHash } from "node:crypto";
import { existsSync } from "node:fs";
import { copyFile, mkdir, mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { createServer } from "node:http";
import { createServer as createNetServer } from "node:net";
import { tmpdir } from "node:os";
import { basename, dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const repoRoot = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const args = parseArgs(process.argv.slice(2));

if (args["self-test"]) {
  runSelfTest();
  process.exit(0);
}

const executable = resolve(
  String(args.exe ?? join(repoRoot, "src-tauri", "target", "release", "atoapi.exe"))
);
const sourceConfigDir = resolve(
  String(args["source-config-dir"] ?? defaultConfigDir())
);
const model = String(args.model ?? "gpt-5.6-terra").trim();
const requestedPort = boundedPort(args.port ?? 18_884, "--port");
const tailDelayMs = boundedInteger(args["tail-delay-ms"] ?? 125, "--tail-delay-ms", 10, 5_000);

if (!existsSync(executable)) {
  throw new Error(`candidate executable is missing: ${executable}`);
}
if (!existsSync(join(sourceConfigDir, "config.toml"))) {
  throw new Error(`source config is missing: ${join(sourceConfigDir, "config.toml")}`);
}
if (!model) throw new Error("--model must not be empty");
if (requestedPort === 18_883) {
  throw new Error("--port 18883 is reserved for the current Atoapi service; use an isolated port");
}

let upstream = null;
let child = null;
let tempRoot = null;
let parentTail = null;
let childTail = null;

try {
  parentTail = deferred();
  const childArrived = deferred();
  childTail = deferred();
  const upstreamRequests = [];
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
    const phase = phaseForInput(parsed.input);
    upstreamRequests.push({ phase, inputItems: Array.isArray(parsed.input) ? parsed.input.length : 0 });
    if (phase === "seed") {
      writeTerminal(response, "resp-handoff-seed", 4_096, 3_968, true);
      return;
    }
    if (phase === "parent") {
      writeTerminal(response, "resp-handoff-parent", 4_224, 4_096, false);
      await parentTail.promise;
      response.end("data: [DONE]\n\n");
      return;
    }
    if (phase === "child") {
      childArrived.resolve();
      writeTerminal(response, "resp-handoff-child", 4_352, 4_224, false);
      await childTail.promise;
      response.end("data: [DONE]\n\n");
      return;
    }
    response.writeHead(400, { "content-type": "application/json" });
    response.end('{"error":"unexpected handoff fixture phase"}');
  });

  const upstreamPort = await listen(upstream, 0);
  tempRoot = await mkdtemp(join(tmpdir(), "atoapi-terminal-handoff-"));
  const configDir = join(tempRoot, "config");
  await createIsolatedConfig(sourceConfigDir, configDir, upstreamPort);
  const isolatedConfigPath = join(configDir, "config.toml");
  const isolatedCacheKeyPath = join(configDir, "cache-key.dpapi");
  const initialConfigHash = await fileHash(isolatedConfigPath);
  const initialCacheKeyHash = existsSync(isolatedCacheKeyPath)
    ? await fileHash(isolatedCacheKeyPath)
    : null;
  const configText = await readFile(isolatedConfigPath, "utf8");
  const localKey = extractTomlString(configText, "local_key");
  if (!localKey) throw new Error("isolated config has no local_key");

  const port = await freePort(requestedPort);
  const baseUrl = `http://127.0.0.1:${port}`;
  child = spawn(executable, [], {
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
  await waitForHealth(baseUrl, child);

  const seedInput = [message("terminal-handoff-seed")];
  await completeRequest(baseUrl, localKey, model, seedInput);
  await waitFor(async () => Number((await getJson(`${baseUrl}/admin/metrics`)).agent_generation?.inbound_requests) === 1,
    10_000,
    "seed request did not settle"
  );

  const parentInput = [...seedInput, message("terminal-handoff-parent")];
  const parent = await beginRequest(baseUrl, localKey, model, parentInput);
  await withTimeout(parent.completed, 10_000, "parent never published response.completed");

  const childInput = [...parentInput, message("terminal-handoff-child")];
  const childRequest = await beginRequest(baseUrl, localKey, model, childInput);
  await withTimeout(childArrived.promise, 10_000, "child did not reach the upstream while parent tail was held");

  // The child is now inside the terminal publication fence. Let the parent
  // finish first, prove its H2 head settled, then let the child finish so the
  // candidate must take the narrow strict-FullReplay rebase path.
  await delay(tailDelayMs);
  parentTail.resolve();
  await parent.drain;
  await waitFor(async () => Number((await getJson(`${baseUrl}/admin/metrics`)).agent_generation?.inbound_requests) >= 2,
    10_000,
    "parent request did not settle before child tail release"
  );
  childTail.resolve();
  await withTimeout(childRequest.completed, 10_000, "child never published response.completed");
  await childRequest.drain;

  const metrics = await waitForValue(
    async () => {
      const value = await getJson(`${baseUrl}/admin/metrics`);
      return Number(value.agent_generation?.inbound_requests) === 3 ? value : null;
    },
    10_000,
    "all terminal handoff requests did not settle"
  );
  assert.equal(upstreamRequests.length, 3, "every handoff inbound must issue one upstream POST");
  assert.deepEqual(
    upstreamRequests.map((item) => item.phase),
    ["seed", "parent", "child"],
    "the fixture must preserve the intended FullReplay extension order"
  );
  const generation = metrics.agent_generation ?? {};
  assert.equal(Number(generation.inbound_requests), 3, "isolated run must record all three inbounds");
  assert.equal(Number(generation.generation_attempts), 3, "each inbound must retain one attempt");
  assert.equal(Number(metrics.upstream_requests), 3, "each inbound must retain one main upstream POST");

  const completed = array(metrics.recent_requests)
    .filter((entry) => Number(entry.status) === 200);
  assert.equal(completed.length, 3, "all handoff requests must complete locally");
  for (const entry of completed) {
    assert.equal(Number(entry.upstream_attempt_index), 1, "each inbound attempt index must be one");
    assert.equal(Number(entry.upstream_attempt_total), 1, "each inbound attempt total must be one");
    assert.notEqual(entry.final_scope_waterline?.outcome, "lineage_rejected");
    assert.notEqual(entry.final_scope_waterline?.outcome, "ambiguous_branch");
  }
  const childLog = completed.find(
    (entry) => Number(entry.final_scope_waterline?.current_input_items) === childInput.length
  );
  assert.ok(childLog, "the child FullReplay must produce a final-scope observation");
  assert.equal(childLog.final_scope_waterline?.outcome, "settled");
  assert.equal(childLog.final_scope_waterline?.predecessor_proof, "exact");
  assert.equal(childLog.final_scope_waterline?.predecessor_exact, true);
  assert.equal(childLog.final_scope_waterline?.predecessor_bound, true);
  assert.equal(
    await fileHash(isolatedConfigPath),
    initialConfigHash,
    "the isolated candidate must not rewrite the migrated configuration during normal startup or relay traffic"
  );
  if (initialCacheKeyHash !== null) {
    assert.equal(
      await fileHash(isolatedCacheKeyPath),
      initialCacheKeyHash,
      "the isolated candidate must not rewrite the encrypted cache-key material"
    );
  }

  console.log(JSON.stringify({
    pass: true,
    candidate: basename(executable),
    model,
    tail_delay_ms: tailDelayMs,
    upstream_posts: upstreamRequests.length,
    generation: {
      inbound_requests: Number(generation.inbound_requests),
      generation_attempts: Number(generation.generation_attempts),
      upstream_requests: Number(metrics.upstream_requests)
    },
    child_final_scope: {
      outcome: childLog.final_scope_waterline?.outcome ?? null,
      predecessor_proof: childLog.final_scope_waterline?.predecessor_proof ?? null,
      predecessor_bound: childLog.final_scope_waterline?.predecessor_bound ?? null,
      current_input_items: childLog.final_scope_waterline?.current_input_items ?? null
    },
    config_audit: {
      isolated_config_unchanged: true,
      isolated_cache_key_unchanged: initialCacheKeyHash !== null
    }
  }, null, 2));
} finally {
  parentTail?.resolve();
  childTail?.resolve();
  if (child) await stopChild(child, "terminal handoff isolated runtime");
  if (upstream) await closeServer(upstream);
  if (tempRoot) await rm(tempRoot, { recursive: true, force: true });
}

function phaseForInput(input) {
  const text = JSON.stringify(input ?? []);
  if (text.includes("terminal-handoff-child")) return "child";
  if (text.includes("terminal-handoff-parent")) return "parent";
  if (text.includes("terminal-handoff-seed")) return "seed";
  return "unknown";
}

function writeTerminal(response, responseId, inputTokens, cachedTokens, close) {
  response.writeHead(200, {
    "cache-control": "no-cache",
    "content-type": "text/event-stream; charset=utf-8"
  });
  response.write([
    "event: response.output_text.delta",
    'data: {"type":"response.output_text.delta","delta":"OK"}',
    "",
    "event: response.completed",
    `data: ${JSON.stringify({
      type: "response.completed",
      response: {
        id: responseId,
        model: "mock",
        status: "completed",
        usage: {
          input_tokens: inputTokens,
          output_tokens: 1,
          input_tokens_details: { cached_tokens: cachedTokens }
        }
      }
    })}`,
    "",
    ""
  ].join("\n"));
  if (close) response.end("data: [DONE]\n\n");
}

async function completeRequest(baseUrl, localKey, model, input) {
  const request = await beginRequest(baseUrl, localKey, model, input);
  await withTimeout(request.completed, 10_000, "fixture request never completed");
  await request.drain;
}

async function beginRequest(baseUrl, localKey, model, input) {
  const response = await fetch(`${baseUrl}/codex/v1/responses`, {
    method: "POST",
    headers: {
      authorization: `Bearer ${localKey}`,
      "content-type": "application/json",
      accept: "text/event-stream",
      "x-codex-turn-metadata": JSON.stringify({
        session_id: "terminal-handoff-session",
        thread_id: "terminal-handoff-thread",
        request_kind: "turn"
      })
    },
    body: JSON.stringify({
      model,
      stream: true,
      store: false,
      max_output_tokens: 16,
      instructions: "Terminal handoff regression fixture. Reply with OK only.",
      input
    }),
    signal: AbortSignal.timeout(20_000)
  });
  assert.equal(response.status, 200, `local proxy rejected terminal handoff fixture: ${response.status}`);
  assert.ok(response.body, "streaming response body is missing");
  const reader = response.body.getReader();
  const decoder = new TextDecoder();
  let text = "";
  let completed = false;
  let resolveCompleted;
  let rejectCompleted;
  const completedPromise = new Promise((resolveDone, rejectDone) => {
    resolveCompleted = resolveDone;
    rejectCompleted = rejectDone;
  });
  const drain = (async () => {
    try {
      for (;;) {
        const { done, value } = await reader.read();
        if (done) break;
        text += decoder.decode(value, { stream: true });
        if (text.includes("response.failed")) {
          throw new Error(`fixture received response.failed: ${text.slice(-800)}`);
        }
        if (!completed && text.includes("response.completed")) {
          completed = true;
          resolveCompleted();
        }
      }
      text += decoder.decode();
      if (!completed) throw new Error(`fixture stream ended without response.completed: ${text.slice(-800)}`);
    } catch (error) {
      if (!completed) rejectCompleted(error);
      throw error;
    }
  })();
  return { completed: completedPromise, drain };
}

async function createIsolatedConfig(sourceConfigDir, targetDir, upstreamPort) {
  await mkdir(targetDir, { recursive: true });
  const sourceConfig = join(sourceConfigDir, "config.toml");
  const targetConfig = join(targetDir, "config.toml");
  await copyFile(sourceConfig, targetConfig);
  try {
    await copyFile(join(sourceConfigDir, "cache-key.dpapi"), join(targetDir, "cache-key.dpapi"));
  } catch {
    // The loopback test can use a plaintext/empty cache when no DPAPI key exists.
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

function message(text) {
  return {
    type: "message",
    role: "user",
    content: [{ type: "input_text", text }]
  };
}

function defaultConfigDir() {
  return join(process.env.APPDATA ?? process.env.XDG_CONFIG_HOME ?? tmpdir(), "Atoapi");
}

function deferred() {
  let resolve;
  const promise = new Promise((resolvePromise) => {
    resolve = resolvePromise;
  });
  return { promise, resolve };
}

function array(value) {
  return Array.isArray(value) ? value : [];
}

async function fileHash(path) {
  return createHash("sha256").update(await readFile(path)).digest("hex");
}

async function listen(server, requestedPort) {
  await new Promise((resolveListen, reject) => {
    server.once("error", reject);
    server.listen(requestedPort, "127.0.0.1", resolveListen);
  });
  const address = server.address();
  assert.ok(address && typeof address === "object");
  return address.port;
}

function closeServer(server) {
  return new Promise((resolveClose) => server.close(() => resolveClose()));
}

async function freePort(preferred) {
  const server = createNetServer();
  try {
    const port = await listen(server, preferred);
    return port;
  } catch {
    const port = await listen(server, 0);
    return port;
  } finally {
    await closeServer(server);
  }
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

async function waitForHealth(baseUrl, processHandle) {
  await waitFor(async () => {
    if (!processIsAlive(processHandle.pid)) {
      throw new Error("isolated Atoapi exited before health");
    }
    try {
      return (await getJson(`${baseUrl}/health`)).ok === true;
    } catch {
      return false;
    }
  }, 30_000, "isolated Atoapi did not become healthy");
}

async function waitFor(predicate, timeoutMs, message) {
  await waitForValue(async () => (await predicate()) ? true : null, timeoutMs, message);
}

async function waitForValue(producer, timeoutMs, message) {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    const value = await producer();
    if (value) return value;
    await delay(40);
  }
  throw new Error(message);
}

function withTimeout(promise, timeoutMs, message) {
  return Promise.race([
    promise,
    delay(timeoutMs).then(() => { throw new Error(message); })
  ]);
}

async function stopChild(processHandle, label) {
  if (!processIsAlive(processHandle.pid)) return;
  processHandle.kill();
  await waitFor(() => !processIsAlive(processHandle.pid), 15_000, `${label} did not exit`);
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

function boundedPort(value, label) {
  return boundedInteger(value, label, 1_024, 65_533);
}

function boundedInteger(value, label, minimum, maximum) {
  const parsed = Number.parseInt(String(value), 10);
  if (!Number.isSafeInteger(parsed) || parsed < minimum || parsed > maximum) {
    throw new Error(`${label} must be an integer from ${minimum} to ${maximum}`);
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

function escapeRegExp(value) {
  return value.replace(/[.*+?^${}()|[\]\\]/gu, "\\$&");
}

function runSelfTest() {
  assert.equal(phaseForInput([message("terminal-handoff-seed")]), "seed");
  assert.equal(
    phaseForInput([message("terminal-handoff-seed"), message("terminal-handoff-parent")]),
    "parent"
  );
  assert.equal(
    phaseForInput([
      message("terminal-handoff-seed"),
      message("terminal-handoff-parent"),
      message("terminal-handoff-child")
    ]),
    "child"
  );
  console.log(JSON.stringify({ self_test: "passed" }));
}
