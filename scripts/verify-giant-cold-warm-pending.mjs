import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { existsSync } from "node:fs";
import { copyFile, mkdir, mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { createServer } from "node:http";
import { createServer as createNetServer } from "node:net";
import { tmpdir } from "node:os";
import { basename, dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const repoRoot = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const args = parseArgs(process.argv.slice(2));
const sourceConfigDir = resolve(String(args["source-config-dir"] ?? defaultConfigDir()));
const executable = resolve(String(
  args.exe ?? join(repoRoot, "src-tauri", "target", "release", "atoapi.exe")
));
const model = String(args.model ?? "gpt-5.6-terra").trim();

if (!existsSync(executable)) {
  throw new Error(`fresh executable is missing: ${executable}`);
}
if (!model) throw new Error("--model must not be empty");

const usageSequence = [
  [272_621, 3_801],
  [278_424, 3_801],
  [281_839, 3_801],
  [283_954, 3_801],
  [285_637, 278_080]
];
let upstream = null;
let tempRoot = null;
let captured = [];
const rootInput = buildGiantToolRoot();

try {
  upstream = createServer(async (request, response) => {
    const body = JSON.parse(await readRequestBody(request));
    const index = captured.length;
    captured.push({ body, headers: safeHeaders(request.headers) });
    const [inputTokens, cachedTokens] = usageSequence[index] ?? usageSequence.at(-1);
    response.writeHead(200, { "content-type": "text/event-stream; charset=utf-8" });
    response.end(sseCompleted(`resp-giant-cold-${index}`, model, inputTokens, cachedTokens));
  });
  const upstreamPort = await listen(upstream);
  tempRoot = await mkdtemp(join(tmpdir(), "atoapi-giant-cold-warm-pending-"));
  const configDir = join(tempRoot, "config");
  await createIsolatedConfig(configDir, upstreamPort);
  const configText = await readFile(join(configDir, "config.toml"), "utf8");
  const localKey = extractTomlString(configText, "local_key");
  if (!localKey) throw new Error("isolated config has no local_key");

  const port = await freePort();
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
    const elapsedMs = [];
    const turnDiagnostics = [];
    for (let turn = 0; turn < usageSequence.length; turn += 1) {
      const started = Date.now();
      const response = await fetch(`${baseUrl}/codex/v1/responses`, {
        method: "POST",
        headers: {
          authorization: `Bearer ${localKey}`,
          "content-type": "application/json",
          accept: "text/event-stream",
          "x-codex-turn-metadata": JSON.stringify({
            session_id: "giant-cold-warm-pending-session",
            thread_id: "giant-cold-warm-pending-thread",
            request_kind: "turn"
          })
        },
        body: JSON.stringify(requestBody(model, turn)),
        signal: AbortSignal.timeout(30_000)
      });
      const text = await response.text();
      elapsedMs.push(Date.now() - started);
      assert.equal(response.status, 200, `turn ${turn}: local proxy rejected the request`);
      assert.match(text, /response\.completed/u, `turn ${turn}: missing completed event`);
      await waitFor(async () => {
        const metrics = await getJson(`${baseUrl}/admin/metrics`);
        if (Number(metrics.upstream_requests) < turn + 1) return false;
        const recent = metrics.recent_requests ?? [];
        const settled = recent.find((request) =>
          Number(request.input_tokens) === usageSequence[turn][0] &&
          Number(request.cache_read_tokens) === usageSequence[turn][1]
        );
        if (!settled) return false;
        turnDiagnostics.push({
          turn,
          prefix_guard_wait_ms: settled.prefix_guard_wait_ms ?? null,
          prefix_guard_wait_reason: settled.prefix_guard_wait_reason ?? null,
          prefix_guard_wait_source: settled.prefix_guard_wait_source ?? null,
          prefix_lag_classification: settled.prefix_lag_classification ?? null,
          cache_new_tail_gap_tokens: settled.cache_new_tail_gap_tokens ?? null,
          cache_provider_unstable_gap_tokens: settled.cache_provider_unstable_gap_tokens ?? null
        });
        return true;
      }, 8_000, `turn ${turn}: terminal settlement did not finish`);
    }

    assert.equal(captured.length, usageSequence.length, "one inbound must produce one upstream POST");
    for (let turn = 0; turn < captured.length; turn += 1) {
      const body = captured[turn].body;
      assert.equal(body.model, model, `turn ${turn}: model changed on the frozen wire`);
      assert.equal(body.stream, true, `turn ${turn}: stream flag changed on the frozen wire`);
      assert.equal(body.store, false, `turn ${turn}: store flag changed on the frozen wire`);
      assert.equal(
        body.input.length,
        rootInput.length + turn,
        `turn ${turn}: full replay did not append exactly one input item`
      );
      if (turn > 0) {
        assert.deepEqual(
          body.input.slice(0, -1),
          captured[turn - 1].body.input,
          `turn ${turn}: stable input prefix drifted`
        );
      }
    }
    const promptCacheKeys = new Set(captured.map((item) => item.body.prompt_cache_key ?? null));
    assert.equal(promptCacheKeys.size, 1, "the cache control key must remain stable across exact children");

    // The root itself is immediate. Each of the three proven direct children
    // receives the 2s total warm-up opportunity (including any legacy 500ms
    // exact settle). After the bounded budget is exhausted, the recovered
    // child must not inherit an additional large wait.
    for (const turn of [1, 2, 3]) {
      assert(
        elapsedMs[turn] >= 1_100,
        `turn ${turn}: expected the proven giant-cold warm window, got ${elapsedMs[turn]}ms`
      );
      assert(
        elapsedMs[turn] <= 5_000,
        `turn ${turn}: warm window exceeded the bounded policy, got ${elapsedMs[turn]}ms`
      );
    }
    assert(
      elapsedMs[4] < 1_250,
      `recovered child inherited a stale warm delay: ${elapsedMs[4]}ms; ${JSON.stringify(turnDiagnostics)}`
    );

    const metrics = await getJson(`${baseUrl}/admin/metrics`);
    const generation = metrics.agent_generation ?? {};
    assert.equal(Number(generation.inbound_requests), usageSequence.length);
    assert.equal(Number(generation.generation_attempts), usageSequence.length);
    assert.equal(Number(metrics.upstream_requests), usageSequence.length);
    const recent = metrics.recent_requests ?? [];
    const lowReads = recent.filter((request) => Number(request.cache_read_tokens) === 3_801);
    assert.equal(lowReads.length, 4, "the mock must preserve all four raw low reads");
    assert(
      lowReads.every((request) => Number(request.cache_new_tail_gap_tokens) === 0),
      "a giant pending cold read must not be recorded as new user tail"
    );
    assert(
      lowReads.every((request) => Number(request.cache_provider_unstable_gap_tokens) > 0),
      "a giant pending cold read must remain provider-warm-up evidence"
    );

    console.log(JSON.stringify({
      pass: true,
      inbound_requests: Number(generation.inbound_requests),
      generation_attempts: Number(generation.generation_attempts),
      upstream_requests: Number(metrics.upstream_requests),
      elapsed_ms: elapsedMs,
      turn_diagnostics: turnDiagnostics,
      prompt_cache_key_stable: true,
      wire_prefix_append_only: true,
      giant_cold_low_reads: lowReads.length
    }, null, 2));
  } finally {
    await stopChild(child);
  }
} finally {
  if (upstream) await closeServer(upstream);
  if (tempRoot) await rm(tempRoot, { recursive: true, force: true });
}

function requestBody(modelId, turn) {
  return {
    model: modelId,
    stream: true,
    store: false,
    max_output_tokens: 16,
    instructions: "Giant cold-prefix warm-pending fixture. Reply with OK only.",
    input: [
      ...rootInput,
      ...Array.from({ length: turn }, (_, index) => ({
        type: "message",
        role: "user",
        content: [{ type: "input_text", text: `stable fixture tail ${index}` }]
      }))
    ]
  };
}

function buildGiantToolRoot() {
  const input = [];
  for (let index = 0; index < 32; index += 1) {
    const callId = `warm-root-call-${index}`;
    input.push({
      type: "function_call",
      call_id: callId,
      name: "fixture_tool",
      arguments: "{}"
    });
    input.push({
      type: "function_call_output",
      call_id: callId,
      output: "x".repeat(3_000)
    });
  }
  input.push({
    type: "message",
    role: "user",
    content: [{ type: "input_text", text: "giant cold root" }]
  });
  return input;
}

function sseCompleted(id, modelId, inputTokens, cachedTokens) {
  return [
    "event: response.output_text.delta",
    'data: {"type":"response.output_text.delta","delta":"OK"}',
    "",
    "event: response.completed",
    `data: ${JSON.stringify({
      type: "response.completed",
      response: {
        id,
        model: modelId,
        status: "completed",
        output: [],
        usage: {
          input_tokens: inputTokens,
          output_tokens: 1,
          input_tokens_details: { cached_tokens: cachedTokens }
        }
      }
    })}`,
    "",
    "data: [DONE]",
    ""
  ].join("\n");
}

async function createIsolatedConfig(configDir, upstreamPort) {
  await rm(configDir, { recursive: true, force: true });
  await mkdir(configDir, { recursive: true });
  const sourceConfig = join(sourceConfigDir, "config.toml");
  const targetConfig = join(configDir, "config.toml");
  await copyFile(sourceConfig, targetConfig);
  try {
    await copyFile(join(sourceConfigDir, "cache-key.dpapi"), join(configDir, "cache-key.dpapi"));
  } catch {
    // The loopback fixture never needs a cache snapshot.
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
  for (const block of tomlArrayBlocks(config, "providers")) {
    if (extractTomlString(block.body, "id") === providerId) {
      return `${config.slice(0, block.start)}${transform(block.body)}${config.slice(block.end)}`;
    }
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
  return pattern.test(block)
    ? block.replace(pattern, `${key} = "${value}"`)
    : `${block.trimEnd()}\n${key} = "${value}"\n`;
}

function replaceTomlBoolean(block, key, value) {
  const pattern = new RegExp(`^${escapeRegExp(key)}\\s*=\\s*(?:true|false)`, "mu");
  return pattern.test(block)
    ? block.replace(pattern, `${key} = ${value}`)
    : `${block.trimEnd()}\n${key} = ${value}\n`;
}

function extractTomlString(text, key) {
  return text.match(new RegExp(`^${escapeRegExp(key)}\\s*=\\s*"([^"]*)"`, "mu"))?.[1] ?? "";
}

function safeHeaders(headers) {
  return Object.fromEntries([
    "accept", "content-type", "content-encoding", "content-length", "user-agent"
  ].map((name) => [name, headers[name] ?? null]));
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

async function waitForHealth(baseUrl, child) {
  await waitFor(async () => {
    if (!processIsAlive(child.pid)) throw new Error("isolated Atoapi exited before health");
    try {
      return (await getJson(`${baseUrl}/health`)).ok === true;
    } catch {
      return false;
    }
  }, 30_000, "isolated Atoapi did not become healthy");
}

async function waitFor(predicate, timeoutMs, message) {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (await predicate()) return;
    await delay(50);
  }
  throw new Error(message);
}

async function stopChild(child) {
  if (!processIsAlive(child.pid)) return;
  child.kill();
  await waitFor(() => !processIsAlive(child.pid), 15_000, "isolated Atoapi did not exit");
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
    if (inline !== undefined) parsed[key] = inline;
    else if (items[index + 1] && !items[index + 1].startsWith("--")) parsed[key] = items[++index];
    else parsed[key] = true;
  }
  return parsed;
}

function escapeRegExp(value) {
  return value.replace(/[.*+?^${}()|[\]\\]/gu, "\\$&");
}
