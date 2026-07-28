import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { existsSync } from "node:fs";
import { copyFile, mkdir, mkdtemp, readFile, readdir, rm, stat, writeFile } from "node:fs/promises";
import { createServer } from "node:http";
import { createServer as createNetServer } from "node:net";
import { tmpdir } from "node:os";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const repoRoot = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const args = parseArgs(process.argv.slice(2));
const executable = await resolveFreshExecutable(repoRoot, args.exe);
const sourceConfigDir = resolve(
  String(args["source-config-dir"] ?? defaultConfigDir())
);
const requestedPort = boundedPort(args.port ?? 18_885, "--port");
const model = String(args.model ?? "gpt-5.6-terra").trim();
const promptCacheKey = "idless-control-prefix-cache-key";

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

try {
  const upstreamRequests = [];
  upstream = createServer(async (request, response) => {
    const body = await readRequestBody(request);
    if (request.method !== "POST" || !request.url?.endsWith("/responses")) {
      response.writeHead(404, { "content-type": "application/json" });
      response.end('{"error":"mock route missing"}');
      return;
    }
    const parsed = JSON.parse(body);
    const phase = phaseForInput(parsed.input);
    upstreamRequests.push({
      phase,
      inputItems: Array.isArray(parsed.input) ? parsed.input.length : 0,
      previousResponseId: parsed.previous_response_id ?? null,
      promptCacheKey: parsed.prompt_cache_key ?? null
    });
    if (phase === "seed") {
      writeIdlessTerminal(response, 4_096, 3_968);
      return;
    }
    if (phase === "follow-up") {
      writeIdlessTerminal(response, 4_224, 4_096);
      return;
    }
    response.writeHead(400, { "content-type": "application/json" });
    response.end('{"error":"unexpected idless fixture phase"}');
  });

  const upstreamPort = await listen(upstream, 0);
  tempRoot = await mkdtemp(join(tmpdir(), "atoapi-idless-control-prefix-"));
  const configDir = join(tempRoot, "config");
  await createIsolatedConfig(sourceConfigDir, configDir, upstreamPort);
  const configText = await readFile(join(configDir, "config.toml"), "utf8");
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

  const largeHistoricalToolOutput = "x".repeat(32 * 1024);
  const seedInput = [
    message("idless-control-prefix-seed"),
    {
      type: "function_call_output",
      call_id: "call-idless-history",
      output: { stdout: largeHistoricalToolOutput, stderr: "", exit_code: 0 }
    }
  ];
  await completeRequest(baseUrl, localKey, model, seedInput, promptCacheKey);
  await waitFor(
    async () => Number((await getJson(`${baseUrl}/admin/metrics`)).agent_generation?.inbound_requests) === 1,
    10_000,
    "idless seed request did not settle"
  );

  const followUp = [...seedInput, message("idless-control-prefix-follow-up")];
  await completeRequest(baseUrl, localKey, model, followUp, promptCacheKey);
  const metrics = await waitForValue(async () => {
    const value = await getJson(`${baseUrl}/admin/metrics`);
    return Number(value.agent_generation?.inbound_requests) === 2 ? value : null;
  }, 10_000, "idless follow-up request did not settle");

  assert.equal(upstreamRequests.length, 2, "each inbound must issue exactly one upstream POST");
  assert.deepEqual(upstreamRequests.map((entry) => entry.phase), ["seed", "follow-up"]);
  assert.deepEqual(
    upstreamRequests.map((entry) => entry.previousResponseId),
    [null, null],
    "control-only continuity must never inject previous_response_id"
  );
  assert.deepEqual(
    upstreamRequests.map((entry) => entry.promptCacheKey),
    [promptCacheKey, promptCacheKey],
    "a caller-owned stable prompt_cache_key must survive both idless FullReplay wires"
  );
  const generation = metrics.agent_generation ?? {};
  assert.equal(Number(generation.inbound_requests), 2);
  assert.equal(Number(generation.generation_attempts), 2);
  assert.equal(Number(metrics.upstream_requests), 2);

  const completed = array(metrics.recent_requests).filter((entry) => Number(entry.status) === 200);
  assert.equal(completed.length, 2, "both idless fixture requests must complete locally");
  const followUpLog = completed.find(
    (entry) => Number(entry.final_scope_waterline?.current_input_items) === followUp.length
  );
  assert.ok(followUpLog, "follow-up must produce a final-scope observation");
  assert.equal(followUpLog.final_scope_waterline?.outcome, "settled");
  assert.equal(followUpLog.final_scope_waterline?.predecessor_proof, "exact");
  assert.equal(followUpLog.final_scope_waterline?.predecessor_bound, true);
  assert.equal(followUpLog.session_anchor_source, "control-prefix");
  assert.equal(Number(followUpLog.tail_tool_output_chars), 0);
  assert.equal(Number(followUpLog.tail_largest_tool_output_chars), 0);
  for (const entry of completed) {
    assert.equal(Number(entry.upstream_attempt_index), 1);
    assert.equal(Number(entry.upstream_attempt_total), 1);
  }

  console.log(JSON.stringify({
    pass: true,
    upstream_posts: upstreamRequests.length,
    follow_up: {
      predecessor_proof: followUpLog.final_scope_waterline?.predecessor_proof ?? null,
      predecessor_bound: followUpLog.final_scope_waterline?.predecessor_bound ?? null,
      session_anchor_source: followUpLog.session_anchor_source ?? null,
      tail_tool_output_chars: Number(followUpLog.tail_tool_output_chars ?? 0),
      prompt_cache_key_preserved: true
    }
  }, null, 2));
} finally {
  if (child) await stopChild(child, "idless control-prefix isolated runtime");
  if (upstream) await closeServer(upstream);
  if (tempRoot) await rm(tempRoot, { recursive: true, force: true });
}

function phaseForInput(input) {
  const text = JSON.stringify(input ?? []);
  if (text.includes("idless-control-prefix-follow-up")) return "follow-up";
  if (text.includes("idless-control-prefix-seed")) return "seed";
  return "unknown";
}

async function resolveFreshExecutable(repoRoot, explicitExecutable) {
  if (explicitExecutable) {
    const executable = resolve(String(explicitExecutable));
    await assertExecutableIsFresh(executable, repoRoot);
    return executable;
  }

  const release = join(repoRoot, "src-tauri", "target", "release", "atoapi.exe");
  const debug = join(repoRoot, "src-tauri", "target", "debug", "atoapi.exe");
  if (await executableIsFresh(release, repoRoot)) return resolve(release);
  if (await executableIsFresh(debug, repoRoot)) return resolve(debug);

  throw new Error(
    "no fresh atoapi.exe is available for the idless-control-prefix check; run `cargo build --bin atoapi` for a source check or build the release artifact before package verification"
  );
}

async function assertExecutableIsFresh(executable, repoRoot) {
  if (!(await executableIsFresh(executable, repoRoot))) {
    throw new Error(
      `candidate executable is stale for this source tree: ${executable}; build it before running this regression so a stale artifact cannot produce a misleading result`
    );
  }
}

async function executableIsFresh(executable, repoRoot) {
  if (!existsSync(executable)) return false;
  const [binary, newestSource] = await Promise.all([
    stat(executable),
    newestRelevantSourceMtime(repoRoot)
  ]);
  return binary.mtimeMs >= newestSource;
}

async function newestRelevantSourceMtime(repoRoot) {
  const roots = [
    join(repoRoot, "src-tauri", "src"),
    join(repoRoot, "src-tauri", "Cargo.toml"),
    join(repoRoot, "src-tauri", "Cargo.lock")
  ];
  let newest = 0;
  for (const root of roots) {
    if (!existsSync(root)) continue;
    const info = await stat(root);
    if (!info.isDirectory()) {
      newest = Math.max(newest, info.mtimeMs);
      continue;
    }
    newest = Math.max(newest, await newestRustSourceMtime(root));
  }
  return newest;
}

async function newestRustSourceMtime(root) {
  let newest = 0;
  for (const entry of await readdir(root, { withFileTypes: true })) {
    const path = join(root, entry.name);
    if (entry.isDirectory()) {
      newest = Math.max(newest, await newestRustSourceMtime(path));
    } else if (entry.isFile() && entry.name.endsWith(".rs")) {
      newest = Math.max(newest, (await stat(path)).mtimeMs);
    }
  }
  return newest;
}

function writeIdlessTerminal(response, inputTokens, cachedTokens) {
  response.writeHead(200, {
    "cache-control": "no-cache",
    "content-type": "text/event-stream; charset=utf-8"
  });
  response.end([
    "event: response.output_text.delta",
    'data: {"type":"response.output_text.delta","delta":"OK"}',
    "",
    "event: response.completed",
    `data: ${JSON.stringify({
      type: "response.completed",
      response: {
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
    "data: [DONE]",
    ""
  ].join("\n"));
}

async function completeRequest(baseUrl, localKey, model, input, promptCacheKey) {
  const response = await fetch(`${baseUrl}/codex/v1/responses`, {
    method: "POST",
    headers: {
      authorization: `Bearer ${localKey}`,
      "content-type": "application/json",
      accept: "text/event-stream",
      "x-codex-turn-metadata": JSON.stringify({
        session_id: "idless-control-prefix-session",
        thread_id: "idless-control-prefix-thread",
        request_kind: "turn"
      })
    },
    body: JSON.stringify({
      model,
      stream: true,
      store: false,
      prompt_cache_key: promptCacheKey,
      max_output_tokens: 16,
      instructions: "Idless control-prefix regression fixture. Reply with OK only.",
      input
    }),
    signal: AbortSignal.timeout(20_000)
  });
  assert.equal(response.status, 200, `local proxy rejected idless fixture: ${response.status}`);
  assert.ok(response.body, "streaming response body is missing");
  const reader = response.body.getReader();
  const decoder = new TextDecoder();
  let text = "";
  for (;;) {
    const { done, value } = await reader.read();
    if (done) break;
    text += decoder.decode(value, { stream: true });
  }
  text += decoder.decode();
  assert.ok(text.includes("response.completed"), `fixture stream ended without completion: ${text.slice(-800)}`);
  assert.ok(!text.includes("response.failed"), `fixture received response.failed: ${text.slice(-800)}`);
}

async function createIsolatedConfig(sourceConfigDir, targetDir, upstreamPort) {
  await mkdir(targetDir, { recursive: true });
  const sourceConfig = join(sourceConfigDir, "config.toml");
  const targetConfig = join(targetDir, "config.toml");
  await copyFile(sourceConfig, targetConfig);
  try {
    await copyFile(join(sourceConfigDir, "cache-key.dpapi"), join(targetDir, "cache-key.dpapi"));
  } catch {
    // A plaintext local test config is supported when no DPAPI key exists.
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

function array(value) {
  return Array.isArray(value) ? value : [];
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
    return await listen(server, preferred);
  } catch {
    return listen(server, 0);
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
  const parsed = Number.parseInt(String(value), 10);
  if (!Number.isSafeInteger(parsed) || parsed < 1_024 || parsed > 65_533) {
    throw new Error(`${label} must be an integer from 1024 to 65533`);
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
