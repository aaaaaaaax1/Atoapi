import { spawn } from "node:child_process";
import { copyFile, mkdir, mkdtemp, readFile, rm, stat, writeFile } from "node:fs/promises";
import { createServer } from "node:net";
import { tmpdir } from "node:os";
import { basename, dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const repoRoot = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const args = parseArgs(process.argv.slice(2));
const PROBE_TIMEOUT_MS = 180_000;
const LIVE_PORT = 18_883;

if (args["self-test"] === true) {
  runSelfTest();
  process.exit(0);
}

const sourceConfigDir = resolve(String(args["source-config-dir"] ?? defaultConfigDir()));
const executable = resolve(String(
  args.exe ?? join(repoRoot, "src-tauri", "target", "release", "atoapi.exe")
));
const providerId = requiredText(args.provider, "--provider");
const modelId = requiredText(args.model, "--model");
const keyId = optionalText(args["key-id"]);
const keyRealmHash = requiredSha256(args["key-realm-hash"], "--key-realm-hash");
const keepRunDir = args["keep-run-dir"] === true;
const outputPath = optionalOutputPath(args.output);

await assertFile(executable, "candidate executable");
await assertFile(join(sourceConfigDir, "config.toml"), "source config.toml");

const tempRoot = await mkdtemp(join(tmpdir(), "atoapi-isolated-response-continuation-"));
let child = null;
let retainedConfigDir = null;

try {
  const configDir = join(tempRoot, "config");
  await copyIsolatedConfig(sourceConfigDir, configDir);
  const localKey = await isolatedLocalKey(configDir);
  const port = await findAvailablePort();
  if (port === LIVE_PORT) throw new Error("isolated probe refused the live 18883 port");
  const baseUrl = `http://127.0.0.1:${port}`;
  child = spawn(executable, [], {
    cwd: repoRoot,
    windowsHide: true,
    stdio: "ignore",
    env: {
      ...process.env,
      ATOAPI_CONFIG_DIR: configDir,
      ATOAPI_ISOLATED_TEST_INSTANCE: "1",
      ATOAPI_HEADLESS_ISOLATED_TEST: "1",
      ATOAPI_TEST_LISTEN_PORT: String(port),
      ATOAPI_AUTOMATIC_CACHE_CANARY: "0"
    }
  });
  await waitForHealth(baseUrl, child);

  const body = {
    provider_id: providerId,
    model_id: modelId,
    key_realm_hash: keyRealmHash
  };
  if (keyId) body.key_id = keyId;
  const response = await fetch(`${baseUrl}/admin/isolated-response-continuation/probe`, {
    method: "POST",
    headers: {
      authorization: `Bearer ${localKey}`,
      "content-type": "application/json"
    },
    body: JSON.stringify(body),
    signal: AbortSignal.timeout(PROBE_TIMEOUT_MS)
  });
  const payload = await response.json().catch(() => null);
  if (!payload || typeof payload !== "object") {
    throw new Error(`isolated response continuation probe failed with HTTP ${response.status}`);
  }

  const result = sanitizeResult(payload);
  result.probe_error_code = allowedProbeErrorCode(payload?.probe_error_code);
  result.ok = result.ok && result.identity.key_realm_hash === keyRealmHash;
  if (outputPath) await writeSanitizedResult(outputPath, result);
  console.log(JSON.stringify(result, null, 2));
  if (!result.ok) process.exitCode = 1;
  if (keepRunDir) retainedConfigDir = configDir;
} finally {
  if (child) await stopChild(child);
  if (!keepRunDir) await rm(tempRoot, { recursive: true, force: true });
  if (retainedConfigDir) {
    console.error(`isolated probe directory retained: ${retainedConfigDir}`);
  }
}

function sanitizeResult(payload) {
  const result = {
    schema: "atoapi-isolated-response-continuation-probe-v1",
    ok: payload?.ok === true,
    isolated: payload?.isolated === true,
    live_18883_touched: payload?.live_18883_touched === true,
    identity: sanitizeIdentity(payload?.identity),
    seed: sanitizeObservation(payload?.seed),
    continuation: sanitizeObservation(payload?.continuation),
    upstream_request_count: finiteCount(payload?.upstream_request_count),
    expected_upstream_request_count: finiteCount(payload?.expected_upstream_request_count),
    no_raw_content: payload?.no_raw_content === true
  };
  if (payload?.probe_error_code) result.ok = false;
  result.ok = result.ok
    && result.isolated
    && !result.live_18883_touched
    && result.identity.channel === "responses"
    && result.identity.same_user_agent
    && result.identity.key_binding_kind !== "unknown"
    && result.identity.key_binding_fingerprint !== null
    && Boolean(result.identity.endpoint_fingerprint)
    && result.identity.transport_policy_fingerprint !== null
    && result.identity.continuation_realm_hash !== null
    && result.seed.accepted
    && result.seed.response_id_present
    && !result.seed.previous_response_id_sent
    && result.seed.sse
    && result.seed.usage_present
    && result.continuation.attempted
    && result.continuation.accepted
    && result.continuation.previous_response_id_sent
    && result.continuation.response_id_present
    && result.continuation.sse
    && result.continuation.usage_present
    && result.upstream_request_count === 2
    && result.expected_upstream_request_count === 2
    && result.no_raw_content;
  return result;
}

function allowedProbeErrorCode(value) {
  const code = String(value ?? "");
  return new Set([
    "key_realm_binding_mismatch",
    "key_selection_failed",
    "seed_transport_failed",
    "continuation_transport_failed",
    "provider_scope_invalid",
    "probe_internal_error"
  ]).has(code) ? code : null;
}

function sanitizeIdentity(value) {
  return {
    provider_id: String(value?.provider_id ?? ""),
    model_id: String(value?.model_id ?? ""),
    channel: String(value?.channel ?? ""),
    key_binding_kind: allowedKeyBindingKind(value?.key_binding_kind),
    key_binding_fingerprint: safeFingerprint(value?.key_binding_fingerprint),
    key_id_present: value?.key_id_present === true,
    key_id_fingerprint: safeFingerprint(value?.key_id_fingerprint),
    key_realm_hash: safeSha256(value?.key_realm_hash),
    endpoint_fingerprint: safeFingerprint(value?.endpoint_fingerprint),
    user_agent_fingerprint: safeFingerprint(value?.user_agent_fingerprint),
    transport_policy_fingerprint: safeFingerprint(value?.transport_policy_fingerprint),
    continuation_realm_hash: safeSha256(value?.continuation_realm_hash),
    same_user_agent: value?.same_user_agent === true
  };
}

function sanitizeObservation(value) {
  return {
    attempted: value?.attempted === true,
    http_status: finiteStatus(value?.http_status),
    semantic_status: allowedSemanticStatus(value?.semantic_status),
    rejection_category: allowedRejectionCategory(value?.rejection_category),
    accepted: value?.accepted === true,
    previous_response_id_sent: value?.previous_response_id_sent === true,
    response_id_present: value?.response_id_present === true,
    response_id_fingerprint: safeFingerprint(value?.response_id_fingerprint),
    sse: value?.sse === true,
    usage_present: value?.usage_present === true
  };
}

async function copyIsolatedConfig(source, target) {
  await mkdir(target, { recursive: true });
  await copyFile(join(source, "config.toml"), join(target, "config.toml"));
  const sourceKey = join(source, "cache-key.dpapi");
  if (await exists(sourceKey)) {
    await copyFile(sourceKey, join(target, basename(sourceKey)));
  }
}

async function isolatedLocalKey(configDir) {
  const text = await readFile(join(configDir, "config.toml"), "utf8");
  const match = text.match(/^\s*local_key\s*=\s*"([^"]+)"\s*$/mu);
  if (!match?.[1]) throw new Error("isolated config.toml has no local_key");
  return match[1];
}

async function waitForHealth(baseUrl, childProcess) {
  const deadline = Date.now() + 30_000;
  while (Date.now() < deadline) {
    if (!isAlive(childProcess?.pid)) {
      throw new Error("isolated Atoapi exited before health became ready");
    }
    try {
      const response = await fetch(`${baseUrl}/health`, {
        signal: AbortSignal.timeout(1_000)
      });
      const health = response.ok ? await response.json() : null;
      if (health?.ok === true) return;
    } catch {
      // The isolated child is still starting; the live service is not contacted.
    }
    await delay(100);
  }
  throw new Error(`isolated Atoapi did not become healthy at ${baseUrl}`);
}

async function stopChild(childProcess) {
  if (!childProcess || !isAlive(childProcess.pid)) return;
  childProcess.kill();
  const deadline = Date.now() + 15_000;
  while (isAlive(childProcess.pid) && Date.now() < deadline) {
    await delay(50);
  }
  if (isAlive(childProcess.pid)) childProcess.kill("SIGKILL");
}

function isAlive(pid) {
  if (!pid) return false;
  try {
    process.kill(pid, 0);
    return true;
  } catch {
    return false;
  }
}

async function findAvailablePort() {
  for (let attempt = 0; attempt < 8; attempt += 1) {
    const port = await new Promise((resolvePort, rejectPort) => {
      const server = createServer();
      server.once("error", rejectPort);
      server.listen(0, "127.0.0.1", () => {
        const address = server.address();
        const selected = typeof address === "object" && address ? address.port : 0;
        server.close((error) => error ? rejectPort(error) : resolvePort(selected));
      });
    });
    if (port > 0 && port !== LIVE_PORT) return port;
  }
  throw new Error("could not reserve a non-live isolated port");
}

async function assertFile(path, label) {
  const info = await stat(path).catch(() => null);
  if (!info?.isFile() || info.size <= 0) {
    throw new Error(`${label} is missing or empty: ${path}`);
  }
}

async function exists(path) {
  return await stat(path).then(() => true).catch(() => false);
}

function defaultConfigDir() {
  const appData = process.env.APPDATA;
  if (!appData) throw new Error("--source-config-dir is required when APPDATA is unavailable");
  return join(appData, "Atoapi");
}

function requiredText(value, label) {
  const normalized = String(value ?? "").trim();
  if (!normalized) throw new Error(`${label} is required`);
  return normalized;
}

function requiredSha256(value, label) {
  const normalized = requiredText(value, label).toLowerCase();
  if (!/^[a-f0-9]{64}$/u.test(normalized)) {
    throw new Error(`${label} must be a 64-character lowercase SHA-256 value`);
  }
  return normalized;
}

function optionalText(value) {
  if (value === undefined || value === null || value === true) return null;
  const normalized = String(value).trim();
  return normalized || null;
}

function optionalOutputPath(value) {
  const normalized = optionalText(value);
  return normalized ? resolve(normalized) : null;
}

async function writeSanitizedResult(path, result) {
  await mkdir(dirname(path), { recursive: true });
  await writeFile(path, JSON.stringify(result, null, 2) + "\n", "utf8");
}

function finiteStatus(value) {
  const status = Number(value);
  return Number.isInteger(status) && status >= 100 && status <= 599 ? status : null;
}

function finiteCount(value) {
  const count = Number(value);
  return Number.isInteger(count) && count >= 0 && count <= 2 ? count : null;
}

function safeFingerprint(value) {
  const fingerprint = String(value ?? "").trim().toLowerCase();
  return /^[a-f0-9]{16}$/u.test(fingerprint) ? fingerprint : null;
}

function safeSha256(value) {
  const digest = String(value ?? "").trim().toLowerCase();
  return /^[a-f0-9]{64}$/u.test(digest) ? digest : null;
}

function allowedSemanticStatus(value) {
  const status = String(value ?? "");
  return new Set(["accepted", "rejected", "not_attempted"]).has(status)
    ? status
    : "unknown";
}

function allowedRejectionCategory(value) {
  const category = String(value ?? "");
  return new Set([
    "none",
    "not_attempted",
    "previous_response_id_rejected",
    "request_rejected"
  ]).has(category) ? category : "unknown";
}

function allowedKeyBindingKind(value) {
  const kind = String(value ?? "");
  return new Set(["pool-key-id", "provider-connection-key"]).has(kind)
    ? kind
    : "unknown";
}

function delay(milliseconds) {
  return new Promise((resolveDelay) => setTimeout(resolveDelay, milliseconds));
}

function parseArgs(values) {
  const parsed = {};
  for (let index = 0; index < values.length; index += 1) {
    const value = values[index];
    if (!value.startsWith("--")) continue;
    const [key, inline] = value.slice(2).split("=", 2);
    if (inline !== undefined) {
      parsed[key] = inline;
      continue;
    }
    const next = values[index + 1];
    if (next !== undefined && !next.startsWith("--")) {
      parsed[key] = next;
      index += 1;
    } else {
      parsed[key] = true;
    }
  }
  return parsed;
}

function runSelfTest() {
  const parsed = parseArgs([
    "--provider", "provider-a", "--model=gpt-test", "--key-id", "key-a",
    "--key-realm-hash", "a".repeat(64), "--self-test"
  ]);
  if (parsed.provider !== "provider-a" || parsed.model !== "gpt-test"
      || parsed["key-id"] !== "key-a" || parsed["key-realm-hash"] !== "a".repeat(64)
      || parsed["self-test"] !== true) {
    throw new Error("argument parsing self-test failed");
  }
  const result = sanitizeResult({
    ok: true,
    isolated: true,
    live_18883_touched: false,
    identity: {
      provider_id: "provider-a",
      model_id: "gpt-test",
      channel: "responses",
      key_binding_kind: "pool-key-id",
      key_binding_fingerprint: "aaaaaaaaaaaaaaaa",
      key_id_present: true,
      key_id_fingerprint: "0123456789abcdef",
      key_realm_hash: "a".repeat(64),
      endpoint_fingerprint: "aaaaaaaaaaaaaaaa",
      user_agent_fingerprint: "fedcba9876543210",
      transport_policy_fingerprint: "0123456789abcdef",
      continuation_realm_hash: "b".repeat(64),
      same_user_agent: true,
      raw_key: "must-not-survive"
    },
    seed: {
      attempted: true,
      http_status: 200,
      semantic_status: "accepted",
      rejection_category: "none",
      accepted: true,
      previous_response_id_sent: false,
      response_id_present: true,
      response_id_fingerprint: "1111111111111111",
      sse: true,
      usage_present: true,
      response_id: "resp-private-seed"
    },
    continuation: {
      attempted: true,
      http_status: 200,
      semantic_status: "accepted",
      rejection_category: "none",
      accepted: true,
      previous_response_id_sent: true,
      response_id_present: true,
      response_id_fingerprint: "2222222222222222",
      sse: true,
      usage_present: true,
      raw_content: "must-not-survive"
    },
    upstream_request_count: 2,
    expected_upstream_request_count: 2,
    no_raw_content: true
  });
  const encoded = JSON.stringify(result);
  if (!result.ok || result.continuation.rejection_category !== "none"
      || encoded.includes("resp-private-seed") || encoded.includes("must-not-survive")) {
    throw new Error("sanitized result self-test failed");
  }
  if (safeFingerprint("not-a-fingerprint") !== null || finiteStatus(99) !== null) {
    throw new Error("bounded scalar sanitization self-test failed");
  }
  console.log("isolated response continuation probe self-test: passed");
}
