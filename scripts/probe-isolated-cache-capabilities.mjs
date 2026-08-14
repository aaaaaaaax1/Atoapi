import { spawn } from "node:child_process";
import { copyFile, mkdir, mkdtemp, readFile, rm, stat } from "node:fs/promises";
import { createServer } from "node:net";
import { tmpdir } from "node:os";
import { basename, dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const repoRoot = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const args = parseArgs(process.argv.slice(2));

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
const channel = String(args.channel ?? "responses").trim();
const keepRunDir = args["keep-run-dir"] === true;

if (!new Set(["responses", "chat"]).has(channel)) {
  throw new Error("--channel must be responses or chat");
}

await assertFile(executable, "candidate executable");
await assertFile(join(sourceConfigDir, "config.toml"), "source config.toml");

const tempRoot = await mkdtemp(join(tmpdir(), "atoapi-isolated-cache-capability-"));
let child = null;
let retainedConfigDir = null;

try {
  const configDir = join(tempRoot, "config");
  await copyIsolatedConfig(sourceConfigDir, configDir);
  const localKey = await isolatedLocalKey(configDir);
  const port = await findAvailablePort();
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

  const response = await fetch(`${baseUrl}/admin/cache-capabilities/probe`, {
    method: "POST",
    headers: {
      authorization: `Bearer ${localKey}`,
      "content-type": "application/json"
    },
    body: JSON.stringify({ provider_id: providerId, model_id: modelId, channel }),
    signal: AbortSignal.timeout(45_000)
  });
  const payload = await response.json().catch(() => null);
  if (!response.ok || !payload || typeof payload !== "object") {
    throw new Error(`isolated cache capability probe failed with HTTP ${response.status}`);
  }

  const fields = Array.isArray(payload.fields)
    ? payload.fields.map(sanitizeField).filter(Boolean)
    : [];
  const result = {
    schema: "atoapi-isolated-cache-capability-probe-v1",
    ok: fields.length === 4 && fields.every((field) => field.status !== "error"),
    isolated: true,
    live_18883_touched: false,
    provider_id: String(payload.provider_id ?? ""),
    model_id: String(payload.model_id ?? ""),
    channel: String(payload.channel ?? ""),
    selected_key_mapped: typeof payload.key_id === "string" && payload.key_id.length > 0,
    baseline_status: finiteStatus(payload.baseline_status),
    management_request_count: 1 + fields.length,
    fields,
    checked_at: String(payload.checked_at ?? "")
  };
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

function sanitizeField(value) {
  const field = String(value?.field ?? "").trim();
  const status = String(value?.status ?? "").trim();
  if (!field || !status) return null;
  return {
    field,
    status,
    enabled: value?.enabled === true,
    effect_status: String(value?.effect_status ?? "unverified"),
    http_status: finiteStatus(value?.http_status)
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
      // The child is still starting; no live service is contacted.
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
  if (isAlive(childProcess.pid)) {
    childProcess.kill("SIGKILL");
  }
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
  return await new Promise((resolvePort, rejectPort) => {
    const server = createServer();
    server.once("error", rejectPort);
    server.listen(0, "127.0.0.1", () => {
      const address = server.address();
      const port = typeof address === "object" && address ? address.port : 0;
      server.close((error) => error ? rejectPort(error) : resolvePort(port));
    });
  });
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

function finiteStatus(value) {
  const status = Number(value);
  return Number.isInteger(status) && status >= 100 && status <= 599 ? status : null;
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
  const parsed = parseArgs(["--provider", "provider-a", "--self-test"]);
  if (parsed.provider !== "provider-a" || parsed["self-test"] !== true) {
    throw new Error("argument parsing self-test failed");
  }
  const field = sanitizeField({
    field: "prompt-cache-breakpoint",
    status: "verified",
    enabled: false,
    effect_status: "unverified",
    http_status: 200,
    message: "must not be retained"
  });
  if (JSON.stringify(field) !== JSON.stringify({
    field: "prompt-cache-breakpoint",
    status: "verified",
    enabled: false,
    effect_status: "unverified",
    http_status: 200
  })) {
    throw new Error("probe result sanitization self-test failed");
  }
  console.log("isolated cache capability probe self-test: passed");
}
