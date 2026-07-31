import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { spawn } from "node:child_process";
import { existsSync } from "node:fs";
import { createServer } from "node:http";
import { copyFile, mkdir, mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { createServer as createNetServer } from "node:net";
import { tmpdir } from "node:os";
import { basename, dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";

import { writeIsolatedResponsesConfig } from "./isolated-responses-fixture.mjs";

const repoRoot = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const args = parseArgs(process.argv.slice(2));
// Release gates are deterministic and secret-free by default. A copied user
// profile is available only for an explicit compatibility investigation.
const copiedUserConfig = !booleanArg(args["synthetic-config"]) &&
  (booleanArg(args["copied-user-config"]) || args["source-config-dir"] !== undefined);
const syntheticConfig = !copiedUserConfig;
const sourceConfigDir = copiedUserConfig
  ? resolve(String(args["source-config-dir"] ?? defaultConfigDir()))
  : null;
const oldExecutable = resolve(
  String(
    args["old-exe"] ??
      process.env.ATOAPI_WIRE_BASELINE_EXE ??
      join(
        repoRoot,
        "releases",
        "v1.3.10-agent-route-sync-20260726",
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
const scenario = String(args.scenario ?? "ordinary").trim().toLowerCase();
const baselineLabel = basename(dirname(oldExecutable));
const COMMIT_MATURITY_PROBE_DELAY_MS = 700;
const SYNTHETIC_CLIENT_PROMPT_CACHE_KEY = "wire-compat-client-cache-key";

if (!model) throw new Error("--model must not be empty");
if (!new Set(["ordinary", "lineage-recovery", "commit-maturity"]).has(scenario)) {
  throw new Error("--scenario must be ordinary, lineage-recovery, or commit-maturity");
}
if (!existsSync(oldExecutable)) {
  throw new Error(`wire baseline executable is missing: ${oldExecutable}`);
}
if (!existsSync(newExecutable)) {
  throw new Error(`FastRelay executable is missing: ${newExecutable}`);
}
if (gateHeaders && concurrency < 2) {
  throw new Error("--gate-headers requires --concurrency of at least 2");
}
if (gateHeaders && scenario !== "ordinary") {
  throw new Error("--gate-headers is supported only by the ordinary scenario");
}
if (scenario !== "ordinary" && concurrency !== 1) {
  throw new Error(`--scenario ${scenario} requires --concurrency 1`);
}

let upstream = null;
let upstreamPort = null;
let tempRoot = null;
let headerGate = null;
let activeMockArm = null;
let activeMockScenario = null;
const mockTurnByArm = new Map();

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
    const arm = activeMockArm ?? "unknown";
    const turn = (mockTurnByArm.get(arm) ?? 0) + 1;
    mockTurnByArm.set(arm, turn);
    captured.push({
      arm,
      body: parsed,
      requestKind: String(request.headers["x-atoapi-request-kind"] ?? ""),
      headers: safeHeaders(request.headers)
    });
    if (headerGate) await headerGate.arrive();
    response.writeHead(200, {
      "cache-control": "no-cache",
      "content-type": "text/event-stream; charset=utf-8"
    });
    const responseId = turn === 1
      ? "resp_wire_seed"
      : turn === 2
        ? "resp_wire_followup"
        : "resp_wire_terminal";
    const output = turn === 1
      ? [{
        type: "message",
        id: "msg_wire_seed",
        status: "completed",
        role: "assistant",
        content: [{ type: "output_text", text: "seed answer" }]
      }]
      : [];
    response.end([
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
          output,
          usage: {
            input_tokens: activeMockScenario === "commit-maturity" ? 32768 : 4096,
            output_tokens: 1,
            input_tokens_details: {
              cached_tokens: activeMockScenario === "commit-maturity" ? 32768 : 3968
            }
          }
        }
      })}`,
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
    gateHeaders,
    syntheticConfig,
    scenario
  });
  const newRun = await runIsolatedCapture({
    label: "fastrelay",
    executable: newExecutable,
    configDir: join(tempRoot, "fastrelay"),
    upstreamPort,
    captured,
    model,
    concurrency,
    gateHeaders,
    syntheticConfig,
    scenario
  });

  const baseline = oldRun.upstreamBody;
  const fastrelay = newRun.upstreamBody;
  const differingPaths = diffPaths(baseline, fastrelay);
  const identity = compareIdentityMarkers(oldRun.identityMarkers, newRun.identityMarkers);
  // v1.3.10 rewrites a caller-owned native Codex cache key.  The FastRelay
  // candidate deliberately preserves it verbatim.  This is a narrow positive
  // compatibility correction, not a license for any other wire drift.
  const clientOwnedCacheKeyCorrection =
    fastrelay?.prompt_cache_key === SYNTHETIC_CLIENT_PROMPT_CACHE_KEY &&
    baseline?.prompt_cache_key !== SYNTHETIC_CLIENT_PROMPT_CACHE_KEY;
  const meaningfulDifferingPaths = differingPaths.filter((path) => !(
    clientOwnedCacheKeyCorrection && path === "$.prompt_cache_key"
  ));
  const expectedIdentityCorrection = clientOwnedCacheKeyCorrection &&
    identity.differingFields.every((field) => field === "provider_prefix_key");
  const ignoreBodyLength = scenario === "lineage-recovery" || clientOwnedCacheKeyCorrection;
  const baselineComparableHeaders = comparableProtocolHeaders(oldRun.upstreamHeaders, ignoreBodyLength);
  const fastrelayComparableHeaders = comparableProtocolHeaders(newRun.upstreamHeaders, ignoreBodyLength);
  const recovery = scenario === "lineage-recovery"
    ? summarizeLineageRecovery(baseline, fastrelay, model)
    : null;
  const scenarioWirePass = scenario !== "lineage-recovery"
    ? meaningfulDifferingPaths.length === 0
    : recovery?.candidate_preserves_complete_replay === true;
  const commitMaturity = scenario === "commit-maturity"
    ? {
      baseline: oldRun.commitMaturity,
      fastrelay: newRun.commitMaturity
    }
    : null;
  const commitMaturityPass = scenario !== "commit-maturity" ||
    (commitMaturity?.baseline?.max_wait_ms === 0 &&
      commitMaturity?.fastrelay?.max_wait_ms === 0 &&
      commitMaturity?.fastrelay?.all_full_hit === true);
  const checks = {
    baseline_one_inbound_one_post: oldRun.oneInboundOnePost,
    candidate_one_inbound_one_post: newRun.oneInboundOnePost,
    same_prefix_header_gate: !gateHeaders || newRun.samePrefixReachedBeforeHeaders,
    final_wire: scenarioWirePass,
    commit_maturity: commitMaturityPass,
    protocol_headers: JSON.stringify(baselineComparableHeaders) === JSON.stringify(fastrelayComparableHeaders),
    local_identity: identity.equal || expectedIdentityCorrection || scenario === "lineage-recovery"
  };
  const report = {
    pass: Object.values(checks).every(Boolean),
    config_mode: syntheticConfig ? "synthetic-no-secret" : "explicit-copied-user-config",
    scenario,
    model,
    concurrency,
    header_gate: gateHeaders,
    baseline: oldRun.summary,
    fastrelay: newRun.summary,
    wire_equal: differingPaths.length === 0,
    wire_difference_expected: scenario === "lineage-recovery" || clientOwnedCacheKeyCorrection,
    differing_paths: differingPaths,
    meaningful_differing_paths: meaningfulDifferingPaths,
    differing_controls: summarizeControlDifferences(baseline, fastrelay, differingPaths),
    control_fingerprints: {
      baseline: controlFingerprints(baseline),
      fastrelay: controlFingerprints(fastrelay)
    },
    lineage_recovery: recovery,
    commit_maturity: commitMaturity,
    headers_equal: checks.protocol_headers,
    baseline_headers: oldRun.upstreamHeaders,
    fastrelay_headers: newRun.upstreamHeaders,
    shadow_identity_equal: identity.equal,
    shadow_identity_differences: identity.differingFields,
    client_owned_cache_key_correction: clientOwnedCacheKeyCorrection,
    checks,
    failure_reasons: Object.entries(checks)
      .filter(([, passed]) => !passed)
      .map(([name]) => name)
  };
  console.log(JSON.stringify(report, null, 2));
  assert.equal(checks.baseline_one_inbound_one_post, true, "baseline must make exactly one upstream POST per inbound");
  assert.equal(checks.candidate_one_inbound_one_post, true, "candidate must make exactly one upstream POST per inbound");
  assert.equal(checks.same_prefix_header_gate, true, "same-prefix header gate must not serialize ordinary dispatch");
  assert.equal(checks.final_wire, true, "final wire differs outside the explicitly attested caller cache-key correction");
  assert.equal(checks.commit_maturity, true, "commit-maturity behavior violated its bounded policy");
  assert.equal(checks.protocol_headers, true, "upstream protocol headers changed unexpectedly");
  assert.equal(checks.local_identity, true, "local identity changed outside the attested caller cache-key correction");
  if (scenario === "ordinary") {
    assert.equal(
      report.meaningful_differing_paths.length,
      0,
      `FastRelay may differ from ${baselineLabel} only by preserving the attested synthetic client cache key`
    );
    assert.equal(
      report.client_owned_cache_key_correction || report.wire_equal,
      true,
      "ordinary wire compatibility must remain exact unless the known client-owned cache-key correction applies"
    );
  } else if (scenario === "lineage-recovery") {
    assert.equal(
      report.lineage_recovery?.candidate_preserves_complete_replay,
      true,
      "v1.4.12 must keep a caller-supplied complete replay intact"
    );
  } else {
    assert.equal(
      report.meaningful_differing_paths.length,
      0,
      "commit maturity must not change the final wire outside the attested caller cache-key correction"
    );
    assert.equal(
      report.client_owned_cache_key_correction || report.wire_equal,
      true,
      "commit maturity may differ only by preserving the attested client-owned cache key"
    );
    assert.equal(
      report.commit_maturity?.baseline?.max_wait_ms,
      0,
      `${baselineLabel} must not wait for an already full-hit fixture`
    );
    assert.equal(
      report.commit_maturity?.fastrelay?.max_wait_ms,
      0,
      "FastRelay must not add an artificial maturity wait when every fixture response is fully cached"
    );
    assert.equal(
      report.commit_maturity?.fastrelay?.all_full_hit,
      true,
      "commit-maturity fixture must prove that the no-wait assertion is evaluating full cache hits"
    );
  }
  assert.equal(
    report.headers_equal,
    true,
    "FastRelay must preserve upstream protocol headers apart from its intentional product-version token"
  );
  assert.equal(
    report.shadow_identity_equal || report.client_owned_cache_key_correction,
    true,
    `FastRelay must preserve ${baselineLabel} shadow affinity identity unless the corrected client-owned cache key changes only its local placement scope`
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
  gateHeaders,
  syntheticConfig,
  scenario
}) {
  await createIsolatedConfig(configDir, upstreamPort, syntheticConfig, model);
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
    activeMockArm = label;
    activeMockScenario = scenario;
    mockTurnByArm.set(label, 0);
    headerGate = gate;
    const sendInbound = async (body) => {
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
        body: JSON.stringify(body),
        signal: AbortSignal.timeout(30_000)
      });
      const responseBody = await response.text();
      assert.equal(response.status, 200, `${label}: local proxy rejected fixture`);
      assert.match(responseBody, /response\.completed/u, `${label}: terminal event missing`);
      return response.status;
    };
    const expectedInbounds = scenario === "lineage-recovery"
      ? 3
      : scenario === "commit-maturity"
        ? 2
        : concurrency;
    const downstreamPromise = scenario === "lineage-recovery"
      ? (async () => [
        await sendInbound(syntheticRecoverySeedBody(model)),
        await sendInbound(syntheticRecoveryDeltaBody(model)),
        await sendInbound(syntheticRecoveryCompleteReplayBody(model))
      ])()
      : scenario === "commit-maturity"
        ? (async () => {
          const first = await sendInbound(syntheticCommitMaturityBody(model));
          await delay(COMMIT_MATURITY_PROBE_DELAY_MS);
          const second = await sendInbound(syntheticCommitMaturityBody(model));
          return [first, second];
        })()
      : Promise.all(
        Array.from({ length: concurrency }, () => sendInbound(syntheticOrdinaryBody(model)))
      );
    const gateResult = gate ? await gate.waitForRelease() : null;
    const downstream = await downstreamPromise;
    await waitFor(
      () => captured.length === before + expectedInbounds,
      5_000,
      `${label}: expected ${expectedInbounds} upstream POSTs`
    );
    const metrics = await getJson(`${baseUrl}/admin/metrics`);
    const upstreamRequests = captured.slice(before);
    const upstreamBody = upstreamRequests.at(-1)?.body;
    const upstreamHeaders = upstreamRequests.at(-1)?.headers ?? {};
    assert.ok(upstreamBody, `${label}: mock did not capture an upstream body`);
    if (syntheticConfig && scenario !== "lineage-recovery") {
      assert.equal(
        label === "fastrelay"
          ? upstreamBody.prompt_cache_key
          : typeof upstreamBody.prompt_cache_key,
        label === "fastrelay"
          ? SYNTHETIC_CLIENT_PROMPT_CACHE_KEY
          : "string",
        `${label}: trusted synthetic Codex cache key must survive the final wire${
          label === "fastrelay" ? " unchanged" : ""
        }`
      );
    }
    if (scenario !== "lineage-recovery") {
      assert(
        upstreamRequests.every((request) => JSON.stringify(request.body) === JSON.stringify(upstreamBody)),
        `${label}: parallel inbounds produced different upstream wire bodies`
      );
      assert(
        upstreamRequests.every((request) => JSON.stringify(request.headers) === JSON.stringify(upstreamHeaders)),
        `${label}: parallel inbounds produced different upstream headers`
      );
    } else {
      assert.equal(upstreamRequests.length, 3, `${label}: recovery scenario must make three POSTs`);
      if (label === "fastrelay") {
        assert.equal(
          upstreamRequests[1].body.previous_response_id,
          undefined,
          "FastRelay recovered delta must not forward the local response id"
        );
        assert.equal(
          upstreamRequests[2].body.previous_response_id,
          undefined,
          "FastRelay complete replay must not forward the local response id"
        );
      }
    }
    const generation = metrics.agent_generation ?? {};
    const oneInboundOnePost = Number(generation.inbound_requests) === expectedInbounds &&
      Number(generation.generation_attempts) === expectedInbounds &&
      Number(metrics.upstream_requests) === expectedInbounds &&
      upstreamRequests.length === expectedInbounds;
    const commitMaturity = scenario === "commit-maturity"
      ? (() => {
        const requestDiagnostics = Array.from(metrics.recent_requests ?? []).map((request) => ({
          input_tokens: Number(request?.input_tokens ?? 0),
          cache_read_tokens: Number(request?.cache_read_tokens ?? 0),
          wait_ms: Number(request?.prefix_guard_wait_ms ?? 0),
          state_age_ms: request?.prefix_guard_state_age_ms ?? null,
          source: request?.prefix_guard_wait_source ?? null,
          skip_reason: request?.prefix_guard_skip_reason ?? null,
          tail_source: request?.tail_source ?? null,
          tail_tool_output_chars: Number(request?.tail_tool_output_chars ?? 0),
          tail_largest_tool_output_chars: Number(request?.tail_largest_tool_output_chars ?? 0)
        }));
        return {
        probe_delay_ms: COMMIT_MATURITY_PROBE_DELAY_MS,
        max_wait_ms: Math.max(
          0,
          ...requestDiagnostics.map((request) => request.wait_ms)
        ),
        all_full_hit: requestDiagnostics.length === expectedInbounds &&
          requestDiagnostics.every((request) =>
            request.input_tokens > 0 && request.cache_read_tokens === request.input_tokens
          ),
        request_diagnostics: requestDiagnostics
      };
      })()
      : null;
    return {
      upstreamBody,
      upstreamHeaders,
      identityMarkers: identityMarkers(metrics.recent_requests?.[0] ?? {}),
      oneInboundOnePost,
      commitMaturity,
      samePrefixReachedBeforeHeaders: !gate || gateResult.arrivalsBeforeRelease === concurrency,
      summary: {
        local_status: downstream[0] ?? null,
        completed_responses: downstream.length,
        concurrency: scenario === "ordinary" ? concurrency : 1,
        fixture_inbounds: expectedInbounds,
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
    if (activeMockArm === label) activeMockArm = null;
    if (activeMockScenario === scenario) activeMockScenario = null;
    mockTurnByArm.delete(label);
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

async function createIsolatedConfig(configDir, upstreamPort, useSyntheticConfig, model) {
  await rm(configDir, { recursive: true, force: true });
  if (useSyntheticConfig) {
    await writeIsolatedResponsesConfig(configDir, upstreamPort, {
      localKey: "wire-compat-local-key",
      model,
      providerId: "wire-compat-provider",
      workspaceFingerprint: "wire-compat-synthetic-workspace"
    });
    return;
  }

  await mkdir(configDir, { recursive: true });
  assert.ok(sourceConfigDir, "a copied isolated configuration requires --source-config-dir");
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

function syntheticRequestBase(model, input, previousResponseId = null) {
  const body = {
    model,
    stream: true,
    store: false,
    max_output_tokens: 16,
    // Mirror the normal Codex FullReplay cache-affinity route without
    // borrowing a caller value. This known synthetic value must be present on
    // the final mock wire in synthetic-config mode.
    prompt_cache_key: SYNTHETIC_CLIENT_PROMPT_CACHE_KEY,
    prompt_cache_retention: "24h",
    instructions: "Wire compatibility fixture. Reply with OK only.",
    tools: [{
      type: "function",
      name: "read_fixture",
      description: "Read a synthetic fixture only.",
      parameters: {
        type: "object",
        properties: { path: { type: "string" } },
        required: ["path"],
        additionalProperties: false
      }
    }],
    tool_choice: "auto",
    parallel_tool_calls: true,
    input
  };
  if (previousResponseId) body.previous_response_id = previousResponseId;
  return body;
}

function syntheticOrdinaryBody(model) {
  return syntheticRequestBase(model, [
    {
      type: "message",
      role: "user",
      content: [{ type: "input_text", text: "fixture root" }]
    },
    {
      type: "function_call",
      id: "fc-wire-compat",
      call_id: "call-wire-compat",
      name: "read_fixture",
      arguments: "{\"path\":\"fixture.txt\"}"
    },
    {
      type: "function_call_output",
      call_id: "call-wire-compat",
      output: { stdout: "synthetic fixture result\\n", exit_code: 0 }
    },
    {
      type: "message",
      role: "user",
      content: [{ type: "input_text", text: "fixture tail" }]
    }
  ]);
}

function syntheticRecoverySeedBody(model) {
  return syntheticRequestBase(model, [
    { type: "message", role: "user", content: "before" }
  ]);
}

function syntheticRecoveryDeltaBody(model) {
  return syntheticRequestBase(
    model,
    [{ type: "message", role: "user", content: "after" }],
    "resp_wire_seed"
  );
}

function syntheticRecoveryCompleteReplayBody(model) {
  return syntheticRequestBase(
    model,
    [
      {
        type: "message",
        role: "user",
        // This is semantically the first user item, but the harmless metadata
        // noise defeats old raw-JSON prefix matching. It is the exact class of
        // local FullReplay recovery that v1.4.12 changed.
        metadata: { trace: "synthetic-replayed-history" },
        content: "before"
      },
      {
        type: "message",
        id: "msg_wire_seed",
        status: "completed",
        role: "assistant",
        content: [{ type: "output_text", text: "seed answer" }]
      },
      { type: "message", role: "user", content: "after" },
      {
        type: "message",
        role: "user",
        content: "complete history must not be prepended again"
      }
    ],
    "resp_wire_followup"
  );
}

function syntheticCommitMaturityBody(model) {
  const body = syntheticOrdinaryBody(model);
  const toolOutput = body.input.find((item) => item.type === "function_call_output");
  toolOutput.output = "m".repeat(4_096);
  return body;
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

function comparableProtocolHeaders(headers, ignoreBodyLength = false) {
  const userAgent = String(headers["user-agent"] ?? "");
  const comparable = {
    ...headers,
    "user-agent": /^Atoapi\/\d+(?:\.\d+){1,3}$/u.test(userAgent)
      ? "Atoapi/<version>"
      : userAgent
  };
  if (ignoreBodyLength) comparable["content-length"] = "<body-length>";
  return comparable;
}

function summarizeLineageRecovery(baseline, candidate, model) {
  const expected = syntheticRecoveryCompleteReplayBody(model).input;
  const baselineInput = Array.isArray(baseline?.input) ? baseline.input : [];
  const candidateInput = Array.isArray(candidate?.input) ? candidate.input : [];
  const candidateExpectedDifferingPaths = diffPaths(expected, candidateInput, "$.input");
  const candidatePreservesCompleteReplay = candidateExpectedDifferingPaths.length === 0;
  return {
    expected_input_items: expected.length,
    baseline_input_items: baselineInput.length,
    candidate_input_items: candidateInput.length,
    baseline_has_duplicate_prepend: baselineInput.length > expected.length &&
      JSON.stringify(baselineInput) !== JSON.stringify(expected),
    candidate_preserves_complete_replay: candidatePreservesCompleteReplay,
    candidate_expected_differing_paths: candidateExpectedDifferingPaths,
    final_body_bytes_delta: JSON.stringify(baseline).length - JSON.stringify(candidate).length
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

function controlFingerprints(body) {
  const promptCacheKey = typeof body?.prompt_cache_key === "string"
    ? `sha256:${createHash("sha256").update(body.prompt_cache_key).digest("hex")}`
    : null;
  return { prompt_cache_key: promptCacheKey };
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
