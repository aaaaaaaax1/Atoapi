import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { spawn } from "node:child_process";
import { existsSync } from "node:fs";
import { createServer } from "node:http";
import { copyFile, mkdir, mkdtemp, readFile, readdir, rm, stat, writeFile } from "node:fs/promises";
import { createServer as createNetServer } from "node:net";
import { tmpdir } from "node:os";
import { basename, dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";

import { writeIsolatedResponsesConfig } from "./isolated-responses-fixture.mjs";

const repoRoot = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const candidateManifest = await readFile(join(repoRoot, "src-tauri", "Cargo.toml"), "utf8");
const candidatePackageVersion = extractTomlString(candidateManifest, "version");
if (!candidatePackageVersion) {
  throw new Error("candidate Cargo package version is missing");
}
const expectedCandidateUserAgent = `Atoapi/${candidatePackageVersion}`;
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
const generatedControls = booleanArg(args["generated-controls"]);
const sequentialToolOutputChars = args["tool-output-chars"] === undefined
  ? 0
  : boundedPositiveInteger(args["tool-output-chars"], "--tool-output-chars", 1_000_000);
const scenario = String(args.scenario ?? "ordinary").trim().toLowerCase();
const baselineLabel = basename(dirname(oldExecutable));
const COMMIT_MATURITY_PROBE_DELAY_MS = 700;
const SYNTHETIC_CLIENT_PROMPT_CACHE_KEY = "wire-compat-client-cache-key";

if (!model) throw new Error("--model must not be empty");
if (!new Set([
  "ordinary",
  "lineage-recovery",
  "commit-maturity",
  "sequential-full-replay",
  "regenerated-tool-ids",
  "provider-waterline-rollback",
  "material-tool-tail-maturity"
]).has(scenario)) {
  throw new Error(
    "--scenario must be ordinary, lineage-recovery, commit-maturity, sequential-full-replay, regenerated-tool-ids, provider-waterline-rollback, or material-tool-tail-maturity"
  );
}
if (!existsSync(oldExecutable)) {
  throw new Error(`wire baseline executable is missing: ${oldExecutable}`);
}
if (!existsSync(newExecutable)) {
  throw new Error(`FastRelay executable is missing: ${newExecutable}`);
}
await assertCandidateExecutableIsFresh(newExecutable, repoRoot);
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
      rawWire: rawWireFingerprint(body),
      requestKind: String(request.headers["x-atoapi-request-kind"] ?? ""),
      headers: safeHeaders(request.headers)
    });
    if (headerGate) await headerGate.arrive();
    response.writeHead(200, {
      "cache-control": "no-cache",
      "content-type": "text/event-stream; charset=utf-8"
    });
    // Keep this downstream identifier arm-specific. The regenerated-tool-id
    // fixture must parse it from the seed's real SSE completion before it
    // builds the replay; a fixed literal would not exercise local lineage.
    const responseId = `resp_wire_${String(arm).replace(/[^A-Za-z0-9_-]/gu, "_")}_${turn}`;
    const output = turn === 1
      ? [{
        type: "message",
        id: "msg_wire_seed",
        status: "completed",
        role: "assistant",
        content: [{ type: "output_text", text: "seed answer" }]
      }]
      : [];
    const usage = mockUsageForScenario(activeMockScenario, turn);
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
            input_tokens: usage.inputTokens,
            output_tokens: 1,
            input_tokens_details: {
              cached_tokens: usage.cachedTokens
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
    generatedControls,
    sequentialToolOutputChars,
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
    generatedControls,
    sequentialToolOutputChars,
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
  // Preserving the caller's native cache key changes the candidate's opaque
  // local placement scope. A current FastRelay build may record that
  // attested request as either a passive steady assignment or the documented
  // non-blocking snapshot-lock fail-open. Neither marker changes the selected
  // provider, Key, or final wire, so permit precisely these known three-field
  // observation deltas and nothing broader.
  const expectedShadowObservation =
    (newRun.identityMarkers.shadow_affinity_lane === "steady" &&
      newRun.identityMarkers.shadow_affinity_decision === "assigned") ||
    (newRun.identityMarkers.shadow_affinity_lane === "transparent" &&
      newRun.identityMarkers.shadow_affinity_decision === "snapshot_lock_busy");
  const expectedIdentityCorrection = clientOwnedCacheKeyCorrection &&
    identity.differingFields.includes("provider_prefix_key") &&
    identity.differingFields.every((field) => new Set([
      "provider_prefix_key",
      "shadow_affinity_lane",
      "shadow_affinity_decision"
    ]).has(field)) &&
    newRun.identityMarkers.shadow_affinity_trusted_identity === true &&
    expectedShadowObservation;
  // Product releases intentionally advance the default `Atoapi/<version>`
  // header. The secret-free synthetic fixture has no custom User-Agent, so
  // only there may the corresponding local-only prefix digest differ without
  // making an otherwise exact wire comparison look regressed. Copied user
  // configurations remain strict, as do all identity fields except the two
  // derived from this header.
  const defaultUserAgentIdentityShift = syntheticConfig &&
    expectedDefaultUserAgentIdentityShift(
      oldRun.upstreamHeaders,
      newRun.upstreamHeaders,
      identity
    );
  const recovery = scenario === "lineage-recovery"
    ? summarizeLineageRecovery(baseline, fastrelay, model)
    : null;
  const sequentialFullReplay = scenario === "sequential-full-replay"
    ? {
      baseline: summarizeSequentialFullReplay(oldRun.upstreamRequests),
      fastrelay: summarizeSequentialFullReplay(newRun.upstreamRequests)
    }
    : null;
  const regeneratedToolIds = scenario === "regenerated-tool-ids"
    ? {
      baseline: summarizeRegeneratedToolIds(
        oldRun.upstreamRequests,
        oldRun.regeneratedToolIdReplay
      ),
      fastrelay: summarizeRegeneratedToolIds(
        newRun.upstreamRequests,
        newRun.regeneratedToolIdReplay
      )
    }
    : null;
  const rawSequenceEqual = sameRawWireSequence(oldRun.upstreamRequests, newRun.upstreamRequests);
  const rawInputSequenceEqual = sameRawInputSequence(oldRun.upstreamRequests, newRun.upstreamRequests);
  const sequentialMetadataCorrection = scenario === "sequential-full-replay" &&
    sequentialFullReplay?.fastrelay?.pass === true &&
    rawInputSequenceEqual &&
    sequentialFullReplay?.baseline?.static_wire_stable === false;
  const regeneratedToolIdCorrection = scenario === "regenerated-tool-ids" &&
    regeneratedToolIds?.fastrelay?.pass === true;
  const meaningfulDifferingPaths = differingPaths.filter((path) => !(
    (clientOwnedCacheKeyCorrection && path === "$.prompt_cache_key") ||
    (sequentialMetadataCorrection && path === "$.client_metadata")
  ));
  const rawWirePass = clientOwnedCacheKeyCorrection || rawSequenceEqual || sequentialMetadataCorrection;
  // A stable baseline is already the expected outcome: the candidate must
  // preserve it, but it does not need to claim a metadata correction. The
  // previous gate treated that no-op comparison as a failure and made a
  // healthy v1.4.22-vs-v1.4.22 replay look regressed.
  const sequentialWirePreserved = scenario === "sequential-full-replay" &&
    rawSequenceEqual && meaningfulDifferingPaths.length === 0;
  const scenarioWirePass = scenario === "lineage-recovery"
    ? recovery?.candidate_preserves_complete_replay === true
    : scenario === "sequential-full-replay"
      ? sequentialMetadataCorrection || sequentialWirePreserved
      : scenario === "regenerated-tool-ids"
        ? regeneratedToolIdCorrection
      : meaningfulDifferingPaths.length === 0 && rawWirePass;
  const ignoreBodyLength = scenario === "lineage-recovery" || clientOwnedCacheKeyCorrection ||
    sequentialMetadataCorrection || regeneratedToolIdCorrection;
  const baselineComparableHeaders = comparableProtocolHeaders(oldRun.upstreamHeaders, ignoreBodyLength);
  const fastrelayComparableHeaders = comparableProtocolHeaders(newRun.upstreamHeaders, ignoreBodyLength);
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
  const providerWaterlineMaturity = scenario === "provider-waterline-rollback"
    ? {
      baseline: oldRun.providerWaterlineMaturity,
      fastrelay: newRun.providerWaterlineMaturity
    }
    : null;
  const providerWaterlineMaturityPass = scenario !== "provider-waterline-rollback" ||
    providerWaterlineMaturity?.fastrelay?.pass === true;
  const materialToolTailMaturity = scenario === "material-tool-tail-maturity"
    ? {
      baseline: oldRun.materialToolTailMaturity,
      fastrelay: newRun.materialToolTailMaturity
    }
    : null;
  const materialToolTailMaturityPass = scenario !== "material-tool-tail-maturity" ||
    materialToolTailMaturity?.fastrelay?.pass === true;
  const checks = {
    baseline_one_inbound_one_post: oldRun.oneInboundOnePost,
    candidate_one_inbound_one_post: newRun.oneInboundOnePost,
    same_prefix_header_gate: !gateHeaders || newRun.samePrefixReachedBeforeHeaders,
    final_wire: scenarioWirePass,
    sequential_full_replay: scenario !== "sequential-full-replay" ||
      sequentialFullReplay?.fastrelay?.pass === true,
    regenerated_tool_ids: scenario !== "regenerated-tool-ids" ||
      regeneratedToolIds?.fastrelay?.pass === true,
    regenerated_tool_local_response_id: scenario !== "regenerated-tool-ids" ||
      regeneratedToolIds?.fastrelay?.local_response_id_captured === true &&
      regeneratedToolIds?.fastrelay?.local_response_id_reused_for_replay === true,
    commit_maturity: commitMaturityPass,
    provider_waterline_maturity: providerWaterlineMaturityPass,
    material_tool_tail_maturity: materialToolTailMaturityPass,
    candidate_user_agent_version:
      String(newRun.upstreamHeaders["user-agent"] ?? "") === expectedCandidateUserAgent,
    protocol_headers: JSON.stringify(baselineComparableHeaders) === JSON.stringify(fastrelayComparableHeaders),
    local_identity: identity.equal || expectedIdentityCorrection || defaultUserAgentIdentityShift ||
      scenario === "lineage-recovery"
  };
  const report = {
    pass: Object.values(checks).every(Boolean),
    config_mode: syntheticConfig ? "synthetic-no-secret" : "explicit-copied-user-config",
    scenario,
    model,
    concurrency,
    header_gate: gateHeaders,
    generated_controls_fixture: generatedControls,
    baseline: oldRun.summary,
    fastrelay: newRun.summary,
    wire_equal: differingPaths.length === 0,
    wire_difference_expected: scenario === "lineage-recovery" || clientOwnedCacheKeyCorrection ||
      sequentialMetadataCorrection || regeneratedToolIdCorrection,
    differing_paths: differingPaths,
    meaningful_differing_paths: meaningfulDifferingPaths,
    differing_controls: summarizeControlDifferences(baseline, fastrelay, differingPaths),
    control_fingerprints: {
      baseline: controlFingerprints(baseline),
      fastrelay: controlFingerprints(fastrelay)
    },
    raw_wire: {
      sequence_equal: rawSequenceEqual,
      input_sequence_equal: rawInputSequenceEqual,
      baseline: rawWireSummary(oldRun.upstreamRequests),
      fastrelay: rawWireSummary(newRun.upstreamRequests)
    },
    lineage_recovery: recovery,
    sequential_full_replay: sequentialFullReplay,
    regenerated_tool_ids: regeneratedToolIds,
    commit_maturity: commitMaturity,
    provider_waterline_maturity: providerWaterlineMaturity,
    material_tool_tail_maturity: materialToolTailMaturity,
    expected_candidate_user_agent: expectedCandidateUserAgent,
    candidate_user_agent: newRun.upstreamHeaders["user-agent"] ?? null,
    headers_equal: checks.protocol_headers,
    baseline_headers: oldRun.upstreamHeaders,
    fastrelay_headers: newRun.upstreamHeaders,
    shadow_identity_equal: identity.equal,
    shadow_identity_differences: identity.differingFields,
    baseline_shadow_observation: shadowObservationClass(oldRun.identityMarkers),
    fastrelay_shadow_observation: shadowObservationClass(newRun.identityMarkers),
    client_owned_cache_key_correction: clientOwnedCacheKeyCorrection,
    default_user_agent_identity_shift: defaultUserAgentIdentityShift,
    sequential_metadata_correction: sequentialMetadataCorrection,
    regenerated_tool_id_correction: regeneratedToolIdCorrection,
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
  assert.equal(checks.sequential_full_replay, true, "sequential full replay did not retain a stable byte-level prefix");
  assert.equal(checks.regenerated_tool_ids, true, "regenerated Codex tool ids did not retain the prior wire prefix");
  assert.equal(
    checks.regenerated_tool_local_response_id,
    true,
    "regenerated-tool fixture did not dynamically capture and reuse the local response id"
  );
  assert.equal(checks.commit_maturity, true, "commit-maturity behavior violated its bounded policy");
  assert.equal(
    checks.provider_waterline_maturity,
    true,
    "proven provider waterline rollback did not receive one bounded safe-child maturity window"
  );
  assert.equal(
    checks.material_tool_tail_maturity,
    true,
    "an exact material tool tail did not give its safe direct child one bounded maturity window"
  );
  assert.equal(
    checks.candidate_user_agent_version,
    true,
    `candidate default User-Agent must match its Cargo package version (${expectedCandidateUserAgent})`
  );
  assert.equal(checks.protocol_headers, true, "upstream protocol headers changed unexpectedly");
  assert.equal(
    checks.local_identity,
    true,
    "local identity changed outside the attested caller cache-key or default product User-Agent version correction"
  );
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
  } else if (scenario === "commit-maturity") {
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
  } else if (scenario === "regenerated-tool-ids") {
    assert.equal(
      report.regenerated_tool_ids?.fastrelay?.exact_prior_prefix,
      true,
      "FastRelay must restore regenerated tool call ids to the exact prior input prefix"
    );
    assert.equal(
      report.regenerated_tool_ids?.fastrelay?.raw_input_append_only,
      true,
      "FastRelay must make the second regenerated-id replay a raw append-only input wire"
    );
    assert.equal(
      report.regenerated_tool_ids?.fastrelay?.tool_outputs_unchanged,
      true,
      "FastRelay must not rewrite tool result values while rebinding call ids"
    );
    assert.equal(
      report.regenerated_tool_ids?.fastrelay?.no_previous_response_id,
      true,
      "FastRelay regenerated-id full replay must not forward previous_response_id"
    );
    assert.equal(
      report.regenerated_tool_ids?.fastrelay?.local_response_id_captured,
      true,
      "FastRelay regenerated-id fixture must parse the first local response id from downstream SSE"
    );
    assert.equal(
      report.regenerated_tool_ids?.fastrelay?.local_response_id_reused_for_replay,
      true,
      "FastRelay regenerated-id fixture must use the captured local response id in the second inbound"
    );
  } else if (scenario === "provider-waterline-rollback") {
    assert.equal(
      report.provider_waterline_maturity?.fastrelay?.rollback_observed,
      true,
      "rollback fixture must prove the final-scope ledger observed a provider waterline rollback"
    );
    assert.equal(
      report.provider_waterline_maturity?.fastrelay?.one_safe_child_wait,
      true,
      "only the direct safe child may receive the new bounded rollback maturity wait"
    );
  } else if (scenario === "material-tool-tail-maturity") {
    assert.equal(
      report.material_tool_tail_maturity?.fastrelay?.material_tail_observed,
      true,
      "fixture must prove that the predecessor carried a material tool tail"
    );
    assert.equal(
      report.material_tool_tail_maturity?.fastrelay?.one_safe_child_wait,
      true,
      "only the safe direct child may receive the new bounded material-tail maturity wait"
    );
  } else {
    assert.equal(
      report.sequential_full_replay?.fastrelay?.raw_input_append_only,
      true,
      "FastRelay sequential full replay must append to the exact previous input wire"
    );
    assert.equal(
      report.sequential_full_replay?.fastrelay?.static_wire_stable,
      true,
      "FastRelay sequential full replay must keep the non-input raw wire stable"
    );
    assert.equal(
      report.sequential_full_replay?.fastrelay?.client_metadata_stripped,
      true,
      "FastRelay must strip the changing attested client metadata carrier from every sequential wire"
    );
    assert.equal(
      report.sequential_full_replay?.fastrelay?.no_previous_response_id,
      true,
      "FastRelay full replay must not forward previous_response_id"
    );
  }
  assert.equal(
    report.headers_equal,
    true,
    "FastRelay must preserve upstream protocol headers apart from its intentional product-version token"
  );
  assert.equal(
    report.shadow_identity_equal ||
      report.client_owned_cache_key_correction ||
      report.default_user_agent_identity_shift,
    true,
    `FastRelay must preserve ${baselineLabel} shadow affinity identity unless the corrected client-owned cache key or default product User-Agent version changes only its local scope`
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

function mockUsageForScenario(activeScenario, turn) {
  if (activeScenario === "commit-maturity") {
    return { inputTokens: 32_768, cachedTokens: 32_768 };
  }
  if (activeScenario === "provider-waterline-rollback") {
    const inputTokens = 65_536 + Math.max(0, turn - 1) * 128;
    const cachedTokens = turn === 2 ? 32_768 : 65_024;
    return { inputTokens, cachedTokens };
  }
  if (activeScenario === "material-tool-tail-maturity") {
    if (turn === 1) return { inputTokens: 65_536, cachedTokens: 65_536 };
    if (turn === 2) return { inputTokens: 73_728, cachedTokens: 65_536 };
    return { inputTokens: 73_856, cachedTokens: 73_216 };
  }
  return { inputTokens: 4_096, cachedTokens: 3_968 };
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
  generatedControls,
  sequentialToolOutputChars,
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
      // Newer candidates can bypass WebView2 completely for this disposable
      // proxy-only verifier.  Older baselines ignore the opt-in, so retain it
      // on both arms to keep the fixture cross-version compatible.
      ATOAPI_HEADLESS_ISOLATED_TEST: "1",
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
      return {
        status: response.status,
        localResponseId: completedResponseIdFromSse(responseBody)
      };
    };
    const expectedInbounds = scenario === "lineage-recovery"
      ? 3
      : scenario === "commit-maturity"
        ? 2
        : scenario === "regenerated-tool-ids"
          ? 2
          : scenario === "provider-waterline-rollback"
            ? 3
          : scenario === "material-tool-tail-maturity"
            ? 3
          : scenario === "sequential-full-replay"
            ? 4
            : concurrency;
    const downstreamPromise = scenario === "lineage-recovery"
      ? (async () => [
        await sendInbound(syntheticRecoverySeedBody(model, generatedControls)),
        await sendInbound(syntheticRecoveryDeltaBody(model, generatedControls)),
        await sendInbound(syntheticRecoveryCompleteReplayBody(model, generatedControls))
      ])()
      : scenario === "commit-maturity"
        ? (async () => {
          const first = await sendInbound(syntheticCommitMaturityBody(model, generatedControls));
          await delay(COMMIT_MATURITY_PROBE_DELAY_MS);
          const second = await sendInbound(syntheticCommitMaturityBody(model, generatedControls));
          return [first, second];
        })()
        : scenario === "regenerated-tool-ids"
          ? (async () => {
            const first = await sendInbound(syntheticRegeneratedToolIdSeedBody(
              model,
              generatedControls,
              sequentialToolOutputChars
            ));
            assert.ok(
              first.localResponseId,
              `${label}: regenerated-id seed did not return a local response id`
            );
            const replayBody = syntheticRegeneratedToolIdReplayBody(
              model,
              first.localResponseId,
              generatedControls,
              sequentialToolOutputChars
            );
            const second = await sendInbound(replayBody);
            const results = [first, second];
            // This value remains transient: the report keeps only the
            // boolean, never the response id supplied to the replay.
            results.localResponseIdReusedForReplay =
              replayBody.previous_response_id === first.localResponseId;
            return results;
          })()
          : scenario === "provider-waterline-rollback"
            ? (async () => {
              const results = [];
              for (const body of syntheticProviderWaterlineRollbackBodies(model, generatedControls)) {
                results.push(await sendInbound(body));
              }
              return results;
            })()
          : scenario === "material-tool-tail-maturity"
            ? (async () => {
              const results = [];
              for (const body of syntheticMaterialToolTailMaturityBodies(model, generatedControls)) {
                results.push(await sendInbound(body));
              }
              return results;
            })()
          : scenario === "sequential-full-replay"
            ? (async () => {
            const results = [];
            for (const body of syntheticSequentialFullReplayBodies(
              model,
              generatedControls,
              sequentialToolOutputChars
            )) {
              results.push(await sendInbound(body));
            }
            return results;
          })()
        : Promise.all(
          Array.from({ length: concurrency }, () => sendInbound(syntheticOrdinaryBody(model, generatedControls)))
        );
    const gateResult = gate ? await gate.waitForRelease() : null;
    const downstream = await downstreamPromise;
    await waitFor(
      () => captured.length === before + expectedInbounds,
      5_000,
      `${label}: expected ${expectedInbounds} upstream POSTs`
    );
    const metrics = await getJson(`${baseUrl}/admin/metrics`, 5_000, localKey);
    const upstreamRequests = captured.slice(before);
    const upstreamBody = upstreamRequests.at(-1)?.body;
    const upstreamHeaders = upstreamRequests.at(-1)?.headers ?? {};
    assert.ok(upstreamBody, `${label}: mock did not capture an upstream body`);
    if (syntheticConfig && scenario !== "lineage-recovery") {
      assert.equal(typeof upstreamBody.prompt_cache_key, "string", `${label}: final wire must retain a cache placement key`);
      assert.equal(upstreamBody.prompt_cache_retention, "24h", `${label}: final wire must retain cache retention`);
      // v1.3.10 rewrites caller-owned native placement keys.  The candidate
      // intentionally corrects that behavior, so only it is required to
      // preserve the synthetic caller key verbatim.  The cross-version diff
      // below explicitly permits this single, positive correction.
      if (!generatedControls && label === "fastrelay") {
        assert.equal(
          upstreamBody.prompt_cache_key,
          SYNTHETIC_CLIENT_PROMPT_CACHE_KEY,
          `${label}: caller-owned synthetic cache key must survive unchanged`
        );
      }
    }
    if (scenario === "ordinary" || scenario === "commit-maturity") {
      assert(
        upstreamRequests.every((request) => request.rawWire.body_sha256 === upstreamRequests[0].rawWire.body_sha256),
        `${label}: parallel inbounds produced different raw upstream wire bodies`
      );
      assert(
        upstreamRequests.every((request) => JSON.stringify(request.headers) === JSON.stringify(upstreamHeaders)),
        `${label}: parallel inbounds produced different upstream headers`
      );
    } else if (scenario === "lineage-recovery") {
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
    } else if (scenario === "regenerated-tool-ids") {
      assert.equal(upstreamRequests.length, 2, `${label}: regenerated-id replay must make two POSTs`);
    } else if (scenario === "provider-waterline-rollback") {
      assert.equal(upstreamRequests.length, 3, `${label}: rollback fixture must make three POSTs`);
    } else if (scenario === "material-tool-tail-maturity") {
      assert.equal(upstreamRequests.length, 3, `${label}: material-tail fixture must make three POSTs`);
    } else {
      assert.equal(upstreamRequests.length, 4, `${label}: sequential full replay must make four POSTs`);
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
    const providerWaterlineMaturity = scenario === "provider-waterline-rollback"
      ? summarizeProviderWaterlineMaturity(metrics.recent_requests, expectedInbounds)
      : null;
    const materialToolTailMaturity = scenario === "material-tool-tail-maturity"
      ? summarizeMaterialToolTailMaturity(metrics.recent_requests, expectedInbounds)
      : null;
    const regeneratedToolIdReplay = scenario === "regenerated-tool-ids"
      ? {
        local_response_id_captured: Boolean(downstream[0]?.localResponseId),
        local_response_id_reused_for_replay:
          downstream.localResponseIdReusedForReplay === true,
        replay_response_completed: Boolean(downstream[1]?.localResponseId)
      }
      : null;
    return {
      upstreamBody,
      upstreamHeaders,
      upstreamRequests,
      identityMarkers: identityMarkers(metrics.recent_requests?.[0] ?? {}),
      oneInboundOnePost,
      commitMaturity,
      providerWaterlineMaturity,
      materialToolTailMaturity,
      regeneratedToolIdReplay,
      samePrefixReachedBeforeHeaders: !gate || gateResult.arrivalsBeforeRelease === concurrency,
      summary: {
        local_status: downstream[0]?.status ?? null,
        completed_responses: downstream.length,
        concurrency: scenario === "ordinary" ? concurrency : 1,
        fixture_inbounds: expectedInbounds,
        sequential_tool_output_chars: scenario === "sequential-full-replay"
          ? sequentialToolOutputChars
          : 0,
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

function shadowObservationClass(markers) {
  const lane = String(markers.shadow_affinity_lane ?? "");
  const decision = String(markers.shadow_affinity_decision ?? "");
  if (lane === "steady" && decision === "assigned") return "steady_assigned";
  if (lane === "transparent" && decision === "snapshot_lock_busy") return "snapshot_lock_busy";
  if (lane === "transparent" && decision === "transparent") return "transparent";
  return "other";
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

function syntheticRequestBase(model, input, previousResponseId = null, generatedControls = false) {
  const body = {
    model,
    stream: true,
    store: false,
    max_output_tokens: 16,
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
  if (!generatedControls) {
    // Mirror a caller-owned Codex FullReplay cache-affinity route. This known
    // synthetic value must survive unchanged on the final mock wire.
    body.prompt_cache_key = SYNTHETIC_CLIENT_PROMPT_CACHE_KEY;
    body.prompt_cache_retention = "24h";
  }
  if (previousResponseId) body.previous_response_id = previousResponseId;
  return body;
}

function completedResponseIdFromSse(responseBody) {
  for (const line of String(responseBody ?? "").split(/\r?\n/u)) {
    if (!line.startsWith("data:")) continue;
    const payload = line.slice("data:".length).trim();
    if (!payload || payload === "[DONE]") continue;
    try {
      const event = JSON.parse(payload);
      const responseId = event?.type === "response.completed"
        ? event?.response?.id
        : null;
      if (
        typeof responseId === "string" &&
        responseId.length > 0 &&
        responseId.length <= 512 &&
        !/[\r\n]/u.test(responseId)
      ) {
        return responseId;
      }
    } catch {
      // Only response.completed is relevant to the local lineage fixture.
    }
  }
  return null;
}

function syntheticOrdinaryBody(model, generatedControls = false) {
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
  ], null, generatedControls);
}

function syntheticRecoverySeedBody(model, generatedControls = false) {
  return syntheticRequestBase(model, [
    { type: "message", role: "user", content: "before" }
  ], null, generatedControls);
}

function syntheticRecoveryDeltaBody(model, generatedControls = false) {
  return syntheticRequestBase(
    model,
    [{ type: "message", role: "user", content: "after" }],
    "resp_wire_seed",
    generatedControls
  );
}

function syntheticRecoveryCompleteReplayBody(model, generatedControls = false) {
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
    "resp_wire_followup",
    generatedControls
  );
}

function syntheticCommitMaturityBody(model, generatedControls = false) {
  const body = syntheticOrdinaryBody(model, generatedControls);
  const toolOutput = body.input.find((item) => item.type === "function_call_output");
  toolOutput.output = "m".repeat(4_096);
  return body;
}

function syntheticProviderWaterlineRollbackBodies(model, generatedControls = false) {
  const input = [
    {
      type: "message",
      role: "user",
      content: [{ type: "input_text", text: "rollback maturity root" }]
    }
  ];
  const bodies = [];
  for (let turn = 0; turn < 3; turn += 1) {
    bodies.push(syntheticRequestBase(
      model,
      input.map((item) => structuredClone(item)),
      null,
      generatedControls
    ));
    input.push({
      type: "message",
      role: "user",
      content: [{ type: "input_text", text: `rollback maturity tail ${turn}` }]
    });
  }
  return bodies;
}

function syntheticMaterialToolTailMaturityBodies(model, generatedControls = false) {
  const root = [
    {
      type: "message",
      role: "user",
      content: [{ type: "input_text", text: "material-tail maturity root" }]
    }
  ];
  const material = [
    ...root.map((item) => structuredClone(item)),
    {
      type: "function_call",
      id: "fc-material-tail",
      call_id: "call-material-tail",
      name: "read_fixture",
      arguments: "{\"path\":\"material-tail.txt\"}"
    },
    {
      type: "function_call_output",
      call_id: "call-material-tail",
      output: "m".repeat(32_768)
    }
  ];
  const child = [
    ...material.map((item) => structuredClone(item)),
    {
      type: "message",
      role: "user",
      content: [{ type: "input_text", text: "safe direct child" }]
    }
  ];
  return [
    syntheticRequestBase(model, root, null, generatedControls),
    syntheticRequestBase(model, material, null, generatedControls),
    syntheticRequestBase(model, child, null, generatedControls)
  ];
}

function syntheticSequentialFullReplayBodies(
  model,
  generatedControls = false,
  toolOutputChars = 0
) {
  const input = [
    {
      type: "message",
      role: "user",
      content: [{ type: "input_text", text: "sequential full replay root" }]
    },
    {
      type: "function_call",
      id: "fc-sequential-wire",
      call_id: "call-sequential-wire",
      name: "read_fixture",
      arguments: "{\"path\":\"sequential.txt\"}"
    },
    {
      type: "function_call_output",
      call_id: "call-sequential-wire",
      output: toolOutputChars > 0
        ? "x".repeat(toolOutputChars)
        : { stdout: "sequential fixture result\\n", exit_code: 0 }
    }
  ];
  const positions = ["first", "middle", "last", "middle"];
  const bodies = [];
  for (let turn = 0; turn < positions.length; turn += 1) {
    const body = syntheticRequestBase(model, input.map((item) => structuredClone(item)), null, generatedControls);
    body.client_metadata = syntheticClientMetadataCarrier(positions[turn], turn);
    bodies.push(body);
    input.push({
      type: "message",
      role: "user",
      content: [{ type: "input_text", text: `sequential full replay tail ${turn}` }]
    });
  }
  return bodies;
}

function regeneratedToolIdStableHistory(toolOutputChars = 0) {
  return [
    {
      type: "message",
      role: "user",
      content: [{ type: "input_text", text: "regenerated tool id root" }]
    },
    {
      type: "function_call",
      id: "fc-regenerated-wire",
      status: "completed",
      call_id: "call-regenerated-prior",
      name: "read_fixture",
      arguments: "{\"path\":\"regenerated.txt\"}"
    },
    {
      type: "function_call_output",
      id: "fco-regenerated-wire",
      status: "completed",
      call_id: "call-regenerated-prior",
      output: toolOutputChars > 0
        ? {
          stdout: "r".repeat(toolOutputChars),
          stderr: "",
          exit_code: 0
        }
        : {
          stdout: "regenerated fixture result\\n",
          stderr: "",
          exit_code: 0
        }
    }
  ];
}

function syntheticRegeneratedToolIdSeedBody(
  model,
  generatedControls = false,
  toolOutputChars = 0
) {
  const stableHistory = regeneratedToolIdStableHistory(toolOutputChars);
  const first = syntheticRequestBase(
    model,
    stableHistory.map((item) => structuredClone(item)),
    null,
    generatedControls
  );
  // The rebind path is intentionally limited to attested Codex lineage.
  // Model the real client carrier on both turns so the local response id can
  // be recognized as belonging to the same FullReplay session.  It is
  // consumed locally and stripped from the final native wire.
  first.client_metadata = syntheticClientMetadataCarrier("first", 0);
  return first;
}

function syntheticRegeneratedToolIdReplayBody(
  model,
  localResponseId,
  generatedControls = false,
  toolOutputChars = 0
) {
  if (typeof localResponseId !== "string" || !localResponseId || /[\r\n]/u.test(localResponseId)) {
    throw new Error("regenerated tool id replay requires a captured local response id");
  }
  const stableHistory = regeneratedToolIdStableHistory(toolOutputChars);
  const replay = stableHistory.map((item) => structuredClone(item));
  // Codex can replay the same settled exchange with a fresh opaque id. The
  // candidate must bind this closed pair back to the exact prior wire before
  // its single upstream POST, then append only the genuine new user tail.
  replay[1].call_id = "call-regenerated-fresh";
  replay[2].call_id = "call-regenerated-fresh";
  replay.push({
    type: "message",
    role: "user",
    content: [{ type: "input_text", text: "regenerated tool id real tail" }]
  });
  // The first response id is local to Atoapi on third-party FullReplay
  // routes. The candidate must recover the complete replay, remove this
  // local-only p_r from the final wire, then rebind regenerated tool ids.
  // This exercises the exact guard that would otherwise leave an equivalent
  // tool prefix cache-distinct merely because the client included a local p_r.
  const second = syntheticRequestBase(model, replay, localResponseId, generatedControls);
  second.client_metadata = syntheticClientMetadataCarrier("first", 1);
  return second;
}

function syntheticClientMetadataCarrier(position, turn) {
  const turnMetadata = JSON.stringify({
    session_id: "wire-compat-session",
    thread_id: "wire-compat-thread",
    request_kind: "turn",
    turn_nonce: `synthetic-sequential-${turn}`
  });
  const carrier = `\"x-codex-turn-metadata\":${turnMetadata}`;
  if (position === "first") return `{${carrier},\"alpha\":\"a\",\"beta\":\"b\"}`;
  if (position === "last") return `{\"alpha\":\"a\",\"beta\":\"b\",${carrier}}`;
  return `{\"alpha\":\"a\",${carrier},\"beta\":\"b\"}`;
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

function expectedDefaultUserAgentIdentityShift(baselineHeaders, candidateHeaders, identity) {
  const baseline = String(baselineHeaders["user-agent"] ?? "");
  const candidate = String(candidateHeaders["user-agent"] ?? "");
  const defaultAtoapiUserAgent = /^Atoapi\/\d+(?:\.\d+){1,3}$/u;
  return baseline !== candidate &&
    defaultAtoapiUserAgent.test(baseline) &&
    defaultAtoapiUserAgent.test(candidate) &&
    identity.differingFields.length > 0 &&
    identity.differingFields.every((field) => new Set([
      "provider_prefix_key",
      "provider_prefix_fingerprint"
    ]).has(field));
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

function summarizeSequentialFullReplay(requests) {
  const expectedClientMetadata = '{"alpha":"a","beta":"b"}';
  const wires = requests.map((request) => request.rawWire);
  const previousInputIdsAbsent = requests.every((request) => request.body.previous_response_id === undefined);
  const clientMetadataStripped = requests.every(
    (request) => request.body.client_metadata === expectedClientMetadata
  );
  const rawInputAppendOnly = wires.slice(1).every((wire, index) => {
    const previous = wires[index].input_inner;
    return wire.input_inner.startsWith(`${previous},`);
  });
  const inputItemsAppendOnly = requests.slice(1).every(
    (request, index) => Array.isArray(request.body.input) &&
      request.body.input.length === requests[index].body.input.length + 1
  );
  const staticWireStable = wires.length > 0 && wires.every((wire) =>
    wire.static_before_input_sha256 === wires[0].static_before_input_sha256 &&
    wire.static_after_input_sha256 === wires[0].static_after_input_sha256
  );
  const onePostPerInbound = requests.length === 4;
  return {
    pass: onePostPerInbound && rawInputAppendOnly && inputItemsAppendOnly &&
      staticWireStable && clientMetadataStripped && previousInputIdsAbsent,
    turns: requests.length,
    raw_input_append_only: rawInputAppendOnly,
    input_items_append_only: inputItemsAppendOnly,
    static_wire_stable: staticWireStable,
    client_metadata_stripped: clientMetadataStripped,
    no_previous_response_id: previousInputIdsAbsent,
    one_post_per_inbound: onePostPerInbound,
    wires: rawWireSummary(requests)
  };
}

function summarizeProviderWaterlineMaturity(requests, expectedInbounds) {
  const diagnostics = Array.from(requests ?? []).map((request) => ({
    input_tokens: Number(request?.input_tokens ?? 0),
    cache_read_tokens: Number(request?.cache_read_tokens ?? 0),
    wait_ms: Number(request?.prefix_guard_wait_ms ?? 0),
    wait_reason: request?.prefix_guard_wait_reason ?? null,
    wait_source: request?.prefix_guard_wait_source ?? null,
    skip_reason: request?.prefix_guard_skip_reason ?? null,
    tail_input_items: Number(request?.tail_input_items ?? 0),
    tail_message_chars: Number(request?.tail_message_chars ?? 0),
    tail_tool_output_chars: Number(request?.tail_tool_output_chars ?? 0),
    final_scope_outcome: request?.final_scope_waterline?.outcome ?? null,
    final_scope_exact: request?.final_scope_waterline?.predecessor_exact ?? false,
    final_scope_bound: request?.final_scope_waterline?.predecessor_bound ?? false,
    final_scope_rollback_tokens_128: Number(
      request?.final_scope_waterline?.rollback_tokens_128 ?? 0
    )
  }));
  const rollbackObserved = diagnostics.some((request) =>
    request.final_scope_outcome === "settled" &&
    request.final_scope_exact === true &&
    request.final_scope_bound === true &&
    request.final_scope_rollback_tokens_128 >= 128
  );
  const rollbackWaits = diagnostics.filter((request) =>
    request.wait_reason === "responses_provider_waterline_rollback_pending" &&
    request.wait_source === "exact" && request.wait_ms > 0 && request.wait_ms <= 500
  );
  const oneSafeChildWait = rollbackWaits.length === 1;
  return {
    pass: diagnostics.length === expectedInbounds && rollbackObserved && oneSafeChildWait,
    rollback_observed: rollbackObserved,
    one_safe_child_wait: oneSafeChildWait,
    rollback_wait_count: rollbackWaits.length,
    max_wait_ms: Math.max(0, ...diagnostics.map((request) => request.wait_ms)),
    request_diagnostics: diagnostics
  };
}

function summarizeMaterialToolTailMaturity(requests, expectedInbounds) {
  const diagnostics = Array.from(requests ?? []).map((request) => ({
    input_tokens: Number(request?.input_tokens ?? 0),
    cache_read_tokens: Number(request?.cache_read_tokens ?? 0),
    wait_ms: Number(request?.prefix_guard_wait_ms ?? 0),
    wait_reason: request?.prefix_guard_wait_reason ?? null,
    wait_source: request?.prefix_guard_wait_source ?? null,
    tail_input_items: Number(request?.tail_input_items ?? 0),
    tail_message_chars: Number(request?.tail_message_chars ?? 0),
    tail_tool_output_chars: Number(request?.tail_tool_output_chars ?? 0),
    tail_noise: request?.tail_tool_output_noise_hint ?? null,
    final_scope_outcome: request?.final_scope_waterline?.outcome ?? null,
    final_scope_exact: request?.final_scope_waterline?.predecessor_exact ?? false,
    final_scope_bound: request?.final_scope_waterline?.predecessor_bound ?? false,
    final_scope_candidate_avoidable_tokens_128: Number(
      request?.final_scope_waterline?.candidate_avoidable_tokens_128 ?? 0
    )
  }));
  const materialTailObserved = diagnostics.some((request) =>
    request.tail_tool_output_chars >= 8_192 &&
    request.final_scope_outcome === "settled" &&
    request.final_scope_exact === true &&
    request.final_scope_bound === true &&
    request.final_scope_candidate_avoidable_tokens_128 === 0
  );
  const waits = diagnostics.filter((request) =>
    request.wait_reason === "responses_material_tool_tail_maturity_pending" &&
    request.wait_source === "exact" && request.wait_ms > 0 && request.wait_ms <= 500 &&
    request.tail_tool_output_chars < 8_000 && request.tail_noise == null
  );
  const oneSafeChildWait = waits.length === 1;
  return {
    pass: diagnostics.length === expectedInbounds && materialTailObserved && oneSafeChildWait,
    material_tail_observed: materialTailObserved,
    one_safe_child_wait: oneSafeChildWait,
    material_tail_wait_count: waits.length,
    max_wait_ms: Math.max(0, ...diagnostics.map((request) => request.wait_ms)),
    request_diagnostics: diagnostics
  };
}

function summarizeRegeneratedToolIds(requests, localReplay = null) {
  if (requests.length !== 2) {
    return {
      pass: false,
      turns: requests.length,
      reason: "expected_exactly_two_turns"
    };
  }
  const [first, second] = requests;
  const firstInput = Array.isArray(first.body?.input) ? first.body.input : [];
  const secondInput = Array.isArray(second.body?.input) ? second.body.input : [];
  const secondPrefix = secondInput.slice(0, firstInput.length);
  const exactPriorPrefix = JSON.stringify(secondPrefix) === JSON.stringify(firstInput);
  const rawInputAppendOnly = second.rawWire.input_inner.startsWith(`${first.rawWire.input_inner},`);
  const inputItemsAppendOnly = secondInput.length === firstInput.length + 1;
  const toolOutputsUnchanged = JSON.stringify(
    secondPrefix
      .filter((item) => item?.type === "function_call_output")
      .map((item) => item.output)
  ) === JSON.stringify(
    firstInput
      .filter((item) => item?.type === "function_call_output")
      .map((item) => item.output)
  );
  const noPreviousResponseId = requests.every(
    (request) => request.body?.previous_response_id === undefined
  );
  const localResponseIdCaptured = localReplay?.local_response_id_captured === true;
  const localResponseIdReusedForReplay =
    localReplay?.local_response_id_reused_for_replay === true;
  const onePostPerInbound = requests.length === 2;
  return {
    pass: onePostPerInbound && exactPriorPrefix && rawInputAppendOnly &&
      inputItemsAppendOnly && toolOutputsUnchanged && noPreviousResponseId &&
      localResponseIdCaptured && localResponseIdReusedForReplay,
    turns: requests.length,
    exact_prior_prefix: exactPriorPrefix,
    raw_input_append_only: rawInputAppendOnly,
    input_items_append_only: inputItemsAppendOnly,
    tool_outputs_unchanged: toolOutputsUnchanged,
    no_previous_response_id: noPreviousResponseId,
    local_response_id_captured: localResponseIdCaptured,
    local_response_id_reused_for_replay: localResponseIdReusedForReplay,
    replay_response_completed: localReplay?.replay_response_completed === true,
    one_post_per_inbound: onePostPerInbound,
    first_prefix_call_ids: firstInput
      .filter((item) => item?.type === "function_call" || item?.type === "function_call_output")
      .map((item) => item.call_id ?? null),
    second_prefix_call_ids: secondPrefix
      .filter((item) => item?.type === "function_call" || item?.type === "function_call_output")
      .map((item) => item.call_id ?? null),
    wires: rawWireSummary(requests)
  };
}

function sameRawWireSequence(left, right) {
  return left.length === right.length && left.every(
    (request, index) => request.rawWire.body_sha256 === right[index]?.rawWire.body_sha256
  );
}

function sameRawInputSequence(left, right) {
  return left.length === right.length && left.every(
    (request, index) => request.rawWire.input_sha256 === right[index]?.rawWire.input_sha256
  );
}

function rawWireSummary(requests) {
  return requests.map((request) => ({
    body_bytes: request.rawWire.body_bytes,
    body_sha256: request.rawWire.body_sha256,
    input_bytes: request.rawWire.input_bytes,
    input_sha256: request.rawWire.input_sha256,
    static_before_input_sha256: request.rawWire.static_before_input_sha256,
    static_after_input_sha256: request.rawWire.static_after_input_sha256
  }));
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

function rawWireFingerprint(body) {
  const input = rawTopLevelArray(body, "input");
  return {
    body_bytes: Buffer.byteLength(body),
    body_sha256: sha256(body),
    input_bytes: Buffer.byteLength(input.value),
    input_sha256: sha256(input.value),
    static_before_input_sha256: sha256(input.before),
    static_after_input_sha256: sha256(input.after),
    // The fixture is synthetic and this field never reaches output. Keeping it
    // only in-memory lets the regression assert byte-level append-only input.
    input_inner: input.value.slice(1, -1)
  };
}

function rawTopLevelArray(body, field) {
  const marker = `"${field}":`;
  const markerIndex = body.indexOf(marker);
  if (markerIndex < 0) throw new Error(`mock request has no top-level ${field} field`);
  let start = markerIndex + marker.length;
  while (/\s/u.test(body[start] ?? "")) start += 1;
  if (body[start] !== "[") throw new Error(`mock request ${field} field is not an array`);
  let depth = 0;
  let inString = false;
  let escaped = false;
  for (let index = start; index < body.length; index += 1) {
    const char = body[index];
    if (inString) {
      if (escaped) {
        escaped = false;
      } else if (char === "\\") {
        escaped = true;
      } else if (char === '"') {
        inString = false;
      }
      continue;
    }
    if (char === '"') {
      inString = true;
    } else if (char === "[") {
      depth += 1;
    } else if (char === "]") {
      depth -= 1;
      if (depth === 0) {
        const end = index + 1;
        return {
          before: body.slice(0, start),
          value: body.slice(start, end),
          after: body.slice(end)
        };
      }
    }
  }
  throw new Error(`mock request ${field} array is unterminated`);
}

function sha256(value) {
  return createHash("sha256").update(value).digest("hex");
}

async function getJson(url, timeoutMs = 5_000, localKey = "") {
  const response = await fetch(url, {
    headers: localKey ? { authorization: `Bearer ${localKey}` } : undefined,
    signal: AbortSignal.timeout(timeoutMs)
  });
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

async function assertCandidateExecutableIsFresh(executable, root) {
  if (await executableIsFresh(executable, root)) return;
  throw new Error(
    `candidate executable is stale for this source tree: ${executable}; build it before running wire compatibility so a stale artifact cannot produce a misleading result`
  );
}

async function executableIsFresh(executable, root) {
  if (!existsSync(executable)) return false;
  const [binary, newestSource] = await Promise.all([
    stat(executable),
    newestRelevantRustSourceMtime(root)
  ]);
  return binary.mtimeMs >= newestSource;
}

async function newestRelevantRustSourceMtime(root) {
  const sources = [
    join(root, "src-tauri", "src"),
    join(root, "src-tauri", "Cargo.toml"),
    join(root, "src-tauri", "Cargo.lock")
  ];
  let newest = 0;
  for (const source of sources) {
    if (!existsSync(source)) continue;
    const info = await stat(source);
    newest = Math.max(
      newest,
      info.isDirectory() ? await newestRustSourceMtime(source) : info.mtimeMs
    );
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
