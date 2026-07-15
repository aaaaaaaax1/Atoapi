import { randomUUID } from "node:crypto";

const args = parseArgs(process.argv.slice(2));
const baseUrl = String(args.url ?? process.env.ATOAPI_BASE_URL ?? "http://127.0.0.1:18883")
  .replace(/\/+$/u, "");
const localKey = String(args.key ?? process.env.ATOAPI_LOCAL_KEY ?? "").trim();
const model = String(args.model ?? process.env.ATOAPI_TEST_MODEL ?? "gpt-5.6-luna").trim();
const mode = String(args.mode ?? "observe").trim().toLowerCase();
const targetPerArm = boundedNumber(args.target ?? 3, 1, 8);
const maxProbes = boundedNumber(args["max-probes"] ?? 80, targetPerArm * 2, 120);
const maxProbeFailures = boundedNumber(args["max-probe-failures"] ?? 5, 0, 20);
const toolChars = boundedNumber(args["tool-chars"] ?? 90_000, 80_000, 120_000);
const maxInputTokens = boundedNumber(args["max-input-tokens"] ?? 1_000_000, 100_000, 5_000_000);

if (!localKey) {
  console.error("Set ATOAPI_LOCAL_KEY or pass --key. The key is never written to output.");
  process.exit(2);
}
if (!model) {
  console.error("Set ATOAPI_TEST_MODEL or pass --model.");
  process.exit(2);
}
if (!new Set(["observe", "canary"]).has(mode)) {
  console.error("--mode must be observe or canary.");
  process.exit(2);
}

const beforeMetrics = await getJson(`${baseUrl}/admin/metrics`);
const beforeAffinity = await getJson(`${baseUrl}/admin/cache-affinity`);
const selected = { baseline: 0, candidate: 0 };
const selectedSessions = { baseline: [], candidate: [] };
const probes = [];
const responses = [];
let requestCount = 0;
let probeFailures = 0;

for (let probeIndex = 0; probeIndex < maxProbes; probeIndex += 1) {
  if (selected.baseline >= targetPerArm && selected.candidate >= targetPerArm) break;
  await enforceTokenBudget();

  const runId = randomUUID();
  const session = {
    sessionId: `atoapi-accept-${runId}`,
    threadId: `atoapi-accept-${runId}`,
    input: [message(`Cohort probe ${runId}: reply with OK only.`)]
  };
  const seed = await sendRequest(session, "seed", true);
  if (!seed.completed) {
    probeFailures += 1;
    probes.push({
      index: probeIndex + 1,
      arm: null,
      status: seed.status,
      selected: false,
      error: seed.error ?? "probe_failed"
    });
    if (probeFailures > maxProbeFailures) {
      throw new Error(`probe failures exceeded hard limit: ${probeFailures}/${maxProbeFailures}`);
    }
    continue;
  }
  const latestMetrics = await getJson(`${baseUrl}/admin/metrics`);
  const latest = latestMetrics.recent_requests?.[0] ?? {};
  const arm = String(latest.shadow_affinity_arm ?? "");
  if (!new Set(["baseline", "candidate"]).has(arm)) {
    throw new Error(`probe ${probeIndex + 1} did not expose a valid shadow arm`);
  }
  probes.push({ index: probeIndex + 1, arm, status: seed.status, selected: false });
  if (selected[arm] >= targetPerArm) continue;

  probes.at(-1).selected = true;
  selected[arm] += 1;
  selectedSessions[arm].push({ ...session, runId, arm });
}

if (selected.baseline < targetPerArm || selected.candidate < targetPerArm) {
  throw new Error(
    `could not find both cohorts within ${maxProbes} probes: baseline=${selected.baseline} candidate=${selected.candidate}`
  );
}

for (let index = 0; index < targetPerArm; index += 1) {
  const pair =
    mode === "canary"
      ? [selectedSessions.candidate[index], selectedSessions.baseline[index]]
      : index % 2 === 0
      ? [selectedSessions.baseline[index], selectedSessions.candidate[index]]
      : [selectedSessions.candidate[index], selectedSessions.baseline[index]];
  for (const session of pair) {
    await runPostBurstWindow(session);
  }
}

const afterMetrics = await getJson(`${baseUrl}/admin/metrics`);
const afterAffinity = await getJson(`${baseUrl}/admin/cache-affinity`);
const expectedEvidence = targetPerArm * 2 * 3;
const delta = {
  inputTokens: number(afterMetrics.usage?.input_tokens) - number(beforeMetrics.usage?.input_tokens),
  inboundRequests:
    number(afterMetrics.agent_generation?.inbound_requests) -
    number(beforeMetrics.agent_generation?.inbound_requests),
  generationAttempts:
    number(afterMetrics.agent_generation?.generation_attempts) -
    number(beforeMetrics.agent_generation?.generation_attempts),
  upstreamRequests:
    number(afterMetrics.upstream_requests) - number(beforeMetrics.upstream_requests),
  automaticApplications:
    number(afterMetrics.shadow_affinity?.applied_decisions) -
    number(beforeMetrics.shadow_affinity?.applied_decisions),
  evidenceRecords:
    number(afterAffinity.evidence_records) - number(beforeAffinity.evidence_records)
};
const postBurstReadiness = (afterAffinity.readiness ?? []).filter(
  (item) => item.lane === "tool_burst_quarantine"
);
const readinessStatuses = new Set(postBurstReadiness.map((item) => item.status));
const promotionEligible = readinessStatuses.has("ready_for_promotion");
const checks = {
  foundBothArms: selected.baseline === targetPerArm && selected.candidate === targetPerArm,
  selectedWindowsCompleted:
    responses.length === requestCount &&
    responses.filter((item) => item.phase !== "seed").every((item) => item.completed),
  probeFailuresWithinBudget: probeFailures <= maxProbeFailures,
  oneInboundPerRequest: delta.inboundRequests === requestCount,
  oneAttemptPerInbound: delta.generationAttempts === requestCount,
  oneUpstreamPerInbound: delta.upstreamRequests === requestCount,
  capturedEverySelectedFollowup: delta.evidenceRecords === expectedEvidence,
  stayedWithinTokenBudget: delta.inputTokens <= maxInputTokens,
  modeResult:
    mode === "observe"
      ? delta.automaticApplications === 0 &&
        (readinessStatuses.has("ready_for_canary") || readinessStatuses.has("canary_healthy"))
      : delta.automaticApplications > 0 &&
        (readinessStatuses.has("canary_healthy") || promotionEligible)
};
const pass = Object.values(checks).every(Boolean);

console.log(
  JSON.stringify(
    {
      baseUrl,
      model,
      mode,
      limits: { targetPerArm, maxProbes, maxProbeFailures, toolChars, maxInputTokens },
      selected,
      probeFailures,
      requestCount,
      probes,
      responses,
      delta,
      affinity: {
        openWindows: number(afterAffinity.open_windows),
        evidenceRecords: number(afterAffinity.evidence_records),
        readiness: postBurstReadiness,
        rollbackKeys: afterAffinity.rollback_keys ?? []
      },
      promotionEligible,
      checks,
      pass
    },
    null,
    2
  )
);

if (!pass) process.exit(1);

async function sendRequest(session, phase, allowFailure = false) {
  await enforceTokenBudget();
  const startedAt = Date.now();
  requestCount += 1;
  let recordedResult = null;
  try {
    const response = await fetch(`${baseUrl}/codex/v1/responses`, {
      method: "POST",
      headers: {
        authorization: `Bearer ${localKey}`,
        "content-type": "application/json",
        accept: "text/event-stream",
        "x-codex-turn-metadata": JSON.stringify({
          session_id: session.sessionId,
          thread_id: session.threadId,
          request_kind: "turn"
        })
      },
      body: JSON.stringify({
        model,
        stream: true,
        max_output_tokens: 16,
        instructions: "Reply with OK only.",
        input: session.input
      }),
      signal: AbortSignal.timeout(180_000)
    });
    const body = await response.text();
    const result = {
      phase,
      status: response.status,
      elapsedMs: Date.now() - startedAt,
      completed: response.ok &&
        (body.includes("response.completed") || body.includes("[DONE]")),
      error: response.ok ? null : body.slice(0, 240)
    };
    recordedResult = result;
    responses.push(result);
    if (!result.completed && !allowFailure) {
      throw new Error(
        `${phase} failed: HTTP ${response.status}; terminal=${result.completed}; body=${body.slice(0, 240)}`
      );
    }
    return result;
  } catch (error) {
    if (recordedResult) {
      if (!allowFailure) throw error;
      return recordedResult;
    }
    const result = {
      phase,
      status: 0,
      elapsedMs: Date.now() - startedAt,
      completed: false,
      error: error instanceof Error ? error.message : String(error)
    };
    responses.push(result);
    if (!allowFailure) throw error;
    return result;
  }
}

async function runPostBurstWindow(session) {
  const toolCallId = `call_${session.runId.replaceAll("-", "")}`;
  session.input.push(
    { type: "function_call", call_id: toolCallId, name: "read_test_log", arguments: "{}" },
    { type: "function_call_output", call_id: toolCallId, output: buildToolOutput(toolChars) },
    message("Use the completed tool result and reply with OK only.")
  );
  await sendRequest(session, `${session.arm}_giant_tail`);
  for (let followup = 1; followup <= 3; followup += 1) {
    session.input.push(message(`Follow-up ${followup}: reply with OK only.`));
    await sendRequest(session, `${session.arm}_followup_${followup}`);
  }
}

async function enforceTokenBudget() {
  const current = await getJson(`${baseUrl}/admin/metrics`);
  const used = number(current.usage?.input_tokens) - number(beforeMetrics.usage?.input_tokens);
  if (used >= maxInputTokens) {
    throw new Error(`live acceptance reached its ${maxInputTokens} input-token hard limit`);
  }
}

function message(text) {
  return {
    type: "message",
    role: "user",
    content: [{ type: "input_text", text }]
  };
}

function parseArgs(items) {
  const parsed = {};
  for (let index = 0; index < items.length; index += 1) {
    const item = items[index];
    if (!item.startsWith("--")) continue;
    const key = item.slice(2);
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

function boundedNumber(value, minimum, maximum) {
  const parsed = Number(value);
  if (!Number.isFinite(parsed)) return minimum;
  return Math.max(minimum, Math.min(maximum, Math.trunc(parsed)));
}

function buildToolOutput(targetChars) {
  const line = "2026-07-15T00:00:00Z G:\\Atoapi\\src\\proxy\\cache_affinity.rs cache evidence line\n";
  return line.repeat(Math.ceil(targetChars / line.length)).slice(0, targetChars);
}

async function getJson(url) {
  const response = await fetch(url, { signal: AbortSignal.timeout(15_000) });
  if (!response.ok) throw new Error(`GET ${url} failed: HTTP ${response.status}`);
  return response.json();
}

function number(value) {
  const parsed = Number(value ?? 0);
  return Number.isFinite(parsed) ? parsed : 0;
}
