import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import { pathToFileURL } from "node:url";
import { transform } from "esbuild";

const sourceUrl = new URL("../src/lib/request-record-state.ts", import.meta.url);
const source = await readFile(sourceUrl, "utf8");
const compiled = await transform(source, { loader: "ts", format: "esm" });
const moduleUrl = `data:text/javascript;base64,${Buffer.from(compiled.code).toString("base64")}`;
const {
  requestAffinityDisplay,
  requestRecordIsBackendColdStart,
  requestRecordState,
  requestRecordStatusDisplay,
  requestTransportDisplay
} = await import(moduleUrl);

const state = (overrides = {}) => requestRecordState({
  status: 200,
  cacheStatus: "bypass",
  upstreamCallSource: "main",
  downstreamDisconnected: false,
  downstreamDisconnectStage: null,
  shadowAffinityLane: "steady",
  prefixLagClassification: "none",
  inputTokens: 30_000,
  cacheReadTokens: 29_000,
  ...overrides
});

const status = (overrides = {}) => requestRecordStatusDisplay({
  status: 200,
  cacheStatus: "bypass",
  upstreamCallSource: "main",
  downstreamDisconnected: false,
  downstreamDisconnectStage: null,
  shadowAffinityLane: "steady",
  prefixLagClassification: "none",
  inputTokens: 30_000,
  cacheReadTokens: 29_000,
  ...overrides
});

assert.deepEqual(
  state({ cacheStatus: "compact", upstreamCallSource: "responses-compaction-v2" }),
  { label: "实际压缩", tone: "compact" }
);
assert.deepEqual(
  state({ cacheStatus: "compact", upstreamCallSource: "responses-sync-main" }),
  { label: "完成", tone: "complete" },
  "ordinary synchronous Responses traffic must not be shown as real compaction"
);
assert.deepEqual(
  state({
    shadowAffinityLane: "compacted_anchor",
    prefixLagClassification: "first_prefix_state",
    inputTokens: 28_974,
    cacheReadTokens: 11_008
  }),
  { label: "压缩后冷启动", tone: "cold" }
);
assert.deepEqual(
  state({
    prefixLagClassification: "first_prefix_state",
    inputTokens: 323_946,
    cacheReadTokens: 323_328
  }),
  { label: "直通", tone: "bypass" },
  "a first local observation with a hot provider prefix is not a cold start"
);
assert.deepEqual(
  state({ inputTokens: 8_192, cacheReadTokens: 0 }),
  { label: "冷启动", tone: "cold" }
);
assert.deepEqual(
  state({ inputTokens: 128, cacheReadTokens: 0, coldStart: true }),
  { label: "冷启动", tone: "cold" },
  "an explicit backend cold-start marker must win over the legacy size heuristic"
);
assert.deepEqual(
  state({ inputTokens: 8_192, cacheReadTokens: 0, coldStart: false }),
  { label: "直通", tone: "bypass" },
  "a repeated cold read in the same session is not shown as another cold start"
);
assert.deepEqual(
  state({
    cacheStatus: "compact",
    upstreamCallSource: "responses-compaction-v2",
    downstreamDisconnected: true
  }),
  { label: "实际压缩", tone: "compact" },
  "a confirmed compaction must remain the primary state when Codex drops the downstream body"
);
assert.deepEqual(
  state({ status: 502, cacheStatus: "error" }),
  { label: "error 502", tone: "error" }
);
assert.deepEqual(
  state({ downstreamDisconnected: true, downstreamDisconnectStage: "after_terminal" }),
  { label: "直通", tone: "bypass" },
  "a downstream close after the terminal event is an expected stream teardown"
);
assert.deepEqual(
  state({ downstreamDisconnected: true, downstreamDisconnectStage: "before_terminal" }),
  { label: "下游已断开", tone: "disconnect" },
  "a downstream close before the terminal event remains a primary warning"
);
assert.deepEqual(
  state({ cacheStatus: "exact", inputTokens: 8_192, cacheReadTokens: 7_936, coldStart: false }),
  { label: "命中", tone: "hit" }
);
assert.deepEqual(
  state({ cacheStatus: "miss", inputTokens: 8_192, cacheReadTokens: 512, coldStart: false }),
  { label: "未命中", tone: "complete" }
);
assert.deepEqual(
  state({ cacheStatus: "other", inputTokens: 8_192, cacheReadTokens: 512, coldStart: false }),
  { label: "完成", tone: "complete" }
);
assert.deepEqual(
  state({ status: 502, cacheStatus: "compact", upstreamCallSource: "compact", coldStart: true }),
  { label: "error 502", tone: "error" },
  "an upstream error must outrank compaction and cold-start display"
);
assert.equal(requestRecordIsBackendColdStart({ coldStart: true }), true);
assert.equal(requestRecordIsBackendColdStart({ coldStart: false }), false);
assert.equal(requestRecordIsBackendColdStart({ coldStart: null }), false);
assert.equal(requestRecordIsBackendColdStart({}), false);
assert.deepEqual(
  requestTransportDisplay({ upstreamCallKind: "stream", cacheStatus: "bypass" }),
  { label: "流式", tone: "stream" },
  "cache bypass must not replace the normal stream mode"
);
assert.deepEqual(
  requestTransportDisplay({ upstreamCallKind: "sync", cacheStatus: "bypass" }),
  { label: "同步", tone: "sync" }
);
assert.deepEqual(
  requestTransportDisplay({ upstreamCallKind: "cache", cacheStatus: "exact" }),
  { label: "缓存", tone: "cache" }
);
assert.deepEqual(
  status(),
  { label: "OK", detail: null, tone: "complete" },
  "a normal completed request should stay concise"
);
assert.deepEqual(
  status({ cacheStatus: "compact", upstreamCallSource: "responses-compaction-v2" }),
  { label: "OK", detail: "实际压缩", tone: "compact" }
);
assert.deepEqual(
  status({ inputTokens: 8_192, cacheReadTokens: 0, coldStart: true }),
  { label: "OK", detail: "冷启动", tone: "cold" }
);
assert.deepEqual(
  status({ status: 502, cacheStatus: "error" }),
  { label: "Error 502", detail: null, tone: "error" }
);
assert.deepEqual(
  status({ downstreamDisconnected: true, downstreamDisconnectStage: "before_terminal" }),
  { label: "下游已断开", detail: null, tone: "disconnect" }
);
assert.deepEqual(
  requestAffinityDisplay({ arm: "baseline", decision: "assigned" }),
  { primaryLabel: null, detailLabel: "baseline shadow", applied: false }
);
assert.deepEqual(
  requestAffinityDisplay({
    arm: "candidate",
    decision: "candidate_skipped_explicit_cache_key"
  }),
  { primaryLabel: null, detailLabel: "candidate shadow 未应用", applied: false }
);
assert.deepEqual(
  requestAffinityDisplay({ arm: "candidate", decision: "candidate_applied" }),
  { primaryLabel: "candidate", detailLabel: "candidate 已应用", applied: true }
);

console.log("request record state regression tests passed");
