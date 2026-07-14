import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import { pathToFileURL } from "node:url";
import { transform } from "esbuild";

const sourceUrl = new URL("../src/lib/request-record-state.ts", import.meta.url);
const source = await readFile(sourceUrl, "utf8");
const compiled = await transform(source, { loader: "ts", format: "esm" });
const moduleUrl = `data:text/javascript;base64,${Buffer.from(compiled.code).toString("base64")}`;
const { requestRecordState } = await import(moduleUrl);

const state = (overrides = {}) => requestRecordState({
  status: 200,
  cacheStatus: "bypass",
  upstreamCallSource: "main",
  downstreamDisconnected: false,
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
assert.equal(
  state({ cacheStatus: "compact", upstreamCallSource: "responses-sync-main" }),
  null,
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
assert.equal(
  state({
    prefixLagClassification: "first_prefix_state",
    inputTokens: 323_946,
    cacheReadTokens: 323_328
  }),
  null,
  "a first local observation with a hot provider prefix is not a cold start"
);
assert.deepEqual(
  state({ inputTokens: 8_192, cacheReadTokens: 0 }),
  { label: "冷启动", tone: "cold" }
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

console.log("request record state regression tests passed");
