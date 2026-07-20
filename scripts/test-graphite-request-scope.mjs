import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import { transform } from "esbuild";

const sourceUrl = new URL("../src/graphite/requestScope.ts", import.meta.url);
const source = await readFile(sourceUrl, "utf8");
const compiled = await transform(source, { loader: "ts", format: "esm" });
const moduleUrl = `data:text/javascript;base64,${Buffer.from(compiled.code).toString("base64")}`;
const {
  recordsForAgent,
  scopesForSuccessfulAgentRequests,
  trafficForAgentScope,
  limitVisibleRequestRecords
} = await import(moduleUrl);

const records = [
  { id: "codex-a", agent_id: "codex", provider_id: "bizd", provider: "bizd" },
  { id: "codex-b", agent_id: "codex", provider_id: "bizd", provider: "bizd" },
  { id: "claude-a", agent_id: "claude", provider_id: "shared", provider: "shared" },
  { id: "legacy", agent_id: null, provider_id: "legacy", provider: "legacy" }
];

const codexRecords = recordsForAgent(records, "codex");
assert.deepEqual(codexRecords.map((record) => record.id), ["codex-a", "codex-b"]);
assert.deepEqual(scopesForSuccessfulAgentRequests(codexRecords), [
  { id: "all", label: "全部", providerId: null },
  { id: "provider:bizd", label: "bizd", providerId: "bizd" }
]);
assert.deepEqual(recordsForAgent(records, "gemini"), []);
assert.deepEqual(scopesForSuccessfulAgentRequests([]), [
  { id: "all", label: "全部", providerId: null }
]);

const lifetime = [
  {
    agent_id: "codex",
    provider_id: "bizd",
    total_requests: 203,
    successful_requests: 201,
    error_statuses: 2,
    input_tokens: 1_000,
    output_tokens: 100,
    cache_read_tokens: 990,
    cache_shortfall_tokens: 10,
    cache_avoidable_gap_tokens: 2,
    cache_new_tail_gap_tokens: 8,
    cold_start_requests: 1,
    cold_start_input_tokens: 20,
    cold_start_output_tokens: 2,
    cold_start_cache_read_tokens: 0,
    cold_start_cache_shortfall_tokens: 10,
    cold_start_cache_avoidable_gap_tokens: 2,
    cold_start_cache_new_tail_gap_tokens: 8
  },
  {
    agent_id: "claude",
    provider_id: "bizd",
    total_requests: 10,
    successful_requests: 9,
    error_statuses: 1,
    input_tokens: 500,
    output_tokens: 50,
    cache_read_tokens: 480
  }
];
assert.deepEqual(trafficForAgentScope(lifetime, "codex", "bizd"), {
  totalRequests: 203,
  successfulRequests: 201,
  errors: 2,
  inputTokens: 1_000,
  outputTokens: 100,
  cachedTokens: 990,
  cacheShortfallTokens: 10,
  cacheAvoidableGapTokens: 2,
  cacheNewTailGapTokens: 8,
  coldStartRequests: 1,
  coldStartInputTokens: 20,
  coldStartOutputTokens: 2,
  coldStartCachedTokens: 0,
  coldStartCacheShortfallTokens: 10,
  coldStartCacheAvoidableGapTokens: 2,
  coldStartCacheNewTailGapTokens: 8
});
assert.equal(
  trafficForAgentScope(undefined, "codex", "bizd"),
  null,
  "missing aggregate support must remain distinguishable from a real zero"
);
assert.equal(
  trafficForAgentScope(lifetime, "codex", "missing"),
  null,
  "an unmatched provider must not masquerade as a real all-zero aggregate"
);
assert.equal(
  trafficForAgentScope(lifetime, "gemini", "bizd"),
  null,
  "an unmatched Agent must not borrow another Agent's traffic"
);
assert.deepEqual(
  limitVisibleRequestRecords(Array.from({ length: 201 }, (_, index) => index), 200),
  Array.from({ length: 200 }, (_, index) => index),
  "the 200-entry cap is display-only and must not change lifetime aggregates"
);

console.log("graphite request scope regression tests passed");
