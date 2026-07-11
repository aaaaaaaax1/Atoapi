import fs from "node:fs";

const MIN_RAW_TOKEN_HIT_RATE = 0.995;

const currentFile = process.argv[2];
const baselineFile = process.argv[3];
if (!currentFile || !baselineFile) {
  console.error(
    "Usage: node scripts/verify-real-metrics.mjs <current-metrics.json> <baseline-metrics.json>"
  );
  process.exit(2);
}

const current = readJson(currentFile);
const baseline = readJson(baselineFile);
const delta = {
  totalRequests: number(current.total_requests) - number(baseline.total_requests),
  upstreamRequests: number(current.upstream_requests) - number(baseline.upstream_requests),
  retries: number(current.retries) - number(baseline.retries),
  inputTokens: number(current.usage?.input_tokens) - number(baseline.usage?.input_tokens),
  cacheReadTokens:
    number(current.usage?.cache_read_tokens) - number(baseline.usage?.cache_read_tokens),
  localCacheHits:
    number(current.response_cache_hits) +
    number(current.semantic_cache_hits) -
    number(baseline.response_cache_hits) -
    number(baseline.semantic_cache_hits),
  backgroundPrewarmAttempts:
    sum(current.background_prewarm, "attempts") - sum(baseline.background_prewarm, "attempts"),
  avoidableGapTokens:
    sum(current.gap_buckets, "avoidable_gap_tokens") -
    sum(baseline.gap_buckets, "avoidable_gap_tokens"),
  providerUnstableGapTokens:
    sum(current.gap_buckets, "provider_unstable_gap_tokens") -
    sum(baseline.gap_buckets, "provider_unstable_gap_tokens")
};

const expectedUpstreamRequests = Math.max(0, delta.totalRequests - delta.localCacheHits);
const rawTokenHitRate = ratio(delta.cacheReadTokens, delta.inputTokens);
const recentUpstreamCalls = Array.isArray(current.recent_upstream_calls)
  ? current.recent_upstream_calls
  : [];
const tokenBearingCalls = recentUpstreamCalls.filter((item) => number(item.input_tokens) > 0);
const tokenBearingKinds = countBy(tokenBearingCalls, (item) => item.upstream_call_kind ?? "unknown");
const tokenBearingSources = countBy(
  tokenBearingCalls,
  (item) => item.upstream_call_source ?? "unknown"
);

const checks = {
  hasRealUsageDelta: delta.inputTokens > 0,
  rawTokenHitRateAtLeast995: rawTokenHitRate >= MIN_RAW_TOKEN_HIT_RATE,
  avoidableGapIsZero: delta.avoidableGapTokens === 0,
  noExtraUpstreamRequests: delta.upstreamRequests === expectedUpstreamRequests,
  noRetries: delta.retries === 0,
  noBackgroundPrewarm: delta.backgroundPrewarmAttempts === 0,
  recentTokenRowsPresent: tokenBearingCalls.length > 0
};
const pass = Object.values(checks).every(Boolean);

console.log(
  JSON.stringify(
    {
      files: { current: currentFile, baseline: baselineFile },
      target: {
        rawTokenHitRate: MIN_RAW_TOKEN_HIT_RATE,
        avoidableGapTokens: 0,
        extraUpstreamRequests: 0,
        retries: 0,
        backgroundPrewarmAttempts: 0
      },
      delta,
      observed: {
        rawTokenHitRate,
        expectedUpstreamRequests,
        extraUpstreamRequests: delta.upstreamRequests - expectedUpstreamRequests,
        providerUnstableGapTokens: delta.providerUnstableGapTokens,
        tokenBearingCallsInRecentWindow: tokenBearingCalls.length,
        tokenBearingKinds,
        tokenBearingSources
      },
      checks,
      pass,
      note:
        "Cumulative token counters include every recorded stream, sync, compact, compression, and cold-start usage. Provider waterline rollback is reported separately from real new tail and avoidable gap. recent_upstream_calls is used only as a coverage audit because it is a bounded window."
    },
    null,
    2
  )
);

if (!pass) {
  process.exit(1);
}

function readJson(file) {
  return JSON.parse(fs.readFileSync(file, "utf8").replace(/^\uFEFF/u, ""));
}

function number(value) {
  const parsed = Number(value ?? 0);
  return Number.isFinite(parsed) ? parsed : 0;
}

function sum(items, key) {
  if (!Array.isArray(items)) return 0;
  return items.reduce((total, item) => total + number(item?.[key]), 0);
}

function ratio(numerator, denominator) {
  return denominator > 0 ? numerator / denominator : 0;
}

function countBy(items, selector) {
  const counts = {};
  for (const item of items) {
    const key = String(selector(item));
    counts[key] = (counts[key] ?? 0) + 1;
  }
  return counts;
}
