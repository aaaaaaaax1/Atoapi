import fs from "node:fs";
import path from "node:path";
import { setTimeout as sleep } from "node:timers/promises";

const args = parseArgs(process.argv.slice(2));
const baseUrl = String(args.url ?? "http://127.0.0.1:3456").replace(/\/+$/u, "");
const intervalMs = Number(args.interval ?? 10_000);
const targetInputTokens = Number(args.tokens ?? 50_000_000);
const maxMinutes = Number(args.minutes ?? 120);
const label = String(args.label ?? timestampLabel());
const outDir = path.resolve("logs");
fs.mkdirSync(outDir, { recursive: true });

const jsonlFile = path.join(outDir, `real-soak-${label}.jsonl`);
const summaryFile = path.join(outDir, `real-soak-${label}-summary.json`);

const startedAt = Date.now();
let first = null;
let latest = null;
let samples = 0;

console.log(
  `Polling ${baseUrl}/admin/metrics until +${targetInputTokens} input tokens or ${maxMinutes} minutes.`
);
console.log(`Writing ${jsonlFile}`);

while (true) {
  const snapshot = await fetchMetrics(baseUrl);
  const compact = compactSnapshot(snapshot);
  samples += 1;
  if (!first) first = compact;
  latest = compact;
  fs.appendFileSync(jsonlFile, `${JSON.stringify(compact)}\n`);

  const deltaInput = compact.usage.inputTokens - first.usage.inputTokens;
  const elapsedMs = Date.now() - startedAt;
  const summary = buildSummary(first, compact, samples, elapsedMs);
  fs.writeFileSync(summaryFile, JSON.stringify(summary, null, 2));

  const hitPct = (summary.delta.cacheTokenRatio * 100).toFixed(2);
  const recentPct = (summary.latest.recentCacheTokenRatio * 100).toFixed(2);
  process.stdout.write(
    `\r${new Date().toLocaleTimeString()} samples=${samples} deltaInput=${deltaInput} hit=${hitPct}% recent=${recentPct}% cold=${summary.delta.diagnostics["provider-cold-start"] ?? 0} break=${summary.delta.diagnostics["provider-prefix-break"] ?? 0}   `
  );

  if (deltaInput >= targetInputTokens || elapsedMs >= maxMinutes * 60_000) {
    process.stdout.write("\n");
    console.log(`Summary ${summaryFile}`);
    break;
  }

  await sleep(intervalMs);
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

async function fetchMetrics(url) {
  const response = await fetch(`${url}/admin/metrics`);
  if (!response.ok) {
    throw new Error(`GET /admin/metrics failed: HTTP ${response.status}`);
  }
  return response.json();
}

function compactSnapshot(snapshot) {
  const requests = Array.isArray(snapshot.recent_requests) ? snapshot.recent_requests : [];
  return {
    at: new Date().toISOString(),
    counters: {
      totalRequests: Number(snapshot.total_requests ?? 0),
      upstreamRequests: Number(snapshot.upstream_requests ?? 0),
      responseCacheHits: Number(snapshot.response_cache_hits ?? 0),
      semanticCacheHits: Number(snapshot.semantic_cache_hits ?? 0),
      cacheMisses: Number(snapshot.cache_misses ?? 0),
      errors: Number(snapshot.errors ?? 0),
      retries: Number(snapshot.retries ?? 0)
    },
    provider: {
      inputTokens: Number(snapshot.provider_input_tokens ?? 0),
      cachedTokens: Number(snapshot.provider_cached_tokens ?? 0),
      cacheTokenRatio: Number(snapshot.provider_cache_token_ratio ?? 0),
      cacheHitRequests: Number(snapshot.provider_cache_hit_requests ?? 0)
    },
    usage: {
      inputTokens: Number(snapshot.usage?.input_tokens ?? 0),
      outputTokens: Number(snapshot.usage?.output_tokens ?? 0),
      cacheReadTokens: Number(snapshot.usage?.cache_read_tokens ?? 0),
      cacheCreationTokens: Number(snapshot.usage?.cache_creation_tokens ?? 0),
      totalTokens: Number(snapshot.usage?.total_tokens ?? 0)
    },
    recent: {
      requests: Number(snapshot.recent_usage?.requests ?? 0),
      inputTokens: Number(snapshot.recent_usage?.input_tokens ?? 0),
      cacheReadTokens: Number(snapshot.recent_usage?.cache_read_tokens ?? 0),
      cacheTokenRatio: Number(snapshot.recent_usage?.cache_token_ratio ?? 0)
    },
    diagnostics: countBy(requests, "provider_cache_diagnostic"),
    prefixKeys: countBy(requests, "provider_prefix_key"),
    prefixFingerprints: countBy(requests, "provider_prefix_fingerprint"),
    recentRequests: requests.map((request) => ({
      at: request.at,
      provider: request.provider,
      model: request.model,
      cacheStatus: request.cache_status,
      inputTokens: Number(request.input_tokens ?? 0),
      cacheReadTokens: Number(request.cache_read_tokens ?? 0),
      shortfallTokens: Number(request.cache_shortfall_tokens ?? 0),
      diagnostic: request.provider_cache_diagnostic ?? null,
      providerPrefixKey: request.provider_prefix_key ?? null,
      providerPrefixFingerprint: request.provider_prefix_fingerprint ?? null
    }))
  };
}

function buildSummary(first, latest, samples, elapsedMs) {
  const deltaInput = latest.usage.inputTokens - first.usage.inputTokens;
  const deltaCached = latest.usage.cacheReadTokens - first.usage.cacheReadTokens;
  const deltaCounters = subtractObject(latest.counters, first.counters);
  return {
    startedAt: first.at,
    endedAt: latest.at,
    elapsedSeconds: Math.round(elapsedMs / 1000),
    samples,
    delta: {
      ...deltaCounters,
      inputTokens: deltaInput,
      cacheReadTokens: deltaCached,
      cacheTokenRatio: ratio(deltaCached, deltaInput),
      diagnostics: latest.diagnostics,
      prefixKeyCount: Object.keys(latest.prefixKeys).filter((key) => key !== "missing").length,
      prefixFingerprintCount: Object.keys(latest.prefixFingerprints).filter((key) => key !== "missing").length
    },
    latest: {
      cumulativeInputTokens: latest.usage.inputTokens,
      cumulativeCacheReadTokens: latest.usage.cacheReadTokens,
      cumulativeCacheTokenRatio: latest.provider.cacheTokenRatio,
      recentInputTokens: latest.recent.inputTokens,
      recentCacheReadTokens: latest.recent.cacheReadTokens,
      recentCacheTokenRatio: latest.recent.cacheTokenRatio
    }
  };
}

function subtractObject(right, left) {
  const result = {};
  for (const key of new Set([...Object.keys(right), ...Object.keys(left)])) {
    result[key] = Number(right[key] ?? 0) - Number(left[key] ?? 0);
  }
  return result;
}

function countBy(items, key) {
  const counts = {};
  for (const item of items) {
    const value = item[key] ?? "missing";
    counts[value] = (counts[value] ?? 0) + 1;
  }
  return counts;
}

function ratio(numerator, denominator) {
  return denominator === 0 ? 0 : numerator / denominator;
}

function timestampLabel() {
  const now = new Date();
  const pad = (value) => String(value).padStart(2, "0");
  return `${now.getFullYear()}${pad(now.getMonth() + 1)}${pad(now.getDate())}-${pad(now.getHours())}${pad(now.getMinutes())}${pad(now.getSeconds())}`;
}
