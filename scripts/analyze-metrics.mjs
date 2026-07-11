import fs from "node:fs";

const TARGET_CUMULATIVE_RAW_TOKEN_HIT_RATE = 0.995;

const file = process.argv[2];
if (!file) {
  console.error("Usage: node scripts/analyze-metrics.mjs <metrics.json> [baseline.json]");
  process.exit(2);
}

const metrics = JSON.parse(fs.readFileSync(file, "utf8").replace(/^\uFEFF/u, ""));
const baselineFile = process.argv[3];
const baselineMetrics = baselineFile
  ? JSON.parse(fs.readFileSync(baselineFile, "utf8").replace(/^\uFEFF/u, ""))
  : null;
const requests = Array.isArray(metrics.recent_requests) ? metrics.recent_requests : [];
const rows = requests
  .filter((item) => Number(item.input_tokens ?? 0) > 0)
  .map((item) => {
    const input = Number(item.input_tokens ?? 0);
    const cached = Number(item.cache_read_tokens ?? 0);
    const bucketMax = Math.floor(input / 512) * 512;
    const bucketGap = Math.max(0, bucketMax - cached);
    return {
      at: item.at,
      provider: item.provider,
      model: item.model,
      status: item.cache_status,
      providerPrefixKey: item.provider_prefix_key ?? null,
      providerPrefixFingerprint: item.provider_prefix_fingerprint ?? null,
      diagnostic: item.provider_cache_diagnostic ?? inferDiagnostic(input, cached, bucketGap),
      input,
      cached,
      bucketMax,
      bucketGap,
      blocksShort: Math.floor(bucketGap / 512),
      rawPct: ratio(cached, input),
      effectivePct: ratio(cached, bucketMax),
      loggedNewTailShortfallTokens:
        item.cache_new_tail_gap_tokens == null ? null : Number(item.cache_new_tail_gap_tokens),
      loggedAvoidableShortfallTokens:
        item.cache_avoidable_gap_tokens == null ? null : Number(item.cache_avoidable_gap_tokens),
      loggedProviderUnstableShortfallTokens:
        item.cache_provider_unstable_gap_tokens == null
          ? null
          : Number(item.cache_provider_unstable_gap_tokens)
    };
  });
annotateGapCausality(rows);

const warm = rows.filter((item) => item.cached > 0);
const cold = rows.filter((item) => item.cached === 0);
const fullBucket = warm.filter((item) => item.bucketGap === 0);
const oneBlock = warm.filter((item) => item.bucketGap === 512);
const manyBlocks = warm.filter((item) => item.bucketGap > 512);

const summary = {
  file,
  rows: rows.length,
  coldStarts: cold.length,
  warmRows: warm.length,
  fullBucket: fullBucket.length,
  oneBlockShort: oneBlock.length,
  manyBlocksShort: manyBlocks.length,
  rawTokenHitRate: ratio(sum(rows, "cached"), sum(rows, "input")),
  warmRawTokenHitRate: ratio(sum(warm, "cached"), sum(warm, "input")),
  effectiveBucketHitRate: ratio(sum(rows, "cached"), sum(rows, "bucketMax")),
  warmEffectiveBucketHitRate: ratio(sum(warm, "cached"), sum(warm, "bucketMax")),
  warmAdjustedBucketHitRate: ratio(
    sum(warm, "cached") + sum(warm, "newTailShortfallTokens"),
    sum(warm, "bucketMax")
  ),
  warmProviderGapTokens: sum(warm, "bucketGap"),
  warmNewTailShortfallTokens: sum(warm, "newTailShortfallTokens"),
  warmAvoidableShortfallTokens: sum(warm, "avoidableShortfallTokens"),
  coldStartRate: ratio(cold.length, rows.length),
  byProvider: groupByProvider(rows),
  byDiagnostic: groupBy(rows, "diagnostic"),
  byPrefixKey: groupBy(rows, "providerPrefixKey"),
  byPrefixFingerprint: groupBy(rows, "providerPrefixFingerprint")
};

const baseline = baselineMetrics
  ? summarizeRequests(
      baselineFile,
      Array.isArray(baselineMetrics.recent_requests) ? baselineMetrics.recent_requests : []
    )
  : null;
const regression = baseline ? compareToBaseline(summary, baseline) : null;
const diagnostics = diagnoseRows(rows, summary);

console.log(JSON.stringify({ summary, baseline, regression, diagnostics, rows }, null, 2));

function summarizeRequests(sourceFile, sourceRequests) {
  const sourceRows = sourceRequests
    .filter((item) => Number(item.input_tokens ?? 0) > 0)
    .map((item) => {
      const input = Number(item.input_tokens ?? 0);
      const cached = Number(item.cache_read_tokens ?? 0);
      const bucketMax = Math.floor(input / 512) * 512;
      const bucketGap = Math.max(0, bucketMax - cached);
      return {
        provider: item.provider,
        model: item.model,
        providerPrefixKey: item.provider_prefix_key ?? null,
        providerPrefixFingerprint: item.provider_prefix_fingerprint ?? null,
        diagnostic: item.provider_cache_diagnostic ?? inferDiagnostic(input, cached, bucketGap),
        input,
        cached,
        bucketMax,
        bucketGap,
        loggedNewTailShortfallTokens:
          item.cache_new_tail_gap_tokens == null ? null : Number(item.cache_new_tail_gap_tokens),
        loggedAvoidableShortfallTokens:
          item.cache_avoidable_gap_tokens == null ? null : Number(item.cache_avoidable_gap_tokens)
      };
    });
  annotateGapCausality(sourceRows);
  const sourceWarm = sourceRows.filter((item) => item.cached > 0);
  const sourceCold = sourceRows.filter((item) => item.cached === 0);
  return {
    file: sourceFile,
    rows: sourceRows.length,
    coldStarts: sourceCold.length,
    warmRows: sourceWarm.length,
    coldStartRate: ratio(sourceCold.length, sourceRows.length),
    rawTokenHitRate: ratio(sum(sourceRows, "cached"), sum(sourceRows, "input")),
    warmRawTokenHitRate: ratio(sum(sourceWarm, "cached"), sum(sourceWarm, "input")),
    effectiveBucketHitRate: ratio(sum(sourceRows, "cached"), sum(sourceRows, "bucketMax")),
    warmEffectiveBucketHitRate: ratio(sum(sourceWarm, "cached"), sum(sourceWarm, "bucketMax")),
    warmAdjustedBucketHitRate: ratio(
      sum(sourceWarm, "cached") + sum(sourceWarm, "newTailShortfallTokens"),
      sum(sourceWarm, "bucketMax")
    ),
    warmProviderGapTokens: sum(sourceWarm, "bucketGap"),
    warmNewTailShortfallTokens: sum(sourceWarm, "newTailShortfallTokens"),
    warmAvoidableShortfallTokens: sum(sourceWarm, "avoidableShortfallTokens"),
    byProvider: groupByProvider(sourceRows),
    byDiagnostic: groupBy(sourceRows, "diagnostic"),
    byPrefixKey: groupBy(sourceRows, "providerPrefixKey"),
    byPrefixFingerprint: groupBy(sourceRows, "providerPrefixFingerprint")
  };
}

function compareToBaseline(current, base) {
  if (current.rows === 0) {
    return {
      targetCumulativeRawTokenHitRate: TARGET_CUMULATIVE_RAW_TOKEN_HIT_RATE,
      status: "no-data",
      pass: null
    };
  }
  const cumulativeDelta = current.rawTokenHitRate - base.rawTokenHitRate;
  const warmDelta = current.warmRawTokenHitRate - base.warmRawTokenHitRate;
  const bucketDelta = current.warmEffectiveBucketHitRate - base.warmEffectiveBucketHitRate;
  const coldDelta = current.coldStartRate - base.coldStartRate;
  return {
    targetCumulativeRawTokenHitRate: TARGET_CUMULATIVE_RAW_TOKEN_HIT_RATE,
    baselineRawTokenHitRate: base.rawTokenHitRate,
    currentRawTokenHitRate: current.rawTokenHitRate,
    rawTokenDelta: cumulativeDelta,
    baselineWarmRawTokenHitRate: base.warmRawTokenHitRate,
    currentWarmRawTokenHitRate: current.warmRawTokenHitRate,
    warmRawDelta: warmDelta,
    baselineWarmEffectiveBucketHitRate: base.warmEffectiveBucketHitRate,
    currentWarmEffectiveBucketHitRate: current.warmEffectiveBucketHitRate,
    warmEffectiveBucketDelta: bucketDelta,
    baselineColdStartRate: base.coldStartRate,
    currentColdStartRate: current.coldStartRate,
    coldStartRateDelta: coldDelta,
    pass:
      current.rawTokenHitRate >= TARGET_CUMULATIVE_RAW_TOKEN_HIT_RATE &&
      cumulativeDelta >= -0.005 &&
      current.coldStartRate <= Math.max(base.coldStartRate + 0.05, 0.2)
  };
}

function diagnoseRows(items, current) {
  const prefixKeyCollisions = findPrefixKeyCollisions(items);
  const prefixKeySplits = findPrefixKeySplits(items);
  const severeShortfalls = items
    .filter((item) => item.cached > 0 && item.effectivePct < 0.99)
    .sort((left, right) => right.bucketGap - left.bucketGap)
    .slice(0, 10);
  const avoidableShortfalls = items
    .filter((item) => item.cached > 0 && Number(item.avoidableShortfallTokens ?? 0) > 0)
    .sort(
      (left, right) =>
        Number(right.avoidableShortfallTokens ?? 0) -
        Number(left.avoidableShortfallTokens ?? 0)
    )
    .slice(0, 10);
  const coldRuns = [];
  let activeRun = [];
  for (const item of [...items].reverse()) {
    if (item.cached === 0) {
      activeRun.push(item);
    } else if (activeRun.length > 0) {
      coldRuns.push(activeRun);
      activeRun = [];
    }
  }
  if (activeRun.length > 0) coldRuns.push(activeRun);

  return {
    verdict:
      current.rows === 0
        ? "no-data"
        : current.rawTokenHitRate >= TARGET_CUMULATIVE_RAW_TOKEN_HIT_RATE &&
            current.coldStartRate <= 0.2
          ? "healthy"
          : "regression",
    likelyCause:
      current.rows === 0
        ? "no requests recorded yet"
        : prefixKeyCollisions.length > 0
        ? "multiple provider_prefix_fingerprints share one provider_prefix_key; scope prompt_cache_key by stable prefix"
        : prefixKeySplits.length > 0
        ? "one provider_prefix_fingerprint is split across multiple provider_prefix_keys; keep prompt_cache_key independent from volatile provider identity"
        : current.warmAvoidableShortfallTokens > 0
        ? "known prefix buckets are not always ready; inspect avoidableShortfalls and prefix settle timing"
        : (current.byDiagnostic ?? []).some((item) => item.key === "provider-prefix-break" && item.rows > 0)
        ? "provider gaps are mostly new tail buckets; response-session or prewarm can reduce real tokens, but provider cannot cache unseen tail content"
        : current.coldStartRate > 0.35
        ? "repeated provider-prefix cold starts; check prompt_cache_key group, selected provider/model, and volatile prefix fields"
        : current.rawTokenHitRate < TARGET_CUMULATIVE_RAW_TOKEN_HIT_RATE
      ? "cumulative provider token hit rate is below target; include cold starts, then inspect severeShortfalls"
      : "no obvious provider-prefix regression in recent rows",
    longestColdRun: coldRuns.reduce((max, run) => Math.max(max, run.length), 0),
    prefixKeyCollisions,
    prefixKeySplits,
    avoidableShortfalls,
    severeShortfalls
  };
}

function annotateGapCausality(items) {
  const groups = new Map();
  const chronological = [...items].sort((left, right) =>
    String(left.at ?? "").localeCompare(String(right.at ?? ""))
  );

  for (const item of chronological) {
    const key = item.providerPrefixKey ?? "missing";
    const state = groups.get(key) ?? { seenBucketMax: 0 };
    let priorSeen = state.seenBucketMax;
    if (priorSeen === 0 && item.cached > 0) {
      priorSeen = item.cached;
    }
    const expectedCached = Math.min(item.bucketMax, priorSeen);
    const inferredAvoidable = Math.max(0, expectedCached - item.cached);
    const loggedAvoidable =
      item.loggedAvoidableShortfallTokens == null
        ? null
        : Math.max(0, Math.min(item.bucketGap, item.loggedAvoidableShortfallTokens));
    const loggedNewTail =
      item.loggedNewTailShortfallTokens == null
        ? null
        : Math.max(0, Math.min(item.bucketGap, item.loggedNewTailShortfallTokens));
    const loggedProviderUnstable =
      item.loggedProviderUnstableShortfallTokens == null
        ? null
        : Math.max(0, Math.min(item.bucketGap, item.loggedProviderUnstableShortfallTokens));
    item.avoidableShortfallTokens =
      loggedAvoidable ?? Math.min(item.bucketGap, inferredAvoidable);
    item.providerUnstableShortfallTokens = loggedProviderUnstable ?? 0;
    item.newTailShortfallTokens =
      loggedNewTail ??
      Math.max(
        0,
        item.bucketGap -
          item.avoidableShortfallTokens -
          item.providerUnstableShortfallTokens
      );
    const loggedTotal =
      item.avoidableShortfallTokens +
      item.providerUnstableShortfallTokens +
      item.newTailShortfallTokens;
    if (loggedTotal > item.bucketGap) {
      item.newTailShortfallTokens = Math.max(
        0,
        item.bucketGap -
          item.avoidableShortfallTokens -
          item.providerUnstableShortfallTokens
      );
    } else if (loggedTotal < item.bucketGap && loggedAvoidable != null && loggedNewTail == null) {
      item.newTailShortfallTokens =
        item.bucketGap -
        item.avoidableShortfallTokens -
        item.providerUnstableShortfallTokens;
    } else if (loggedTotal < item.bucketGap && loggedAvoidable == null && loggedNewTail != null) {
      item.avoidableShortfallTokens =
        item.bucketGap -
        item.newTailShortfallTokens -
        item.providerUnstableShortfallTokens;
    }
    item.gapCause =
      item.cached === 0
        ? "cold"
        : item.avoidableShortfallTokens > 0
        ? "avoidable-prefix-gap"
        : item.providerUnstableShortfallTokens > 0
        ? "provider-waterline-rollback"
        : item.newTailShortfallTokens > 0
        ? "new-tail-gap"
        : "full-bucket";

    state.seenBucketMax = Math.max(state.seenBucketMax, item.bucketMax, item.cached);
    groups.set(key, state);
  }
}

function findPrefixKeyCollisions(items) {
  const groups = new Map();
  for (const item of items) {
    if (!item.providerPrefixKey) continue;
    const group =
      groups.get(item.providerPrefixKey) ??
      {
        providerPrefixKey: item.providerPrefixKey,
        rows: 0,
        fingerprints: new Set(),
        input: 0,
        cached: 0,
        maxBucketGap: 0
      };
    group.rows += 1;
    if (item.providerPrefixFingerprint) group.fingerprints.add(item.providerPrefixFingerprint);
    group.input += item.input;
    group.cached += item.cached;
    group.maxBucketGap = Math.max(group.maxBucketGap, item.bucketGap);
    groups.set(item.providerPrefixKey, group);
  }
  return [...groups.values()]
    .filter((group) => group.fingerprints.size > 1)
    .map((group) => ({
      providerPrefixKey: group.providerPrefixKey,
      rows: group.rows,
      fingerprintCount: group.fingerprints.size,
      input: group.input,
      cached: group.cached,
      rawTokenHitRate: ratio(group.cached, group.input),
      maxBucketGap: group.maxBucketGap
    }))
    .sort((left, right) => right.input - left.input);
}

function findPrefixKeySplits(items) {
  const groups = new Map();
  for (const item of items) {
    if (!item.providerPrefixFingerprint) continue;
    const group =
      groups.get(item.providerPrefixFingerprint) ??
      {
        providerPrefixFingerprint: item.providerPrefixFingerprint,
        rows: 0,
        keys: new Set(),
        input: 0,
        cached: 0,
        maxBucketGap: 0
      };
    group.rows += 1;
    if (item.providerPrefixKey) group.keys.add(item.providerPrefixKey);
    group.input += item.input;
    group.cached += item.cached;
    group.maxBucketGap = Math.max(group.maxBucketGap, item.bucketGap);
    groups.set(item.providerPrefixFingerprint, group);
  }
  return [...groups.values()]
    .filter((group) => group.keys.size > 1)
    .map((group) => ({
      providerPrefixFingerprint: group.providerPrefixFingerprint,
      rows: group.rows,
      keyCount: group.keys.size,
      input: group.input,
      cached: group.cached,
      rawTokenHitRate: ratio(group.cached, group.input),
      maxBucketGap: group.maxBucketGap
    }))
    .sort((left, right) => right.input - left.input);
}

function inferDiagnostic(input, cached, bucketGap) {
  const bucketMax = Math.floor(input / 512) * 512;
  if (bucketMax < 1024) return "provider-prefix-ineligible-small";
  if (cached === 0) return "provider-cold-start";
  if (bucketGap === 0) return "provider-warm-full";
  if (bucketGap <= 512) return "provider-small-gap";
  if (ratio(cached, bucketMax) >= 0.99) return "provider-warm-99";
  return "provider-prefix-break";
}

function groupByProvider(items) {
  const groups = new Map();
  for (const item of items) {
    const key = item.provider || "unknown";
    const group =
      groups.get(key) ??
      {
        provider: key,
        rows: 0,
        coldStarts: 0,
        warmRows: 0,
        fullBucket: 0,
        oneBlockShort: 0,
        manyBlocksShort: 0,
        input: 0,
        cached: 0,
        bucketMax: 0,
        newTailShortfallTokens: 0,
        avoidableShortfallTokens: 0
      };
    group.rows += 1;
    group.input += item.input;
    group.cached += item.cached;
    group.bucketMax += item.bucketMax;
    group.newTailShortfallTokens += Number(item.newTailShortfallTokens ?? 0);
    group.avoidableShortfallTokens += Number(item.avoidableShortfallTokens ?? 0);
    if (item.cached === 0) group.coldStarts += 1;
    else group.warmRows += 1;
    if (item.cached > 0 && item.bucketGap === 0) group.fullBucket += 1;
    if (item.cached > 0 && item.bucketGap === 512) group.oneBlockShort += 1;
    if (item.cached > 0 && item.bucketGap > 512) group.manyBlocksShort += 1;
    groups.set(key, group);
  }
  return [...groups.values()].map((group) => ({
    ...group,
    rawTokenHitRate: ratio(group.cached, group.input),
    effectiveBucketHitRate: ratio(group.cached, group.bucketMax),
    adjustedBucketHitRate: ratio(group.cached + group.newTailShortfallTokens, group.bucketMax)
  }));
}

function groupBy(items, keyName) {
  const groups = new Map();
  for (const item of items) {
    const key = item[keyName] ?? "missing";
    const group =
      groups.get(key) ??
      {
        key,
        rows: 0,
        coldStarts: 0,
        warmRows: 0,
        input: 0,
        cached: 0,
        bucketMax: 0,
        maxBucketGap: 0,
        newTailShortfallTokens: 0,
        avoidableShortfallTokens: 0
      };
    group.rows += 1;
    group.input += item.input;
    group.cached += item.cached;
    group.bucketMax += item.bucketMax;
    group.newTailShortfallTokens += Number(item.newTailShortfallTokens ?? 0);
    group.avoidableShortfallTokens += Number(item.avoidableShortfallTokens ?? 0);
    group.maxBucketGap = Math.max(group.maxBucketGap, item.bucketGap);
    if (item.cached === 0) group.coldStarts += 1;
    else group.warmRows += 1;
    groups.set(key, group);
  }
  return [...groups.values()]
    .map((group) => ({
      ...group,
      rawTokenHitRate: ratio(group.cached, group.input),
      effectiveBucketHitRate: ratio(group.cached, group.bucketMax),
      adjustedBucketHitRate: ratio(group.cached + group.newTailShortfallTokens, group.bucketMax)
    }))
    .sort((left, right) => right.input - left.input)
    .slice(0, 20);
}

function sum(items, key) {
  return items.reduce((total, item) => total + Number(item[key] ?? 0), 0);
}

function ratio(numerator, denominator) {
  return denominator === 0 ? 0 : numerator / denominator;
}
