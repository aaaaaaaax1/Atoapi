import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";

const host = await readFile(new URL("../src/GraphitePrototypeHost.tsx", import.meta.url), "utf8");
const controlPlane = await readFile(new URL("../src/useGraphiteControlPlane.ts", import.meta.url), "utf8");

const bridgeStart = host.indexOf("const bridgeSource = String.raw`");
const bridgeEnd = host.indexOf("`;\n\n// The embedded prototype", bridgeStart);
assert.ok(bridgeStart >= 0 && bridgeEnd >= 0, "the Graphite bridge source must remain extractable");
const bridgeDefinition = host.slice(bridgeStart, bridgeEnd + 1);
const bridgeSource = Function(`${bridgeDefinition}; return bridgeSource;`)();
new Function(bridgeSource);

assert.match(
  host,
  /kind: "metrics-delta",[\s\S]{0,260}metrics: state\.metrics/,
  "metric refreshes must use the lightweight iframe delta message"
);
assert.match(
  host,
  /previous === null \|\| requiresFullGraphiteState\(previous, props\)/,
  "the first ready frame must still receive a complete UI state"
);
const deltaStart = bridgeSource.indexOf("function applyMetricsDelta(metrics, nextRequests)");
const deltaEnd = bridgeSource.indexOf("function applyRenderSuspended(suspended)", deltaStart);
assert.ok(deltaStart >= 0 && deltaEnd > deltaStart, "the delta handler must precede bridge message dispatch");
const deltaSource = bridgeSource.slice(deltaStart, deltaEnd);
assert.match(
  deltaSource,
  /applyMetrics\(nextState\)[\s\S]{0,120}syncTrendController\(true\)/,
  "a metrics delta must update only its dynamic UI path"
);
assert.doesNotMatch(
  deltaSource,
  /renderAll\(\)/,
  "a metrics-only update must not rebuild the full Graphite document"
);
assert.match(
  bridgeSource,
  /message\.kind === "metrics-delta"\) applyMetricsDelta\(message\.metrics, message\.requests\)/,
  "the iframe must accept metric deltas separately from full state"
);
assert.match(
  host,
  /getCurrentWindow\(\)[\s\S]{0,1800}onMoved\(settleAfterWindowActivity\)[\s\S]{0,260}onResized\(settleAfterWindowActivity\)/,
  "native move and resize must temporarily quiesce iframe rendering"
);
assert.match(
  host,
  /kind: "render-suspension", suspended: renderSuspended/,
  "the host must tell the iframe when native window activity is in progress"
);
assert.match(
  bridgeSource,
  /function applyRenderSuspended\(suspended\)[\s\S]{0,560}performance-paused/,
  "the iframe must pause compositing-heavy rendering while the native window is active"
);
assert.match(
  bridgeSource,
  /if \(renderSuspended\) \{[\s\S]{0,440}deferredMetricsDelta/,
  "metric updates must coalesce instead of repainting while rendering is suspended"
);
assert.match(
  bridgeSource,
  /const RELEASE_CHAMPION_AUTO_REFRESH_MS = 5_000;[\s\S]{0,220}releaseChampionRequestInFlight/,
  "release-champion polling must have an explicit bounded refresh cadence and a single-flight gate"
);
assert.match(
  bridgeSource,
  /function scheduleReleaseChampionRefresh\(\)[\s\S]{0,1200}RELEASE_CHAMPION_AUTO_REFRESH_MS/,
  "unchanged metric deltas must debounce their expensive historical champion query"
);
assert.match(
  bridgeSource,
  /function releaseChampionAutoRefreshAllowed\(\)[\s\S]{0,520}function cancelReleaseChampionRefresh\(\)/,
  "backgrounded or render-suspended windows must be able to cancel deferred champion refreshes"
);
assert.match(
  bridgeSource,
  /function scheduleReleaseChampionRefresh\(\)[\s\S]{0,900}!releaseChampionAutoRefreshAllowed\(\)/,
  "a deferred champion refresh must not run while the overview is inactive"
);
assert.match(
  bridgeSource,
  /const hasLoadedCurrentContext = contextKey === latestReleaseChampionContextKey;[\s\S]{0,160}const snapshot = hasLoadedCurrentContext \? latestReleaseChampion : null;/,
  "a newly requested scope must not render a previously loaded scope's champion"
);
assert.match(
  bridgeSource,
  /releaseChampionRequestInFlight = false;[\s\S]{0,720}scheduleReleaseChampionRefresh\(\)/,
  "a settled champion query must safely drain one coalesced refresh instead of leaving the UI stale"
);
assert.match(
  controlPlane,
  /function loadMetricsSnapshot\(forceAfterCurrent = false\): Promise<MetricsSnapshotFetch \| null> \{[\s\S]{0,1000}command<MetricsSnapshot>\("get_metrics"\)/,
  "all metrics IPC must flow through one single-flight loader"
);
assert.doesNotMatch(
  controlPlane.match(/function loadMetricsSnapshot\(forceAfterCurrent = false\): Promise<MetricsSnapshotFetch \| null> \{[\s\S]*?\n  \}/)?.[0] ?? "",
  /get_cache_validation_status/,
  "the metrics hot path must not force a full Graphite state refresh"
);
assert.match(
  controlPlane,
  /const metricsRefreshInFlight = useRef<Promise<MetricsSnapshotFetch \| null> \| null>\(null\);/,
  "metrics IPC must retain a shared in-flight task"
);
assert.match(
  controlPlane,
  /const inFlight = metricsRefreshInFlight\.current;[\s\S]{0,600}return forceAfterCurrent[\s\S]{0,260}: inFlight;/,
  "slow metrics IPC calls must be single-flight rather than piling up while the app is busy"
);
assert.match(
  controlPlane,
  /command<AppConfig>\("reload_config"\),[\s\S]{0,180}loadMetricsSnapshot\(\)/,
  "the full refresh path must share the same metrics single-flight loader"
);
assert.match(
  controlPlane,
  /action === "clear-cache"[\s\S]{0,260}await refreshMetrics\(true\)/,
  "a cache clear must queue a fresh snapshot after an older poll has settled"
);
assert.match(
  controlPlane,
  /const CACHE_VALIDATION_REFRESH_MS = 5_000/,
  "cache-validation refreshes must use a dedicated slower cadence"
);
assert.match(
  controlPlane,
  /metricsRefreshPolicy === "manual" \|\| !cacheValidation \|\| cacheValidation\.mode === "auto"\) return/,
  "active cache-validation UI may refresh on a slower dedicated cadence"
);

console.log("graphite render scheduling regression tests passed");
