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
  controlPlane,
  /async function refreshMetrics\(\) \{[\s\S]{0,260}command<MetricsSnapshot>\("get_metrics"\)[\s\S]{0,180}setMetrics\(nextMetrics\)/,
  "one-second metrics refreshes must not also fetch cache-validation state"
);
assert.doesNotMatch(
  controlPlane.match(/async function refreshMetrics\(\) \{[\s\S]*?\n  \}/)?.[0] ?? "",
  /get_cache_validation_status/,
  "the metrics hot path must not force a full Graphite state refresh"
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
