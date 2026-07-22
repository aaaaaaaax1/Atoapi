import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";

const html = await readFile(
  new URL("../prototype/atoapi-graphite-ui.html", import.meta.url),
  "utf8"
);
const host = await readFile(
  new URL("../src/GraphitePrototypeHost.tsx", import.meta.url),
  "utf8"
);
const api = await readFile(new URL("../src/lib/api.ts", import.meta.url), "utf8");
const combined = `${html}\n${host}\n${api}`;

// Vite imports the Graphite document and bridge as raw strings, so TypeScript
// does not parse their DOM-side JavaScript. Parse both bodies without running
// them to catch accidental template-string syntax drift.
for (const match of html.matchAll(/<script(?:\s[^>]*)?>([\s\S]*?)<\/script>/gi)) {
  if (match[1].trim()) new Function(match[1]);
}
const bridgeStart = host.indexOf("const bridgeSource = String.raw`");
const bridgeEnd = host.indexOf("`;\n\nfunction createDocument", bridgeStart);
assert.ok(bridgeStart >= 0 && bridgeEnd >= 0, "the Graphite bridge source must remain extractable");
const bridgeDefinition = host.slice(bridgeStart, bridgeEnd + 1);
const bridgeSource = Function(`${bridgeDefinition}; return bridgeSource;`)();
new Function(bridgeSource);

const trendToken = String.raw`(?:cache|metrics)[-_]?[A-Za-z0-9_:-]*trend|trend[-_]?[A-Za-z0-9_:-]*(?:cache|metrics)?`;

assert.match(
  html,
  new RegExp(
    String.raw`<(?:article|aside|div|section)[^>]+(?:id|class)=["'][^"']*(?:${trendToken})[^"']*["']`,
    "i"
  ),
  "the Graphite shell must contain a dedicated cache-metrics trend card"
);

for (const range of ["today", "1d", "7d", "14d", "30d", "custom"]) {
  assert.match(
    html,
    new RegExp(String.raw`data-trend-range=["']${range}["']`, "i"),
    `the cache trend range controls must retain the ${range} option`
  );
}

assert.match(
  api,
  /["']get_metrics_trend["']/,
  "cache trend data must use the independent get_metrics_trend command"
);
assert.match(
  host,
  /command\s*<\s*MetricsTrendSnapshot\s*>\s*\(\s*["']get_metrics_trend["']\s*,\s*\{\s*input\s*\}\s*\)/,
  "the Graphite host must query get_metrics_trend directly instead of deriving trend data from get_metrics"
);

const trendInputDefinitions = [
  ...api.matchAll(
    /(?:export\s+)?(?:interface|type)\s+([A-Za-z0-9_]*(?:MetricsTrend|TrendMetrics)[A-Za-z0-9_]*Input)\b[\s\S]*?\{([\s\S]*?)\n\}/g
  )
];
assert.ok(
  trendInputDefinitions.length > 0,
  "src/lib/api.ts must expose a named metrics-trend input contract"
);
const trendInput = trendInputDefinitions.map((match) => match[2]).join("\n");
for (const field of [
  "start_utc",
  "end_utc",
  "agent_id",
  "provider_id",
  "include_cold_starts"
]) {
  assert.match(
    trendInput,
    new RegExp(String.raw`\b${field}\s*\??\s*:`),
    `the metrics-trend input contract must include ${field}`
  );
}

const trendBridgeStart = host.search(/send\s*\(\s*["']load-metrics-trend["']/i);
assert.notEqual(
  trendBridgeStart,
  -1,
  "the iframe must expose a dedicated load-metrics-trend bridge action"
);
const trendBridgeRequest = host.slice(trendBridgeStart, trendBridgeStart + 1_200);
assert.ok(
  /\bsequence\s*:/.test(trendBridgeRequest) && /\brangeKey\s*:/.test(trendBridgeRequest),
  "the Graphite bridge request must carry both sequence and rangeKey"
);
for (const field of [
  "start_utc",
  "end_utc",
  "agent_id",
  "provider_id",
  "include_cold_starts"
]) {
  assert.match(
    trendBridgeRequest,
    new RegExp(String.raw`\b${field}\s*:`),
    `the Graphite trend bridge input must send ${field}`
  );
}
assert.ok(
  /if\s*\([^)]*[A-Za-z0-9_]*sequence[A-Za-z0-9_]*[^)]*[A-Za-z0-9_]*rangeKey[A-Za-z0-9_]*[^)]*\)\s*(?:\{[\s\S]{0,160})?return\b/i.test(combined) ||
    /if\s*\([^)]*[A-Za-z0-9_]*rangeKey[A-Za-z0-9_]*[^)]*[A-Za-z0-9_]*sequence[A-Za-z0-9_]*[^)]*\)\s*(?:\{[\s\S]{0,160})?return\b/i.test(combined),
  "out-of-order trend responses must be rejected by a sequence/range stale guard"
);

const uiElementTags = [...html.matchAll(/<[^>]+>/g)].map((match) => match[0].toLowerCase());
for (const element of ["chart", "tooltip", "loading", "empty", "error", "retry"]) {
  assert.ok(
    uiElementTags.some((tag) => tag.includes("trend") && tag.includes(element)),
    `the cache trend card must provide a dedicated ${element} element/state`
  );
}

assert.match(
  html,
  /当前范围暂无趋势数据[\s\S]{0,240}趋势从本版本开始持续记录/,
  "empty trend history must explain that recording starts with this version instead of drawing fake zero data"
);
const trendCardSource = html.slice(
  html.indexOf('id="cacheTrendCard"'),
  html.indexOf('id="requestsPanel"')
);
assert.doesNotMatch(
  trendCardSource,
  /成本|cost/i,
  "the cache trend summary must not invent a cost metric"
);
assert.match(
  host,
  /agent\?\.sourceId\s*\|\|\s*agent\?\.id/,
  "metrics trend queries must use the current Agent sourceId when available"
);
assert.match(
  combined,
  /cacheTrendScopeSelect[\s\S]{0,1200}requestScopeId|requestScopeId[\s\S]{0,1200}cacheTrendScopeSelect/,
  "the trend provider selector and requestScopeId must remain bidirectionally connected"
);

const mediaSegments = [...html.matchAll(/@media\s*\([^)]*max-width\s*:\s*(\d+)px[^)]*\)\s*\{/gi)]
  .map((match, index, matches) => ({
    width: Number(match[1]),
    source: html.slice(match.index, matches[index + 1]?.index ?? html.length)
  }));
for (const { label, widths } of [
  { label: "980px", widths: [980, 1040] },
  { label: "760px", widths: [760] },
  { label: "520px", widths: [520, 560] }
]) {
  assert.ok(
    mediaSegments.some(
      (segment) => widths.includes(segment.width) && new RegExp(trendToken, "i").test(segment.source)
    ),
    `the cache trend UI must define responsive rules for the ${label} tier`
  );
}

const hostTrendLines = host
  .split(/\r?\n/)
  .filter((line) => /trend/i.test(line))
  .join("\n");
assert.doesNotMatch(
  hostTrendLines,
  /["']get_metrics["']/,
  "the trend bridge must not piggyback on the per-second get_metrics snapshot"
);
assert.doesNotMatch(
  host,
  /setInterval\s*\([\s\S]{0,800}["']get_metrics["']/i,
  "the Graphite trend UI must not poll get_metrics every second"
);

assert.match(
  html,
  /id=["']providerSessionReuseModelInput["'][^>]*list=["']providerModelCandidates["']/,
  "the session-reuse model selector must remain present after adding the trend card"
);
assert.match(
  host,
  /#providerSessionReuseModelInput/,
  "the Graphite bridge must retain providerSessionReuseModelInput behavior"
);

assert.match(
  html,
  /summary\.successful_requests\s*===\s*0/,
  "all-zero hourly buckets must render the honest empty trend state instead of a fake 0% line"
);
assert.match(
  html,
  /function request\([^)]*\)[\s\S]{0,220}pinned\s*=\s*false/,
  "a new trend request must release a previously pinned tooltip"
);
assert.match(
  host,
  /target\.id === "metricsRefreshButton"[\s\S]{0,360}trendController\(\)\?\.request\("refresh"\)/,
  "the visible metrics refresh action must reload the independent trend as well"
);

assert.match(
  html,
  /id=["']cacheTrendScopeTrigger["'][^>]*aria-haspopup=["']listbox["']/,
  "the trend scope control must use a themeable custom listbox trigger instead of a browser-white native popup"
);
assert.match(
  html,
  /id=["']cacheTrendScopeMenu["'][^>]*role=["']listbox["']/,
  "the trend scope control must keep its option list in the Graphite theme"
);
assert.match(
  html,
  /function\s+openDatePicker\(input\)[\s\S]{0,300}input\.showPicker\(\)/,
  "clicking any part of a custom range date field must request the native date picker"
);
assert.match(
  html,
  /命中率（右轴）/,
  "the dashed hit-rate series must explicitly identify its right-side percentage axis"
);
const trendControllerSource = html.slice(
  html.indexOf("function createCacheTrendController()"),
  html.indexOf("const cacheTrend = createCacheTrendController()")
);
assert.match(
  trendControllerSource,
  /function syncScopes[\s\S]{0,900}renderScopeControl\(\)/,
  "scope options supplied by the host must render into the custom scope menu"
);
assert.match(
  trendControllerSource,
  /scopeTrigger\.addEventListener\("click"[\s\S]{0,240}aria-expanded/,
  "the custom scope trigger must open and close its Graphite menu"
);
assert.match(
  trendControllerSource,
  /scopeMenu\.addEventListener\("click"[\s\S]{0,520}scopeSelect\.dispatchEvent/,
  "selecting a themed scope option must still drive the existing trend request path"
);
assert.match(
  trendControllerSource,
  /\[startInput, endInput\][\s\S]{0,260}pointerdown[\s\S]{0,160}openDatePicker/,
  "the full custom date field, not only its calendar icon, must activate the picker"
);

console.log("graphite metrics trend UI regression tests passed");
