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
const controlPlane = await readFile(
  new URL("../src/useGraphiteControlPlane.ts", import.meta.url),
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
const bridgeEnd = host.indexOf("`;\n\n// The embedded prototype", bridgeStart);
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
  "include_cold_starts",
  "include_compactions",
  "provider_realm_id",
  "model",
  "client_channel",
  "upstream_channel",
  "upstream_call_kind",
  "stable_prefix_cohort_id"
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
  "include_cold_starts",
  "include_compactions"
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
assert.match(
  host,
  /function exactHistoricalScope\(scope\)[\s\S]{0,640}provider_realm_id[\s\S]{0,400}upstream_call_kind/,
  "the trend bridge must construct an opaque exact historical token-hit scope"
);
assert.match(
  host,
  /\.\.\.exactHistoricalTrendScope\(scope\)/,
  "the release-champion request must send the selected exact token-history scope when available"
);
assert.match(
  host,
  /function exactHistoricalTrendScope\(scope\)[\s\S]{0,360}stable_prefix_cohort_id/,
  "the trend request must add the opaque stable-prefix family when history can prove it"
);
assert.match(
  host,
  /\.\.\.exactHistoricalTrendScope\(scope\)/,
  "the trend request must use the stricter stable-prefix history scope"
);
assert.match(
  host,
  /historicalScope:\s*exactHistoricalScopeForProvider\(scope\.providerId\)/,
  "the selected Provider scope must come from the latest successful token-bearing request"
);
assert.match(
  host,
  /callKind === "stream" \|\| callKind === "sync"/,
  "local cache and prewarm records must not select the historical upstream token-hit cohort"
);
assert.match(
  host,
  /const configuredModel = selectedAgent\?\.model_id\?\.trim\(\);/,
  "the historical cohort picker must read the model currently bound to the selected Agent"
);
assert.match(
  host,
  /candidate\.model === configuredModel \|\| candidate\.requested_model === configuredModel/,
  "the historical cohort picker must prefer the model currently bound to the selected Agent"
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

assert.doesNotMatch(
  html,
  /providerCompatibility|providerSessionReuseModelInput|probeSessionReuseButton/,
  "the retired session-reuse qualification panel must not remain in the provider editor"
);
assert.doesNotMatch(
  host,
  /probe-session-reuse|set-session-reuse|compatibilityModelId/,
  "the Graphite bridge must not retain automatic or manual session-reuse qualification actions"
);
assert.match(
  host,
  /const transport = \$bridge\("#providerTransport"\)/,
  "cache validation must target the transport-and-cache panel after removing session reuse"
);
assert.match(
  host,
  /transport\.append\(section\)/,
  "cache validation must remain available after removing session reuse"
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
  api,
  /const includeCompactions = input\?\.include_compactions !== false/,
  "the browser fallback must honor the compaction-filter input"
);
assert.match(
  api,
  /compaction_filter_complete:\s*includeCompactions/,
  "the browser fallback must mark compaction-excluded data as inexact"
);
assert.match(
  host,
  /function filteredMetricValue\([\s\S]{0,520}includeColdStarts[\s\S]{0,180}includeCompactions/,
  "the metrics layer must retain exact cold-start and compaction filtering before the combined UI control applies both"
);

assert.match(
  html,
  /aria-label=["']计入冷启动和压缩["']/,
  "the policy panel must expose one combined special-request inclusion switch"
);
assert.doesNotMatch(
  html,
  /aria-label=["'](?:计入冷启动|计入压缩)["']/,
  "the old separate cold-start and compaction switches must not remain in the policy panel"
);
assert.doesNotMatch(
  host,
  /ensureCompactionPolicySwitch/,
  "the bridge must not dynamically reinsert a second compaction switch"
);
assert.match(
  host,
  /const includeSpecialRequests = metricState\.includeColdStarts !== false &&[\s\S]{0,120}metricState\.includeCompactions !== false/,
  "the combined switch must reflect both filter dimensions"
);
assert.match(
  host,
  /setSwitch\("计入冷启动和压缩", includeSpecialRequests\)/,
  "the combined switch must render one coherent on/off state"
);
assert.match(
  host,
  /send\("set-include-special-requests", \{ enabled: target\.getAttribute\("aria-checked"\) !== "true" \}\)/,
  "the policy click must toggle both filter dimensions through one bridge action"
);
assert.doesNotMatch(
  host,
  /send\("set-include-(?:cold-starts|compactions)"/,
  "the visible policy UI must not send one-sided legacy filter actions"
);
assert.match(
  controlPlane,
  /action === "set-include-special-requests"[\s\S]{0,240}setIncludeColdStarts\(enabled\);[\s\S]{0,120}setIncludeCompactions\(enabled\);/,
  "the control plane must update cold-start and compaction filters atomically from the one UI switch"
);
assert.match(
  host,
  /successDetails\.hidden = !includeSpecialRequests;[\s\S]{0,260}: "";/,
  "when the combined filter is off, the success-card detail node must be hidden instead of wrapping into a second line"
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
  /\.cache-trend-chart\s+svg:focus\s*,\s*\.cache-trend-chart\s+svg:focus-visible\s*\{[\s\S]{0,240}outline\s*:\s*none[\s\S]{0,120}box-shadow\s*:\s*none/i,
  "pointer and keyboard focus must not draw a white frame around the trend chart"
);
assert.match(
  html,
  /id=["']cacheTrendSvg["'][^>]*tabindex=["']0["']/,
  "removing the trend focus frame must retain keyboard chart navigation"
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
assert.match(
  host,
  /const cacheTailDetail = "新 " \+ requestTokens\(request\.cacheNewTailGapTokens\)[\s\S]{0,220}Number\(request\.cacheAvoidableGapTokens \|\| 0\) > 0/,
  "the request hit cell must always show the new tail and conditionally include a nonzero avoidable gap"
);
assert.match(
  host,
  /const CACHE_DISPLAY_BUCKET_TOKENS = 128;/,
  "the request-row cache hint must use the provider's 128-token cache bucket"
);
assert.match(
  host,
  /function cacheTailDisplayForRequest\([\s\S]{0,220}cache_provider_unstable_gap_tokens/,
  "the request-row display must classify the 128-aligned gap without labeling provider instability as a new tail"
);
assert.match(
  host,
  /const cacheTailDisplay = cacheTailDisplayForRequest\(request\);/,
  "the request-row projection must derive its display values from existing raw usage"
);
assert.match(
  host,
  /cacheShortfallTokens: cacheTailDisplay\.shortfallTokens,/,
  "the request-row shortfall must use the 128-token projection"
);
assert.match(
  host,
  /cacheNewTailGapTokens: cacheTailDisplay\.newTailTokens,/,
  "only the request-row projection may derive 128-token values from existing raw usage; no backend history rewrite is required"
);
assert.doesNotMatch(
  host,
  /cacheGapBucket|· 桶 /,
  "the request hit cell must not crowd the compact view with shortfall or bucket diagnostics"
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

assert.match(
  html,
  /id=["']releaseChampionSummary["'][\s\S]{0,260}同 Provider · Key realm · 模型 · 请求族/,
  "the cache overview must reserve a compact, explicit version-champion comparison slot"
);
assert.match(
  api,
  /export interface ReleaseChampionQueryInput[\s\S]{0,520}stable_prefix_cohort_id/,
  "the frontend API must expose the same cohort filter dimensions as the trend API"
);
assert.match(
  api,
  /export type ReleaseChampionStatus[\s\S]{0,420}["']regressed["'][\s\S]{0,420}["']legacy_history_unattributed["']/,
  "the UI contract must distinguish a regression from legacy data that cannot be attributed"
);
assert.match(
  host,
  /send\(["']load-release-champion["'][\s\S]{0,820}exactHistoricalTrendScope\(scope\)/,
  "the iframe must request a release comparison for the active token-history scope and both filters"
);
assert.match(
  host,
  /function historicalScopeKey\(scope\)[\s\S]{0,240}historicalRouteScopeKey\(scope\)/,
  "the historical scope key must compose the route key instead of recursively calling itself"
);
assert.match(
  host,
  /command\s*<\s*ReleaseChampionSnapshot\s*>\s*\(\s*["']get_release_champion["']\s*,\s*\{\s*input\s*\}\s*\)/,
  "the host must call the dedicated cohort command rather than infer version status from hourly metrics"
);
assert.match(
  host,
  /regressed:\s*\[["']is-regressed["'][\s\S]{0,160}低于冠军/,
  "a negative optimization must be visibly labeled as below champion"
);
assert.match(
  host,
  /legacy_history_unattributed:\s*\[["']is-pending["'][\s\S]{0,160}历史未归属版本/,
  "unattributed old history must fail closed in the UI instead of being called a champion"
);

console.log("graphite metrics trend UI regression tests passed");
