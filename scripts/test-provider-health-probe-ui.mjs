import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";

const html = await readFile(new URL("../prototype/atoapi-graphite-ui.html", import.meta.url), "utf8");
const host = await readFile(new URL("../src/GraphitePrototypeHost.tsx", import.meta.url), "utf8");
const controlPlane = await readFile(new URL("../src/useGraphiteControlPlane.ts", import.meta.url), "utf8");
const api = await readFile(new URL("../src/lib/api.ts", import.meta.url), "utf8");

const bridgeStart = host.indexOf("const bridgeSource = String.raw`");
const bridgeEnd = host.indexOf("`;\n\n// The embedded prototype", bridgeStart);
assert.ok(bridgeStart >= 0 && bridgeEnd >= 0, "the Graphite bridge source must remain extractable");
const bridgeDefinition = host.slice(bridgeStart, bridgeEnd + 1);
const bridgeSource = Function(`${bridgeDefinition}; return bridgeSource;`)();
new Function(bridgeSource);

assert.match(html, /id="healthProbeOverlay"/, "the health probe must use a dedicated modal");
assert.match(html, /id="healthProbePromptInput"[^>]*>hi<\/textarea>/, "the health probe prompt must default to hi");
assert.match(html, /value="responses_streaming"/, "Responses streaming must be selectable");
assert.match(html, /value="chat_streaming"/, "Chat streaming must be selectable");
assert.match(html, /value="chat_json"/, "Chat JSON must be selectable");
assert.match(html, /value="responses_json"/, "Responses JSON must be selectable");
assert.match(html, /id="testAllKeysHealthButton"/, "multi-Key testing must open the unified health flow");
assert.doesNotMatch(html, /providerBalanceProbeUrlInput|余额 API URL/, "balance probing must not require a manually configured URL");

assert.match(bridgeSource, /function openHealthProbe\(providerId, target = "current", keyIds = \[\]\)/, "provider and multi-Key paths must share one target-aware modal");
assert.match(bridgeSource, /send\("fetch-health-probe-models", \{ providerId \}\)/, "opening the modal must automatically fetch models");
assert.match(bridgeSource, /send\("run-health-probe", \{[\s\S]{0,260}target:[\s\S]{0,260}mode,[\s\S]{0,260}prompt/, "the selected target, mode, and prompt must be passed to the probe");
assert.match(bridgeSource, /data-health-probe-provider/, "provider rows must expose a health probe action");
assert.match(bridgeSource, /data-test-provider/, "provider rows must retain the separate connection-test action");
assert.match(bridgeSource, /openHealthProbe\(providerId, "all_enabled"\)/, "all-Key testing must not reuse the provider card's current-Key target");
assert.match(bridgeSource, /data-balance-probe/, "provider rows must expose a balance probe action");
assert.match(bridgeSource, /balanceProbeLabel\(provider\.balance\)/, "balance results must remain visible on provider rows");
assert.doesNotMatch(
  bridgeSource.slice(bridgeSource.indexOf("renderProviders = function"), bridgeSource.indexOf("function requestTimingTone")),
  /Agent 直传/,
  "provider rows must no longer display Agent direct delivery"
);

assert.match(controlPlane, /command<ModelConfig\[\]>\("fetch_provider_health_models"/, "modal model discovery must use the pool-safe backend command");
assert.match(controlPlane, /command<ProviderHealthProbeResult>\("probe_provider_health"/, "actual health checks must use the dedicated backend command");
assert.match(controlPlane, /command<ProviderKeyTestResult>\("test_active_provider_key"/, "the retained connection action must test the current Key separately from model health probing");
assert.match(controlPlane, /command<ProviderBalanceProbeResult>\("probe_provider_balance"/, "balance clicks must invoke a separate explicit management action");
assert.doesNotMatch(controlPlane, /save_provider_balance_probe_config|balance_probe_url/, "the editor must not save a manual balance endpoint");
assert.match(api, /export type ProviderHealthProbeMode/, "API types must describe all probe shapes");
assert.match(api, /export type ProviderHealthProbeTarget/, "API types must describe health probe targets");
assert.match(api, /export interface ProviderBalanceProbeResult/, "API types must describe safe balance outcomes");

console.log("graphite provider health-probe UI regression tests passed");
