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
assert.match(html, /<select id="healthProbeModeInput"[^>]*><option value="responses_streaming">/, "Responses streaming must be the visible default shape");
assert.doesNotMatch(html, /<option value="minimal_cost"/, "the internal minimal-cost mode must not be exposed in the UI");
assert.match(html, /value="chat_streaming"/, "Chat streaming must remain selectable");
assert.match(html, /value="chat_json"/, "Chat JSON must remain selectable");
assert.match(html, /value="responses_json"/, "Responses JSON must remain selectable");
assert.match(html, /value="anthropic_streaming"/, "Anthropic streaming must remain selectable");
assert.match(html, /value="anthropic_json"/, "Anthropic JSON must remain selectable");
assert.match(html, /id="testAllKeysHealthButton"/, "multi-Key testing must open the unified health flow");
assert.doesNotMatch(html, /providerBalanceProbeUrlInput|balance_probe_url/, "balance probing must not require a manual URL");
assert.match(html, /\.balance-probe-chip\.is-unknown/, "unknown balances must have a yellow visual state");
assert.match(html, /\.balance-probe-chip\.is-depleted/, "zero or negative balances must have a red visual state");

assert.match(bridgeSource, /function openHealthProbe\(providerId, target = "current", keyIds = \[\]\)/, "provider and multi-Key paths must share one target-aware modal");
assert.match(bridgeSource, /send\("fetch-health-probe-models", \{ providerId \}\)/, "opening the modal must automatically fetch models");
assert.match(bridgeSource, /send\("run-health-probe", \{[\s\S]{0,260}target:[\s\S]{0,260}mode,[\s\S]{0,260}prompt/, "the selected target, mode, and prompt must be passed to the probe");
assert.match(bridgeSource, /data-health-probe-provider/, "provider rows must expose a health probe action");
assert.match(bridgeSource, /data-test-provider/, "provider rows must retain the separate connection-test action");
assert.match(bridgeSource, /openHealthProbe\(providerId, "all_enabled"\)/, "all-Key testing must not reuse the provider card's current-Key target");
assert.match(bridgeSource, /data-balance-probe/, "provider rows must expose a balance probe action");
assert.match(bridgeSource, /balanceProbeLabel\(provider\.balance\)/, "balance results must remain visible on provider rows");
assert.match(bridgeSource, /health-probe-timing/, "health probe results must expose a dedicated timing row");
assert.match(bridgeSource, /mode\.value = "responses_streaming"/, "health probes must default to Responses streaming");
assert.match(bridgeSource, /const selectedMode = \$bridge\("#healthProbeModeInput"\)\?\.value \|\| "responses_streaming"/, "missing UI mode must fall back to Responses streaming");
assert.match(bridgeSource, /selectedMode === "minimal_cost" \? "responses_streaming"/, "the old internal mode must remain compatible with Responses streaming");
assert.match(bridgeSource, /function balanceProbeTone\(result\)[\s\S]{0,420}is-depleted/, "zero or negative balances must use the red depleted state");
assert.match(bridgeSource, /function balanceProbeTone\(result\)[\s\S]{0,260}is-unknown/, "unknown or unmeasurable balances must use the yellow unknown state");
assert.match(bridgeSource, /function balanceProbeDisplayValue\(value\)[\s\S]{0,260}toFixed\(2\)/, "numeric balances must be compacted to two decimal places in the UI");

assert.match(controlPlane, /command<ModelConfig\[\]>\("fetch_provider_health_models"/, "modal model discovery must use the pool-safe backend command");
assert.match(controlPlane, /const supportedModes[\s\S]{0,360}if \(!supportedModes\.includes\(requestedMode\)\)/, "valid health-probe modes must not be rejected by an inverted guard");
assert.match(controlPlane, /command<ProviderHealthProbeResult>\("probe_provider_health"/, "actual health checks must use the dedicated backend command");
assert.match(controlPlane, /requestedMode === "minimal_cost" \? "responses_streaming"/, "the old internal mode must remain compatible with Responses streaming");
assert.match(controlPlane, /command<ProviderConnectionPathTestResult>\("test_provider_connection_paths"/, "the saved-provider connection action must report the faster direct or system-proxy path");
assert.match(controlPlane, /command<ProviderBalanceProbeResult>\("probe_provider_balance"/, "balance clicks must invoke a separate explicit management action");
assert.match(controlPlane, /const PROVIDER_BALANCE_REFRESH_MS = 15 \* 60 \* 1000;/, "provider balances must refresh every fifteen minutes");
assert.match(controlPlane, /providersForOpenAgents\(config\)/, "startup probes must be limited to upstreams belonging to enabled agents");
assert.match(controlPlane, /probeConnectionsOnOpen\(\)/, "startup connectivity probes must run alongside the initial balance probe");
assert.doesNotMatch(controlPlane, /save_provider_balance_probe_config|balance_probe_url/, "the editor must not save a manual balance endpoint");
assert.match(api, /export type ProviderHealthProbeMode/, "API types must describe all probe shapes");
assert.match(api, /export type ProviderHealthProbeTarget/, "API types must describe all probe targets");
assert.match(api, /export interface ProviderBalanceProbeResult/, "API types must describe safe balance outcomes");

console.log("graphite provider health-probe UI regression tests passed");
