import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";

const html = await readFile(new URL("../prototype/atoapi-graphite-ui.html", import.meta.url), "utf8");
const host = await readFile(new URL("../src/GraphitePrototypeHost.tsx", import.meta.url), "utf8");
const controlPlane = await readFile(new URL("../src/useGraphiteControlPlane.ts", import.meta.url), "utf8");

const bridgeStart = host.indexOf("const bridgeSource = String.raw`");
const bridgeEnd = host.indexOf("`;\n\n// The embedded prototype", bridgeStart);
assert.ok(bridgeStart >= 0 && bridgeEnd >= 0, "the Graphite bridge source must remain extractable");
const bridgeDefinition = host.slice(bridgeStart, bridgeEnd + 1);
const bridgeSource = Function(`${bridgeDefinition}; return bridgeSource;`)();
new Function(bridgeSource);

assert.match(
  html,
  /data-key-enabled="\$\{key\.id\}"[^>]*aria-label="启用 \$\{escapeHtml\(key\.name\)\}"/,
  "each key row must expose an accessible enable switch"
);
assert.match(
  html,
  /key\.enabled = key\.enabled === false/,
  "the standalone Graphite prototype must keep the per-key switch interactive"
);
assert.match(
  bridgeSource,
  /enabled:\s*item\.enabled\s*!==\s*false/,
  "saved key enabled state must hydrate into the Graphite draft"
);
assert.match(
  bridgeSource,
  /function toggleKeyEnabled\(keyId\)/,
  "the iframe bridge must own the real per-key enable interaction"
);
assert.match(
  bridgeSource,
  /function keyPoolCountBadge\(provider\)[\s\S]{0,360}provider\?\.keyPoolEnabled !== true/,
  "the provider-list Key-count badge must render only for an enabled multi-Key pool"
);
assert.match(
  bridgeSource,
  /keyPoolCountBadge\(provider\) \+ '<button class="balance-probe-chip /,
  "an enabled multi-Key count must appear immediately to the left of the balance badge"
);
assert.match(
  host,
  /keyPoolEnabled:\s*provider\.key_pool\?\.enabled === true/,
  "the host must pass the multi-Key enabled boundary into the provider-list state"
);
assert.match(
  host,
  /keyPoolCount:\s*provider\.key_pool\?\.keys\.length \?\? 0/,
  "the provider-list Key count must use the pool's configured Key count"
);
assert.match(
  html,
  /\.key-pool-count-chip\s*\{[\s\S]{0,420}white-space:\s*nowrap/,
  "the Key-count chip must remain compact beside the balance chip"
);
assert.match(
  bridgeSource,
  /key\.enabledDirty\s*=\s*true/,
  "a local key enable edit must be marked dirty before a health refresh"
);
assert.match(
  bridgeSource,
  /function mergeKeyPoolHealth\(keyPoolHealth\)/,
  "manual test responses must merge health into the open editor"
);
assert.match(
  bridgeSource,
  /function syncOpenProviderKeyPoolHealth\(\)[\s\S]{0,420}sync-provider-key-pool-health/,
  "an open saved-provider editor must request live multi-Key health snapshots"
);
assert.match(
  bridgeSource,
  /window\.setInterval\(syncOpenProviderKeyPoolHealth, 500\)/,
  "the open multi-Key editor must reconcile a runtime quota failure within half a second"
);
assert.match(
  bridgeSource,
  /openProviderEditor = function\(providerId = null\)[\s\S]{0,180}syncOpenProviderKeyPoolHealth\(\)/,
  "opening the provider editor must perform an immediate Key-health reconciliation"
);
assert.match(
  controlPlane,
  /action === "sync-provider-key-pool-health"[\s\S]{0,420}command<ProviderKeyPoolHealthSnapshot>\("get_provider_key_pool_health"/,
  "the health reconciliation must use the lightweight live Key snapshot command"
);
assert.match(
  bridgeSource,
  /!key\.enabledDirty[\s\S]{0,160}persisted\.enabled/,
  "a health refresh must not overwrite an unsaved enable edit"
);
assert.match(
  bridgeSource,
  /const orderedKeys = keyPool\.filter[\s\S]{0,560}enabled:\s*key\.enabled\s*!==\s*false/,
  "serialization must retain the draft order and per-key enabled value"
);
assert.match(
  bridgeSource,
  /send\("test-provider-key",\s*\{\s*providerId:\s*editingProviderId\s*\|\|\s*"",\s*keyId:\s*target\.dataset\.keyTest,\s*provider:\s*serializeEditor\(\)/,
  "a Key test must not fall back to the currently bound provider while a new provider is being edited"
);
assert.match(
  controlPlane,
  /enabled:\s*key\.enabled\s*\?\?\s*prior\?\.enabled\s*\?\?\s*true/,
  "saving a provider must persist a Graphite per-key enable choice"
);
assert.match(
  controlPlane,
  /function graphiteKeyPoolHealth\(config: AppConfig, providerId: string\)/,
  "key test commands must return the persisted health snapshot"
);
assert.match(
  controlPlane,
  /payload:\s*\{ keyPoolHealth \}/,
  "single and pool key tests must acknowledge fresh health to the iframe"
);
assert.match(
  controlPlane,
  /const draft = providerPayload\(\);[\s\S]{0,680}draftProviderKeyTestInput\(draft, keyId, draftSecret\)/,
  "a Key test must use the current editor draft, including an unsaved Key"
);
assert.match(
  controlPlane,
  /function draftProviderKeyTestInput\(/,
  "the control plane must retain a saved key ID while testing an edited draft endpoint"
);

const providerListTest = controlPlane.match(
  /async function testSavedProviderKeyHealth\(providerId: string\): Promise<GraphiteBridgeResponse> \{([\s\S]*?)\n  \}\n\n  async function testDraftProviderConnection/
)?.[1];
assert.ok(providerListTest, "the saved-provider Key-health test must remain bounded");
assert.match(
  providerListTest,
  /command<ProviderConnectionPathTestResult>\("test_provider_connection_paths",\s*\{\s*input:\s*providerConnectionTestInput\(provider\)\s*\}\)/,
  "the provider-list connection button must compare the saved provider's direct and system-proxy paths"
);
assert.doesNotMatch(
  providerListTest,
  /test_active_provider_key/,
  "the provider-list connection button must not report the old current-Key-only result"
);
assert.match(
  providerListTest,
  /payload:\s*\{ connectionTest: result \}/,
  "the provider-list connection button must expose the measured path result"
);
assert.match(
  controlPlane,
  /if \(providerId && !\("provider" in payload\)\) \{\s*return testSavedProviderKeyHealth\(providerId\);/,
  "a provider-list connection test must remain separate from the editor draft path"
);

assert.match(
  html,
  /<select><option>轮询<\/option><option>优先级<\/option><option>最低使用<\/option><option selected>顺序<\/option><\/select>/,
  "new Graphite key pools must default to top-to-bottom sequential selection"
);
assert.match(
  bridgeSource,
  /const DEFAULT_KEY_PRIORITY = 5;/,
  "new key rows must share one stable default priority instead of receiving a positional rank"
);
assert.doesNotMatch(
  bridgeSource,
  /key\.priority = keyPool\.length - index/,
  "reordering or adding a key must not silently rewrite every key priority"
);
assert.match(
  bridgeSource,
  /strategyFromUi[\s\S]{0,360}\|\| "sequential"/,
  "an untouched editor must serialize its default strategy as sequential"
);
assert.match(
  controlPlane,
  /editablePayload\.key_pool\?\.strategy \?\? existing\?\.key_pool\?\.strategy \?\? "sequential"/,
  "new saved providers must retain the UI's sequential default"
);
assert.match(
  bridgeSource,
  /document\.addEventListener\("pointerdown",[\s\S]{0,640}\[data-key-drag\]/,
  "the Key handle must begin a pointer-driven drag path in the embedded WebView"
);
assert.match(
  bridgeSource,
  /document\.addEventListener\("pointermove",[\s\S]{0,640}elementFromPoint/,
  "pointer drag must identify the row beneath the pointer rather than rely on native HTML drop delivery"
);
assert.match(
  bridgeSource,
  /document\.addEventListener\("pointerup",[\s\S]{0,640}reorderKeyFromBridge/,
  "releasing a Key drag over another row must apply the draft reorder immediately"
);

console.log("graphite key-pool UI regression tests passed");
