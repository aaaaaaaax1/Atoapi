import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";

const html = await readFile(new URL("../prototype/atoapi-graphite-ui.html", import.meta.url), "utf8");
const host = await readFile(new URL("../src/GraphitePrototypeHost.tsx", import.meta.url), "utf8");
const controlPlane = await readFile(new URL("../src/useGraphiteControlPlane.ts", import.meta.url), "utf8");

const bridgeStart = host.indexOf("const bridgeSource = String.raw`");
const bridgeEnd = host.indexOf("`;\n\nfunction createDocument", bridgeStart);
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
  /command<ProviderKeyTestResult>\("test_active_provider_key",\s*\{\s*providerId:\s*provider\.id,\s*provider_id:\s*provider\.id\s*\}\)/,
  "the provider-list health button must use the Key the next ordinary inbound would select"
);
assert.doesNotMatch(
  providerListTest,
  /test_provider_connection_paths/,
  "the provider-list health button must not dispatch through the editor connection-key path"
);
assert.match(
  providerListTest,
  /payload:\s*\{ keyPoolHealth \}/,
  "the provider-list health button must refresh the persisted Key-health state"
);
assert.match(
  controlPlane,
  /if \(providerId && !\("provider" in payload\)\) \{\s*return testSavedProviderKeyHealth\(providerId\);/,
  "a provider-list test must not accidentally take the editor connection-test path"
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
