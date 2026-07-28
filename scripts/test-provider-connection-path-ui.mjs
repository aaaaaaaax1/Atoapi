import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";

const controlPlane = await readFile(
  new URL("../src/useGraphiteControlPlane.ts", import.meta.url),
  "utf8"
);
const host = await readFile(
  new URL("../src/GraphitePrototypeHost.tsx", import.meta.url),
  "utf8"
);
const api = await readFile(new URL("../src/lib/api.ts", import.meta.url), "utf8");
const prototype = await readFile(
  new URL("../prototype/atoapi-graphite-ui.html", import.meta.url),
  "utf8"
);

assert.match(
  api,
  /export interface ProviderConnectionPathTestResult/,
  "the control plane needs a typed dual-path connection-test result"
);
assert.match(
  controlPlane,
  /command<ProviderConnectionPathTestResult>\("test_provider_connection_paths", \{ input \}\)/,
  "the editor connection test must call the dual-path command"
);
const providerConnectionAction = controlPlane.match(
  /async function testDraftProviderConnection\([\s\S]*?\): Promise<GraphiteBridgeResponse> \{([\s\S]*?)\n  \}\n\n  async function onBridgeAction/
)?.[1];
assert.ok(providerConnectionAction, "the editor must retain a bounded dual-path connection-test helper");
assert.doesNotMatch(
  providerConnectionAction,
  /command<ProviderKeyTestResult>\("test_provider_key"/,
  "the editor connection test must not fall back to a single selected path"
);
assert.doesNotMatch(
  providerConnectionAction,
  /test_active_provider_key/,
  "the editor connection test must compare both paths even for a saved provider"
);
assert.match(
  controlPlane,
  /draftProviderTestInput\(draft, null, savedProvider\?\.is_full_url \?\? false\)/,
  "saved editors must preserve their endpoint mode while the backend resolves the active pool Key"
);
assert.match(
  controlPlane,
  /payload: \{ connectionTest: result \}/,
  "the dual-path result must update the active editor draft"
);
assert.match(
  controlPlane,
  /if \(providerId && !\("provider" in payload\)\) \{\s*return testSavedProviderKeyHealth\(providerId\);/,
  "the editor and provider-list tests must keep their distinct Key semantics"
);
assert.match(
  host,
  /function applyConnectionPathTest\(result\)/,
  "the Graphite bridge must display the path recommendation"
);
assert.doesNotMatch(
  host,
  /function applyConnectionPathTest\(result\)[\s\S]{0,2000}setSwitch\("使用系统代理"/,
  "a test recommendation must never mutate the user's proxy switch"
);
assert.match(
  host,
  /function syncSystemProxySelection\(checked\)/,
  "loading an editor must synchronize the proxy description with the actual switch value"
);
assert.match(
  host,
  /if \(label === "使用系统代理"\) syncSystemProxySelection\(Boolean\(checked\)\)/,
  "programmatic proxy-state hydration must update the visible current selection"
);
assert.match(
  prototype,
  /id="providerConnectionTestResult"/,
  "the editor must visibly show a recommendation without replacing the current setting"
);
assert.match(
  host,
  /message\.payload\?\.connectionTest\) applyConnectionPathTest/,
  "connection-test results must be applied through the normal bridge acknowledgement"
);

console.log("provider connection-path UI regression tests passed");
