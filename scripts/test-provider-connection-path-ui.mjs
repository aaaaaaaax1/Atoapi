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
assert.doesNotMatch(
  controlPlane,
  /if \(action === "test-provider"\)[\s\S]{0,2400}command<ProviderKeyTestResult>\("test_provider_key"/,
  "the editor connection test must not fall back to a single selected path"
);
assert.match(
  controlPlane,
  /payload: \{ connectionTest: result \}/,
  "the dual-path result must update the active editor draft"
);
assert.match(
  host,
  /function applyConnectionPathTest\(result\)/,
  "the Graphite bridge must consume the path recommendation"
);
assert.match(
  host,
  /setSwitch\("使用系统代理", useSystemProxy\)/,
  "the faster path must be reflected in the editor switch before save"
);
assert.match(
  host,
  /message\.payload\?\.connectionTest\) applyConnectionPathTest/,
  "connection-test results must be applied through the normal bridge acknowledgement"
);

console.log("provider connection-path UI regression tests passed");
