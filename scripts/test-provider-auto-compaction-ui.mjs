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

assert.match(
  html,
  /id="providerAutoCompactTokenLimit"[^>]*type="number"/,
  "the per-upstream editor must expose a numeric automatic-compaction threshold"
);
assert.match(
  html,
  /不会为同一条消息额外发送上游请求/,
  "the UI must state the one-inbound/one-upstream invariant"
);
assert.match(
  bridgeSource,
  /autoCompactLimit\.value\s*=\s*detail\?\.auto_compact_token_limit/,
  "editing an upstream must hydrate its own saved threshold"
);
assert.match(
  bridgeSource,
  /auto_compact_token_limit:\s*autoCompactTokenLimit/,
  "saving an upstream must serialize the threshold"
);
assert.match(
  controlPlane,
  /auto_compact_token_limit:\s*editablePayload\.auto_compact_token_limit\s*\?\?\s*null/,
  "the control plane must forward an explicit clear as null"
);
assert.match(
  controlPlane,
  /auto_compact_token_limit_configured:\s*true/,
  "the control plane must distinguish an explicit clear from an older caller that omitted the setting"
);
assert.match(
  api,
  /auto_compact_token_limit\?:\s*number\s*\|\s*null/,
  "the frontend API contract must model a nullable per-provider threshold"
);

console.log("provider auto-compaction editor regression tests passed");
