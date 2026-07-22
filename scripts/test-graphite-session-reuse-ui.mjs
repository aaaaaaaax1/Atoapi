import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";

const html = await readFile(new URL("../prototype/atoapi-graphite-ui.html", import.meta.url), "utf8");
const host = await readFile(new URL("../src/GraphitePrototypeHost.tsx", import.meta.url), "utf8");

assert.match(
  html,
  /id="providerSessionReuseModelInput"[^>]*list="providerModelCandidates"/,
  "the compatibility panel must expose an editable model input backed by fetched model candidates"
);
assert.match(
  html,
  /id="fetchCompatibilityModelsButton"/,
  "the compatibility panel must expose its own fetch-models action"
);
assert.match(
  host,
  /function compatibilityModelId\(\)/,
  "session-reuse actions must read a dedicated compatibility model value"
);
assert.ok(
  host.includes('send("probe-session-reuse", { providerId: selectedProviderId(), modelId: compatibilityModelId() })'),
  "qualification must use the compatibility model input instead of the first mapping"
);
assert.ok(
  host.includes('send("set-session-reuse", { providerId, modelId: compatibilityModelId(), enabled:'),
  "the enable switch must target the same explicitly selected model"
);

console.log("graphite session reuse model selector regression tests passed");
