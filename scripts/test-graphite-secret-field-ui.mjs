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
  bridgeSource,
  /const SAVED_SECRET_MASK\s*=/,
  "saved secrets must have a non-secret visual mask instead of an empty editor field"
);
assert.match(
  bridgeSource,
  /function setSecretInputState\([^)]*hasSavedSecret[^)]*\)/,
  "all secret editors must use one state initializer"
);
assert.match(
  bridgeSource,
  /setSecretInputState\(key,\s*Boolean\(detail\?\.has_api_key\)/,
  "an existing provider API key must open masked rather than blank"
);
assert.match(
  bridgeSource,
  /setSecretInputState\(localKey,\s*Boolean\(settings\.hasLocalKey\)/,
  "the saved local key must use the same initial masked state"
);
assert.match(
  bridgeSource,
  /hasSavedSecret:\s*item\.has_saved_secret\s*===\s*true/,
  "existing multi-key entries must retain a non-secret saved-key marker"
);
assert.match(
  bridgeSource,
  /data-saved-secret=/,
  "secret inputs must expose their saved-mask state without storing the secret value"
);
assert.match(
  bridgeSource,
  /message\.payload\?\.secret[\s\S]{0,700}remove\("has-saved-secret"\)/,
  "revealing a key must replace only the visual mask after the explicit reveal response"
);

assert.match(
  controlPlane,
  /const savedProvider = input\.provider_id[\s\S]{0,420}!input\.api_key\?\.trim\(\)\s*&&\s*!savedProvider/,
  "testing an existing provider must be allowed to use its saved backend key while a new draft still requires a typed key"
);
assert.match(
  controlPlane,
  /draftProviderTestInput\(draft, null\)/,
  "the provider test must preserve the editable URL while relying on the saved key by provider id"
);

for (const id of ["providerApiKeyInput", "settingsLocalKeyInput"]) {
  assert.match(
    html,
    new RegExp(`id=["']${id}["'][^>]*type=["']password["']`),
    `${id} must remain a password input`
  );
}

console.log("graphite secret-field regression tests passed");
