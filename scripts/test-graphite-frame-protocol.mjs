import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import { fileURLToPath } from "node:url";
import { dirname, resolve } from "node:path";

const root = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const host = await readFile(resolve(root, "src/GraphitePrototypeHost.tsx"), "utf8");
const protocol = await readFile(resolve(root, "src/graphite/frameProtocol.ts"), "utf8");
const documentFactory = await readFile(resolve(root, "src/graphite/frameDocument.ts"), "utf8");

assert.match(
  protocol,
  /export const GRAPHITE_BRIDGE_CHANNEL = "atoapi\.graphite\.bridge\.v1"/,
  "the bridge channel must remain explicit and versioned"
);
assert.match(host, /const CHANNEL = "atoapi\.graphite\.bridge\.v1"/, "the iframe bridge must keep its wire channel");
assert.match(host, /event\.source !== frameRef\.current\?\.contentWindow/, "the host must still reject foreign window messages");
assert.match(host, /onBridgeActionRef\.current\(action, payload\)/, "the stable listener must call the latest control-plane action");
assert.match(host, /\}, \[send\]\);/, "the message listener must not rebind on every metrics refresh");
assert.match(host, /createGraphitePrototypeDocument\(bridgeSource\)/, "the host must create one static iframe document from the bridge source");
assert.match(documentFactory, /<link rel="preload" as="script" href="\$\{lucideUmdUrl\}">/, "the iframe document must preload the local icon bundle");
assert.doesNotMatch(host, /useLayoutEffect/, "the static iframe document must not reset during normal refreshes");

console.log("graphite frame protocol regression checks passed");
