import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import { fileURLToPath } from "node:url";

const root = new URL("../", import.meta.url);
const html = await readFile(fileURLToPath(new URL("prototype/atoapi-graphite-ui.html", root)), "utf8");
const host = await readFile(fileURLToPath(new URL("src/GraphitePrototypeHost.tsx", root)), "utf8");

assert.match(html, /#requestRouteGroupFilter\s*,[\s\S]*?color-scheme:\s*dark/);
assert.match(html, /#requestRouteGroupFilter option[\s\S]*?background-color:\s*#181c25\s*!important;[\s\S]*?color:\s*#edf0f8\s*!important;/);
assert.match(html, /id="requestRouteGroupMenu"[^>]*role="listbox"/);
assert.match(html, /\.request-route-group-menu\s*\{[\s\S]*?background:\s*#171b20/);
assert.match(html, /\.request-route-group-option:hover,[\s\S]*?aria-selected="true"/);
assert.match(host, /关闭后该上游完全不参与此分组选路/);
assert.match(host, /关闭备选后，该上游不会用于当前分组/);
assert.match(host, /data-request-route-group-option/);

console.log("Codex route group UI checks passed.");
